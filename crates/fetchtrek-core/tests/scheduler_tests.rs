use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use fetchtrek_core::{AtomicSegmentCursor, SegmentScheduler, CLUSTER_ALIGNMENT};
use fetchtrek_types::ByteRange;

#[test]
fn test_cluster_alignment_on_bisection() {
    let total_size = 10 * 1024 * 1024; // 10 MiB
    let range = ByteRange::new(0, total_size - 1);
    let cursor = AtomicSegmentCursor::new(range);
    let min_split_size = 64 * 1024; // 64 KiB

    // Advance cursor by arbitrary unaligned offset
    cursor.advance_cursor(12_345);

    let stolen = cursor
        .try_bisect(min_split_size)
        .expect("bisection must succeed on 10 MiB range with small progress");

    // Invariant 1: 64 KiB cluster alignment
    assert_eq!(
        stolen.start % CLUSTER_ALIGNMENT,
        0,
        "Stolen range start must align to 64 KiB cluster boundary: {}",
        stolen.start
    );

    let (current_pos, new_limit) = cursor.current_progress();
    assert_eq!(current_pos, 12_345);
    assert_eq!(
        new_limit + 1,
        stolen.start,
        "Original segment limit must cleanly abut stolen segment start with zero gap"
    );
    assert_eq!(
        stolen.end,
        total_size - 1,
        "Stolen segment end must match original upper bound"
    );

    // Bisect stolen segment further
    let stolen_cursor = AtomicSegmentCursor::new(stolen);
    let second_stolen = stolen_cursor
        .try_bisect(min_split_size)
        .expect("second bisection should succeed");

    assert_eq!(
        second_stolen.start % CLUSTER_ALIGNMENT,
        0,
        "Second bisected range start must also align to 64 KiB boundary: {}",
        second_stolen.start
    );
    assert_eq!(
        stolen_cursor.limit_byte.load(Ordering::Acquire) + 1,
        second_stolen.start,
        "Abutting boundary must have zero gap"
    );
}

#[test]
fn test_bisection_rejection_under_minimum_split() {
    let min_split = 64 * 1024; // 64 KiB

    // Case 1: Remaining work strictly less than 2 * min_split_size (131071 bytes)
    let small_range = ByteRange::new(0, (2 * min_split) - 2);
    let cursor = AtomicSegmentCursor::new(small_range);
    assert!(
        cursor.try_bisect(min_split).is_none(),
        "Must reject bisection when remaining bytes < 2 * min_split_size"
    );

    // Case 2: Cursor advanced such that remaining work drops below 2 * min_split_size
    let larger_range = ByteRange::new(0, (4 * min_split) - 1); // 256 KiB
    let advancing_cursor = AtomicSegmentCursor::new(larger_range);
    advancing_cursor.advance_cursor(3 * min_split); // remaining = 64 KiB < 128 KiB
    assert!(
        advancing_cursor.try_bisect(min_split).is_none(),
        "Must reject bisection after cursor advance leaves < 2 * min_split_size"
    );

    // Case 3: Completed cursor
    let completed_range = ByteRange::new(0, 100);
    let completed_cursor = AtomicSegmentCursor::new(completed_range);
    completed_cursor.advance_cursor(101);
    assert!(completed_cursor.is_completed());
    assert!(completed_cursor.try_bisect(min_split).is_none());

    // Case 4: Exactly 2 * min_split_size aligned
    let exact_range = ByteRange::new(0, (2 * min_split) - 1); // 131,072 bytes
    let exact_cursor = AtomicSegmentCursor::new(exact_range);
    let stolen = exact_cursor.try_bisect(min_split);
    assert!(
        stolen.is_some(),
        "Must allow bisection at exactly 2 * min_split_size"
    );
    let stolen_range = stolen.unwrap();
    assert_eq!(stolen_range.start, min_split);
    assert_eq!(stolen_range.len(), min_split);
    assert_eq!(exact_cursor.remaining_bytes(), min_split);

    // Cannot bisect again since both segments now have min_split_size (< 2 * min_split)
    assert!(exact_cursor.try_bisect(min_split).is_none());
}

#[test]
fn test_scheduler_initial_split_invariants() {
    let file_size = 10_000_000u64;
    let total_range = ByteRange::new(0, file_size - 1);
    let scheduler = SegmentScheduler::new(total_range);

    let segments = scheduler.split_initial(4);
    assert_eq!(segments.len(), 4);

    // Check boundary continuity and no gaps
    assert_eq!(segments[0].start, 0);
    for i in 1..segments.len() {
        assert_eq!(
            segments[i - 1].end + 1,
            segments[i].start,
            "Gaps or overlaps detected between segment {} and {}",
            i - 1,
            i
        );
        assert_eq!(
            segments[i].start % CLUSTER_ALIGNMENT,
            0,
            "Segment start {} must align to 64 KiB cluster",
            segments[i].start
        );
    }
    assert_eq!(segments.last().unwrap().end, file_size - 1);

    // Total byte preservation
    let total_segmented_bytes: u64 = segments.iter().map(|s| s.len()).sum();
    assert_eq!(total_segmented_bytes, file_size);
    assert_eq!(scheduler.total_remaining_bytes(), file_size);
}

#[test]
fn test_concurrent_work_stealing_stress() {
    let file_size = 32 * 1024 * 1024u64; // 32 MiB
    let total_range = ByteRange::new(0, file_size - 1);
    let scheduler = Arc::new(SegmentScheduler::new(total_range));

    let initial_workers = 4;
    scheduler.split_initial(initial_workers);

    let num_threads = 16;
    let min_split_size = 64 * 1024; // 64 KiB
    let chunk_step = 8192u64; // 8 KiB download step

    let mut handles = Vec::with_capacity(num_threads);

    for thread_idx in 0..num_threads {
        let sched = Arc::clone(&scheduler);
        let handle = thread::spawn(move || {
            // Initial workers take an initial cursor; other threads start stealing
            let mut current_cursor = if thread_idx < initial_workers {
                let cursors = sched.cursors();
                cursors.get(thread_idx).cloned()
            } else {
                sched.steal_work_cursor(min_split_size).map(|(_, c)| c)
            };

            let mut idle_spins = 0;
            loop {
                if let Some(ref cursor) = current_cursor {
                    let limit = cursor.limit_byte.load(Ordering::Acquire);
                    let cur = cursor.current_cursor.load(Ordering::Acquire);

                    if cur > limit {
                        // Segment finished, attempt to steal work
                        current_cursor = sched.steal_work_cursor(min_split_size).map(|(_, c)| c);
                        idle_spins = 0;
                    } else {
                        let remaining = limit - cur + 1;
                        let step = remaining.min(chunk_step);
                        cursor.advance_cursor(step);
                    }
                } else {
                    // No cursor held, attempt to steal
                    current_cursor = sched.steal_work_cursor(min_split_size).map(|(_, c)| c);

                    if current_cursor.is_none() {
                        if sched.is_all_completed() {
                            break;
                        }
                        idle_spins += 1;
                        if idle_spins > 100 {
                            thread::yield_now();
                        }
                    } else {
                        idle_spins = 0;
                    }
                }

                if current_cursor.is_none() && sched.is_all_completed() {
                    break;
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Worker thread panicked");
    }

    // Invariant Verification
    assert!(
        scheduler.is_all_completed(),
        "All segments must be reported completed"
    );

    let cursors = scheduler.cursors();
    assert!(
        cursors.len() >= initial_workers,
        "Must have created stolen segments"
    );

    let mut ranges: Vec<ByteRange> = cursors
        .iter()
        .map(|c| ByteRange::new(c.range_start, c.limit_byte.load(Ordering::Acquire)))
        .collect();

    // Sort by range start to check continuity
    ranges.sort_by_key(|r| r.start);

    // Invariant 1: Zero gaps and exact coverage
    assert_eq!(ranges[0].start, 0, "First segment must start at byte 0");
    for i in 1..ranges.len() {
        assert_eq!(
            ranges[i - 1].end + 1,
            ranges[i].start,
            "Gap detected between segment {} [{}] and {} [{}]",
            i - 1,
            ranges[i - 1],
            i,
            ranges[i]
        );
    }
    assert_eq!(
        ranges.last().unwrap().end,
        file_size - 1,
        "Last segment must end at file_size - 1"
    );

    // Invariant 2: Zero overlapping ranges
    for i in 0..ranges.len() {
        for j in (i + 1)..ranges.len() {
            assert!(
                !ranges[i].overlaps(&ranges[j]),
                "Ranges must not overlap: {} and {}",
                ranges[i],
                ranges[j]
            );
        }
    }

    // Invariant 3: Exact total byte preservation
    let mut total_processed = 0u64;
    for c in &cursors {
        let cur = c.current_cursor.load(Ordering::Acquire);
        let limit = c.limit_byte.load(Ordering::Acquire);
        let processed = (cur - c.range_start).min(limit - c.range_start + 1);
        total_processed += processed;
        assert_eq!(c.remaining_bytes(), 0);
    }
    assert_eq!(
        total_processed, file_size,
        "Sum of processed bytes must exactly equal file size"
    );

    // Invariant 4: Cluster alignment on bisected segments
    for r in &ranges {
        if r.start > 0 {
            assert_eq!(
                r.start % CLUSTER_ALIGNMENT,
                0,
                "Segment start must align to 64 KiB: {}",
                r.start
            );
        }
    }
}
