use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_persistence::{
    Database, IntervalSet, NewTask, PersistenceError, SegmentCheckpoint, TaskSegmentRecord,
};
use fetchtrek_types::TaskState;

struct TempDbGuard {
    path: PathBuf,
}

impl TempDbGuard {
    fn new(name: &str) -> Self {
        let mut path = std::env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique = format!(
            "fetchtrek_repo_test_{}_{}_{}.sqlite3",
            std::process::id(),
            timestamp,
            name
        );
        path.push(unique);
        Self { path }
    }
}

impl Drop for TempDbGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
    }
}

#[test]
fn test_task_crud_lifecycle_and_cascades() {
    let db = Database::open_in_memory().expect("in-memory database creation failed");

    let new_task = NewTask::new(
        "uuid-crud-001",
        "https://cdn.example.com/archive.tar.zst",
        "/downloads/archive.tar.zst",
        "archive.tar.zst",
        "https",
        104_857_600, // 100 MiB
    );

    let task_id = db.create_task(&new_task).expect("create_task failed");
    assert!(task_id > 0);

    // Retrieve by ID
    let task_by_id = db
        .get_task_by_id(task_id)
        .expect("get_task_by_id failed")
        .expect("task not found");

    assert_eq!(task_by_id.id, task_id);
    assert_eq!(task_by_id.uuid, "uuid-crud-001");
    assert_eq!(task_by_id.url, "https://cdn.example.com/archive.tar.zst");
    assert_eq!(task_by_id.destination_path, "/downloads/archive.tar.zst");
    assert_eq!(task_by_id.filename, "archive.tar.zst");
    assert_eq!(task_by_id.protocol, "https");
    assert_eq!(task_by_id.total_bytes, 104_857_600);
    assert_eq!(task_by_id.downloaded_bytes, 0);
    assert_eq!(task_by_id.status, TaskState::Queued);
    assert_eq!(task_by_id.priority, 1);
    assert!(task_by_id.completed_at.is_none());

    // Retrieve by UUID
    let task_by_uuid = db
        .get_task_by_uuid("uuid-crud-001")
        .expect("get_task_by_uuid failed")
        .expect("task not found");
    assert_eq!(task_by_id, task_by_uuid);

    // Non-existent lookups
    assert!(db.get_task_by_id(9999).unwrap().is_none());
    assert!(db.get_task_by_uuid("uuid-missing").unwrap().is_none());

    // Filtered list
    let queued_tasks = db.list_tasks(Some(TaskState::Queued)).unwrap();
    assert_eq!(queued_tasks.len(), 1);
    assert_eq!(queued_tasks[0].id, task_id);

    let active_tasks = db.list_tasks(Some(TaskState::Transferring)).unwrap();
    assert!(active_tasks.is_empty());

    // Update status to Transferring
    db.update_task_status(task_id, TaskState::Transferring)
        .unwrap();
    let updated = db.get_task_by_id(task_id).unwrap().unwrap();
    assert_eq!(updated.status, TaskState::Transferring);
    assert!(updated.completed_at.is_none());

    // Save & get headers
    let headers = [
        ("Authorization", "Bearer secret-token-xyz"),
        ("User-Agent", "FetchTrek/1.0"),
    ];
    db.save_headers(task_id, &headers).unwrap();
    let saved_headers = db.get_headers(task_id).unwrap();
    assert_eq!(saved_headers.len(), 2);
    assert_eq!(
        saved_headers[0],
        (
            "Authorization".to_string(),
            "Bearer secret-token-xyz".to_string()
        )
    );
    assert_eq!(
        saved_headers[1],
        ("User-Agent".to_string(), "FetchTrek/1.0".to_string())
    );

    // Save & get segments
    let seg1 = TaskSegmentRecord::new(task_id, 0, 0, 52_428_799, 0, 0);
    let seg2 = TaskSegmentRecord::new(task_id, 1, 52_428_800, 104_857_599, 52_428_800, 0);
    db.save_segments(task_id, &[seg1.clone(), seg2.clone()])
        .unwrap();

    let saved_segments = db.get_segments(task_id).unwrap();
    assert_eq!(saved_segments.len(), 2);
    assert_eq!(saved_segments[0], seg1);
    assert_eq!(saved_segments[1], seg2);

    // Complete task status
    db.update_task_status(task_id, TaskState::Completed)
        .unwrap();
    let completed = db.get_task_by_id(task_id).unwrap().unwrap();
    assert_eq!(completed.status, TaskState::Completed);
    assert!(completed.completed_at.is_some());

    // Delete task and assert foreign key cascade removes all associated segments and headers
    db.delete_task(task_id).unwrap();
    assert!(db.get_task_by_id(task_id).unwrap().is_none());
    assert!(db.get_headers(task_id).unwrap().is_empty());
    assert!(db.get_segments(task_id).unwrap().is_empty());
}

#[test]
fn test_acid_micro_transaction_atomicity() {
    let db = Database::open_in_memory().expect("in-memory database creation failed");

    let total_bytes = 16 * 1024 * 1024u64; // 16 MiB
    let seg_size = 4 * 1024 * 1024u64; // 4 MiB each
    let new_task = NewTask::new(
        "uuid-acid-001",
        "https://cdn.example.com/testfile.bin",
        "/tmp/testfile.bin",
        "testfile.bin",
        "https",
        total_bytes,
    );

    let task_id = db.create_task(&new_task).unwrap();

    // Insert 4 initial segments
    let mut initial_segments = Vec::new();
    for i in 0..4u32 {
        let start = (i as u64) * seg_size;
        let end = start + seg_size - 1;
        initial_segments.push(TaskSegmentRecord::new(task_id, i, start, end, start, 1));
    }
    db.save_segments(task_id, &initial_segments).unwrap();

    // Create 4 checkpoints with non-empty interval sets
    let mut checkpoints = Vec::new();
    let mut total_downloaded = 0u64;

    for i in 0..4u32 {
        let start = (i as u64) * seg_size;
        let downloaded_in_seg = 1024 * 1024u64; // 1 MiB in each segment
        let cursor = start + downloaded_in_seg;
        total_downloaded += downloaded_in_seg;

        let mut interval_set = IntervalSet::new();
        interval_set.add_range(start, cursor - 1).unwrap();

        checkpoints.push(SegmentCheckpoint::new(i, cursor, 1, interval_set));
    }

    // Execute atomic micro-transaction checkpoint
    db.checkpoint_progress(task_id, total_downloaded, &checkpoints)
        .unwrap();

    // Verify task downloaded_bytes updated
    let task = db.get_task_by_id(task_id).unwrap().unwrap();
    assert_eq!(task.downloaded_bytes, total_downloaded);

    // Verify segment cursors and interval BLOBs persisted accurately
    let segments = db.get_segments(task_id).unwrap();
    assert_eq!(segments.len(), 4);
    for i in 0..4usize {
        assert_eq!(segments[i].current_cursor, checkpoints[i].current_cursor);
        assert_eq!(segments[i].status, checkpoints[i].status);
        assert_eq!(
            segments[i].verified_intervals,
            checkpoints[i].verified_intervals
        );
        assert_eq!(
            segments[i].verified_intervals.total_verified_bytes(),
            1024 * 1024
        );
    }

    // Test Rollback: Checkpoint with invalid segment index 999 must fail and roll back everything
    let mut bad_checkpoints = checkpoints.clone();
    bad_checkpoints.push(SegmentCheckpoint::new(999, 0, 0, IntervalSet::new()));

    let res = db.checkpoint_progress(task_id, total_downloaded + 500_000, &bad_checkpoints);
    assert!(matches!(res, Err(PersistenceError::Corruption(_))));

    // Verify atomic rollback: task downloaded_bytes and segments remain at previous state
    let task_after_fail = db.get_task_by_id(task_id).unwrap().unwrap();
    assert_eq!(
        task_after_fail.downloaded_bytes, total_downloaded,
        "downloaded_bytes must not be modified after transaction rollback"
    );

    let segments_after_fail = db.get_segments(task_id).unwrap();
    for i in 0..4usize {
        assert_eq!(
            segments_after_fail[i].current_cursor,
            checkpoints[i].current_cursor
        );
        assert_eq!(
            segments_after_fail[i].verified_intervals,
            checkpoints[i].verified_intervals
        );
    }
}

#[test]
fn test_micro_transaction_latency_gate() {
    let temp = TempDbGuard::new("wal_latency");
    let db = Database::open(&temp.path).expect("disk db open failed");

    let total_bytes = 64 * 1024 * 1024u64; // 64 MiB
    let seg_size = 16 * 1024 * 1024u64; // 4 segments of 16 MiB
    let new_task = NewTask::new(
        "uuid-latency-gate",
        "https://speed.example.com/large.iso",
        "/var/tmp/large.iso",
        "large.iso",
        "https",
        total_bytes,
    );
    let task_id = db.create_task(&new_task).unwrap();

    let mut initial_segments = Vec::new();
    for i in 0..4u32 {
        let start = (i as u64) * seg_size;
        let end = start + seg_size - 1;
        initial_segments.push(TaskSegmentRecord::new(task_id, i, start, end, start, 1));
    }
    db.save_segments(task_id, &initial_segments).unwrap();

    // Perform 50 sequential micro-transaction checkpoints against on-disk WAL SQLite
    let iterations: u64 = 50;
    let mut intervals = [
        IntervalSet::new(),
        IntervalSet::new(),
        IntervalSet::new(),
        IntervalSet::new(),
    ];

    let start_bench = Instant::now();

    for iter in 1..=iterations {
        let mut checkpoints = Vec::with_capacity(4);
        let mut total_downloaded = 0u64;

        for (i, interval_set) in intervals.iter_mut().enumerate() {
            let seg_start = (i as u64) * seg_size;
            let chunk_start = seg_start + (iter - 1) * 65536;
            let chunk_end = chunk_start + 65535;
            interval_set.add_range(chunk_start, chunk_end).unwrap();

            let cursor = chunk_end + 1;
            total_downloaded += cursor - seg_start;

            checkpoints.push(SegmentCheckpoint::new(
                i as u32,
                cursor,
                1,
                interval_set.clone(),
            ));
        }

        db.checkpoint_progress(task_id, total_downloaded, &checkpoints)
            .expect("checkpoint_progress failed in benchmark loop");
    }

    let total_duration = start_bench.elapsed();
    let avg_latency = total_duration / iterations as u32;

    println!(
        "50 sequential micro-transactions took {:?} (average: {:?} / tx)",
        total_duration, avg_latency
    );

    // Acceptance requirement: avg commit latency < 1.0ms (< 0.8ms target)
    assert!(
        avg_latency < Duration::from_millis(1),
        "Average micro-transaction latency must be < 1.0ms, got {:?}",
        avg_latency
    );

    // Verify final state matches exactly after 50 transactions
    let final_task = db.get_task_by_id(task_id).unwrap().unwrap();
    let final_segments = db.get_segments(task_id).unwrap();
    assert_eq!(final_segments.len(), 4);

    let expected_bytes_per_seg = iterations * 65536;
    for (i, seg) in final_segments.iter().enumerate() {
        assert_eq!(
            seg.verified_intervals.total_verified_bytes(),
            expected_bytes_per_seg
        );
        let expected_start = (i as u64) * seg_size;
        assert_eq!(seg.current_cursor, expected_start + expected_bytes_per_seg);
    }
    assert_eq!(final_task.downloaded_bytes, expected_bytes_per_seg * 4);
}
