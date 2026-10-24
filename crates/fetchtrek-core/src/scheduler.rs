use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use fetchtrek_types::ByteRange;

pub const CLUSTER_ALIGNMENT: u64 = 0x10000; // 64 KiB

#[derive(Debug)]
pub struct AtomicSegmentCursor {
    pub range_start: u64,
    pub current_cursor: AtomicU64,
    pub limit_byte: AtomicU64,
}

impl AtomicSegmentCursor {
    pub fn new(range: ByteRange) -> Self {
        Self {
            range_start: range.start,
            current_cursor: AtomicU64::new(range.start),
            limit_byte: AtomicU64::new(range.end),
        }
    }

    pub fn advance_cursor(&self, bytes: u64) -> u64 {
        self.current_cursor.fetch_add(bytes, Ordering::AcqRel) + bytes
    }

    pub fn remaining_bytes(&self) -> u64 {
        let cursor = self.current_cursor.load(Ordering::Acquire);
        let limit = self.limit_byte.load(Ordering::Acquire);
        if cursor > limit {
            0
        } else {
            limit - cursor + 1
        }
    }

    pub fn is_completed(&self) -> bool {
        let cursor = self.current_cursor.load(Ordering::Acquire);
        let limit = self.limit_byte.load(Ordering::Acquire);
        cursor > limit
    }

    pub fn current_progress(&self) -> (u64, u64) {
        (
            self.current_cursor.load(Ordering::Acquire),
            self.limit_byte.load(Ordering::Acquire),
        )
    }

    /// Atomically bisects the upper half of this segment if work >= 2 * min_split_size.
    /// Aligns split to a 64 KiB cluster boundary.
    pub fn try_bisect(&self, min_split_size: u64) -> Option<ByteRange> {
        let mut limit = self.limit_byte.load(Ordering::Acquire);
        loop {
            let cursor = self.current_cursor.load(Ordering::Acquire);
            if cursor > limit {
                return None;
            }
            let remaining = limit - cursor + 1;
            if remaining < 2 * min_split_size {
                return None;
            }

            let raw_midpoint = cursor + ((limit - cursor) / 2);
            let aligned_mid = (raw_midpoint + 0xFFFF) & !0xFFFF;

            if aligned_mid >= limit || aligned_mid <= cursor {
                return None;
            }

            match self.limit_byte.compare_exchange(
                limit,
                aligned_mid - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Detect if cursor raced past aligned_mid
                    if self.current_cursor.load(Ordering::Acquire) >= aligned_mid {
                        // Rollback limit_byte
                        let _ = self.limit_byte.compare_exchange(
                            aligned_mid - 1,
                            limit,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        );
                        return None;
                    }
                    return Some(ByteRange::new(aligned_mid, limit));
                }
                Err(actual) => limit = actual,
            }
        }
    }
}

#[derive(Debug)]
pub struct SegmentScheduler {
    pub total_range: ByteRange,
    cursors: RwLock<Vec<Arc<AtomicSegmentCursor>>>,
}

impl SegmentScheduler {
    pub fn new(total_range: ByteRange) -> Self {
        Self {
            total_range,
            cursors: RwLock::new(Vec::new()),
        }
    }

    /// Splits `total_range` into `num_segments` initial non-overlapping segments.
    pub fn split_initial(&self, num_segments: usize) -> Vec<ByteRange> {
        if num_segments == 0 || self.total_range.is_empty() {
            return Vec::new();
        }

        let total_bytes = self.total_range.len();
        let num_segments = (num_segments as u64).min(total_bytes) as usize;

        let mut segments = Vec::with_capacity(num_segments);
        let mut cursors_guard = self.cursors.write().unwrap();
        cursors_guard.clear();

        if num_segments == 1 {
            let range = self.total_range;
            segments.push(range);
            cursors_guard.push(Arc::new(AtomicSegmentCursor::new(range)));
            return segments;
        }

        let base_chunk = total_bytes / (num_segments as u64);
        let mut current_start = self.total_range.start;

        for i in 0..num_segments {
            let end = if i == num_segments - 1 {
                self.total_range.end
            } else {
                let target = current_start + base_chunk - 1;
                // Align to 64 KiB cluster boundary if possible
                let aligned = (target + 0xFFFF) & !0xFFFF;
                if aligned > current_start && aligned < self.total_range.end {
                    aligned - 1
                } else {
                    target.min(self.total_range.end)
                }
            };

            let range = ByteRange::new(current_start, end);
            segments.push(range);
            cursors_guard.push(Arc::new(AtomicSegmentCursor::new(range)));

            current_start = end + 1;
            if current_start > self.total_range.end {
                break;
            }
        }

        segments
    }

    /// Finds the active segment with the largest remaining byte gap and attempts atomic bisection.
    /// Returns the stolen `ByteRange` and the newly registered cursor upon success.
    pub fn steal_work_cursor(
        &self,
        min_split_size: u64,
    ) -> Option<(ByteRange, Arc<AtomicSegmentCursor>)> {
        let cursors = {
            let guard = self.cursors.read().unwrap();
            guard.clone()
        };

        let mut candidates: Vec<Arc<AtomicSegmentCursor>> = cursors
            .into_iter()
            .filter(|c| c.remaining_bytes() >= 2 * min_split_size)
            .collect();

        candidates.sort_by_key(|c| std::cmp::Reverse(c.remaining_bytes()));

        for candidate in candidates {
            if let Some(stolen_range) = candidate.try_bisect(min_split_size) {
                let new_cursor = Arc::new(AtomicSegmentCursor::new(stolen_range));
                self.cursors.write().unwrap().push(Arc::clone(&new_cursor));
                return Some((stolen_range, new_cursor));
            }
        }

        None
    }

    /// Finds the active segment with the largest remaining byte gap and attempts atomic bisection.
    /// Returns the stolen `ByteRange` upon success.
    pub fn steal_work(&self, min_split_size: u64) -> Option<ByteRange> {
        self.steal_work_cursor(min_split_size)
            .map(|(range, _)| range)
    }

    pub fn cursors(&self) -> Vec<Arc<AtomicSegmentCursor>> {
        self.cursors.read().unwrap().clone()
    }

    pub fn active_cursor_count(&self) -> usize {
        self.cursors.read().unwrap().len()
    }

    pub fn total_remaining_bytes(&self) -> u64 {
        self.cursors
            .read()
            .unwrap()
            .iter()
            .map(|c| c.remaining_bytes())
            .sum()
    }

    pub fn is_all_completed(&self) -> bool {
        let cursors = self.cursors.read().unwrap();
        if cursors.is_empty() {
            return self.total_range.is_empty();
        }
        cursors.iter().all(|c| c.is_completed())
    }
}
