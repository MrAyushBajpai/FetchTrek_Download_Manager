use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

use super::MediaChunk;

/// The lifecycle state of an individual media chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkState {
    /// Unclaimed and ready for ingestion.
    Pending,
    /// Actively being downloaded by a worker.
    Downloading { worker_id: usize, attempt: usize },
    /// Successfully downloaded and verified on disk.
    Completed { bytes: u64 },
    /// Retries exhausted; terminal failure.
    Failed { error: String },
}

/// Internal bookkeeping record for a media chunk.
#[derive(Debug, Clone)]
struct ScheduledChunk {
    chunk: MediaChunk,
    state: ChunkState,
    attempts: usize,
}

/// A snapshot of the overall media scheduler state.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaSchedulerSnapshot {
    pub total_chunks: usize,
    pub completed_chunks: usize,
    pub failed_chunks: usize,
    pub in_flight_chunks: usize,
    pub downloaded_bytes: u64,
    pub progress_percent: f64,
}

/// Dynamic, lock-free/concurrent work-stealing scheduler for discrete media chunks.
pub struct MediaChunkScheduler {
    chunks: Mutex<Vec<ScheduledChunk>>,
    total_chunks: usize,
    completed_chunks: AtomicUsize,
    failed_chunks: AtomicUsize,
    downloaded_bytes: AtomicU64,
    max_retries: usize,
}

impl MediaChunkScheduler {
    /// Creates a new scheduler for the given chunks and maximum retries per chunk.
    pub fn new(chunks: Vec<MediaChunk>, max_retries: usize) -> Self {
        let total_chunks = chunks.len();
        let scheduled = chunks
            .into_iter()
            .map(|chunk| ScheduledChunk {
                chunk,
                state: ChunkState::Pending,
                attempts: 0,
            })
            .collect();

        Self {
            chunks: Mutex::new(scheduled),
            total_chunks,
            completed_chunks: AtomicUsize::new(0),
            failed_chunks: AtomicUsize::new(0),
            downloaded_bytes: AtomicU64::new(0),
            max_retries,
        }
    }

    /// Claims the next lowest-indexed `Pending` chunk atomically.
    /// Returns `None` if all chunks are in-flight, completed, or failed.
    pub fn claim_next_chunk(&self, worker_id: usize) -> Option<MediaChunk> {
        let mut chunks = self.chunks.lock().unwrap();
        for item in chunks.iter_mut() {
            if item.state == ChunkState::Pending {
                item.attempts += 1;
                item.state = ChunkState::Downloading {
                    worker_id,
                    attempt: item.attempts,
                };
                return Some(item.chunk.clone());
            }
        }
        None
    }

    /// Transitions a chunk state to `Completed` and tracks downloaded byte counters.
    pub fn mark_completed(&self, chunk_index: usize, bytes: u64) {
        let mut chunks = self.chunks.lock().unwrap();
        if let Some(item) = chunks.iter_mut().find(|c| c.chunk.index == chunk_index) {
            if !matches!(item.state, ChunkState::Completed { .. }) {
                item.state = ChunkState::Completed { bytes };
                self.completed_chunks.fetch_add(1, Ordering::SeqCst);
                self.downloaded_bytes.fetch_add(bytes, Ordering::SeqCst);
            }
        }
    }

    /// Records a chunk failure.
    /// If `attempt < max_retries`, returns `true` (retryable) and resets state to `Pending`.
    /// If retries are exhausted, returns `false` (terminal) and marks state as `Failed`.
    pub fn mark_failed(&self, chunk_index: usize, error: &str) -> bool {
        let mut chunks = self.chunks.lock().unwrap();
        if let Some(item) = chunks.iter_mut().find(|c| c.chunk.index == chunk_index) {
            if item.attempts < self.max_retries {
                item.state = ChunkState::Pending;
                true
            } else {
                if !matches!(item.state, ChunkState::Failed { .. }) {
                    item.state = ChunkState::Failed {
                        error: error.to_string(),
                    };
                    self.failed_chunks.fetch_add(1, Ordering::SeqCst);
                }
                false
            }
        } else {
            false
        }
    }

    /// Returns `true` when every chunk has transitioned to `Completed`.
    pub fn is_finished(&self) -> bool {
        self.completed_chunks.load(Ordering::SeqCst) == self.total_chunks
    }

    /// Returns `true` if any chunk suffered terminal failure (retries exhausted).
    pub fn has_terminal_failure(&self) -> bool {
        self.failed_chunks.load(Ordering::SeqCst) > 0
    }

    /// Captures a thread-safe atomic snapshot of the current scheduler metrics.
    pub fn snapshot(&self) -> MediaSchedulerSnapshot {
        let chunks = self.chunks.lock().unwrap();
        let in_flight_chunks = chunks
            .iter()
            .filter(|c| matches!(c.state, ChunkState::Downloading { .. }))
            .count();
        let completed_chunks = self.completed_chunks.load(Ordering::SeqCst);
        let failed_chunks = self.failed_chunks.load(Ordering::SeqCst);
        let downloaded_bytes = self.downloaded_bytes.load(Ordering::SeqCst);
        let progress_percent = if self.total_chunks == 0 {
            100.0
        } else {
            (completed_chunks as f64 / self.total_chunks as f64) * 100.0
        };

        MediaSchedulerSnapshot {
            total_chunks: self.total_chunks,
            completed_chunks,
            failed_chunks,
            in_flight_chunks,
            downloaded_bytes,
            progress_percent,
        }
    }

    /// Returns the current state of a chunk by index, if present.
    pub fn chunk_state(&self, chunk_index: usize) -> Option<ChunkState> {
        let chunks = self.chunks.lock().unwrap();
        chunks
            .iter()
            .find(|c| c.chunk.index == chunk_index)
            .map(|c| c.state.clone())
    }

    /// Returns the number of download attempts made for a chunk by index.
    pub fn chunk_attempts(&self, chunk_index: usize) -> Option<usize> {
        let chunks = self.chunks.lock().unwrap();
        chunks
            .iter()
            .find(|c| c.chunk.index == chunk_index)
            .map(|c| c.attempts)
    }
}
