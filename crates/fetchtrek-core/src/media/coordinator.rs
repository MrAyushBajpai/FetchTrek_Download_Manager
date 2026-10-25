use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fetchtrek_storage::SlabPool;
use fetchtrek_types::{FetchTrekError, MediaError, NetworkError, StorageError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::scheduler::MediaChunkScheduler;
use super::{MediaChunk, MediaDownloadConfig};
use crate::transport::NetworkTransport;

/// Execution summary of a parallel media chunk ingestion run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaDownloadSummary {
    pub total_chunks: usize,
    pub total_bytes: u64,
    pub staging_dir: PathBuf,
    pub duration: Duration,
}

/// Coordinates parallel downloading of discrete media chunks across concurrent workers.
pub struct MediaIngestionCoordinator {
    pub config: MediaDownloadConfig,
    pub transport: Arc<dyn NetworkTransport>,
    pub scheduler: Arc<MediaChunkScheduler>,
    pub memory_pool: Arc<SlabPool>,
    pub downloaded_bytes: Arc<AtomicU64>,
    pub speed_bps: Arc<AtomicU64>,
}

impl MediaIngestionCoordinator {
    /// Creates a new media ingestion coordinator with default chunk scheduler.
    pub fn new(
        config: MediaDownloadConfig,
        transport: Arc<dyn NetworkTransport>,
        memory_pool: Arc<SlabPool>,
    ) -> Self {
        let scheduler = Arc::new(MediaChunkScheduler::new(
            config.chunks.clone(),
            config.max_retries_per_chunk,
        ));
        Self {
            config,
            transport,
            scheduler,
            memory_pool,
            downloaded_bytes: Arc::new(AtomicU64::new(0)),
            speed_bps: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Creates a coordinator using an existing shared scheduler.
    pub fn with_scheduler(
        config: MediaDownloadConfig,
        transport: Arc<dyn NetworkTransport>,
        memory_pool: Arc<SlabPool>,
        scheduler: Arc<MediaChunkScheduler>,
    ) -> Self {
        Self {
            config,
            transport,
            scheduler,
            memory_pool,
            downloaded_bytes: Arc::new(AtomicU64::new(0)),
            speed_bps: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Spawns the ingestion execution loop on Tokio and returns a JoinHandle.
    pub fn start(
        self: &Arc<Self>,
        cancel_rx: watch::Receiver<bool>,
    ) -> JoinHandle<Result<MediaDownloadSummary, FetchTrekError>> {
        let coordinator = Arc::clone(self);
        tokio::spawn(async move { coordinator.run(cancel_rx).await })
    }

    /// Runs the parallel ingestion workflow directly.
    pub async fn run(
        self: &Arc<Self>,
        cancel_rx: watch::Receiver<bool>,
    ) -> Result<MediaDownloadSummary, FetchTrekError> {
        let start_time = Instant::now();

        if *cancel_rx.borrow() {
            return Err(FetchTrekError::Network(NetworkError::ConnectionReset));
        }

        // 1. Create staging directory if it does not exist
        tokio::fs::create_dir_all(&self.config.destination_dir)
            .await
            .map_err(|_| FetchTrekError::Storage(StorageError::IoFailure))?;

        if self.config.chunks.is_empty() {
            return Ok(MediaDownloadSummary {
                total_chunks: 0,
                total_bytes: 0,
                staging_dir: self.config.destination_dir.clone(),
                duration: Duration::from_millis(0),
            });
        }

        // 2. Spawn lightweight speed measurement ticker
        let speed_bps = Arc::clone(&self.speed_bps);
        let downloaded_bytes = Arc::clone(&self.downloaded_bytes);
        let mut ticker_cancel = cancel_rx.clone();
        let ticker_handle = tokio::spawn(async move {
            let mut last_bytes = downloaded_bytes.load(Ordering::Relaxed);
            let mut last_time = Instant::now();
            loop {
                tokio::select! {
                    _ = ticker_cancel.changed() => break,
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        let now = Instant::now();
                        let cur_bytes = downloaded_bytes.load(Ordering::Relaxed);
                        let elapsed = now.duration_since(last_time).as_secs_f64();
                        if elapsed > 0.0 {
                            let delta_bytes = cur_bytes.saturating_sub(last_bytes);
                            let bps = ((delta_bytes as f64 / elapsed) * 8.0) as u64;
                            speed_bps.store(bps, Ordering::Relaxed);
                        }
                        last_bytes = cur_bytes;
                        last_time = now;
                    }
                }
            }
        });

        // 3. Spawn concurrent worker tasks
        let num_workers = self.config.connections.clamp(1, 16);
        let first_error: Arc<Mutex<Option<FetchTrekError>>> = Arc::new(Mutex::new(None));
        let mut worker_handles = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            let coordinator = Arc::clone(self);
            let mut cancel_rx = cancel_rx.clone();
            let first_error = Arc::clone(&first_error);

            let handle = tokio::spawn(async move {
                loop {
                    if *cancel_rx.borrow() || coordinator.scheduler.has_terminal_failure() {
                        break;
                    }

                    let chunk = match coordinator.scheduler.claim_next_chunk(worker_id) {
                        Some(c) => c,
                        None => {
                            if coordinator.scheduler.is_finished()
                                || coordinator.scheduler.has_terminal_failure()
                            {
                                break;
                            }
                            tokio::select! {
                                _ = cancel_rx.changed() => {
                                    if *cancel_rx.borrow() {
                                        break;
                                    }
                                }
                                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                            }
                            continue;
                        }
                    };

                    let part_path = coordinator
                        .config
                        .destination_dir
                        .join(format!("part_{:06}.part", chunk.index));

                    match coordinator
                        .download_single_chunk(&chunk, &part_path, &mut cancel_rx)
                        .await
                    {
                        Ok(bytes) => {
                            coordinator.scheduler.mark_completed(chunk.index, bytes);
                        }
                        Err(err) => {
                            let _ = tokio::fs::remove_file(&part_path).await;
                            let is_retryable = coordinator
                                .scheduler
                                .mark_failed(chunk.index, &err.to_string());
                            if !is_retryable {
                                let mut guard = first_error.lock().unwrap();
                                if guard.is_none() {
                                    *guard = Some(err);
                                }
                                break;
                            }
                        }
                    }
                }
            });

            worker_handles.push(handle);
        }

        // 4. Await all workers
        for handle in worker_handles {
            let _ = handle.await;
        }

        ticker_handle.abort();

        // 5. Check outcome
        if *cancel_rx.borrow() {
            return Err(FetchTrekError::Network(NetworkError::ConnectionReset));
        }

        if let Some(err) = first_error.lock().unwrap().take() {
            return Err(err);
        }

        if !self.scheduler.is_finished() {
            return Err(FetchTrekError::Media(MediaError::SegmentMissing));
        }

        let snapshot = self.scheduler.snapshot();
        Ok(MediaDownloadSummary {
            total_chunks: snapshot.total_chunks,
            total_bytes: snapshot.downloaded_bytes,
            staging_dir: self.config.destination_dir.clone(),
            duration: start_time.elapsed(),
        })
    }

    /// Ingests a single chunk, buffering data through the shared SlabPool with backpressure.
    async fn download_single_chunk(
        &self,
        chunk: &MediaChunk,
        part_path: &Path,
        cancel_rx: &mut watch::Receiver<bool>,
    ) -> Result<u64, FetchTrekError> {
        if *cancel_rx.borrow() {
            return Err(FetchTrekError::Network(NetworkError::ConnectionReset));
        }

        let mut stream = match chunk.byte_range {
            Some(range) => self
                .transport
                .open_range_stream(&chunk.url, range, &self.config.context)
                .await
                .map_err(FetchTrekError::Network)?,
            None => self
                .transport
                .open_stream(&chunk.url, &self.config.context)
                .await
                .map_err(FetchTrekError::Network)?,
        };

        let mut file = tokio::fs::File::create(part_path)
            .await
            .map_err(|_| FetchTrekError::Storage(StorageError::IoFailure))?;

        let mut bytes_written = 0u64;

        loop {
            if *cancel_rx.borrow() {
                return Err(FetchTrekError::Network(NetworkError::ConnectionReset));
            }

            // Backpressure check
            while self.memory_pool.is_under_backpressure() {
                tokio::select! {
                    _ = cancel_rx.changed() => {
                        if *cancel_rx.borrow() {
                            return Err(FetchTrekError::Network(NetworkError::ConnectionReset));
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
            }

            // Acquire slab
            let mut slab = loop {
                if let Some(s) = self.memory_pool.acquire() {
                    break s;
                }
                tokio::select! {
                    _ = cancel_rx.changed() => {
                        if *cancel_rx.borrow() {
                            return Err(FetchTrekError::Network(NetworkError::ConnectionReset));
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
            };

            let buf = slab.as_mut_slice();
            let read_fut = stream.read(buf);

            let n = tokio::select! {
                _ = cancel_rx.changed() => {
                    if *cancel_rx.borrow() {
                        return Err(FetchTrekError::Network(NetworkError::ConnectionReset));
                    }
                    0
                }
                res = read_fut => {
                    match res {
                        Ok(0) => 0,
                        Ok(n) => n,
                        Err(_) => return Err(FetchTrekError::Network(NetworkError::SocketTimeout)),
                    }
                }
            };

            if n == 0 {
                break;
            }

            if let Some(ref limiter) = self.config.rate_limiter {
                limiter.acquire(n).await;
            }

            file.write_all(&buf[..n])
                .await
                .map_err(|_| FetchTrekError::Storage(StorageError::IoFailure))?;

            bytes_written += n as u64;
            self.downloaded_bytes.fetch_add(n as u64, Ordering::Relaxed);
        }

        file.flush()
            .await
            .map_err(|_| FetchTrekError::Storage(StorageError::IoFailure))?;

        Ok(bytes_written)
    }
}
