use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use fetchtrek_storage::{AsyncStorage, SlabPool, SLAB_SIZE};
use fetchtrek_types::state::InvalidTransitionError;
use fetchtrek_types::{
    ByteRange, FetchTrekError, NetworkError, StorageError, TaskEvent, TaskState,
};
use tokio::io::AsyncReadExt;

use crate::scheduler::SegmentScheduler;
use crate::throttle::BandwidthLimiter;
use crate::transport::{NetworkTransport, SessionContext};

#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub url: String,
    pub destination: PathBuf,
    pub connections: usize,
    pub context: SessionContext,
    pub min_split_size: u64,
    pub rate_limiter: Option<Arc<BandwidthLimiter>>,
}

impl DownloadConfig {
    pub fn new(url: impl Into<String>, destination: impl Into<PathBuf>) -> Self {
        Self {
            url: url.into(),
            destination: destination.into(),
            connections: 4,
            context: SessionContext::default(),
            min_split_size: 64 * 1024,
            rate_limiter: None,
        }
    }

    pub fn with_connections(mut self, n: usize) -> Self {
        self.connections = n;
        self
    }

    pub fn with_context(mut self, context: SessionContext) -> Self {
        self.context = context;
        self
    }

    pub fn with_min_split_size(mut self, size: u64) -> Self {
        self.min_split_size = size;
        self
    }

    pub fn with_rate_limiter(mut self, limiter: Arc<BandwidthLimiter>) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadSummary {
    pub destination: PathBuf,
    pub total_bytes: u64,
    pub final_state: TaskState,
    pub finalization_duration: std::time::Duration,
}

pub struct DownloadCoordinator {
    transport: Arc<dyn NetworkTransport>,
    storage: Arc<dyn AsyncStorage>,
    slab_pool: Arc<SlabPool>,
    state: RwLock<TaskState>,
    bytes_ingested: Arc<AtomicU64>,
    active_connections: Arc<AtomicUsize>,
}

pub type IngestionEngine = DownloadCoordinator;

impl DownloadCoordinator {
    pub fn new(
        transport: Arc<dyn NetworkTransport>,
        storage: Arc<dyn AsyncStorage>,
        slab_pool: Arc<SlabPool>,
    ) -> Self {
        Self {
            transport,
            storage,
            slab_pool,
            state: RwLock::new(TaskState::Queued),
            bytes_ingested: Arc::new(AtomicU64::new(0)),
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn state(&self) -> TaskState {
        *self.state.read().unwrap()
    }

    fn set_state(&self, new_state: TaskState) {
        *self.state.write().unwrap() = new_state;
    }

    fn apply_event(&self, event: TaskEvent) -> Result<TaskState, InvalidTransitionError> {
        let mut guard = self.state.write().unwrap();
        let next = fetchtrek_types::transition(*guard, event)?;
        *guard = next;
        Ok(next)
    }

    pub fn bytes_ingested(&self) -> u64 {
        self.bytes_ingested.load(Ordering::Acquire)
    }

    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    pub async fn download(
        &self,
        config: DownloadConfig,
    ) -> Result<DownloadSummary, FetchTrekError> {
        self.bytes_ingested.store(0, Ordering::Release);

        // Phase 1: Probe & Plan
        let _ = self.apply_event(TaskEvent::StartGranted);
        let probe_res = match self.transport.probe(&config.url, &config.context).await {
            Ok(res) => res,
            Err(e) => {
                self.set_state(TaskState::FatalError);
                return Err(FetchTrekError::Network(e));
            }
        };

        let total_bytes = match probe_res.total_bytes {
            Some(b) => b,
            None => {
                self.set_state(TaskState::FatalError);
                return Err(FetchTrekError::Network(NetworkError::NoRangeSupport));
            }
        };

        if !probe_res.supports_ranges && config.connections > 1 {
            self.set_state(TaskState::FatalError);
            return Err(FetchTrekError::Network(NetworkError::NoRangeSupport));
        }

        let _ = self.apply_event(TaskEvent::ProbeSucceeded);

        // Handle empty file case
        if total_bytes == 0 {
            if let Some(parent) = config.destination.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(e) = self.storage.allocate_sparse(&config.destination, 0).await {
                self.set_state(TaskState::FatalError);
                return Err(FetchTrekError::Storage(e));
            }
            let _ = self.apply_event(TaskEvent::SparseCreated);
            let _ = self.apply_event(TaskEvent::LastByteFlushed);
            if let Err(e) = self.storage.finalize_file(&config.destination, 0).await {
                self.set_state(TaskState::FatalError);
                return Err(FetchTrekError::Storage(e));
            }
            let _ = self.apply_event(TaskEvent::DirectFinalize);
            return Ok(DownloadSummary {
                destination: config.destination,
                total_bytes: 0,
                final_state: TaskState::Completed,
                finalization_duration: std::time::Duration::ZERO,
            });
        }

        // Phase 2: Direct-to-Disk Pre-Allocation
        if let Some(parent) = config.destination.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        if let Err(e) = self
            .storage
            .allocate_sparse(&config.destination, total_bytes)
            .await
        {
            self.set_state(TaskState::FatalError);
            let _ = std::fs::remove_file(&config.destination);
            return Err(FetchTrekError::Storage(e));
        }

        let _ = self.apply_event(TaskEvent::SparseCreated);

        // Partition segments
        let total_range = ByteRange::new(0, total_bytes - 1);
        let scheduler = Arc::new(SegmentScheduler::new(total_range));
        let num_connections = config.connections.max(1);
        let _ = scheduler.split_initial(num_connections);
        let initial_cursors = scheduler.cursors();

        // Phase 3: Concurrent Streaming & Work-Stealing Workers
        let mut worker_handles = Vec::with_capacity(num_connections);

        for worker_idx in 0..num_connections {
            let transport = Arc::clone(&self.transport);
            let storage = Arc::clone(&self.storage);
            let slab_pool = Arc::clone(&self.slab_pool);
            let scheduler = Arc::clone(&scheduler);
            let bytes_ingested = Arc::clone(&self.bytes_ingested);
            let active_connections = Arc::clone(&self.active_connections);
            let rate_limiter = config.rate_limiter.clone();

            let url = config.url.clone();
            let dest = config.destination.clone();
            let context = config.context.clone();
            let min_split_size = config.min_split_size;
            let initial_cursor = initial_cursors.get(worker_idx).cloned();

            let handle = tokio::spawn(async move {
                struct ConnectionGuard(Arc<AtomicUsize>);
                impl Drop for ConnectionGuard {
                    fn drop(&mut self) {
                        self.0.fetch_sub(1, Ordering::SeqCst);
                    }
                }
                active_connections.fetch_add(1, Ordering::SeqCst);
                let _guard = ConnectionGuard(active_connections);

                let mut current_cursor = initial_cursor;

                loop {
                    let cursor = match current_cursor.take() {
                        Some(c) => c,
                        None => {
                            if let Some((_stolen_range, stolen_cursor)) =
                                scheduler.steal_work_cursor(min_split_size)
                            {
                                stolen_cursor
                            } else {
                                break;
                            }
                        }
                    };

                    let start_offset = cursor.current_cursor.load(Ordering::Acquire);
                    let limit_offset = cursor.limit_byte.load(Ordering::Acquire);

                    if start_offset > limit_offset {
                        continue;
                    }

                    let stream_range = ByteRange::new(start_offset, limit_offset);
                    let mut stream = transport
                        .open_range_stream(&url, stream_range, &context)
                        .await
                        .map_err(FetchTrekError::Network)?;

                    loop {
                        let cur_pos = cursor.current_cursor.load(Ordering::Acquire);
                        let cur_limit = cursor.limit_byte.load(Ordering::Acquire);

                        if cur_pos > cur_limit {
                            break;
                        }

                        let remaining = cur_limit - cur_pos + 1;
                        let max_read = (remaining as usize).min(SLAB_SIZE);

                        while slab_pool.is_under_backpressure() {
                            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                        }

                        let mut slab = loop {
                            if let Some(s) = slab_pool.acquire() {
                                break s;
                            }
                            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                        };

                        let buf = slab.as_mut_slice();
                        let n = match stream.read(&mut buf[..max_read]).await {
                            Ok(0) => {
                                if cur_pos <= cur_limit {
                                    return Err(FetchTrekError::Network(
                                        NetworkError::ConnectionReset,
                                    ));
                                }
                                break;
                            }
                            Ok(n) => n,
                            Err(_) => {
                                return Err(FetchTrekError::Network(NetworkError::SocketTimeout));
                            }
                        };

                        if let Some(ref limiter) = rate_limiter {
                            limiter.acquire(n).await;
                        }

                        slab.valid_bytes = n;

                        storage
                            .write_positional(&dest, cur_pos, slab)
                            .await
                            .map_err(FetchTrekError::Storage)?;

                        cursor.advance_cursor(n as u64);
                        bytes_ingested.fetch_add(n as u64, Ordering::Relaxed);
                    }
                }

                Ok::<(), FetchTrekError>(())
            });

            worker_handles.push(handle);
        }

        // Await all workers
        for handle in worker_handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    self.set_state(TaskState::FatalError);
                    let _ = std::fs::remove_file(&config.destination);
                    return Err(e);
                }
                Err(_) => {
                    self.set_state(TaskState::FatalError);
                    let _ = std::fs::remove_file(&config.destination);
                    return Err(FetchTrekError::Storage(StorageError::IoFailure));
                }
            }
        }

        // Phase 4: Instant Finalization
        let finalize_start = std::time::Instant::now();
        let _ = self.apply_event(TaskEvent::LastByteFlushed);

        if let Err(e) = self
            .storage
            .finalize_file(&config.destination, total_bytes)
            .await
        {
            self.set_state(TaskState::FatalError);
            let _ = std::fs::remove_file(&config.destination);
            return Err(FetchTrekError::Storage(e));
        }

        let _ = self.apply_event(TaskEvent::DirectFinalize);
        let finalization_duration = finalize_start.elapsed();

        Ok(DownloadSummary {
            destination: config.destination,
            total_bytes,
            final_state: TaskState::Completed,
            finalization_duration,
        })
    }
}
