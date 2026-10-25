use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fetchtrek_core::{
    AtomicSegmentCursor, BandwidthLimiter, NetworkTransport, SegmentScheduler, SessionContext,
};
use fetchtrek_ipc::{TaskProgress, TelemetrySnapshot};
use fetchtrek_persistence::{
    rewind_cursor_to_cluster, Database, IntervalSet, NewTask, SegmentCheckpoint, TaskRecord,
    TaskSegmentRecord,
};
use fetchtrek_storage::{AsyncStorage, PositionalStorage, SlabPool, SLAB_SIZE};
use fetchtrek_types::{ByteRange, TaskState};
use tokio::io::AsyncReadExt;
use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;

use crate::error::DaemonError;

#[derive(Debug)]
pub struct ActiveTaskSession {
    pub task_id: i64,
    pub uuid: String,
    pub cancel_tx: watch::Sender<bool>,
    pub downloaded_bytes: Arc<AtomicU64>,
    pub speed_bps: Arc<AtomicU64>,
    pub active_connections: Arc<AtomicU16>,
    pub context: Arc<SessionContext>,
    pub join_handle: Option<JoinHandle<Result<(), DaemonError>>>,
}

pub struct TaskController {
    pub database: Arc<std::sync::Mutex<Database>>,
    pub storage: Arc<PositionalStorage>,
    pub memory_pool: Arc<SlabPool>,
    pub limiter: Arc<BandwidthLimiter>,
    pub transport: Arc<dyn NetworkTransport>,
    pub active_sessions: Arc<RwLock<HashMap<i64, ActiveTaskSession>>>,
    pub task_contexts: Arc<RwLock<HashMap<i64, Arc<SessionContext>>>>,
    pub max_concurrent_tasks: usize,
    pub default_download_dir: PathBuf,
}

impl TaskController {
    pub fn new(
        database: Arc<std::sync::Mutex<Database>>,
        storage: Arc<PositionalStorage>,
        memory_pool: Arc<SlabPool>,
        limiter: Arc<BandwidthLimiter>,
        transport: Arc<dyn NetworkTransport>,
        max_concurrent_tasks: usize,
        default_download_dir: PathBuf,
    ) -> Self {
        Self {
            database,
            storage,
            memory_pool,
            limiter,
            transport,
            active_sessions: Arc::new(RwLock::new(HashMap::new())),
            task_contexts: Arc::new(RwLock::new(HashMap::new())),
            max_concurrent_tasks,
            default_download_dir,
        }
    }

    pub async fn add_task(
        self: &Arc<Self>,
        url: &str,
        destination_path: Option<&str>,
        filename: Option<&str>,
        connections: Option<usize>,
        session_context: Option<SessionContext>,
    ) -> Result<TaskRecord, DaemonError> {
        let context = session_context.unwrap_or_default();
        let probe_res = self
            .transport
            .probe(url, &context)
            .await
            .map_err(|e| DaemonError::Internal(format!("Probe failed: {}", e)))?;

        let total_bytes = probe_res.total_bytes.unwrap_or(0);
        let resolved_filename = filename
            .map(|s| s.to_string())
            .or(probe_res.suggested_filename)
            .unwrap_or_else(|| {
                url.split('/')
                    .next_back()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("download.bin")
                    .to_string()
            });

        let resolved_dest = destination_path
            .map(PathBuf::from)
            .unwrap_or_else(|| self.default_download_dir.join(&resolved_filename));

        let uuid = format!(
            "task-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );

        let mut new_task = NewTask::new(
            &uuid,
            url,
            resolved_dest.to_string_lossy(),
            &resolved_filename,
            "http",
            total_bytes,
        );
        new_task.etag = probe_res.etag;
        new_task.mime_type = probe_res.mime_type;
        new_task.status = TaskState::Queued;

        let task_id = {
            let db = self
                .database
                .lock()
                .map_err(|e| DaemonError::Internal(e.to_string()))?;
            db.create_task(&new_task)?
        };

        self.task_contexts
            .write()
            .await
            .insert(task_id, Arc::new(context));

        // Pre-create initial segments
        let num_connections = connections.unwrap_or(4).max(1);
        if total_bytes > 0 {
            let total_range = ByteRange::new(0, total_bytes - 1);
            let scheduler = SegmentScheduler::new(total_range);
            let initial_ranges = scheduler.split_initial(num_connections);
            let mut db_segments = Vec::with_capacity(initial_ranges.len());
            for (idx, r) in initial_ranges.iter().enumerate() {
                db_segments.push(TaskSegmentRecord::new(
                    task_id, idx as u32, r.start, r.end, r.start, 0,
                ));
            }
            let db = self
                .database
                .lock()
                .map_err(|e| DaemonError::Internal(e.to_string()))?;
            db.save_segments(task_id, &db_segments)?;
        }

        // Automatically start download if under concurrency ceiling
        let active_count = self.active_sessions.read().await.len();
        if active_count < self.max_concurrent_tasks {
            self.start_task(task_id).await?;
        }

        let record = {
            let db = self
                .database
                .lock()
                .map_err(|e| DaemonError::Internal(e.to_string()))?;
            db.get_task_by_id(task_id)?
                .ok_or_else(|| DaemonError::Internal(format!("Task {} not found", task_id)))?
        };

        Ok(record)
    }

    pub fn start_task(
        self: &Arc<Self>,
        task_id: i64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DaemonError>> + Send>> {
        let self_clone = Arc::clone(self);
        Box::pin(async move {
            let task = {
                let db = self_clone
                    .database
                    .lock()
                    .map_err(|e| DaemonError::Internal(e.to_string()))?;
                db.get_task_by_id(task_id)?
                    .ok_or_else(|| DaemonError::Internal(format!("Task {} not found", task_id)))?
            };

            if task.status == TaskState::Completed {
                return Ok(());
            }

            {
                let sessions = self_clone.active_sessions.read().await;
                if sessions.contains_key(&task_id) {
                    return Ok(());
                }
            }

            // 1. Transition state to Active in DB
            {
                let db = self_clone
                    .database
                    .lock()
                    .map_err(|e| DaemonError::Internal(e.to_string()))?;
                db.update_task_status(task_id, TaskState::Active)?;
            }

            let dest_path = PathBuf::from(&task.destination_path);
            if let Some(parent) = dest_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }

            // 2. Pre-allocate sparse file if total_bytes > 0
            if task.total_bytes > 0 {
                self_clone
                    .storage
                    .allocate_sparse(&dest_path, task.total_bytes)
                    .await
                    .map_err(DaemonError::Storage)?;
            }

            // 3. Load or create segment records
            let mut segments = {
                let db = self_clone
                    .database
                    .lock()
                    .map_err(|e| DaemonError::Internal(e.to_string()))?;
                db.get_segments(task_id)?
            };

            if segments.is_empty() && task.total_bytes > 0 {
                let total_range = ByteRange::new(0, task.total_bytes - 1);
                let scheduler = SegmentScheduler::new(total_range);
                let initial_ranges = scheduler.split_initial(4);
                for (idx, r) in initial_ranges.iter().enumerate() {
                    segments.push(TaskSegmentRecord::new(
                        task_id, idx as u32, r.start, r.end, r.start, 0,
                    ));
                }
                let db = self_clone
                    .database
                    .lock()
                    .map_err(|e| DaemonError::Internal(e.to_string()))?;
                db.save_segments(task_id, &segments)?;
            }

            // 4. Session channels and metrics
            let task_context = self_clone
                .task_contexts
                .read()
                .await
                .get(&task_id)
                .cloned()
                .unwrap_or_else(|| Arc::new(SessionContext::default()));
            let (cancel_tx, cancel_rx) = watch::channel(false);
            let downloaded_bytes = Arc::new(AtomicU64::new(task.downloaded_bytes));
            let speed_bps = Arc::new(AtomicU64::new(0));
            let active_connections = Arc::new(AtomicU16::new(0));

            let active_cursors: Vec<(u32, Arc<AtomicSegmentCursor>)> = segments
                .iter()
                .map(|seg| {
                    (
                        seg.segment_index,
                        Arc::new(AtomicSegmentCursor {
                            range_start: seg.range_start,
                            current_cursor: AtomicU64::new(seg.current_cursor),
                            limit_byte: AtomicU64::new(seg.range_end),
                        }),
                    )
                })
                .collect();

            // 5. Spawn background ingestion coordinator task
            let controller_clone = Arc::clone(&self_clone);
            let dest_clone = dest_path.clone();
            let total_bytes = task.total_bytes;
            let url_clone = task.url.clone();
            let downloaded_bytes_clone = Arc::clone(&downloaded_bytes);
            let speed_bps_clone = Arc::clone(&speed_bps);
            let active_conns_clone = Arc::clone(&active_connections);
            let cancel_rx_task = cancel_rx.clone();
            let cursors_for_workers = active_cursors.clone();
            let context_for_session = Arc::clone(&task_context);

            let join_handle = tokio::spawn(async move {
                let mut worker_handles = Vec::with_capacity(cursors_for_workers.len());

                for (_seg_idx, cursor) in &cursors_for_workers {
                    let cur = cursor.current_cursor.load(Ordering::Acquire);
                    let limit = cursor.limit_byte.load(Ordering::Acquire);
                    if cur > limit {
                        continue;
                    }

                    let transport = Arc::clone(&controller_clone.transport);
                    let storage = Arc::clone(&controller_clone.storage);
                    let slab_pool = Arc::clone(&controller_clone.memory_pool);
                    let limiter = Arc::clone(&controller_clone.limiter);
                    let active_conns = Arc::clone(&active_conns_clone);
                    let downloaded_bytes = Arc::clone(&downloaded_bytes_clone);
                    let cursor_clone = Arc::clone(cursor);
                    let mut cancel_rx_worker = cancel_rx_task.clone();
                    let url = url_clone.clone();
                    let dest = dest_clone.clone();
                    let worker_context = Arc::clone(&context_for_session);

                    let worker_handle = tokio::spawn(async move {
                        struct ConnGuard(Arc<AtomicU16>);
                        impl Drop for ConnGuard {
                            fn drop(&mut self) {
                                self.0.fetch_sub(1, Ordering::SeqCst);
                            }
                        }
                        active_conns.fetch_add(1, Ordering::SeqCst);
                        let _guard = ConnGuard(active_conns);

                        let start_pos = cursor_clone.current_cursor.load(Ordering::Acquire);
                        let end_pos = cursor_clone.limit_byte.load(Ordering::Acquire);
                        if start_pos > end_pos {
                            return Ok::<(), DaemonError>(());
                        }

                        let stream_range = ByteRange::new(start_pos, end_pos);
                        let mut stream = match transport
                            .open_range_stream(&url, stream_range, &worker_context)
                            .await
                        {
                            Ok(s) => s,
                            Err(e) => {
                                return Err(DaemonError::Internal(format!(
                                    "Failed to open range stream: {}",
                                    e
                                )))
                            }
                        };

                        loop {
                            if *cancel_rx_worker.borrow() {
                                break;
                            }

                            let cur_pos = cursor_clone.current_cursor.load(Ordering::Acquire);
                            let cur_limit = cursor_clone.limit_byte.load(Ordering::Acquire);
                            if cur_pos > cur_limit {
                                break;
                            }

                            let remaining = cur_limit - cur_pos + 1;
                            let max_read = (remaining as usize).min(SLAB_SIZE);

                            while slab_pool.is_under_backpressure() {
                                if *cancel_rx_worker.borrow() {
                                    return Ok(());
                                }
                                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                            }

                            let mut slab = loop {
                                if *cancel_rx_worker.borrow() {
                                    return Ok(());
                                }
                                if let Some(s) = slab_pool.acquire() {
                                    break s;
                                }
                                tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                            };

                            let buf = slab.as_mut_slice();
                            let n = tokio::select! {
                                _ = cancel_rx_worker.changed() => {
                                    if *cancel_rx_worker.borrow() {
                                        return Ok(());
                                    }
                                    continue;
                                }
                                res = stream.read(&mut buf[..max_read]) => {
                                    match res {
                                        Ok(0) => {
                                            if cur_pos <= cur_limit {
                                                return Err(DaemonError::Internal("Connection reset early".to_string()));
                                            }
                                            break;
                                        }
                                        Ok(n) => n,
                                        Err(e) => return Err(DaemonError::Internal(format!("Read error: {}", e))),
                                    }
                                }
                            };

                            if !limiter.is_unlimited() {
                                limiter.acquire(n).await;
                            }

                            slab.valid_bytes = n;
                            storage
                                .write_positional(&dest, cur_pos, slab)
                                .await
                                .map_err(DaemonError::Storage)?;

                            cursor_clone.advance_cursor(n as u64);
                            downloaded_bytes.fetch_add(n as u64, Ordering::Relaxed);
                        }

                        Ok(())
                    });

                    worker_handles.push(worker_handle);
                }

                // Spawn progress & SQLite micro-transaction ticker (1,000ms cadence)
                let progress_database = Arc::clone(&controller_clone.database);
                let progress_downloaded = Arc::clone(&downloaded_bytes_clone);
                let progress_speed = Arc::clone(&speed_bps_clone);
                let mut progress_cancel = cancel_rx_task.clone();
                let cursors_for_progress = cursors_for_workers.clone();

                let progress_handle = tokio::spawn(async move {
                    let mut interval =
                        tokio::time::interval(tokio::time::Duration::from_millis(1000));
                    let mut last_bytes = progress_downloaded.load(Ordering::Acquire);

                    loop {
                        tokio::select! {
                            _ = progress_cancel.changed() => {
                                if *progress_cancel.borrow() {
                                    break;
                                }
                            }
                            _ = interval.tick() => {
                                let cur_bytes = progress_downloaded.load(Ordering::Acquire);
                                let delta = cur_bytes.saturating_sub(last_bytes);
                                last_bytes = cur_bytes;
                                progress_speed.store(delta, Ordering::Release);

                                let checkpoints: Vec<SegmentCheckpoint> = cursors_for_progress
                                    .iter()
                                    .map(|(idx, c)| {
                                        let (cur, limit) = c.current_progress();
                                        let status = if cur > limit { 3 } else { 1 };
                                        SegmentCheckpoint::new(*idx, cur, status, IntervalSet::new())
                                    })
                                    .collect();

                                if let Ok(db) = progress_database.lock() {
                                    let _ = db.checkpoint_progress(task_id, cur_bytes, &checkpoints);
                                }
                            }
                        }
                    }
                });

                // Await workers
                let mut worker_error = None;
                for handle in worker_handles {
                    match handle.await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            worker_error = Some(e);
                            break;
                        }
                        Err(e) => {
                            worker_error =
                                Some(DaemonError::Internal(format!("Worker panicked: {}", e)));
                            break;
                        }
                    }
                }

                progress_handle.abort();
                let _ = progress_handle.await;

                let was_cancelled = *cancel_rx_task.borrow();

                if let Some(err) = worker_error {
                    if !was_cancelled {
                        if let Ok(db) = controller_clone.database.lock() {
                            let _ = db.update_task_status(task_id, TaskState::Failed);
                        }
                        controller_clone
                            .active_sessions
                            .write()
                            .await
                            .remove(&task_id);
                        controller_clone.check_and_start_next_queued().await;
                        return Err(err);
                    }
                }

                if was_cancelled {
                    // Rewind segment cursors to 64 KiB boundary and persist Paused
                    let checkpoints: Vec<SegmentCheckpoint> = cursors_for_workers
                        .iter()
                        .map(|(idx, c)| {
                            let (cur, _) = c.current_progress();
                            let rewound = rewind_cursor_to_cluster(cur, c.range_start);
                            SegmentCheckpoint::new(*idx, rewound, 0, IntervalSet::new())
                        })
                        .collect();

                    let mut total_rewound_downloaded = 0u64;
                    for c in &checkpoints {
                        if let Some((_, cursor)) = cursors_for_workers
                            .iter()
                            .find(|(idx, _)| *idx == c.segment_index)
                        {
                            total_rewound_downloaded +=
                                c.current_cursor.saturating_sub(cursor.range_start);
                        }
                    }

                    if let Ok(db) = controller_clone.database.lock() {
                        let _ =
                            db.checkpoint_progress(task_id, total_rewound_downloaded, &checkpoints);
                        let _ = db.update_task_status(task_id, TaskState::Paused);
                    }
                } else {
                    // Instant finalization with 0.00s post-download merge
                    if total_bytes > 0 {
                        controller_clone
                            .storage
                            .finalize_file(&dest_clone, total_bytes)
                            .await
                            .map_err(DaemonError::Storage)?;
                    }

                    let final_checkpoints: Vec<SegmentCheckpoint> = cursors_for_workers
                        .iter()
                        .map(|(idx, c)| {
                            SegmentCheckpoint::new(
                                *idx,
                                c.limit_byte.load(Ordering::Acquire) + 1,
                                3,
                                IntervalSet::new(),
                            )
                        })
                        .collect();

                    if let Ok(db) = controller_clone.database.lock() {
                        let _ = db.checkpoint_progress(task_id, total_bytes, &final_checkpoints);
                        let _ = db.update_task_status(task_id, TaskState::Completed);
                    }
                }

                controller_clone
                    .active_sessions
                    .write()
                    .await
                    .remove(&task_id);
                controller_clone.check_and_start_next_queued().await;

                Ok(())
            });

            let session = ActiveTaskSession {
                task_id,
                uuid: task.uuid,
                cancel_tx,
                downloaded_bytes,
                speed_bps,
                active_connections,
                context: task_context,
                join_handle: Some(join_handle),
            };

            self_clone
                .active_sessions
                .write()
                .await
                .insert(task_id, session);

            Ok(())
        })
    }

    pub async fn pause_task(self: &Arc<Self>, task_id: i64) -> Result<(), DaemonError> {
        let session = {
            let mut sessions = self.active_sessions.write().await;
            sessions.remove(&task_id)
        };

        if let Some(session) = session {
            let _ = session.cancel_tx.send(true);
            if let Some(handle) = session.join_handle {
                let _ = handle.await;
            }
        }

        // Rewind segment cursors in DB to 64 KiB boundary and persist Paused
        {
            let db = self
                .database
                .lock()
                .map_err(|e| DaemonError::Internal(e.to_string()))?;
            let segments = db.get_segments(task_id)?;
            let mut total_rewound_downloaded = 0u64;
            let checkpoints: Vec<SegmentCheckpoint> = segments
                .into_iter()
                .map(|seg| {
                    let rewound_cursor =
                        rewind_cursor_to_cluster(seg.current_cursor, seg.range_start);
                    total_rewound_downloaded += rewound_cursor.saturating_sub(seg.range_start);
                    SegmentCheckpoint::new(seg.segment_index, rewound_cursor, 0, IntervalSet::new())
                })
                .collect();

            db.checkpoint_progress(task_id, total_rewound_downloaded, &checkpoints)?;
            db.update_task_status(task_id, TaskState::Paused)?;
        }

        self.check_and_start_next_queued().await;

        Ok(())
    }

    pub async fn resume_task(self: &Arc<Self>, task_id: i64) -> Result<(), DaemonError> {
        let active_count = self.active_sessions.read().await.len();
        if active_count < self.max_concurrent_tasks {
            self.start_task(task_id).await?;
        } else {
            let db = self
                .database
                .lock()
                .map_err(|e| DaemonError::Internal(e.to_string()))?;
            db.update_task_status(task_id, TaskState::Queued)?;
        }
        Ok(())
    }

    pub async fn cancel_task(
        self: &Arc<Self>,
        task_id: i64,
        delete_file: bool,
    ) -> Result<(), DaemonError> {
        let session = {
            let mut sessions = self.active_sessions.write().await;
            sessions.remove(&task_id)
        };
        self.task_contexts.write().await.remove(&task_id);

        if let Some(session) = session {
            let _ = session.cancel_tx.send(true);
            if let Some(handle) = session.join_handle {
                let _ = handle.await;
            }
        }

        let dest_path = {
            let db = self
                .database
                .lock()
                .map_err(|e| DaemonError::Internal(e.to_string()))?;
            let task = db.get_task_by_id(task_id)?;
            db.update_task_status(task_id, TaskState::Cancelled)?;
            task.map(|t| PathBuf::from(t.destination_path))
        };

        if delete_file {
            if let Some(path) = dest_path {
                if path.exists() {
                    let _ = std::fs::remove_file(path);
                }
            }
        }

        self.check_and_start_next_queued().await;

        Ok(())
    }

    pub async fn collect_telemetry(&self) -> Result<TelemetrySnapshot, DaemonError> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let sessions = self.active_sessions.read().await;
        let mut tasks = Vec::with_capacity(sessions.len());
        let mut global_speed = 0u64;

        let db = self
            .database
            .lock()
            .map_err(|e| DaemonError::Internal(e.to_string()))?;

        for session in sessions.values() {
            let downloaded = session.downloaded_bytes.load(Ordering::Acquire);
            let speed = session.speed_bps.load(Ordering::Acquire);
            let conns = session.active_connections.load(Ordering::Acquire);
            global_speed += speed;

            let total_bytes = db
                .get_task_by_id(session.task_id)
                .ok()
                .flatten()
                .map(|t| t.total_bytes)
                .unwrap_or(0);

            tasks.push(TaskProgress {
                task_id: session.task_id,
                downloaded_bytes: downloaded,
                total_bytes,
                speed_bps: speed,
                active_connections: conns,
                status: TaskState::Active,
            });
        }

        Ok(TelemetrySnapshot {
            timestamp_ms: now_ms,
            global_download_speed_bps: global_speed,
            global_upload_speed_bps: 0,
            active_task_count: tasks.len() as u32,
            tasks,
        })
    }

    pub async fn check_and_start_next_queued(self: &Arc<Self>) {
        let active_count = self.active_sessions.read().await.len();
        if active_count < self.max_concurrent_tasks {
            let next_task = {
                if let Ok(db) = self.database.lock() {
                    db.list_tasks(Some(TaskState::Queued))
                        .ok()
                        .and_then(|list| list.into_iter().next())
                } else {
                    None
                }
            };

            if let Some(task) = next_task {
                let self_clone = Arc::clone(self);
                tokio::spawn(async move {
                    let _ = self_clone.start_task(task.id).await;
                });
            }
        }
    }
}
