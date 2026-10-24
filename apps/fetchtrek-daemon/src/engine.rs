use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fetchtrek_core::{BandwidthLimiter, NetworkTransport};
use fetchtrek_ipc::{
    decode_request, default_endpoint, encode_response, encode_telemetry, read_frame, write_frame,
    IpcFrame, IpcRequest, IpcResponse, IpcServer, IpcStream, TaskProgress, TaskSummary,
    TelemetryBroadcaster, TelemetrySnapshot, OP_PING, TELEMETRY_INTERVAL_MS,
};
use fetchtrek_persistence::{
    default_vault, CredentialVault, Database, PersistenceError, TaskRecord,
};
use fetchtrek_protocols::HttpTransport;
use fetchtrek_storage::{PositionalStorage, SlabPool};
use fetchtrek_types::TaskState;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;

use crate::error::DaemonError;

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub data_dir: PathBuf,
    pub ipc_endpoint: Option<String>,
    pub max_active_tasks: usize,
    pub global_bandwidth_limit_bps: Option<u64>,
    pub slab_pool_size_mb: usize,
}

impl DaemonConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            ipc_endpoint: None,
            max_active_tasks: 5,
            global_bandwidth_limit_bps: None,
            slab_pool_size_mb: 16,
        }
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.ipc_endpoint = Some(endpoint.into());
        self
    }
}

impl Default for DaemonConfig {
    fn default() -> Self {
        let base_dir = std::env::var("LOCALAPPDATA")
            .map(PathBuf::from)
            .or_else(|_| std::env::var("HOME").map(|h| PathBuf::from(h).join(".fetchtrek")))
            .unwrap_or_else(|_| std::env::temp_dir().join("fetchtrek"));

        Self {
            data_dir: base_dir,
            ipc_endpoint: None,
            max_active_tasks: 5,
            global_bandwidth_limit_bps: None,
            slab_pool_size_mb: 16,
        }
    }
}

pub struct DaemonEngine {
    pub config: DaemonConfig,
    pub database: std::sync::Mutex<Database>,
    pub memory_pool: Arc<SlabPool>,
    pub storage: Arc<PositionalStorage>,
    pub limiter: Arc<BandwidthLimiter>,
    pub transport: Arc<dyn NetworkTransport>,
    pub vault: Arc<dyn CredentialVault>,
    pub shutdown_tx: broadcast::Sender<()>,
    accept_handle: Mutex<Option<JoinHandle<()>>>,
    telemetry_handle: Mutex<Option<JoinHandle<()>>>,
}

impl DaemonEngine {
    pub async fn start(config: DaemonConfig) -> Result<Arc<Self>, DaemonError> {
        // 1. Initialize data_dir and open SQLite database
        std::fs::create_dir_all(&config.data_dir)?;
        let db_path = config.data_dir.join("fetchtrek.db");
        let db = Database::open(&db_path)?;

        // 2. Run startup crash recovery restoring uncommitted/interrupted tasks to Paused
        let _recovered = db.recover_interrupted_tasks()?;

        // 3. Allocate memory arena (16 MiB fixed pool) and PositionalStorage
        let memory_pool = SlabPool::new();
        let storage = Arc::new(PositionalStorage::new());

        // 4. Bandwidth limiter & Transport & Vault
        let limiter = match config.global_bandwidth_limit_bps {
            Some(limit) => BandwidthLimiter::new(limit),
            None => BandwidthLimiter::unlimited(),
        };
        let transport: Arc<dyn NetworkTransport> = Arc::new(HttpTransport::new());
        let vault: Arc<dyn CredentialVault> = Arc::from(default_vault(None));

        // 5. Bind IPC Server to configured endpoint
        let endpoint = config.ipc_endpoint.clone().unwrap_or_else(default_endpoint);
        let mut server = IpcServer::bind(&endpoint)?;

        // 6. Broadcast channels
        let (shutdown_tx, _) = broadcast::channel::<()>(16);
        let (telemetry_tx, _) = broadcast::channel::<IpcFrame>(64);

        let engine = Arc::new(Self {
            config,
            database: std::sync::Mutex::new(db),
            memory_pool,
            storage,
            limiter,
            transport,
            vault,
            shutdown_tx: shutdown_tx.clone(),
            accept_handle: Mutex::new(None),
            telemetry_handle: Mutex::new(None),
        });

        // 7. Spawn 10 Hz Telemetry broadcast emitter
        let telemetry_engine = Arc::clone(&engine);
        let mut telemetry_shutdown_rx = shutdown_tx.subscribe();
        let telemetry_tx_clone = telemetry_tx.clone();

        let telemetry_handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(TELEMETRY_INTERVAL_MS));
            let mut broadcaster = TelemetryBroadcaster::new();

            loop {
                tokio::select! {
                    _ = telemetry_shutdown_rx.recv() => {
                        break;
                    }
                    _ = interval.tick() => {
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;

                        if broadcaster.should_emit(now_ms) {
                            let active_tasks = {
                                if let Ok(db) = telemetry_engine.database.lock() {
                                    db.list_tasks(Some(TaskState::Transferring)).unwrap_or_default()
                                } else {
                                    Vec::new()
                                }
                            };

                            let active_count = active_tasks.len() as u32;
                            let tasks = active_tasks.into_iter().map(|t| TaskProgress {
                                task_id: t.id,
                                downloaded_bytes: t.downloaded_bytes,
                                total_bytes: t.total_bytes,
                                speed_bps: 0,
                                active_connections: 0,
                                status: t.status,
                            }).collect();

                            let snapshot = TelemetrySnapshot {
                                timestamp_ms: now_ms,
                                global_download_speed_bps: 0,
                                global_upload_speed_bps: 0,
                                active_task_count: active_count,
                                tasks,
                            };

                            if let Ok(frame) = encode_telemetry(&snapshot) {
                                let _ = telemetry_tx_clone.send(frame);
                            }
                        }
                    }
                }
            }
        });

        // 8. Spawn IPC client accept loop
        let accept_engine = Arc::clone(&engine);
        let mut accept_shutdown_rx = shutdown_tx.subscribe();
        let accept_shutdown_tx = shutdown_tx.clone();
        let accept_telemetry_tx = telemetry_tx.clone();

        let accept_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = accept_shutdown_rx.recv() => {
                        break;
                    }
                    accept_res = server.accept() => {
                        match accept_res {
                            Ok(stream) => {
                                let client_engine = Arc::clone(&accept_engine);
                                let client_shutdown = accept_shutdown_tx.subscribe();
                                let client_telemetry = accept_telemetry_tx.subscribe();
                                tokio::spawn(async move {
                                    handle_client_connection(
                                        stream,
                                        client_engine,
                                        client_shutdown,
                                        client_telemetry,
                                    ).await;
                                });
                            }
                            Err(_) => {
                                break;
                            }
                        }
                    }
                }
            }
        });

        *engine.telemetry_handle.lock().await = Some(telemetry_handle);
        *engine.accept_handle.lock().await = Some(accept_handle);

        Ok(engine)
    }

    pub async fn shutdown(&self) -> Result<(), DaemonError> {
        // 1. Broadcast shutdown signal to all listeners
        let _ = self.shutdown_tx.send(());

        // 2. Await background tasks termination
        if let Some(handle) = self.accept_handle.lock().await.take() {
            let _ = handle.await;
        }
        if let Some(handle) = self.telemetry_handle.lock().await.take() {
            let _ = handle.await;
        }

        // 3. Flush pending database micro-transactions and truncate WAL
        let db = self
            .database
            .lock()
            .map_err(|e| DaemonError::Internal(e.to_string()))?;
        db.connection()
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(|e| DaemonError::Internal(e.to_string()))?;

        Ok(())
    }

    pub fn quick_check(&self) -> Result<(), PersistenceError> {
        self.database
            .lock()
            .map_err(|e| PersistenceError::Migration(format!("Database lock poisoned: {}", e)))?
            .quick_check()
    }

    pub fn list_tasks(
        &self,
        filter_status: Option<TaskState>,
    ) -> Result<Vec<TaskRecord>, PersistenceError> {
        self.database
            .lock()
            .map_err(|e| PersistenceError::Migration(format!("Database lock poisoned: {}", e)))?
            .list_tasks(filter_status)
    }

    pub fn get_task_by_id(&self, id: i64) -> Result<Option<TaskRecord>, PersistenceError> {
        self.database
            .lock()
            .map_err(|e| PersistenceError::Migration(format!("Database lock poisoned: {}", e)))?
            .get_task_by_id(id)
    }
}

async fn handle_client_connection(
    stream: IpcStream,
    engine: Arc<DaemonEngine>,
    mut shutdown_rx: broadcast::Receiver<()>,
    mut telemetry_rx: broadcast::Receiver<IpcFrame>,
) {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::channel::<IpcFrame>(64);
    let (inbound_tx, mut inbound_rx) = tokio::sync::mpsc::channel::<IpcFrame>(64);

    let writer_task = tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            if write_frame(&mut writer, &frame).await.is_err() {
                break;
            }
        }
    });

    let reader_task = tokio::spawn(async move {
        while let Ok(frame) = read_frame(&mut reader).await {
            if inbound_tx.send(frame).await.is_err() {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                break;
            }
            inbound_opt = inbound_rx.recv() => {
                match inbound_opt {
                    Some(frame) => {
                        let resp_frame = dispatch_ipc_frame(&engine, &frame);
                        if outbound_tx.send(resp_frame).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            telemetry_res = telemetry_rx.recv() => {
                if let Ok(frame) = telemetry_res {
                    let _ = outbound_tx.try_send(frame);
                }
            }
        }
    }

    reader_task.abort();
    writer_task.abort();
    let _ = reader_task.await;
    let _ = writer_task.await;
}

fn dispatch_ipc_frame(engine: &DaemonEngine, frame: &IpcFrame) -> IpcFrame {
    let correlation_id = frame.header.correlation_id;
    let (resp, corr) = match decode_request(frame) {
        Ok((IpcRequest::Ping, corr)) => (IpcResponse::Pong, corr),
        Ok((IpcRequest::ListTasks { status_filter }, corr)) => {
            let res = engine
                .database
                .lock()
                .map_err(|e| format!("Lock poisoned: {}", e))
                .and_then(|db| db.list_tasks(status_filter).map_err(|e| e.to_string()));
            match res {
                Ok(tasks) => {
                    let summaries = tasks.into_iter().map(task_record_to_summary).collect();
                    (IpcResponse::TaskList { tasks: summaries }, corr)
                }
                Err(e) => (
                    IpcResponse::Error {
                        code: 500,
                        message: e,
                    },
                    corr,
                ),
            }
        }
        Ok((IpcRequest::QueryTask { task_id }, corr)) => {
            let res = engine
                .database
                .lock()
                .map_err(|e| format!("Lock poisoned: {}", e))
                .and_then(|db| db.get_task_by_id(task_id).map_err(|e| e.to_string()));
            match res {
                Ok(Some(task)) => (
                    IpcResponse::TaskDetail {
                        task: task_record_to_summary(task),
                    },
                    corr,
                ),
                Ok(None) => (
                    IpcResponse::Error {
                        code: 404,
                        message: format!("Task {} not found", task_id),
                    },
                    corr,
                ),
                Err(e) => (
                    IpcResponse::Error {
                        code: 500,
                        message: e,
                    },
                    corr,
                ),
            }
        }
        Ok((_, corr)) => (
            IpcResponse::Error {
                code: 400,
                message: "Operation not yet implemented".to_string(),
            },
            corr,
        ),
        Err(_) if frame.header.message_id == OP_PING => (IpcResponse::Pong, correlation_id),
        Err(e) => (
            IpcResponse::Error {
                code: 400,
                message: format!("Invalid request payload: {}", e),
            },
            correlation_id,
        ),
    };

    encode_response(&resp, corr).unwrap_or_else(|_| {
        let fallback = IpcResponse::Error {
            code: 500,
            message: "Response encoding failed".to_string(),
        };
        encode_response(&fallback, correlation_id).expect("infallible fallback response")
    })
}

fn task_record_to_summary(t: TaskRecord) -> TaskSummary {
    let progress_percent = if t.total_bytes > 0 {
        ((t.downloaded_bytes as f64 / t.total_bytes as f64) * 100.0) as f32
    } else {
        0.0
    };
    TaskSummary {
        id: t.id,
        uuid: t.uuid,
        url: t.url,
        destination_path: t.destination_path,
        filename: t.filename,
        total_bytes: t.total_bytes,
        downloaded_bytes: t.downloaded_bytes,
        status: t.status,
        speed_bps: 0,
        progress_percent,
    }
}
