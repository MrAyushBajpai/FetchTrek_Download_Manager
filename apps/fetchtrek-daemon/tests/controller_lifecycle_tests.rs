use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fetchtrek_core::BandwidthLimiter;
use fetchtrek_daemon::TaskController;
use fetchtrek_persistence::{
    rewind_cursor_to_cluster, Database, NewTask, TaskSegmentRecord, CLUSTER_SIZE,
};
use fetchtrek_protocols::HttpTransport;
use fetchtrek_storage::{PositionalStorage, SlabPool};
use fetchtrek_types::TaskState;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(prefix: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "fetchtrek_lifecycle_{}_{}_{}",
            prefix,
            std::process::id(),
            timestamp
        ));
        let _ = std::fs::create_dir_all(&path);
        Self { path }
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn create_test_controller(temp_dir: &TempDirGuard, max_concurrent: usize) -> Arc<TaskController> {
    let db_path = temp_dir.path.join("fetchtrek.db");
    let db = Database::open(&db_path).expect("open test db failed");
    let db_arc = Arc::new(std::sync::Mutex::new(db));
    let storage = Arc::new(PositionalStorage::new());
    let memory_pool = SlabPool::new();
    let limiter = BandwidthLimiter::unlimited();
    let transport = Arc::new(HttpTransport::new());

    Arc::new(TaskController::new(
        db_arc,
        storage,
        memory_pool,
        limiter,
        transport,
        max_concurrent,
        temp_dir.path.clone(),
    ))
}

async fn start_slow_mock_server() -> (String, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind slow mock server");
    let addr = listener.local_addr().expect("local addr");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let server_url = format!("http://127.0.0.1:{}", addr.port());
    let total_size = 4 * 1024 * 1024; // 4 MiB

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                Ok((mut socket, _)) = listener.accept() => {
                    tokio::spawn(async move {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let mut buf = vec![0u8; 4096];
                        let n = match socket.read(&mut buf).await {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };
                        let req = String::from_utf8_lossy(&buf[..n]);
                        let method = req.split_whitespace().next().unwrap_or("");

                        if method == "HEAD" {
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {total_size}\r\nAccept-Ranges: bytes\r\nETag: \"etag-slow\"\r\nConnection: close\r\n\r\n"
                            );
                            let _ = socket.write_all(resp.as_bytes()).await;
                            let _ = socket.shutdown().await;
                        } else if method == "GET" {
                            let mut start = 0usize;
                            let mut end = total_size - 1;
                            for line in req.lines() {
                                if let Some(range_val) = line
                                    .strip_prefix("range: bytes=")
                                    .or_else(|| line.strip_prefix("Range: bytes="))
                                {
                                    if let Some(dash_idx) = range_val.find('-') {
                                        if let Ok(s) = range_val[..dash_idx].trim().parse::<usize>()
                                        {
                                            start = s;
                                        }
                                        if let Ok(e) =
                                            range_val[dash_idx + 1..].trim().parse::<usize>()
                                        {
                                            end = e;
                                        }
                                    }
                                }
                            }
                            let chunk_len = end.saturating_sub(start) + 1;
                            let header = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nETag: \"etag-slow\"\r\nConnection: close\r\n\r\n",
                                start, end, total_size, chunk_len
                            );
                            let _ = socket.write_all(header.as_bytes()).await;
                            let chunk = vec![0xAB; 64 * 1024];
                            let mut remaining = chunk_len;
                            while remaining > 0 {
                                let to_write = remaining.min(chunk.len());
                                if socket.write_all(&chunk[..to_write]).await.is_err() {
                                    break;
                                }
                                remaining -= to_write;
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                            let _ = socket.shutdown().await;
                        }
                    });
                }
            }
        }
    });

    (server_url, shutdown_tx)
}

#[tokio::test]
async fn test_pause_joins_workers_before_session_removal() {
    let (server_url, _stop) = start_slow_mock_server().await;
    let temp_dir = TempDirGuard::new("pause_join");
    let controller = create_test_controller(&temp_dir, 4);

    let destination_path = temp_dir.path.join("pause_download.bin");
    let dest_str = destination_path.to_str().unwrap();

    let record = controller
        .add_task(
            &format!("{server_url}/pause_download.bin"),
            Some(dest_str),
            Some("pause_download.bin"),
            Some(4),
            None,
        )
        .await
        .expect("add task");

    let task_id = record.id;

    // Allow workers to start streaming
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Confirm session is active
    {
        let sessions = controller.active_sessions.read().await;
        assert!(sessions.contains_key(&task_id));
    }

    // Call pause_task
    controller
        .pause_task(task_id)
        .await
        .expect("pause_task must succeed");

    // 1. Session is removed from active_sessions table immediately upon worker completion
    {
        let sessions = controller.active_sessions.read().await;
        assert!(
            !sessions.contains_key(&task_id),
            "Active session must be removed after workers joined"
        );
    }

    // 2. Database state transitioned to Paused
    let db_task = {
        let db = controller.database.lock().unwrap();
        db.get_task_by_id(task_id).unwrap().unwrap()
    };
    assert_eq!(db_task.status, TaskState::Paused);

    // 3. Segment cursors in database are rewound to 64 KiB boundary
    let segments = {
        let db = controller.database.lock().unwrap();
        db.get_segments(task_id).unwrap()
    };
    for seg in &segments {
        assert_eq!(
            seg.status, 0,
            "Segment status must be reset to pending/paused"
        );
        assert!(
            seg.current_cursor % CLUSTER_SIZE == 0 || seg.current_cursor == seg.range_start,
            "Segment cursor must be 64 KiB aligned or equal to range_start"
        );
    }

    // 4. Concurrently resume task twice - transition lock guarantees no duplicate sessions or duplicate workers
    let c1 = Arc::clone(&controller);
    let c2 = Arc::clone(&controller);
    let (r1, r2) = tokio::join!(c1.resume_task(task_id), c2.resume_task(task_id));
    assert!(r1.is_ok());
    assert!(r2.is_ok());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Must still have exactly 1 session in active_sessions
    {
        let sessions = controller.active_sessions.read().await;
        assert_eq!(
            sessions.len(),
            1,
            "Must never spawn duplicate session instances"
        );
    }

    // Clean up
    controller
        .cancel_task(task_id, true)
        .await
        .expect("cleanup cancel");
}

#[tokio::test]
async fn test_monotonic_progress_clamped() {
    let temp_dir = TempDirGuard::new("progress_clamp");
    let controller = create_test_controller(&temp_dir, 4);

    // 1. Test ActiveTaskSession::progress_percent directly
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);
    let downloaded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(50_000));
    let bytes_received = Arc::new(std::sync::atomic::AtomicU64::new(60_000));
    let bytes_committed = Arc::clone(&downloaded_bytes);
    let speed_bps = Arc::new(std::sync::atomic::AtomicU64::new(1024));
    let active_connections = Arc::new(std::sync::atomic::AtomicU16::new(1));

    let session = fetchtrek_daemon::ActiveTaskSession {
        task_id: 1,
        uuid: "uuid-test-clamp".to_string(),
        cancel_tx,
        downloaded_bytes,
        bytes_received,
        bytes_committed: Arc::clone(&bytes_committed),
        speed_bps,
        active_connections,
        context: Arc::new(fetchtrek_core::SessionContext::default()),
        join_handle: None,
    };

    // 50,000 / 100,000 = 50.0%
    assert_eq!(session.progress_percent(100_000), 50.0);

    // Exact completion
    bytes_committed.store(100_000, Ordering::SeqCst);
    assert_eq!(session.progress_percent(100_000), 100.0);

    // Overshoot / duplicate chunks: 120,000 / 100,000 must clamp to 100.0%
    bytes_committed.store(120_000, Ordering::SeqCst);
    assert_eq!(session.progress_percent(100_000), 100.0);

    // Zero total bytes handles gracefully
    assert_eq!(session.progress_percent(0), 0.0);

    // 2. Test collect_telemetry clamps downloaded_bytes to total_bytes
    {
        let db = controller.database.lock().unwrap();
        let mut new_task = NewTask::new(
            "uuid-clamp-telemetry",
            "https://fetchtrek.local/test.bin",
            "/tmp/test.bin",
            "test.bin",
            "http",
            50_000,
        );
        new_task.status = TaskState::Active;
        db.create_task(&new_task).unwrap();
    }

    // Insert active session with inflated downloaded_bytes: 80_000
    {
        let (tx, _) = tokio::sync::watch::channel(false);
        let s = fetchtrek_daemon::ActiveTaskSession {
            task_id: 1,
            uuid: "uuid-clamp-telemetry".to_string(),
            cancel_tx: tx,
            downloaded_bytes: Arc::new(std::sync::atomic::AtomicU64::new(80_000)),
            bytes_received: Arc::new(std::sync::atomic::AtomicU64::new(90_000)),
            bytes_committed: Arc::new(std::sync::atomic::AtomicU64::new(80_000)),
            speed_bps: Arc::new(std::sync::atomic::AtomicU64::new(500)),
            active_connections: Arc::new(std::sync::atomic::AtomicU16::new(1)),
            context: Arc::new(fetchtrek_core::SessionContext::default()),
            join_handle: None,
        };
        controller.active_sessions.write().await.insert(1, s);
    }

    let telemetry = controller
        .collect_telemetry()
        .await
        .expect("collect_telemetry");
    assert_eq!(telemetry.tasks.len(), 1);
    assert_eq!(
        telemetry.tasks[0].downloaded_bytes, 50_000,
        "Telemetry downloaded_bytes must clamp to total_bytes"
    );
}

#[test]
fn test_crash_recovery_rewind_and_reconciliation() {
    let temp_dir = TempDirGuard::new("crash_reconcile");
    let db_path = temp_dir.path.join("crash_test.db");
    let dest_file = temp_dir.path.join("large_payload.bin");

    let task_id = {
        let db = Database::open(&db_path).expect("open db");
        let mut new_task = NewTask::new(
            "uuid-crash-reconcile",
            "https://fetchtrek.local/large.bin",
            dest_file.to_str().unwrap(),
            "large.bin",
            "http",
            1_000_000,
        );
        new_task.status = TaskState::Active;
        new_task.downloaded_bytes = 180_000;

        let id = db.create_task(&new_task).expect("create task");

        // Segment cursor in database records 180_000 bytes
        let seg = TaskSegmentRecord::new(id, 0, 0, 999_999, 180_000, 1);
        db.save_segments(id, &[seg]).expect("save segments");
        id
        // Drop db connection abruptly simulating ungraceful shutdown
    };

    // Staging file on disk only has 120_000 bytes (tail write uncommitted before crash)
    let staging_file = temp_dir
        .path
        .join(format!(".fetchtrek_temp_{task_id}.part"));
    std::fs::write(&staging_file, vec![0xCCu8; 120_000]).expect("write staging file");

    // Reopen database and perform crash recovery
    let reopened_db = Database::open(&db_path).expect("reopen db");
    let recovered = reopened_db
        .recover_interrupted_tasks()
        .expect("recover_interrupted_tasks");

    assert_eq!(recovered.len(), 1);
    let rec = &recovered[0];
    assert_eq!(rec.task.id, task_id);
    assert_eq!(rec.task.status, TaskState::Paused);

    // Disk length (120_000) was detected, bounding cursor to 120_000,
    // then rewinding to 64 KiB boundary: 65_536
    let expected_cursor = rewind_cursor_to_cluster(120_000, 0);
    assert_eq!(expected_cursor, 65_536);
    assert_eq!(rec.segments[0].current_cursor, 65_536);

    // Discarded tail bytes: 180_000 - 65_536 = 114_464
    assert_eq!(rec.discarded_tail_bytes, 180_000 - 65_536);
    assert_eq!(rec.task.downloaded_bytes, 65_536);

    // Verify database record has updated values
    let db_task = reopened_db.get_task_by_id(task_id).unwrap().unwrap();
    assert_eq!(db_task.status, TaskState::Paused);
    assert_eq!(db_task.downloaded_bytes, 65_536);
}
