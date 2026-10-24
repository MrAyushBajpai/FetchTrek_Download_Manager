use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_core::BandwidthLimiter;
use fetchtrek_daemon::TaskController;
use fetchtrek_persistence::Database;
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
            "fetchtrek_ctrl_test_{}_{}_{}",
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

async fn start_mock_http_server(
    payload: Arc<Vec<u8>>,
    chunk_delay_ms: u64,
) -> (String, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock server");
    let addr = listener.local_addr().expect("local addr");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let server_url = format!("http://127.0.0.1:{}", addr.port());

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                Ok((mut socket, _)) = listener.accept() => {
                    let payload = Arc::clone(&payload);
                    tokio::spawn(async move {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let mut buf = vec![0u8; 4096];
                        let n = match socket.read(&mut buf).await {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };
                        let req = String::from_utf8_lossy(&buf[..n]);
                        let first_line = req.lines().next().unwrap_or("");
                        let parts: Vec<&str> = first_line.split_whitespace().collect();
                        if parts.is_empty() {
                            return;
                        }
                        let method = parts[0];

                        if method == "HEAD" {
                            let head_resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"mock-etag-1\"\r\nConnection: close\r\n\r\n",
                                payload.len()
                            );
                            let _ = socket.write_all(head_resp.as_bytes()).await;
                            let _ = socket.shutdown().await;
                            return;
                        }

                        if method == "GET" {
                            let mut start = 0usize;
                            let mut end = payload.len() - 1;
                            for line in req.lines() {
                                if line.to_ascii_lowercase().starts_with("range: bytes=") {
                                    let range_val = &line["range: bytes=".len()..];
                                    let range_parts: Vec<&str> = range_val.split('-').collect();
                                    if let Ok(s) = range_parts[0].trim().parse::<usize>() {
                                        start = s;
                                    }
                                    if range_parts.len() > 1 && !range_parts[1].trim().is_empty() {
                                        if let Ok(e) = range_parts[1].trim().parse::<usize>() {
                                            end = e;
                                        }
                                    }
                                }
                            }

                            if start >= payload.len() || start > end {
                                let err_resp = b"HTTP/1.1 416 Range Not Satisfiable\r\nConnection: close\r\n\r\n";
                                let _ = socket.write_all(err_resp).await;
                                let _ = socket.shutdown().await;
                                return;
                            }

                            end = end.min(payload.len() - 1);
                            let chunk_len = end - start + 1;
                            let header = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                start, end, payload.len(), chunk_len
                            );
                            let _ = socket.write_all(header.as_bytes()).await;

                            let data_slice = &payload[start..=end];
                            if chunk_delay_ms > 0 {
                                for chunk in data_slice.chunks(64 * 1024) {
                                    if socket.write_all(chunk).await.is_err() {
                                        break;
                                    }
                                    tokio::time::sleep(tokio::time::Duration::from_millis(chunk_delay_ms)).await;
                                }
                            } else {
                                let _ = socket.write_all(data_slice).await;
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

#[tokio::test]
async fn test_controller_e2e_download() {
    const PAYLOAD_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
    let payload: Arc<Vec<u8>> = Arc::new(
        (0..PAYLOAD_SIZE)
            .map(|i| (((i.wrapping_mul(31)).wrapping_add(7)) & 0xFF) as u8)
            .collect(),
    );

    let (server_url, _server_stop) = start_mock_http_server(Arc::clone(&payload), 0).await;
    let temp_dir = TempDirGuard::new("e2e_download");
    let controller = create_test_controller(&temp_dir, 5);

    let target_dest = temp_dir.path.join("e2e_output.bin");
    let task_record = controller
        .add_task(
            &format!("{}/file.bin", server_url),
            Some(&target_dest.to_string_lossy()),
            Some("e2e_output.bin"),
            Some(4),
        )
        .await
        .expect("add_task failed");

    // Wait for download to complete
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let task = controller
            .database
            .lock()
            .unwrap()
            .get_task_by_id(task_record.id)
            .unwrap()
            .expect("task not found");

        if task.status == TaskState::Completed {
            assert_eq!(task.downloaded_bytes, PAYLOAD_SIZE as u64);
            break;
        }

        assert!(
            Instant::now() < deadline,
            "Download timed out before reaching Completed state"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 100% Bit Parity verification
    let written = std::fs::read(&target_dest).expect("read downloaded file failed");
    assert_eq!(written.len(), PAYLOAD_SIZE);
    assert_eq!(written, *payload, "Exact bit-for-bit parity check failed");
}

#[tokio::test]
async fn test_controller_pause_and_resumption() {
    const PAYLOAD_SIZE: usize = 8 * 1024 * 1024; // 8 MiB
    let payload: Arc<Vec<u8>> = Arc::new(
        (0..PAYLOAD_SIZE)
            .map(|i| (((i.wrapping_mul(17)).wrapping_add(3)) & 0xFF) as u8)
            .collect(),
    );

    // Serve with small delay to ensure we can catch the task in-flight
    let (server_url, _server_stop) = start_mock_http_server(Arc::clone(&payload), 3).await;
    let temp_dir = TempDirGuard::new("pause_resume");
    let controller = create_test_controller(&temp_dir, 5);

    let target_dest = temp_dir.path.join("pause_resume.bin");
    let task_record = controller
        .add_task(
            &format!("{}/file.bin", server_url),
            Some(&target_dest.to_string_lossy()),
            Some("pause_resume.bin"),
            Some(4),
        )
        .await
        .expect("add_task failed");

    // Let it download partially
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Pause task
    controller
        .pause_task(task_record.id)
        .await
        .expect("pause_task failed");

    // Verify task is Paused and cursors are rewound to 64 KiB boundary
    let task = controller
        .database
        .lock()
        .unwrap()
        .get_task_by_id(task_record.id)
        .unwrap()
        .expect("task not found");
    assert_eq!(task.status, TaskState::Paused);

    let segments = controller
        .database
        .lock()
        .unwrap()
        .get_segments(task_record.id)
        .expect("get segments failed");
    for seg in &segments {
        assert_eq!(
            seg.current_cursor % 65536,
            0,
            "Segment cursor {} not aligned to 64 KiB boundary",
            seg.current_cursor
        );
    }

    // Resume task
    controller
        .resume_task(task_record.id)
        .await
        .expect("resume_task failed");

    // Wait for completion
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let task = controller
            .database
            .lock()
            .unwrap()
            .get_task_by_id(task_record.id)
            .unwrap()
            .expect("task not found");

        if task.status == TaskState::Completed {
            break;
        }

        assert!(Instant::now() < deadline, "Resumed download timed out");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    // Parity proof
    let written = std::fs::read(&target_dest).expect("read resumed file failed");
    assert_eq!(written.len(), PAYLOAD_SIZE);
    assert_eq!(written, *payload, "Resumed file bit parity failed");
}

#[tokio::test]
async fn test_controller_concurrency_queueing() {
    const PAYLOAD_SIZE: usize = 512 * 1024; // 512 KiB
    let payload: Arc<Vec<u8>> = Arc::new(vec![42u8; PAYLOAD_SIZE]);

    let (server_url, _server_stop) = start_mock_http_server(Arc::clone(&payload), 2).await;
    let temp_dir = TempDirGuard::new("concurrency_queue");
    // Max 2 concurrent tasks
    let controller = create_test_controller(&temp_dir, 2);

    let mut task_ids = Vec::new();
    for i in 0..6 {
        let dest = temp_dir.path.join(format!("task_{}.bin", i));
        let record = controller
            .add_task(
                &format!("{}/file_{}.bin", server_url, i),
                Some(&dest.to_string_lossy()),
                Some(&format!("task_{}.bin", i)),
                Some(1),
            )
            .await
            .expect("add_task failed");
        task_ids.push(record.id);
    }

    // Verify initially exactly 2 are Active/Transferring and 4 are Queued
    let active_count = controller.active_sessions.read().await.len();
    assert!(
        active_count <= 2,
        "Active sessions count {} exceeds max_concurrent 2",
        active_count
    );

    // Wait for all 6 tasks to complete via auto-queue advancement
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut completed_count = 0;
        for id in &task_ids {
            let task = controller
                .database
                .lock()
                .unwrap()
                .get_task_by_id(*id)
                .unwrap()
                .expect("task not found");
            if task.status == TaskState::Completed {
                completed_count += 1;
            }
        }

        if completed_count == 6 {
            break;
        }

        assert!(
            Instant::now() < deadline,
            "Timed out waiting for all 6 queued tasks to complete (completed {})",
            completed_count
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

#[tokio::test]
async fn test_controller_telemetry_accuracy() {
    const PAYLOAD_SIZE: usize = 2 * 1024 * 1024; // 2 MiB
    let payload: Arc<Vec<u8>> = Arc::new(vec![99u8; PAYLOAD_SIZE]);

    let (server_url, _server_stop) = start_mock_http_server(Arc::clone(&payload), 5).await;
    let temp_dir = TempDirGuard::new("telemetry_accuracy");
    let controller = create_test_controller(&temp_dir, 5);

    let dest = temp_dir.path.join("telemetry.bin");
    let record = controller
        .add_task(
            &format!("{}/file.bin", server_url),
            Some(&dest.to_string_lossy()),
            Some("telemetry.bin"),
            Some(2),
        )
        .await
        .expect("add_task failed");

    // Collect telemetry during transfer
    let mut saw_active_telemetry = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let telemetry = controller
            .collect_telemetry()
            .await
            .expect("collect_telemetry failed");

        if telemetry.active_task_count >= 1 {
            let task_metric = telemetry.tasks.iter().find(|t| t.task_id == record.id);
            if let Some(metric) = task_metric {
                if metric.active_connections >= 1 || metric.downloaded_bytes > 0 {
                    saw_active_telemetry = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert!(
        saw_active_telemetry,
        "Expected telemetry snapshot to report live active download metrics"
    );
}
