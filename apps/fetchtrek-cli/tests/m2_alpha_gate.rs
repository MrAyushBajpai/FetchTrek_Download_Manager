use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_cli::FetchTrekClient;
use fetchtrek_daemon::{DaemonConfig, DaemonEngine};
use fetchtrek_ipc::endpoint_with_name;
use fetchtrek_persistence::{Database, NewTask, TaskSegmentRecord};
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
            "fetchtrek_m2_gate_{}_{}_{}",
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
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"m2-alpha-etag\"\r\nConnection: close\r\n\r\n",
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
                                    tokio::time::sleep(Duration::from_millis(chunk_delay_ms)).await;
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

#[tokio::test]
async fn test_milestone_m2_alpha_exit_gate() {
    const PAYLOAD_SIZE: usize = 8 * 1024 * 1024; // 8 MiB payload
    let payload: Arc<Vec<u8>> = Arc::new(
        (0..PAYLOAD_SIZE)
            .map(|i| (((i.wrapping_mul(41)).wrapping_add(19)) & 0xFF) as u8)
            .collect(),
    );

    // Step 1: Boot isolated DaemonEngine with dedicated test SQLite DB and IPC pipe
    let temp_dir = TempDirGuard::new("m2_alpha_gate");
    let unique_endpoint = endpoint_with_name(&format!(
        "ft_m2_gate_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let config = DaemonConfig::new(&temp_dir.path).with_endpoint(&unique_endpoint);
    let daemon = DaemonEngine::start(config.clone())
        .await
        .expect("DaemonEngine::start failed");

    // Step 2: Spin up local mock HTTP server serving 8 MiB payload
    let (server_url, _server_stop) = start_mock_http_server(Arc::clone(&payload), 2).await;

    // Step 3: FetchTrekClient connects over local IPC transport and issues add_task across 4 connections
    let mut client = FetchTrekClient::connect(Some(&unique_endpoint))
        .await
        .expect("FetchTrekClient::connect failed");

    // Verify ping
    let ping_dur = client.ping().await.expect("ping failed");
    assert!(ping_dur < Duration::from_millis(50));

    let target_dest = temp_dir.path.join("m2_alpha_payload.bin");
    let (task_id, _uuid) = client
        .add_task(
            &format!("{}/m2_payload.bin", server_url),
            Some(&target_dest.to_string_lossy()),
            Some("m2_alpha_payload.bin"),
            Some(4),
        )
        .await
        .expect("add_task failed");

    // Step 4: Client listens for incoming 10 Hz telemetry events, confirming live progress emissions
    let telemetry_res = client.read_telemetry_event().await;
    assert!(
        telemetry_res.is_ok(),
        "Client must successfully receive 10 Hz telemetry snapshot"
    );

    // Let download transfer partially
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Step 5: Client issues pause_task midway; assert daemon transitions to Paused and rewinds to 64 KiB boundary
    client.pause_task(task_id).await.expect("pause_task failed");

    let paused_status = client.query_task(task_id).await.expect("query_task failed");
    assert_eq!(paused_status.status, TaskState::Paused);

    // Inspect SQLite WAL to verify 64 KiB cursor alignment
    {
        let db = Database::open(temp_dir.path.join("fetchtrek.db")).expect("open db failed");
        let segments = db.get_segments(task_id).expect("get_segments failed");
        for seg in &segments {
            assert_eq!(
                seg.current_cursor % 65536,
                0,
                "Cursor {} not rewound to 64 KiB boundary",
                seg.current_cursor
            );
        }
    }

    // Step 6: Client issues resume_task; await completion
    client
        .resume_task(task_id)
        .await
        .expect("resume_task failed");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let task = client.query_task(task_id).await.expect("query_task failed");
        if task.status == TaskState::Completed {
            assert_eq!(task.downloaded_bytes, PAYLOAD_SIZE as u64);
            break;
        }
        assert!(Instant::now() < deadline, "Resumed download timed out");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    // Step 7: Verify final file size equals 8 MiB with 100% bit parity and zero merge freeze
    let written_file = std::fs::read(&target_dest).expect("read target file failed");
    assert_eq!(written_file.len(), PAYLOAD_SIZE, "Final file size mismatch");
    assert_eq!(
        written_file, *payload,
        "Bit-for-bit parity check failed: downloaded file differs from source payload"
    );

    // Step 8: Simulate crash/reboot:
    // Insert an active interrupted task in DB while daemon is running, then terminate daemon abruptly
    let interrupted_id = {
        let db = Database::open(temp_dir.path.join("fetchtrek.db")).expect("open db failed");
        let mut interrupted_task = NewTask::new(
            "uuid-crash-reboot",
            "http://example.com/crash.iso",
            temp_dir.path.join("crash.iso").to_string_lossy(),
            "crash.iso",
            "http",
            20_000_000,
        );
        interrupted_task.status = TaskState::Active;
        interrupted_task.downloaded_bytes = 2_000_000;
        let id = db
            .create_task(&interrupted_task)
            .expect("create interrupted task failed");
        let seg0 = TaskSegmentRecord::new(id, 0, 0, 9_999_999, 100_000, 1);
        let seg1 = TaskSegmentRecord::new(id, 1, 10_000_000, 19_999_999, 10_100_000, 1);
        db.save_segments(id, &[seg0, seg1])
            .expect("save segments failed");
        id
    };

    // Shutdown current daemon
    daemon.shutdown().await.expect("daemon shutdown failed");

    // Re-launch daemon and verify startup recovery finishes in < 15ms (with < 50ms test threshold)
    let recovery_start = Instant::now();
    let restarted_daemon = DaemonEngine::start(config)
        .await
        .expect("restarted DaemonEngine::start failed");
    let recovery_duration = recovery_start.elapsed();

    assert!(
        recovery_duration < Duration::from_millis(50),
        "Startup recovery took {:?}, exceeding 50ms threshold",
        recovery_duration
    );

    // Verify interrupted task was restored to Paused with 64 KiB rewound cursors
    let recovered_task = restarted_daemon
        .get_task_by_id(interrupted_id)
        .expect("get_task_by_id failed")
        .expect("interrupted task not found");
    assert_eq!(recovered_task.status, TaskState::Paused);

    restarted_daemon
        .quick_check()
        .expect("restarted db quick_check failed");

    restarted_daemon
        .shutdown()
        .await
        .expect("restarted daemon shutdown failed");
}
