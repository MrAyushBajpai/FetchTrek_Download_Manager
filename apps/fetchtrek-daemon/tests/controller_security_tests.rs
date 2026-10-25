use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fetchtrek_core::BandwidthLimiter;
use fetchtrek_daemon::{
    is_windows_reserved_device_name, task_staging_path, validate_destination_path,
    validate_download_url, validate_filename, TaskController,
};
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
            "fetchtrek_sec_test_{}_{}_{}",
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

/// Spawns a server whose HEAD succeeds but GET fails immediately.
async fn start_failing_mock_server() -> (String, oneshot::Sender<()>) {
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
                            let resp = "HTTP/1.1 200 OK\r\nContent-Length: 4194304\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n";
                            let _ = socket.write_all(resp.as_bytes()).await;
                            let _ = socket.shutdown().await;
                        } else {
                            // Drop connection / fail immediately on GET
                            let resp = "HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n\r\n";
                            let _ = socket.write_all(resp.as_bytes()).await;
                            let _ = socket.shutdown().await;
                        }
                    });
                }
            }
        }
    });

    (server_url, shutdown_tx)
}

/// Spawns a slow mock server that serves chunks with artificial delay.
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
                                "HTTP/1.1 200 OK\r\nContent-Length: {total_size}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
                            );
                            let _ = socket.write_all(resp.as_bytes()).await;
                            let _ = socket.shutdown().await;
                        } else if method == "GET" {
                            let header = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-{}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                total_size - 1, total_size, total_size
                            );
                            let _ = socket.write_all(header.as_bytes()).await;
                            let chunk = vec![0xAB; 64 * 1024];
                            for _ in 0..(total_size / (64 * 1024)) {
                                if socket.write_all(&chunk).await.is_err() {
                                    break;
                                }
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
async fn test_preexisting_file_preserved_on_failure() {
    let (server_url, _stop_server) = start_failing_mock_server().await;
    let temp_dir = TempDirGuard::new("preserve_preexisting");
    let controller = create_test_controller(&temp_dir, 4);

    let destination_path = temp_dir.path.join("user_existing_document.pdf");
    let sentinel_data = b"SENTINEL_USER_FILE_DATA_DO_NOT_CORRUPT_2026";
    std::fs::write(&destination_path, sentinel_data).expect("write sentinel file");

    let dest_str = destination_path.to_str().unwrap();
    let download_url = format!("{server_url}/failing.pdf");

    // Add task pointing to pre-existing user file destination
    let record = controller
        .add_task(
            &download_url,
            Some(dest_str),
            Some("user_existing_document.pdf"),
            Some(2),
            None,
        )
        .await
        .expect("add_task should succeed");

    let task_id = record.id;
    let staging_file = task_staging_path(&destination_path, task_id);

    // Wait up to 2 seconds for task to fail
    let start_wait = std::time::Instant::now();
    loop {
        let status = {
            let db = controller.database.lock().unwrap();
            db.get_task_by_id(task_id).unwrap().unwrap().status
        };
        if status == TaskState::Failed {
            break;
        }
        if start_wait.elapsed() > Duration::from_secs(2) {
            panic!("Task did not transition to Failed within 2s, current: {status:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // 1. Assert pre-existing destination file is strictly preserved and identical
    let actual_content = std::fs::read(&destination_path).expect("read pre-existing file");
    assert_eq!(
        actual_content, sentinel_data,
        "Pre-existing user file MUST remain untouched on download failure"
    );

    // 2. Assert staging file was cleaned up on failure
    assert!(
        !staging_file.exists(),
        "Staging file (.fetchtrek_temp_*) MUST be deleted on failure"
    );
}

#[tokio::test]
async fn test_daemon_input_validation() {
    let temp_dir = TempDirGuard::new("input_validation");
    let controller = create_test_controller(&temp_dir, 4);

    // 1. Reject invalid URL schemes (FT-QA-012)
    assert!(validate_download_url("file:///etc/shadow").is_err());
    assert!(validate_download_url("ftp://example.com/file.zip").is_err());
    assert!(validate_download_url("gopher://old.net").is_err());
    assert!(validate_download_url("javascript:alert(1)").is_err());
    assert!(validate_download_url("http://example.com/file.zip").is_ok());
    assert!(validate_download_url("https://example.com/file.zip").is_ok());
    assert!(
        validate_download_url("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567")
            .is_ok()
    );

    assert!(controller
        .add_task("file:///etc/shadow", None, None, None, None)
        .await
        .is_err());
    assert!(controller
        .add_task("ftp://example.com/file.zip", None, None, None, None)
        .await
        .is_err());
    assert!(controller
        .add_task("gopher://old.net", None, None, None, None)
        .await
        .is_err());
    assert!(controller
        .add_task("javascript:alert(1)", None, None, None, None)
        .await
        .is_err());

    // 2. Reject Windows reserved device names
    assert!(is_windows_reserved_device_name("CON"));
    assert!(is_windows_reserved_device_name("con.txt"));
    assert!(is_windows_reserved_device_name("PRN"));
    assert!(is_windows_reserved_device_name("prn.dat"));
    assert!(is_windows_reserved_device_name("AUX"));
    assert!(is_windows_reserved_device_name("aux.bin"));
    assert!(is_windows_reserved_device_name("NUL"));
    assert!(is_windows_reserved_device_name("nul"));
    assert!(is_windows_reserved_device_name("COM1"));
    assert!(is_windows_reserved_device_name("com9.tar.gz"));
    assert!(is_windows_reserved_device_name("LPT1"));
    assert!(is_windows_reserved_device_name("lpt5.log"));
    assert!(!is_windows_reserved_device_name("normal_file.txt"));

    assert!(validate_filename("CON.txt").is_err());
    assert!(validate_filename("nul").is_err());
    assert!(validate_filename("aux.bin").is_err());
    assert!(validate_filename("COM1.log").is_err());
    assert!(validate_filename("LPT9.dat").is_err());

    // 3. Reject directory traversal sequences and control characters
    assert!(validate_filename("../escape.txt").is_err());
    assert!(validate_filename("sub/file.txt").is_err());
    assert!(validate_filename("sub\\file.txt").is_err());
    assert!(validate_filename("null\0byte.txt").is_err());
    assert!(validate_filename("ctrl\x07bell.txt").is_err());
    assert!(validate_destination_path(&PathBuf::from("/safe/../escape")).is_err());

    // 4. Verify connection bounds clamping [1, 32]
    let (server_url, _stop) = start_failing_mock_server().await;

    // > 32 clamped to 32
    let task_clamped_high = controller
        .add_task(
            &format!("{server_url}/item1.bin"),
            None,
            Some("item1.bin"),
            Some(128),
            None,
        )
        .await
        .expect("add task with 128 connections");
    let segments_high = controller
        .database
        .lock()
        .unwrap()
        .get_segments(task_clamped_high.id)
        .unwrap();
    assert_eq!(
        segments_high.len(),
        32,
        "Connections > 32 must be clamped to 32"
    );

    // < 1 clamped to 1
    let task_clamped_low = controller
        .add_task(
            &format!("{server_url}/item2.bin"),
            None,
            Some("item2.bin"),
            Some(0),
            None,
        )
        .await
        .expect("add task with 0 connections");
    let segments_low = controller
        .database
        .lock()
        .unwrap()
        .get_segments(task_clamped_low.id)
        .unwrap();
    assert_eq!(
        segments_low.len(),
        1,
        "Connections < 1 must be clamped to 1"
    );
}

#[tokio::test]
async fn test_cancel_joins_workers_before_cleanup() {
    let (server_url, _stop) = start_slow_mock_server().await;
    let temp_dir = TempDirGuard::new("cancel_join");
    let controller = create_test_controller(&temp_dir, 4);

    let destination_path = temp_dir.path.join("slow_download.bin");
    let dest_str = destination_path.to_str().unwrap();

    let record = controller
        .add_task(
            &format!("{server_url}/slow_download.bin"),
            Some(dest_str),
            Some("slow_download.bin"),
            Some(4),
            None,
        )
        .await
        .expect("add task");

    let task_id = record.id;
    let staging_file = task_staging_path(&destination_path, task_id);

    // Allow workers to connect and begin streaming
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Verify session is active
    {
        let sessions = controller.active_sessions.read().await;
        assert!(sessions.contains_key(&task_id));
    }

    // Cancel task with delete_file: true
    controller
        .cancel_task(task_id, true)
        .await
        .expect("cancel task should succeed");

    // 1. Verify session was removed and all workers joined cleanly
    {
        let sessions = controller.active_sessions.read().await;
        assert!(
            !sessions.contains_key(&task_id),
            "Active session must be removed"
        );
    }

    // 2. Verify staging file is deleted
    assert!(
        !staging_file.exists(),
        "Staging file must be cleaned up on cancel"
    );

    // 3. Verify task status is Cancelled in database
    let db_status = {
        let db = controller.database.lock().unwrap();
        db.get_task_by_id(task_id).unwrap().unwrap().status
    };
    assert_eq!(db_status, TaskState::Cancelled);

    // 4. Ensure no lingering worker writes recreate the staging file
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !staging_file.exists(),
        "No dangling worker write may recreate staging file after cancel"
    );
}
