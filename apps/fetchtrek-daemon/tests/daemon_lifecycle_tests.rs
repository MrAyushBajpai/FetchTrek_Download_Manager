use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_daemon::{DaemonConfig, DaemonEngine};
use fetchtrek_ipc::{
    decode_response, encode_request, endpoint_with_name, IpcClient, IpcRequest, IpcResponse,
    MessageType,
};
use fetchtrek_persistence::{Database, NewTask, TaskSegmentRecord};
use fetchtrek_types::TaskState;

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
            "fetchtrek_daemon_test_{}_{}_{}",
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

#[tokio::test]
async fn test_daemon_startup_and_recovery() {
    let temp_dir = TempDirGuard::new("startup_recovery");
    let db_path = temp_dir.path.join("fetchtrek.db");

    // 1. Seed database with active interrupted tasks prior to daemon start
    let task_id = {
        let db = Database::open(&db_path).expect("open test database failed");

        let mut new_task = NewTask::new(
            "uuid-daemon-rec-001",
            "https://example.com/test.iso",
            temp_dir.path.join("test.iso").to_string_lossy(),
            "test.iso",
            "https",
            10_000_000,
        );
        new_task.status = TaskState::Transferring;
        new_task.downloaded_bytes = 1_000_000;

        let id = db.create_task(&new_task).expect("create_task failed");
        let seg0 = TaskSegmentRecord::new(id, 0, 0, 4_999_999, 100_000, 1);
        let seg1 = TaskSegmentRecord::new(id, 1, 5_000_000, 9_999_999, 5_100_000, 1);
        db.save_segments(id, &[seg0, seg1])
            .expect("save_segments failed");

        id
        // Drop db abruptly simulating daemon kill/crash
    };

    // 2. Boot DaemonEngine and measure startup latency
    let unique_endpoint = endpoint_with_name(&format!(
        "ft_daemon_test_rec_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let config = DaemonConfig::new(&temp_dir.path).with_endpoint(unique_endpoint);

    let start_instant = Instant::now();
    let engine = DaemonEngine::start(config)
        .await
        .expect("DaemonEngine::start failed");
    let startup_elapsed = start_instant.elapsed();

    // Verify startup completes in < 500ms
    assert!(
        startup_elapsed < Duration::from_millis(500),
        "Daemon startup took {:?}, exceeding 500ms threshold",
        startup_elapsed
    );

    // 3. Verify task is restored to TaskState::Paused
    let task = engine
        .get_task_by_id(task_id)
        .expect("query task failed")
        .expect("task not found");
    assert_eq!(task.status, TaskState::Paused);

    // 4. Verify database passes quick_check
    engine
        .quick_check()
        .expect("database quick_check failed after daemon startup");

    // Clean up
    engine.shutdown().await.expect("daemon shutdown failed");
}

#[tokio::test]
async fn test_daemon_ipc_ping_pong_loopback() {
    let temp_dir = TempDirGuard::new("ping_pong");
    let unique_endpoint = endpoint_with_name(&format!(
        "ft_daemon_test_ping_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let config = DaemonConfig::new(&temp_dir.path).with_endpoint(&unique_endpoint);
    let engine = DaemonEngine::start(config)
        .await
        .expect("DaemonEngine::start failed");

    // Connect client
    let mut client = IpcClient::connect(&unique_endpoint)
        .await
        .expect("IpcClient::connect failed");

    // Send Ping request and measure response latency
    let correlation_id = 999;
    let request_frame =
        encode_request(&IpcRequest::Ping, correlation_id).expect("encode ping request failed");

    let ping_start = Instant::now();
    client
        .write_frame(&request_frame)
        .await
        .expect("write ping frame failed");

    // Read until response frame is received (filter out any 10 Hz telemetry events)
    let response_frame = tokio::time::timeout(Duration::from_millis(2000), async {
        loop {
            let frame = client
                .read_frame()
                .await
                .expect("read response frame failed");
            if frame.header.message_type == MessageType::Response.as_u8() {
                break frame;
            }
        }
    })
    .await
    .expect("ping response timed out");
    let ping_elapsed = ping_start.elapsed();

    let (response, resp_corr) = decode_response(&response_frame).expect("decode response failed");
    assert_eq!(response, IpcResponse::Pong);
    assert_eq!(resp_corr, correlation_id);

    // Latency invariant: < 100ms (generous margin over local loopback < 1ms)
    assert!(
        ping_elapsed < Duration::from_millis(100),
        "Ping-pong round trip took {:?}, exceeding 100ms threshold",
        ping_elapsed
    );

    engine.shutdown().await.expect("daemon shutdown failed");
}

#[tokio::test]
async fn test_daemon_graceful_shutdown() {
    let temp_dir = TempDirGuard::new("graceful_shutdown");
    let unique_endpoint = endpoint_with_name(&format!(
        "ft_daemon_test_shut_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let config = DaemonConfig::new(&temp_dir.path).with_endpoint(&unique_endpoint);
    let engine = DaemonEngine::start(config)
        .await
        .expect("DaemonEngine::start failed");

    // Connect an active client
    let mut client = IpcClient::connect(&unique_endpoint)
        .await
        .expect("IpcClient::connect failed");

    // Initiate graceful shutdown and measure elapsed time
    let shutdown_start = Instant::now();
    engine.shutdown().await.expect("engine.shutdown failed");
    let shutdown_elapsed = shutdown_start.elapsed();

    // Verify shutdown completes in < 500ms
    assert!(
        shutdown_elapsed < Duration::from_millis(500),
        "Daemon shutdown took {:?}, exceeding 500ms threshold",
        shutdown_elapsed
    );

    // Client connection should read EOF / connection closed
    let client_read = tokio::time::timeout(Duration::from_millis(1000), client.read_frame()).await;
    assert!(
        client_read.is_err() || client_read.unwrap().is_err(),
        "Client should encounter disconnected stream upon daemon shutdown"
    );

    // Database WAL should be cleanly checkpointed; reopening succeeds and passes quick_check
    let reopened_db = Database::open(temp_dir.path.join("fetchtrek.db")).expect("reopen db failed");
    reopened_db
        .quick_check()
        .expect("quick_check failed on post-shutdown database");
}
