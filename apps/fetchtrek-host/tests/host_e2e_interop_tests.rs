use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fetchtrek_daemon::{DaemonConfig, DaemonEngine};
use fetchtrek_host::framing::{read_native_message, write_native_message};
use fetchtrek_ipc::endpoint_with_name;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::process::Command;
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
            "fetchtrek_interop_{}_{}_{}",
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

async fn start_mock_http_server(data: Arc<Vec<u8>>) -> (String, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock server");
    let port = listener.local_addr().expect("local addr").port();
    let url = format!("http://127.0.0.1:{}/asset.bin", port);
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                res = listener.accept() => {
                    let (mut socket, _) = match res {
                        Ok(conn) => conn,
                        Err(_) => break,
                    };
                    let payload = Arc::clone(&data);

                    tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        let n = socket.read(&mut buf).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        let req_str = String::from_utf8_lossy(&buf[..n]);
                        let first_line = req_str.lines().next().unwrap_or("");
                        let method = first_line.split_whitespace().next().unwrap_or("");

                        if method == "HEAD" {
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"interop-123\"\r\nConnection: close\r\n\r\n",
                                payload.len()
                            );
                            let _ = socket.write_all(resp.as_bytes()).await;
                            let _ = socket.shutdown().await;
                        } else if method == "GET" {
                            let resp = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-{}/{}\r\nContent-Length: {}\r\nETag: \"interop-123\"\r\nConnection: close\r\n\r\n",
                                payload.len() - 1,
                                payload.len(),
                                payload.len()
                            );
                            let _ = socket.write_all(resp.as_bytes()).await;
                            let _ = socket.write_all(&payload).await;
                            let _ = socket.shutdown().await;
                        }
                    });
                }
            }
        }
    });

    (url, shutdown_tx)
}

#[tokio::test]
async fn test_host_stdio_ipc_daemon_interop() {
    let db_dir = TempDirGuard::new("db");
    let mock_data = Arc::new(vec![0xAAu8; 64 * 1024]);
    let (mock_url, server_shutdown) = start_mock_http_server(mock_data).await;

    let unique_endpoint = endpoint_with_name(&format!(
        "ft_interop_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    // 1. Start daemon engine
    let config = DaemonConfig::new(&db_dir.path).with_endpoint(&unique_endpoint);
    let daemon = DaemonEngine::start(config)
        .await
        .expect("DaemonEngine start");

    // 2. Launch fetchtrek-host binary with stdio pipes
    let host_exe = env!("CARGO_BIN_EXE_fetchtrek-host");
    let mut child = Command::new(host_exe)
        .env("FETCHTREK_IPC_ENDPOINT", &unique_endpoint)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn fetchtrek-host");

    let mut stdin = child.stdin.take().expect("child stdin");
    let mut stdout = child.stdout.take().expect("child stdout");

    // 3. Test Ping message via stdio
    let ping_req = r#"{"action":"ping"}"#;
    write_native_message(&mut stdin, ping_req)
        .await
        .expect("write ping");
    let pong_resp_str = read_native_message(&mut stdout).await.expect("read pong");
    let pong_json: serde_json::Value =
        serde_json::from_str(&pong_resp_str).expect("parse pong json");
    assert_eq!(pong_json["status"], "ok");
    assert_eq!(pong_json["action"], "pong");

    // 4. Test AddDownload (alias "add_download" to InterceptDownload) via stdio
    let add_req = format!(
        r#"{{"action":"add_download","url":"{}","filename":"interop_asset.bin"}}"#,
        mock_url
    );
    write_native_message(&mut stdin, &add_req)
        .await
        .expect("write add_download");
    let add_resp_str = read_native_message(&mut stdout)
        .await
        .expect("read add_download response");
    let add_json: serde_json::Value =
        serde_json::from_str(&add_resp_str).expect("parse add_download json");

    assert_eq!(add_json["status"], "ok");
    assert_eq!(add_json["action"], "task_created");
    assert_eq!(add_json["success"], true);
    assert!(
        add_json["task_id"].as_i64().is_some() && add_json["task_id"].as_i64().unwrap() > 0,
        "Expected positive task_id, got {:?}",
        add_json["task_id"]
    );
    assert!(
        add_json["uuid"].as_str().is_some() && !add_json["uuid"].as_str().unwrap().is_empty(),
        "Expected non-empty uuid, got {:?}",
        add_json["uuid"]
    );

    // 5. Clean close: drop stdin to trigger EOF in stdio loop
    drop(stdin);
    let exit_status = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .expect("host exit timeout")
        .expect("host exit status");
    assert!(
        exit_status.success(),
        "fetchtrek-host exited with error: {:?}",
        exit_status
    );

    // 6. Teardown
    let _ = server_shutdown.send(());
    let _ = daemon.shutdown().await;
}
