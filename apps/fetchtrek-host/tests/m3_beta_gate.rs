use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_daemon::{DaemonConfig, DaemonEngine};
use fetchtrek_host::framing::{read_native_message, write_native_message};
use fetchtrek_host::manifest::{
    create_chromium_manifest, create_firefox_manifest, ChromiumHostManifest, FirefoxHostManifest,
    HOST_DESCRIPTION, HOST_NAME, HOST_TYPE,
};
use fetchtrek_host::protocol::{BrowserRequest, HostResponse};
use fetchtrek_host::registration::{
    check_manifest_in_dir, register_manifest_in_dir, unregister_manifest_in_dir,
};
use fetchtrek_host::relay::HostRelay;
use fetchtrek_ipc::endpoint_with_name;
use fetchtrek_persistence::Database;
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
            "fetchtrek_m3_gate_{}_{}_{}",
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

fn sha256(data: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg = data.to_vec();
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0x00);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[4 * i],
                chunk[4 * i + 1],
                chunk[4 * i + 2],
                chunk[4 * i + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut h_val = h[7];

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h_val
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            h_val = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(h_val);
    }

    let mut digest = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        digest[4 * i..4 * i + 4].copy_from_slice(&word.to_be_bytes());
    }
    digest
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

struct MockServerStats {
    head_requests: Arc<AtomicUsize>,
    get_requests: Arc<AtomicUsize>,
    unauthorized_attempts: Arc<AtomicUsize>,
}

async fn start_protected_mock_http_server(
    payload: Arc<Vec<u8>>,
) -> (String, oneshot::Sender<()>, Arc<MockServerStats>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock server");
    let addr = listener.local_addr().expect("local addr");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let stats = Arc::new(MockServerStats {
        head_requests: Arc::new(AtomicUsize::new(0)),
        get_requests: Arc::new(AtomicUsize::new(0)),
        unauthorized_attempts: Arc::new(AtomicUsize::new(0)),
    });

    let server_url = format!("http://127.0.0.1:{}", addr.port());
    let stats_clone = Arc::clone(&stats);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                Ok((mut socket, _)) = listener.accept() => {
                    let payload = Arc::clone(&payload);
                    let stats = Arc::clone(&stats_clone);

                    tokio::spawn(async move {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};

                        let mut buf = Vec::with_capacity(4096);
                        let mut temp = [0u8; 1024];

                        // Read full HTTP request headers
                        loop {
                            let n = match socket.read(&mut temp).await {
                                Ok(n) if n > 0 => n,
                                _ => break,
                            };
                            buf.extend_from_slice(&temp[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() > 16384 {
                                break;
                            }
                        }

                        let req = String::from_utf8_lossy(&buf);
                        let first_line = req.lines().next().unwrap_or("");
                        let parts: Vec<&str> = first_line.split_whitespace().collect();
                        if parts.is_empty() {
                            return;
                        }
                        let method = parts[0];

                        // Header extraction
                        let mut cookie_val = None;
                        let mut user_agent_val = None;
                        let mut referer_val = None;
                        let mut range_val = None;

                        for line in req.lines() {
                            let lower = line.to_ascii_lowercase();
                            if lower.starts_with("cookie:") {
                                cookie_val = Some(line["cookie:".len()..].trim().to_string());
                            } else if lower.starts_with("user-agent:") {
                                user_agent_val = Some(line["user-agent:".len()..].trim().to_string());
                            } else if lower.starts_with("referer:") {
                                referer_val = Some(line["referer:".len()..].trim().to_string());
                            } else if lower.starts_with("range: bytes=") {
                                range_val = Some(line["range: bytes=".len()..].trim().to_string());
                            }
                        }

                        // Strict Auth Verification
                        let has_cookies = cookie_val
                            .as_ref()
                            .map(|c| c.contains("session_token=test_session_xyz987") && c.contains("cf_clearance=cf_pass_valid"))
                            .unwrap_or(false);
                        let has_ua = user_agent_val.as_deref() == Some("FetchTrek-Test-Browser/1.0");
                        let has_ref = referer_val.as_deref() == Some("https://fetchtrek.org/download");

                        if !has_cookies || !has_ua || !has_ref {
                            stats.unauthorized_attempts.fetch_add(1, Ordering::SeqCst);
                            let err_resp = b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                            let _ = socket.write_all(err_resp).await;
                            let _ = socket.shutdown().await;
                            return;
                        }

                        if method == "HEAD" {
                            stats.head_requests.fetch_add(1, Ordering::SeqCst);
                            let head_resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"m3-beta-etag\"\r\nConnection: close\r\n\r\n",
                                payload.len()
                            );
                            let _ = socket.write_all(head_resp.as_bytes()).await;
                            let _ = socket.shutdown().await;
                            return;
                        }

                        if method == "GET" {
                            stats.get_requests.fetch_add(1, Ordering::SeqCst);
                            let mut start = 0usize;
                            let mut end = payload.len() - 1;

                            if let Some(r) = range_val {
                                let range_parts: Vec<&str> = r.split('-').collect();
                                if let Ok(s) = range_parts[0].trim().parse::<usize>() {
                                    start = s;
                                }
                                if range_parts.len() > 1 && !range_parts[1].trim().is_empty() {
                                    if let Ok(e) = range_parts[1].trim().parse::<usize>() {
                                        end = e;
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
                            let _ = socket.write_all(&payload[start..=end]).await;
                            let _ = socket.shutdown().await;
                        }
                    });
                }
            }
        }
    });

    (server_url, shutdown_tx, stats)
}

#[tokio::test]
async fn test_milestone_m3_beta_exit_gate() {
    // -------------------------------------------------------------------------
    // Phase 1: Environment & Daemon Initialization
    // -------------------------------------------------------------------------
    println!("\n=== [Phase 1] Initializing isolated test environment & Daemon ===");

    const PAYLOAD_SIZE: usize = 8 * 1024 * 1024; // 8 MiB deterministic test payload
    let payload: Arc<Vec<u8>> = Arc::new(
        (0..PAYLOAD_SIZE)
            .map(|i| (((i.wrapping_mul(43)).wrapping_add(29)) & 0xFF) as u8)
            .collect(),
    );

    let expected_digest = sha256(&payload);
    let expected_hex = to_hex(&expected_digest);
    println!("Payload Size: {} bytes (8 MiB)", PAYLOAD_SIZE);
    println!("Expected SHA-256: {}", expected_hex);

    let db_temp_dir = TempDirGuard::new("m3_db");
    let dl_temp_dir = TempDirGuard::new("m3_dl");

    let (server_url, server_shutdown_tx, server_stats) =
        start_protected_mock_http_server(Arc::clone(&payload)).await;
    println!("Protected HTTP Mock Server listening at: {}", server_url);

    let unique_endpoint = endpoint_with_name(&format!(
        "ft_m3_gate_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let config = DaemonConfig::new(&db_temp_dir.path).with_endpoint(&unique_endpoint);
    let daemon = DaemonEngine::start(config)
        .await
        .expect("DaemonEngine::start failed");

    let mut relay = HostRelay::new(unique_endpoint.clone());
    let ping_duration = relay.ping_daemon().await.expect("ping daemon failed");
    println!(
        "Daemon online and responding to ping in {:?}",
        ping_duration
    );
    assert!(
        ping_duration < Duration::from_millis(100),
        "Daemon ping latency exceeded 100ms threshold"
    );

    // -------------------------------------------------------------------------
    // Phase 2: Browser Download Ingestion via Stdio Native Framing
    // -------------------------------------------------------------------------
    println!("\n=== [Phase 2] Browser Download Ingestion via Stdio Native Framing ===");

    let target_dest = dl_temp_dir.path.join("m3_verified_artifact.bin");
    let mut headers_map = HashMap::new();
    headers_map.insert(
        "Referer".to_string(),
        "https://fetchtrek.org/download".to_string(),
    );
    headers_map.insert(
        "User-Agent".to_string(),
        "FetchTrek-Test-Browser/1.0".to_string(),
    );

    let browser_intercept_req = BrowserRequest::InterceptDownload {
        url: format!("{}/m3_artifact.bin", server_url),
        filename: Some("m3_verified_artifact.bin".to_string()),
        destination_path: Some(target_dest.to_string_lossy().to_string()),
        headers: Some(headers_map),
        cookies: Some("session_token=test_session_xyz987; cf_clearance=cf_pass_valid".to_string()),
        user_agent: Some("FetchTrek-Test-Browser/1.0".to_string()),
        referer: Some("https://fetchtrek.org/download".to_string()),
        connections: Some(4),
    };

    // Serialize to JSON and encode with 32-bit LE length prefix
    let raw_json =
        serde_json::to_string(&browser_intercept_req).expect("serialize browser request");
    let mut framed_stream = Vec::new();
    write_native_message(&mut framed_stream, &raw_json)
        .await
        .expect("write_native_message failed");

    // Decode framed bytes using native message reader
    let mut cursor = std::io::Cursor::new(framed_stream);
    let decoded_json = read_native_message(&mut cursor)
        .await
        .expect("read_native_message failed");
    assert_eq!(decoded_json, raw_json);

    let decoded_req: BrowserRequest =
        serde_json::from_str(&decoded_json).expect("deserialize browser request");
    let host_resp = relay.dispatch(decoded_req).await;

    // Verify TaskCreated response
    let (task_id, uuid) = match host_resp {
        HostResponse::TaskCreated {
            status,
            action,
            task_id,
            uuid,
        } => {
            assert_eq!(status, "ok");
            assert_eq!(action, "task_created");
            assert!(task_id > 0);
            assert!(!uuid.is_empty());
            (task_id, uuid)
        }
        other => panic!("Expected TaskCreated response, got: {:?}", other),
    };
    println!(
        "Task successfully ingested: task_id={}, uuid={}",
        task_id, uuid
    );

    // -------------------------------------------------------------------------
    // Phase 3: Concurrent Multi-Socket Download & Progress Telemetry
    // -------------------------------------------------------------------------
    println!("\n=== [Phase 3] Concurrent Multi-Socket Download & Progress Telemetry ===");

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut saw_progress = false;
    let mut completed = false;

    while Instant::now() < deadline {
        let query_req = BrowserRequest::QueryStatus { task_id };
        let resp = relay.dispatch(query_req).await;

        match resp {
            HostResponse::TaskStatus {
                status,
                action,
                task,
            } => {
                assert_eq!(status, "ok");
                assert_eq!(action, "task_status");
                assert_ne!(
                    task.status,
                    TaskState::Failed,
                    "Task unexpectedly failed during transfer"
                );

                if task.downloaded_bytes > 0 {
                    saw_progress = true;
                }

                if task.status == TaskState::Completed {
                    assert_eq!(
                        task.downloaded_bytes, PAYLOAD_SIZE as u64,
                        "Completed task downloaded bytes mismatch"
                    );
                    completed = true;
                    break;
                }
            }
            other => panic!("Expected TaskStatus response, got: {:?}", other),
        }

        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    assert!(
        saw_progress,
        "Native status query should have reported non-zero progress"
    );
    assert!(
        completed,
        "Download timed out before reaching Completed state"
    );
    println!("Task reached Completed state successfully.");

    // -------------------------------------------------------------------------
    // Phase 4: Bit Parity, Zero-Merge Invariant & File Verification
    // -------------------------------------------------------------------------
    println!("\n=== [Phase 4] Bit Parity, Zero-Merge Invariant & File Verification ===");

    assert!(
        target_dest.exists(),
        "Downloaded file must exist on disk at {:?}",
        target_dest
    );

    let written_bytes = std::fs::read(&target_dest).expect("read downloaded file failed");
    assert_eq!(
        written_bytes.len(),
        PAYLOAD_SIZE,
        "Downloaded file byte count mismatch"
    );

    let actual_digest = sha256(&written_bytes);
    let actual_hex = to_hex(&actual_digest);
    println!("Downloaded SHA-256: {}", actual_hex);

    assert_eq!(
        actual_digest, expected_digest,
        "Exact SHA-256 digest match failed! Expected {}, got {}",
        expected_hex, actual_hex
    );
    assert_eq!(
        written_bytes, *payload,
        "100% bit-for-bit parity check failed"
    );

    // Auth and concurrency assertions
    let unauthorized_count = server_stats.unauthorized_attempts.load(Ordering::SeqCst);
    let get_count = server_stats.get_requests.load(Ordering::SeqCst);
    let head_count = server_stats.head_requests.load(Ordering::SeqCst);

    println!(
        "Server Request Stats: HEAD={}, GET={}, 403 Forbidden={}",
        head_count, get_count, unauthorized_count
    );
    assert_eq!(
        unauthorized_count, 0,
        "Strict auth failed! Mock server responded with 403 Forbidden"
    );
    assert!(
        get_count >= 4,
        "Download must have leveraged at least 4 parallel range streams, got: {}",
        get_count
    );

    // -------------------------------------------------------------------------
    // Phase 5: Native Host Discovery & OS Registration Lifecycle Gate
    // -------------------------------------------------------------------------
    println!("\n=== [Phase 5] Native Host Discovery & OS Registration Lifecycle Gate ===");

    let reg_temp = TempDirGuard::new("m3_reg");
    let mock_host_exe = reg_temp.path.join("fetchtrek-host.exe");
    std::fs::write(&mock_host_exe, b"mock-host-binary").expect("create mock host exe");

    // Chromium schema
    let chrome_manifest = create_chromium_manifest(
        &mock_host_exe,
        &["chrome-extension://abcdefghijklmnopqrstuvwxyz/".to_string()],
    );
    assert_eq!(chrome_manifest.name, HOST_NAME);
    assert_eq!(chrome_manifest.description, HOST_DESCRIPTION);
    assert_eq!(chrome_manifest.type_, HOST_TYPE);
    assert_eq!(
        chrome_manifest.allowed_origins,
        vec!["chrome-extension://abcdefghijklmnopqrstuvwxyz/"]
    );

    let chrome_json =
        serde_json::to_string_pretty(&chrome_manifest).expect("serialize chrome manifest");
    let parsed_chrome: ChromiumHostManifest =
        serde_json::from_str(&chrome_json).expect("deserialize chrome manifest");
    assert_eq!(chrome_manifest, parsed_chrome);

    // Firefox schema
    let firefox_manifest =
        create_firefox_manifest(&mock_host_exe, &["fetchtrek@fetchtrek.org".to_string()]);
    assert_eq!(firefox_manifest.name, HOST_NAME);
    assert_eq!(firefox_manifest.type_, HOST_TYPE);
    assert_eq!(
        firefox_manifest.allowed_extensions,
        vec!["fetchtrek@fetchtrek.org"]
    );

    let firefox_json =
        serde_json::to_string_pretty(&firefox_manifest).expect("serialize firefox manifest");
    let parsed_firefox: FirefoxHostManifest =
        serde_json::from_str(&firefox_json).expect("deserialize firefox manifest");
    assert_eq!(firefox_manifest, parsed_firefox);

    // Lifecycle registration & validation
    let manifest_dir = reg_temp.path.join("manifests");
    let registered_path = register_manifest_in_dir(
        &manifest_dir,
        "com.fetchtrek.downloadmanager.json",
        &chrome_json,
    )
    .expect("register_manifest_in_dir failed");

    let (registered, path_opt, valid) =
        check_manifest_in_dir(&manifest_dir, "com.fetchtrek.downloadmanager.json");
    assert!(registered);
    assert_eq!(path_opt, Some(registered_path));
    assert!(valid, "Manifest must be valid with existing binary");

    let removed = unregister_manifest_in_dir(&manifest_dir, "com.fetchtrek.downloadmanager.json")
        .expect("unregister_manifest_in_dir failed");
    assert!(removed);

    let (registered_after, _, valid_after) =
        check_manifest_in_dir(&manifest_dir, "com.fetchtrek.downloadmanager.json");
    assert!(!registered_after);
    assert!(!valid_after);
    println!("Registration manifest lifecycle verified.");

    // -------------------------------------------------------------------------
    // Phase 6: Clean Shutdown & Graceful WAL Flush
    // -------------------------------------------------------------------------
    println!("\n=== [Phase 6] Clean Shutdown & Graceful WAL Flush ===");

    daemon.shutdown().await.expect("daemon shutdown failed");
    let _ = server_shutdown_tx.send(());

    // Verify SQLite WAL was truncated and quick_check passes
    {
        let db = Database::open(db_temp_dir.path.join("fetchtrek.db")).expect("open database");
        db.quick_check().expect("database quick_check must pass");
    }
    println!("SQLite WAL flushed and verified clean via PRAGMA quick_check.");
    println!("\n>>> MILESTONE M3 BETA EXIT GATE: ALL 6 PHASES PASSED SUCCESSFULLY <<<\n");
}
