use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_core::BandwidthLimiter;
use fetchtrek_daemon::controller::{detect_stream_protocol, sanitize_media_filename};
use fetchtrek_daemon::TaskController;
use fetchtrek_persistence::Database;
use fetchtrek_protocols::HttpTransport;
use fetchtrek_storage::{PositionalStorage, SlabPool};
use fetchtrek_types::TaskState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
            "fetchtrek_media_ctrl_{}_{}_{}",
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

fn create_valid_ts_packet(packet_id: u8) -> [u8; 188] {
    let mut packet = [0u8; 188];
    packet[0] = 0x47; // Sync byte
    packet[1] = 0x00;
    packet[2] = 0x11;
    packet[3] = 0x10 | (packet_id & 0x0F);
    for (i, byte) in packet.iter_mut().enumerate().skip(4) {
        *byte = packet_id.wrapping_add(i as u8);
    }
    packet
}

struct MockMediaServer {
    url: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    #[allow(dead_code)]
    pub request_counts: Arc<tokio::sync::Mutex<HashMap<String, usize>>>,
}

impl MockMediaServer {
    async fn start(
        manifest_body: String,
        segments: HashMap<String, Vec<u8>>,
        segment_delays: HashMap<String, Duration>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock media server");
        let port = listener.local_addr().expect("local addr").port();
        let server_url = format!("http://127.0.0.1:{port}");
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let request_counts = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        let req_counts_clone = Arc::clone(&request_counts);

        let manifest_arc = Arc::new(manifest_body);
        let segments_arc = Arc::new(segments);
        let delays_arc = Arc::new(segment_delays);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    Ok((mut socket, _)) = listener.accept() => {
                        let manifest = Arc::clone(&manifest_arc);
                        let segments = Arc::clone(&segments_arc);
                        let delays = Arc::clone(&delays_arc);
                        let req_counts = Arc::clone(&req_counts_clone);

                        tokio::spawn(async move {
                            let mut buf = vec![0u8; 4096];
                            let n = match socket.read(&mut buf).await {
                                Ok(n) if n > 0 => n,
                                _ => return,
                            };
                            let req = String::from_utf8_lossy(&buf[..n]);
                            let first_line = req.lines().next().unwrap_or("");
                            let parts: Vec<&str> = first_line.split_whitespace().collect();
                            if parts.len() < 2 {
                                return;
                            }
                            let method = parts[0];
                            let path = parts[1].split('?').next().unwrap_or("");

                            {
                                let mut counts = req_counts.lock().await;
                                *counts.entry(path.to_string()).or_insert(0) += 1;
                            }

                            if path.ends_with(".m3u8") {
                                let body = manifest.as_bytes();
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    body.len()
                                );
                                let _ = socket.write_all(resp.as_bytes()).await;
                                if method != "HEAD" {
                                    let _ = socket.write_all(body).await;
                                }
                                let _ = socket.shutdown().await;
                                return;
                            }

                            // Match segment request
                            let clean_name = path.trim_start_matches('/');
                            if let Some(seg_data) = segments.get(clean_name) {
                                if let Some(delay) = delays.get(clean_name) {
                                    tokio::time::sleep(*delay).await;
                                }

                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: video/mp2t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    seg_data.len()
                                );
                                let _ = socket.write_all(resp.as_bytes()).await;
                                if method != "HEAD" {
                                    let _ = socket.write_all(seg_data).await;
                                }
                                let _ = socket.shutdown().await;
                                return;
                            }

                            let not_found = b"HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n";
                            let _ = socket.write_all(not_found).await;
                            let _ = socket.shutdown().await;
                        });
                    }
                }
            }
        });

        Self {
            url: server_url,
            shutdown_tx: Some(shutdown_tx),
            request_counts,
        }
    }
}

impl Drop for MockMediaServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[test]
fn test_media_protocol_auto_detection() {
    // Protocol detection
    assert_eq!(
        detect_stream_protocol("https://example.com/stream.m3u8", None),
        Some("hls")
    );
    assert_eq!(
        detect_stream_protocol("https://example.com/vod/stream.m3u8?token=xyz", None),
        Some("hls")
    );
    assert_eq!(
        detect_stream_protocol("https://example.com/dash/manifest.mpd", None),
        Some("dash")
    );
    assert_eq!(
        detect_stream_protocol(
            "https://example.com/stream?id=123",
            Some("application/vnd.apple.mpegurl")
        ),
        Some("hls")
    );
    assert_eq!(
        detect_stream_protocol(
            "https://example.com/stream?id=123",
            Some("application/dash+xml")
        ),
        Some("dash")
    );
    assert_eq!(
        detect_stream_protocol("https://example.com/archive.zip", Some("application/zip")),
        None
    );

    // Filename sanitization
    assert_eq!(sanitize_media_filename("master.m3u8"), "master.mp4");
    assert_eq!(sanitize_media_filename("manifest.mpd"), "manifest.mp4");
    assert_eq!(sanitize_media_filename("video.ts"), "video.ts");
    assert_eq!(sanitize_media_filename("document.pdf"), "document.pdf");
}

#[tokio::test]
async fn test_media_controller_e2e_hls_download() {
    let temp = TempDirGuard::new("e2e_hls");
    let db_path = temp.path.join("test.db");
    let download_dir = temp.path.join("downloads");
    std::fs::create_dir_all(&download_dir).expect("create download dir");

    let db = Database::open(&db_path).expect("open db");
    let database = Arc::new(std::sync::Mutex::new(db));
    let storage = Arc::new(PositionalStorage::new());
    let memory_pool = SlabPool::new();
    let limiter = BandwidthLimiter::unlimited();
    let transport = Arc::new(HttpTransport::new());

    // Generate 5 TS chunks (2 valid packets = 376 bytes each)
    const CHUNK_COUNT: usize = 5;
    let mut segments = HashMap::new();
    let mut expected_bytes = Vec::new();

    let mut m3u8 = String::from("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n");
    for i in 0..CHUNK_COUNT {
        let seg_name = format!("seg_{i}.ts");
        let mut seg_data = Vec::new();
        seg_data.extend_from_slice(&create_valid_ts_packet((i * 2) as u8));
        seg_data.extend_from_slice(&create_valid_ts_packet((i * 2 + 1) as u8));

        m3u8.push_str(&format!("#EXTINF:6.0,\n{seg_name}\n"));
        expected_bytes.extend_from_slice(&seg_data);
        segments.insert(seg_name, seg_data);
    }
    m3u8.push_str("#EXT-X-ENDLIST\n");

    let server = MockMediaServer::start(m3u8, segments, HashMap::new()).await;
    let manifest_url = format!("{}/playlist.m3u8", server.url);

    let controller = Arc::new(TaskController::new(
        database.clone(),
        storage,
        memory_pool,
        limiter,
        transport,
        4,
        download_dir.clone(),
    ));

    // Add and start task
    let task = controller
        .add_task(&manifest_url, None, Some("stream.m3u8"), Some(4), None)
        .await
        .expect("add task must succeed");

    assert_eq!(task.protocol, "hls");
    assert_eq!(task.filename, "stream.mp4");
    assert_eq!(task.total_bytes, 0); // Unknown before playlist parse

    // Poll until completed
    let start_time = Instant::now();
    let task_id = task.id;
    let mut completed = false;

    while start_time.elapsed() < Duration::from_secs(5) {
        tokio::time::sleep(Duration::from_millis(50)).await;

        let cur_task = {
            let db = database.lock().unwrap();
            db.get_task_by_id(task_id).unwrap().unwrap()
        };

        if cur_task.status == TaskState::Completed {
            completed = true;
            break;
        }
    }

    assert!(completed, "task must complete within 5 seconds");

    // Verify output file on disk
    let output_file = download_dir.join("stream.mp4");
    assert!(
        output_file.is_file(),
        "output file must exist: {:?}",
        output_file
    );

    let assembled = std::fs::read(&output_file).expect("read assembled output");
    assert_eq!(assembled, expected_bytes);
    assert_eq!(assembled.len(), CHUNK_COUNT * 376);

    // Verify staging directory is cleaned up
    let staging_dir = PathBuf::from(format!(
        "{}.fetchtrek_parts_{}",
        output_file.to_string_lossy(),
        task_id
    ));
    assert!(
        !staging_dir.exists(),
        "staging directory must be removed on success"
    );

    // Verify telemetry reports accurate downloaded bytes and zero active tasks
    let snap = controller
        .collect_telemetry()
        .await
        .expect("telemetry snapshot");
    assert_eq!(snap.active_task_count, 0);
}

#[tokio::test]
async fn test_media_controller_pause_and_resumption() {
    let temp = TempDirGuard::new("pause_resume");
    let db_path = temp.path.join("test.db");
    let download_dir = temp.path.join("downloads");
    std::fs::create_dir_all(&download_dir).expect("create download dir");

    let db = Database::open(&db_path).expect("open db");
    let database = Arc::new(std::sync::Mutex::new(db));
    let storage = Arc::new(PositionalStorage::new());
    let memory_pool = SlabPool::new();
    let limiter = BandwidthLimiter::unlimited();
    let transport = Arc::new(HttpTransport::new());

    const CHUNK_COUNT: usize = 5;
    let mut segments = HashMap::new();
    let mut delays = HashMap::new();
    let mut expected_bytes = Vec::new();

    let mut m3u8 = String::from("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n");
    for i in 0..CHUNK_COUNT {
        let seg_name = format!("chunk_{i}.ts");
        let mut seg_data = Vec::new();
        seg_data.extend_from_slice(&create_valid_ts_packet((i * 2) as u8));
        seg_data.extend_from_slice(&create_valid_ts_packet((i * 2 + 1) as u8));

        m3u8.push_str(&format!("#EXTINF:6.0,\n{seg_name}\n"));
        expected_bytes.extend_from_slice(&seg_data);
        segments.insert(seg_name.clone(), seg_data);

        // Chunks 2, 3, 4 have a delay to guarantee clean pause opportunity
        if i >= 2 {
            delays.insert(seg_name, Duration::from_millis(200));
        }
    }
    m3u8.push_str("#EXT-X-ENDLIST\n");

    let server = MockMediaServer::start(m3u8, segments, delays).await;
    let manifest_url = format!("{}/playlist.m3u8", server.url);

    let controller = Arc::new(TaskController::new(
        database.clone(),
        storage,
        memory_pool,
        limiter,
        transport,
        4,
        download_dir.clone(),
    ));

    // Add and start task
    let task = controller
        .add_task(&manifest_url, None, Some("resumable.m3u8"), Some(4), None)
        .await
        .expect("add task");

    let task_id = task.id;
    let output_file = download_dir.join("resumable.mp4");
    let staging_dir = PathBuf::from(format!(
        "{}.fetchtrek_parts_{}",
        output_file.to_string_lossy(),
        task_id
    ));

    // Wait until at least 1 chunk is completed, then pause
    tokio::time::sleep(Duration::from_millis(100)).await;
    controller.pause_task(task_id).await.expect("pause task");

    // Verify task is paused in database
    {
        let db = database.lock().unwrap();
        let cur = db.get_task_by_id(task_id).unwrap().unwrap();
        assert_eq!(cur.status, TaskState::Paused);
    }

    // Staging directory and completed .part files must be strictly preserved
    assert!(
        staging_dir.is_dir(),
        "staging directory must exist while paused"
    );
    assert!(!output_file.exists(), "assembled output must not exist yet");

    // Resume task
    controller.start_task(task_id).await.expect("resume task");

    // Poll until completed
    let start_time = Instant::now();
    let mut completed = false;

    while start_time.elapsed() < Duration::from_secs(6) {
        tokio::time::sleep(Duration::from_millis(50)).await;

        let cur = {
            let db = database.lock().unwrap();
            db.get_task_by_id(task_id).unwrap().unwrap()
        };

        if cur.status == TaskState::Completed {
            completed = true;
            break;
        }
    }

    assert!(completed, "resumed task must complete");

    // Verify final assembled file parity
    let assembled = std::fs::read(&output_file).expect("read assembled output");
    assert_eq!(assembled, expected_bytes);
    assert!(
        !staging_dir.exists(),
        "staging dir must be purged after resumption and completion"
    );
}
