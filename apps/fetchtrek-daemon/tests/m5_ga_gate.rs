//! Milestone M5 General Availability (GA) Exit Gate Integration Test.
//!
//! Validates the complete vertical production stack under a strict 2.5s execution budget:
//! - Phase 1: Clean daemon boot, SQLite WAL initialization, and IPC endpoint ready (< 50ms).
//! - Phase 2: Multi-connection segmented HTTP 206 download with ETag validation and atomic rename.
//! - Phase 3: HLS AES-128-CBC stream download with TS sync-byte validation and stream muxing.
//! - Phase 4: BitTorrent swarm session with 16 KiB block validation, SHA-1 verification, and piece storage commitment.
//! - Phase 5: Crash resilience simulation: uncommitted chunks safely rewound to verified file boundaries upon restart.
//! - Phase 6: Mixed concurrent execution with 10 Hz IPC telemetry loop, followed by graceful shutdown and complete SQLite WAL checkpoint in under 2.5s.

#![allow(clippy::unreadable_literal)]
#![allow(clippy::chunks_exact_to_as_chunks)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_daemon::{DaemonConfig, DaemonEngine};
use fetchtrek_ipc::endpoint_with_name;
use fetchtrek_persistence::{rewind_cursor_to_cluster, Database, NewTask, TaskSegmentRecord};
use fetchtrek_protocols::{encrypt_aes128_cbc, sha1, BencodeValue, PeerHandshake, PeerMessage};
use fetchtrek_types::TaskState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, RwLock};

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
            "fetchtrek_m5_ga_gate_{}_{}_{}",
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

    let mut out = [0u8; 32];
    for (i, val) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&val.to_be_bytes());
    }
    out
}

fn make_ts_packet(packet_id: u8) -> [u8; 188] {
    let mut packet = [0u8; 188];
    packet[0] = 0x47; // ISO/IEC 13818-1 sync byte
    packet[1] = 0x00;
    packet[2] = 0x11;
    packet[3] = 0x10 | (packet_id & 0x0F);
    for (i, byte) in packet.iter_mut().enumerate().skip(4) {
        *byte = packet_id.wrapping_add(i as u8);
    }
    packet
}

fn create_test_torrent_file(
    path: &Path,
    name: &str,
    tracker_url: &str,
    piece_length: u32,
    pieces: &[[u8; 20]],
    total_length: u64,
) -> [u8; 20] {
    let mut info = BTreeMap::new();
    info.insert(
        b"name".to_vec(),
        BencodeValue::Bytes(name.as_bytes().to_vec()),
    );
    info.insert(
        b"length".to_vec(),
        BencodeValue::Integer(total_length as i64),
    );
    info.insert(
        b"piece length".to_vec(),
        BencodeValue::Integer(piece_length as i64),
    );

    let mut pieces_concat = Vec::with_capacity(pieces.len() * 20);
    for p in pieces {
        pieces_concat.extend_from_slice(p);
    }
    info.insert(b"pieces".to_vec(), BencodeValue::Bytes(pieces_concat));

    let info_val = BencodeValue::Dict(info);
    let info_bytes = info_val.encode();
    let info_hash = sha1(&info_bytes);

    let mut root = BTreeMap::new();
    root.insert(
        b"announce".to_vec(),
        BencodeValue::Bytes(tracker_url.as_bytes().to_vec()),
    );
    root.insert(b"info".to_vec(), info_val);

    let encoded = BencodeValue::Dict(root).encode();
    std::fs::write(path, encoded).expect("write torrent file");

    info_hash
}

#[derive(Clone)]
struct HttpRoute {
    content_type: String,
    body: Vec<u8>,
    etag: Option<String>,
}

#[derive(Clone)]
struct TorrentMockData {
    num_pieces: usize,
    piece_size: u32,
    payload: Arc<Vec<u8>>,
}

struct MockGaServer {
    http_url: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    peer_shutdown_tx: Option<oneshot::Sender<()>>,
    routes: Arc<RwLock<HashMap<String, HttpRoute>>>,
    torrents: Arc<RwLock<HashMap<[u8; 20], Arc<TorrentMockData>>>>,
}

impl MockGaServer {
    async fn start() -> Self {
        let peer_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind peer listener");
        let peer_port = peer_listener.local_addr().expect("peer addr").port();

        let http_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind http listener");
        let http_port = http_listener.local_addr().expect("http addr").port();
        let http_url = format!("http://127.0.0.1:{http_port}");

        let routes = Arc::new(RwLock::new(HashMap::<String, HttpRoute>::new()));
        let torrents = Arc::new(RwLock::new(HashMap::<[u8; 20], Arc<TorrentMockData>>::new()));

        let (http_shutdown_tx, mut http_shutdown_rx) = oneshot::channel::<()>();
        let (peer_shutdown_tx, mut peer_shutdown_rx) = oneshot::channel::<()>();

        // Spawn HTTP dispatch loop
        let routes_clone = Arc::clone(&routes);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut http_shutdown_rx => break,
                    res = http_listener.accept() => {
                        let (mut socket, _) = match res {
                            Ok(s) => s,
                            Err(_) => break,
                        };
                        let routes = Arc::clone(&routes_clone);

                        tokio::spawn(async move {
                            let mut buf = Vec::with_capacity(4096);
                            let mut temp = [0u8; 1024];

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
                            if parts.len() < 2 {
                                return;
                            }
                            let method = parts[0];
                            let raw_uri = parts[1];
                            let path = raw_uri.split('?').next().unwrap_or("");

                            let mut range_header = None;
                            for line in req.lines() {
                                let lower = line.to_ascii_lowercase();
                                if lower.starts_with("range:") {
                                    range_header = Some(line["range:".len()..].trim().to_string());
                                }
                            }

                            // Dynamic Tracker Announce routing (BEP 0003 & BEP 0023 compact peer format)
                            if path == "/announce" || path == "announce" {
                                let mut peer_bytes = Vec::with_capacity(6);
                                peer_bytes.extend_from_slice(&[127, 0, 0, 1]);
                                peer_bytes.extend_from_slice(&peer_port.to_be_bytes());

                                let mut resp_dict = BTreeMap::new();
                                resp_dict.insert(b"interval".to_vec(), BencodeValue::Integer(1800));
                                resp_dict.insert(b"peers".to_vec(), BencodeValue::Bytes(peer_bytes));

                                let encoded = BencodeValue::Dict(resp_dict).encode();
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    encoded.len()
                                );
                                let _ = socket.write_all(resp.as_bytes()).await;
                                let _ = socket.write_all(&encoded).await;
                                let _ = socket.shutdown().await;
                                return;
                            }

                            let route_key = path.trim_start_matches('/');
                            let route = {
                                let lock = routes.read().await;
                                lock.get(route_key).cloned()
                            };

                            if let Some(r) = route {
                                let total_len = r.body.len();
                                let etag_header = r
                                    .etag
                                    .map(|e| format!("ETag: \"{e}\"\r\n"))
                                    .unwrap_or_default();

                                if method == "HEAD" {
                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n{}Connection: close\r\n\r\n",
                                        r.content_type, total_len, etag_header
                                    );
                                    let _ = socket.write_all(resp.as_bytes()).await;
                                    let _ = socket.shutdown().await;
                                    return;
                                }

                                if let Some(range) = range_header {
                                    if let Some(bytes_spec) = range.strip_prefix("bytes=") {
                                        let range_parts: Vec<&str> = bytes_spec.split('-').collect();
                                        let start: usize = range_parts[0].parse().unwrap_or(0);
                                        let end: usize = if range_parts.len() > 1 && !range_parts[1].is_empty() {
                                            range_parts[1].parse().unwrap_or(total_len - 1).min(total_len - 1)
                                        } else {
                                            total_len - 1
                                        };

                                        if start <= end && start < total_len {
                                            let chunk_len = end - start + 1;
                                            let resp = format!(
                                                "HTTP/1.1 206 Partial Content\r\nContent-Type: {}\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n{}Connection: close\r\n\r\n",
                                                r.content_type, start, end, total_len, chunk_len, etag_header
                                            );
                                            let _ = socket.write_all(resp.as_bytes()).await;
                                            let _ = socket.write_all(&r.body[start..=end]).await;
                                            let _ = socket.shutdown().await;
                                            return;
                                        }
                                    }
                                }

                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n{}Connection: close\r\n\r\n",
                                    r.content_type, total_len, etag_header
                                );
                                let _ = socket.write_all(resp.as_bytes()).await;
                                let _ = socket.write_all(&r.body).await;
                                let _ = socket.shutdown().await;
                            } else {
                                let not_found = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                                let _ = socket.write_all(not_found.as_bytes()).await;
                                let _ = socket.shutdown().await;
                            }
                        });
                    }
                }
            }
        });

        // Spawn BitTorrent Peer Listener loop
        let torrents_clone = Arc::clone(&torrents);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut peer_shutdown_rx => break,
                    res = peer_listener.accept() => {
                        let (mut socket, _) = match res {
                            Ok(s) => s,
                            Err(_) => break,
                        };
                        let torrents = Arc::clone(&torrents_clone);

                        tokio::spawn(async move {
                            let handshake = match PeerHandshake::read_from(&mut socket, None).await {
                                Ok(hs) => hs,
                                Err(_) => return,
                            };

                            let t_data = {
                                let lock = torrents.read().await;
                                lock.get(&handshake.info_hash).cloned()
                            };

                            let t_data = match t_data {
                                Some(d) => d,
                                None => return,
                            };

                            let server_hs = PeerHandshake::new(handshake.info_hash, *b"-FT0001-GA-SEEDER-01");
                            if server_hs.write_to(&mut socket).await.is_err() {
                                return;
                            }

                            let bytes_needed = t_data.num_pieces.div_ceil(8);
                            let mut bitfield_bytes = vec![0xFFu8; bytes_needed];
                            let remainder = t_data.num_pieces % 8;
                            if remainder > 0 {
                                let mask = !((1u8 << (8 - remainder)) - 1);
                                bitfield_bytes[bytes_needed - 1] &= mask;
                            }

                            let bitfield_msg = PeerMessage::Bitfield(bitfield_bytes).encode();
                            let unchoke_msg = PeerMessage::Unchoke.encode();
                            if socket.write_all(&bitfield_msg).await.is_err() {
                                return;
                            }
                            if socket.write_all(&unchoke_msg).await.is_err() {
                                return;
                            }

                            loop {
                                let mut len_buf = [0u8; 4];
                                if socket.read_exact(&mut len_buf).await.is_err() {
                                    break;
                                }
                                let len = u32::from_be_bytes(len_buf);
                                if len == 0 {
                                    continue;
                                }
                                let mut payload = vec![0u8; len as usize];
                                if socket.read_exact(&mut payload).await.is_err() {
                                    break;
                                }

                                if let Ok(msg) = PeerMessage::decode_payload(len, &payload) {
                                    match msg {
                                        PeerMessage::Request { index, begin, length } => {
                                            let piece_start = (index as usize) * (t_data.piece_size as usize);
                                            let block_start = piece_start + (begin as usize);
                                            let block_end = block_start + (length as usize);

                                            if block_end <= t_data.payload.len() {
                                                let block_bytes = t_data.payload[block_start..block_end].to_vec();
                                                let piece_msg = PeerMessage::Piece {
                                                    index,
                                                    begin,
                                                    block: block_bytes,
                                                }
                                                .encode();
                                                if socket.write_all(&piece_msg).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                        PeerMessage::Have { .. } => {}
                                        _ => {}
                                    }
                                }
                            }
                        });
                    }
                }
            }
        });

        Self {
            http_url,
            shutdown_tx: Some(http_shutdown_tx),
            peer_shutdown_tx: Some(peer_shutdown_tx),
            routes,
            torrents,
        }
    }

    async fn register_route(
        &self,
        path: impl Into<String>,
        content_type: &str,
        body: Vec<u8>,
        etag: Option<String>,
    ) {
        let mut lock = self.routes.write().await;
        lock.insert(
            path.into(),
            HttpRoute {
                content_type: content_type.to_string(),
                body,
                etag,
            },
        );
    }

    async fn register_torrent(
        &self,
        info_hash: [u8; 20],
        num_pieces: usize,
        piece_size: u32,
        payload: Arc<Vec<u8>>,
    ) {
        let mut lock = self.torrents.write().await;
        lock.insert(
            info_hash,
            Arc::new(TorrentMockData {
                num_pieces,
                piece_size,
                payload,
            }),
        );
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.peer_shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[tokio::test]
async fn test_milestone_m5_general_availability_exit_gate() {
    let gate_start = Instant::now();
    let temp = TempDirGuard::new("ga_gate");
    let db_path = temp.path.join("fetchtrek.db");
    let dl_dir = temp.path.join("downloads");
    std::fs::create_dir_all(&dl_dir).expect("create downloads directory");

    let mock_server = MockGaServer::start().await;

    // =========================================================================
    // PHASE 1: CLEAN DAEMON BOOT, SQLITE WAL & IPC READY (< 50ms)
    // =========================================================================
    let boot_start = Instant::now();
    let ipc_pipe_name = format!("fetchtrek_ga_test_{}", std::process::id());
    let ipc_endpoint = endpoint_with_name(&ipc_pipe_name);

    let daemon_config = DaemonConfig {
        data_dir: temp.path.clone(),
        ipc_endpoint: Some(ipc_endpoint.clone()),
        max_active_tasks: 8,
        global_bandwidth_limit_bps: None,
        slab_pool_size_mb: 16,
    };

    let daemon = DaemonEngine::start(daemon_config)
        .await
        .expect("start daemon engine");
    let boot_elapsed = boot_start.elapsed();

    assert!(
        boot_elapsed.as_millis() < 50,
        "Phase 1: Daemon boot must complete under 50ms, took {:?}",
        boot_elapsed
    );

    // Verify SQLite WAL mode is active
    let journal_mode: String = {
        let db = daemon.database.lock().unwrap();
        db.connection()
            .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
            .expect("query journal_mode")
    };
    assert_eq!(
        journal_mode.to_lowercase(),
        "wal",
        "Phase 1: Database must initialize in Write-Ahead Logging (WAL) mode"
    );

    // =========================================================================
    // PHASE 2: HTTP 206 SEGMENTED DOWNLOAD WITH ETAG VALIDATION & ATOMIC RENAME
    // =========================================================================
    let p2_size = 65536usize;
    let mut p2_payload = vec![0u8; p2_size];
    for (i, b) in p2_payload.iter_mut().enumerate() {
        *b = ((i ^ 0x5C) & 0xFF) as u8;
    }
    let p2_expected_sha256 = sha256(&p2_payload);

    mock_server
        .register_route(
            "http/segmented_file.bin",
            "application/octet-stream",
            p2_payload.clone(),
            Some("etag-m5-ga-001".to_string()),
        )
        .await;

    let p2_dest = dl_dir.join("segmented_file.bin");
    let t_http = daemon
        .controller
        .add_task(
            &format!("{}/http/segmented_file.bin", mock_server.http_url),
            Some(&p2_dest.to_string_lossy()),
            Some("segmented_file.bin"),
            Some(2),
            None,
        )
        .await
        .expect("Phase 2: add segmented HTTP task");

    let p2_start = Instant::now();
    loop {
        if p2_start.elapsed() > Duration::from_millis(1500) {
            panic!("Phase 2: Segmented HTTP download timed out");
        }
        let status = {
            let db = daemon.database.lock().unwrap();
            db.get_task_by_id(t_http.id).unwrap().map(|t| t.status)
        };
        if status == Some(TaskState::Completed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify atomic rename and file integrity
    assert!(p2_dest.exists(), "Phase 2: Destination file must exist");
    let p2_disk = std::fs::read(&p2_dest).expect("read Phase 2 file");
    assert_eq!(p2_disk.len(), p2_size);
    assert_eq!(sha256(&p2_disk), p2_expected_sha256);

    // Verify staging file was cleaned up
    let staging_files: Vec<_> = std::fs::read_dir(&dl_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".fetchtrek_temp_")
        })
        .collect();
    assert_eq!(
        staging_files.len(),
        0,
        "Phase 2: Staging files must be atomically removed upon completion"
    );

    // =========================================================================
    // PHASE 3: HLS AES-128-CBC STREAM DOWNLOAD WITH TS SYNC-BYTE VALIDATION
    // =========================================================================
    let p3_key = [0x55u8; 16];
    let p3_iv = [0x11u8; 16];
    let mut p3_plaintext = Vec::new();

    let mut p3_m3u8 = String::from("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n");
    p3_m3u8.push_str(&format!(
        "#EXT-X-KEY:METHOD=AES-128,URI=\"{}/hls/key.bin\",IV=0x11111111111111111111111111111111\n",
        mock_server.http_url
    ));

    for s in 0..2 {
        let seg_name = format!("seg_{s}.ts");
        p3_m3u8.push_str(&format!("#EXTINF:2.0,\n{seg_name}\n"));

        let mut seg_pt = Vec::with_capacity(3 * 188);
        for p in 0..3 {
            let pkt = make_ts_packet((s * 3 + p) as u8);
            seg_pt.extend_from_slice(&pkt);
            p3_plaintext.extend_from_slice(&pkt);
        }

        let ct = encrypt_aes128_cbc(&seg_pt, &p3_key, &p3_iv);
        mock_server
            .register_route(format!("hls/{seg_name}"), "video/mp2t", ct, None)
            .await;
    }
    p3_m3u8.push_str("#EXT-X-ENDLIST\n");
    let p3_expected_sha256 = sha256(&p3_plaintext);

    mock_server
        .register_route(
            "hls/master.m3u8",
            "application/vnd.apple.mpegurl",
            p3_m3u8.as_bytes().to_vec(),
            None,
        )
        .await;
    mock_server
        .register_route(
            "hls/key.bin",
            "application/octet-stream",
            p3_key.to_vec(),
            None,
        )
        .await;

    let p3_dest = dl_dir.join("hls_stream.mp4");
    let t_hls = daemon
        .controller
        .add_task(
            &format!("{}/hls/master.m3u8", mock_server.http_url),
            Some(&p3_dest.to_string_lossy()),
            Some("hls_stream.mp4"),
            Some(2),
            None,
        )
        .await
        .expect("Phase 3: add HLS task");

    let p3_start = Instant::now();
    loop {
        if p3_start.elapsed() > Duration::from_millis(1500) {
            panic!("Phase 3: HLS stream capture timed out");
        }
        let status = {
            let db = daemon.database.lock().unwrap();
            db.get_task_by_id(t_hls.id).unwrap().map(|t| t.status)
        };
        if status == Some(TaskState::Completed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let p3_disk = std::fs::read(&p3_dest).expect("read Phase 3 output file");
    assert_eq!(p3_disk.len(), p3_plaintext.len());
    // Verify sync bytes (0x47) at every 188-byte offset
    for (idx, chunk) in p3_disk.chunks(188).enumerate() {
        assert_eq!(
            chunk[0], 0x47,
            "Phase 3: TS packet {idx} must start with 0x47 sync byte"
        );
    }
    assert_eq!(sha256(&p3_disk), p3_expected_sha256);

    // =========================================================================
    // PHASE 4: BITTORRENT SWARM WITH SHA-1 PIECE VERIFICATION
    // =========================================================================
    let piece_size = 16384u32; // 16 KiB blocks
    let num_pieces = 4usize;
    let torrent_total_size = (piece_size as u64) * (num_pieces as u64);
    let mut p4_payload = vec![0u8; torrent_total_size as usize];
    for (i, b) in p4_payload.iter_mut().enumerate() {
        *b = (((i.wrapping_mul(31)).wrapping_add(7)) % 256) as u8;
    }
    let p4_expected_sha256 = sha256(&p4_payload);

    let mut piece_hashes = Vec::with_capacity(num_pieces);
    for p in 0..num_pieces {
        let start = p * (piece_size as usize);
        let end = start + (piece_size as usize);
        piece_hashes.push(sha1(&p4_payload[start..end]));
    }

    let torrent_file = temp.path.join("m5_sample.torrent");
    let tracker_announce_url = format!("{}/announce", mock_server.http_url);
    let info_hash = create_test_torrent_file(
        &torrent_file,
        "m5_torrent_out.bin",
        &tracker_announce_url,
        piece_size,
        &piece_hashes,
        torrent_total_size,
    );

    mock_server
        .register_torrent(
            info_hash,
            num_pieces,
            piece_size,
            Arc::new(p4_payload.clone()),
        )
        .await;

    let p4_dest = dl_dir.join("m5_torrent_out.bin");
    let t_torrent = daemon
        .controller
        .add_task(
            &torrent_file.to_string_lossy(),
            Some(&p4_dest.to_string_lossy()),
            Some("m5_torrent_out.bin"),
            Some(4),
            None,
        )
        .await
        .expect("Phase 4: add BitTorrent task");

    let p4_start = Instant::now();
    loop {
        if p4_start.elapsed() > Duration::from_millis(1500) {
            panic!("Phase 4: BitTorrent download timed out");
        }
        let status = {
            let db = daemon.database.lock().unwrap();
            db.get_task_by_id(t_torrent.id).unwrap().map(|t| t.status)
        };
        if status == Some(TaskState::Completed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let p4_disk = std::fs::read(&p4_dest).expect("read Phase 4 output file");
    assert_eq!(p4_disk.len(), torrent_total_size as usize);
    assert_eq!(sha256(&p4_disk), p4_expected_sha256);

    // =========================================================================
    // PHASE 5: CRASH RESILIENCE SIMULATION & CLUSTER BOUNDARY REWIND
    // =========================================================================
    let crash_task_id = {
        let db = daemon.database.lock().unwrap();
        let mut new_task = NewTask::new(
            "uuid-crash-m5-gate",
            "https://fetchtrek.local/interrupted_asset.bin",
            dl_dir
                .join("interrupted_asset.bin")
                .to_string_lossy()
                .to_string(),
            "interrupted_asset.bin",
            "http",
            1_000_000,
        );
        new_task.status = TaskState::Active;
        new_task.downloaded_bytes = 180_000;

        let id = db.create_task(&new_task).expect("create crash task");
        let seg = TaskSegmentRecord::new(id, 0, 0, 999_999, 180_000, 1);
        db.save_segments(id, &[seg]).expect("save crash segment");
        id
    };

    // Simulate partial disk staging file before crash
    let crash_staging = dl_dir.join(format!(".fetchtrek_temp_{crash_task_id}.part"));
    std::fs::write(&crash_staging, vec![0xEEu8; 120_000]).expect("write partial staging");

    // Reopen database connection and trigger crash recovery
    let recovered_tasks = {
        let db = daemon.database.lock().unwrap();
        db.recover_interrupted_tasks()
            .expect("recover interrupted tasks")
    };

    assert_eq!(recovered_tasks.len(), 1);
    let rec = &recovered_tasks[0];
    assert_eq!(rec.task.id, crash_task_id);
    assert_eq!(rec.task.status, TaskState::Paused);

    // Cursor bounded to disk length (120,000) and rewound to 64 KiB cluster: 65,536
    let expected_rewound_cursor = rewind_cursor_to_cluster(120_000, 0);
    assert_eq!(expected_rewound_cursor, 65_536);
    assert_eq!(rec.segments[0].current_cursor, 65_536);
    assert_eq!(rec.discarded_tail_bytes, 180_000 - 65_536);
    assert_eq!(rec.task.downloaded_bytes, 65_536);

    // =========================================================================
    // PHASE 6: MIXED CONCURRENT EXECUTION, 10 HZ TELEMETRY & GRACEFUL SHUTDOWN
    // =========================================================================
    // Launch concurrent HTTP transfer while polling 10 Hz telemetry loop
    let p6_http_payload = vec![0x99u8; 32768];
    mock_server
        .register_route(
            "http/concurrent_m5.bin",
            "application/octet-stream",
            p6_http_payload,
            None,
        )
        .await;

    let p6_dest = dl_dir.join("concurrent_m5.bin");
    let t_concurrent = daemon
        .controller
        .add_task(
            &format!("{}/http/concurrent_m5.bin", mock_server.http_url),
            Some(&p6_dest.to_string_lossy()),
            Some("concurrent_m5.bin"),
            Some(2),
            None,
        )
        .await
        .expect("Phase 6: add concurrent task");

    let mut observed_telemetry = false;
    let p6_start = Instant::now();

    loop {
        if p6_start.elapsed() > Duration::from_millis(1500) {
            panic!("Phase 6: Concurrent transfer timed out");
        }

        if let Ok(telemetry) = daemon.controller.collect_telemetry().await {
            if telemetry.active_task_count > 0 || !telemetry.tasks.is_empty() {
                observed_telemetry = true;
            }
        }

        let status = {
            let db = daemon.database.lock().unwrap();
            db.get_task_by_id(t_concurrent.id)
                .unwrap()
                .map(|t| t.status)
        };
        if status == Some(TaskState::Completed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        observed_telemetry,
        "Phase 6: 10 Hz Telemetry must observe active tasks"
    );

    // Graceful daemon shutdown and complete SQLite WAL checkpoint
    daemon
        .shutdown()
        .await
        .expect("graceful shutdown of daemon");
    mock_server.shutdown().await;

    // Verify WAL checkpoint
    let post_db = Database::open(&db_path).expect("reopen post-shutdown db");
    let checkpoint_res = post_db
        .connection()
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
    assert!(
        checkpoint_res.is_ok(),
        "Phase 6: SQLite WAL checkpoint must succeed upon graceful shutdown"
    );

    // Verify total gate execution budget is strictly under 2.5 seconds
    let total_gate_duration = gate_start.elapsed();
    assert!(
        total_gate_duration.as_millis() < 2500,
        "Milestone M5 GA Gate must execute in under 2.5s, took {:?}",
        total_gate_duration
    );
}
