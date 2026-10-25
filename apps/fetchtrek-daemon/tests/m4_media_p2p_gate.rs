//! Milestone M4 Feature Complete Exit Gate Integration Test.
//!
//! Validates end-to-end multi-protocol media stream capture (HLS AES-128, MPEG-DASH fMP4)
//! and P2P BitTorrent swarm distribution with SHA-1 piece verification, mixed-protocol
//! concurrency, and atomic 10 Hz telemetry under a sub-4.0s execution budget.

#![allow(clippy::unreadable_literal)]
#![allow(clippy::chunks_exact_to_as_chunks)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_daemon::{DaemonConfig, DaemonEngine, TaskController};
use fetchtrek_ipc::endpoint_with_name;
use fetchtrek_persistence::TaskRecord;
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
            "fetchtrek_m4_gate_{}_{}_{}",
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
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn create_valid_ts_packet(packet_id: u8) -> [u8; 188] {
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

fn create_synthetic_fmp4_init() -> Vec<u8> {
    let mut init = Vec::new();
    // ftyp box (24 bytes)
    init.extend_from_slice(&24u32.to_be_bytes());
    init.extend_from_slice(b"ftyp");
    init.extend_from_slice(b"iso6");
    init.extend_from_slice(&1u32.to_be_bytes());
    init.extend_from_slice(b"mp41dash");

    // moov box (32 bytes)
    init.extend_from_slice(&32u32.to_be_bytes());
    init.extend_from_slice(b"moov");
    init.extend_from_slice(&24u32.to_be_bytes());
    init.extend_from_slice(b"mvhd");
    init.extend_from_slice(&[0u8; 16]);

    init
}

fn create_synthetic_fmp4_fragment(seq: u32, payload_size: usize) -> Vec<u8> {
    let mut fragment = Vec::new();

    // moof box (32 bytes)
    fragment.extend_from_slice(&32u32.to_be_bytes());
    fragment.extend_from_slice(b"moof");
    fragment.extend_from_slice(&24u32.to_be_bytes());
    fragment.extend_from_slice(b"mfhd");
    fragment.extend_from_slice(&[0u8; 4]);
    fragment.extend_from_slice(&seq.to_be_bytes());
    fragment.extend_from_slice(&[0u8; 8]);

    // mdat box (8 + payload_size bytes)
    let mdat_size = (payload_size + 8) as u32;
    fragment.extend_from_slice(&mdat_size.to_be_bytes());
    fragment.extend_from_slice(b"mdat");
    for i in 0..payload_size {
        fragment.push((seq as u8).wrapping_add(i as u8));
    }

    fragment
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
    is_stream_file: bool,
}

#[derive(Clone)]
struct TorrentMockData {
    num_pieces: usize,
    piece_size: u32,
    payload: Arc<Vec<u8>>,
}

type HaveRecordList = Arc<std::sync::Mutex<Vec<(u32, [u8; 20])>>>;

struct MockIntegratedServer {
    http_url: String,
    peer_port: u16,
    shutdown_tx: Option<oneshot::Sender<()>>,
    peer_shutdown_tx: Option<oneshot::Sender<()>>,
    routes: Arc<RwLock<HashMap<String, HttpRoute>>>,
    torrents: Arc<RwLock<HashMap<[u8; 20], Arc<TorrentMockData>>>>,
    have_recorded: HaveRecordList,
}

impl MockIntegratedServer {
    async fn start() -> Self {
        // 1. Bind Peer TCP Listener
        let peer_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind peer listener");
        let peer_port = peer_listener.local_addr().expect("peer addr").port();

        // 2. Bind HTTP Listener
        let http_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind http listener");
        let http_port = http_listener.local_addr().expect("http addr").port();
        let http_url = format!("http://127.0.0.1:{http_port}");

        let routes = Arc::new(RwLock::new(HashMap::<String, HttpRoute>::new()));
        let torrents = Arc::new(RwLock::new(HashMap::new()));
        let have_recorded = Arc::new(std::sync::Mutex::new(Vec::new()));

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

                            // Dynamic Tracker Announce routing (BEP 0003 & BEP 0023 compact peer response)
                            if path == "/announce" || path == "announce" {
                                let mut peer_bytes = Vec::with_capacity(6);
                                peer_bytes.extend_from_slice(&[127, 0, 0, 1]);
                                peer_bytes.extend_from_slice(&peer_port.to_be_bytes());

                                let mut resp_dict = BTreeMap::new();
                                resp_dict.insert(b"interval".to_vec(), BencodeValue::Integer(1800));
                                resp_dict.insert(b"peers".to_vec(), BencodeValue::Bytes(peer_bytes));
                                let body = BencodeValue::Dict(resp_dict).encode();

                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    body.len()
                                );
                                let _ = socket.write_all(resp.as_bytes()).await;
                                if method != "HEAD" {
                                    let _ = socket.write_all(&body).await;
                                }
                                let _ = socket.shutdown().await;
                                return;
                            }

                            let clean_path = path.trim_start_matches('/');
                            let route_opt = {
                                let r_lock = routes.read().await;
                                r_lock.get(clean_path).cloned()
                            };

                            if let Some(route) = route_opt {
                                if route.is_stream_file {
                                    if method == "HEAD" {
                                        let resp = format!(
                                            "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nETag: \"m4-gate-etag\"\r\nConnection: close\r\n\r\n",
                                            route.content_type,
                                            route.body.len()
                                        );
                                        let _ = socket.write_all(resp.as_bytes()).await;
                                        let _ = socket.shutdown().await;
                                        return;
                                    }

                                    if let Some(ref range_str) = range_header {
                                        let r_val = range_str.trim_start_matches("bytes=");
                                        let (start, end) = parse_range_slice(r_val, route.body.len());
                                        let slice = &route.body[start..=end];
                                        let resp = format!(
                                            "HTTP/1.1 206 Partial Content\r\nContent-Type: {}\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                            route.content_type,
                                            start,
                                            end,
                                            route.body.len(),
                                            slice.len()
                                        );
                                        let _ = socket.write_all(resp.as_bytes()).await;
                                        let _ = socket.write_all(slice).await;
                                        let _ = socket.shutdown().await;
                                        return;
                                    }

                                    let resp = format!(
                                        "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                                        route.content_type,
                                        route.body.len()
                                    );
                                    let _ = socket.write_all(resp.as_bytes()).await;
                                    let _ = socket.write_all(&route.body).await;
                                    let _ = socket.shutdown().await;
                                    return;
                                }

                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    route.content_type,
                                    route.body.len()
                                );
                                let _ = socket.write_all(resp.as_bytes()).await;
                                if method != "HEAD" {
                                    let _ = socket.write_all(&route.body).await;
                                }
                                let _ = socket.shutdown().await;
                                return;
                            }

                            let not_found = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                            let _ = socket.write_all(not_found).await;
                            let _ = socket.shutdown().await;
                        });
                    }
                }
            }
        });

        // Spawn BitTorrent TCP Peer loop
        let torrents_clone = Arc::clone(&torrents);
        let have_rec_clone = Arc::clone(&have_recorded);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut peer_shutdown_rx => break,
                    res = peer_listener.accept() => {
                        let (socket, _) = match res {
                            Ok(s) => s,
                            Err(_) => break,
                        };
                        let t_map = Arc::clone(&torrents_clone);
                        let h_rec = Arc::clone(&have_rec_clone);
                        tokio::spawn(async move {
                            handle_mock_peer(socket, t_map, h_rec).await;
                        });
                    }
                }
            }
        });

        Self {
            http_url,
            peer_port,
            shutdown_tx: Some(http_shutdown_tx),
            peer_shutdown_tx: Some(peer_shutdown_tx),
            routes,
            torrents,
            have_recorded,
        }
    }

    async fn register_route(
        &self,
        path: impl Into<String>,
        content_type: impl Into<String>,
        body: Vec<u8>,
        is_stream_file: bool,
    ) {
        let mut r = self.routes.write().await;
        r.insert(
            path.into().trim_start_matches('/').to_string(),
            HttpRoute {
                content_type: content_type.into(),
                body,
                is_stream_file,
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
        let mut t = self.torrents.write().await;
        t.insert(
            info_hash,
            Arc::new(TorrentMockData {
                num_pieces,
                piece_size,
                payload,
            }),
        );
    }

    fn get_recorded_haves_for(&self, info_hash: [u8; 20]) -> Vec<u32> {
        let rec = self.have_recorded.lock().unwrap();
        rec.iter()
            .filter(|(_, ih)| *ih == info_hash)
            .map(|(p, _)| *p)
            .collect()
    }
}

impl Drop for MockIntegratedServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.peer_shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

fn parse_range_slice(range_val: &str, total_len: usize) -> (usize, usize) {
    let parts: Vec<&str> = range_val.split('-').collect();
    let mut start = 0usize;
    let mut end = total_len.saturating_sub(1);

    if let Some(s_str) = parts.first() {
        if let Ok(s) = s_str.trim().parse::<usize>() {
            start = s;
        }
    }
    if parts.len() > 1 && !parts[1].trim().is_empty() {
        if let Ok(e) = parts[1].trim().parse::<usize>() {
            end = e;
        }
    }

    if start > end {
        start = end;
    }
    end = end.min(total_len.saturating_sub(1));
    (start, end)
}

async fn handle_mock_peer(
    mut socket: tokio::net::TcpStream,
    torrents: Arc<RwLock<HashMap<[u8; 20], Arc<TorrentMockData>>>>,
    have_recorded: HaveRecordList,
) {
    let client_hs = match PeerHandshake::read_from(&mut socket, None).await {
        Ok(hs) => hs,
        Err(_) => return,
    };

    let server_hs = PeerHandshake::new(client_hs.info_hash, *b"-FT0001-MOCKPEER0001");
    if server_hs.write_to(&mut socket).await.is_err() {
        return;
    }

    let torrent = {
        let t_guard = torrents.read().await;
        t_guard.get(&client_hs.info_hash).cloned()
    };

    let torrent = match torrent {
        Some(t) => t,
        None => return,
    };

    // Bitfield message: all pieces available
    let num_bytes = torrent.num_pieces.div_ceil(8);
    let mut bitfield = vec![0u8; num_bytes];
    for p in 0..torrent.num_pieces {
        bitfield[p / 8] |= 1 << (7 - (p % 8));
    }

    let bf_msg = PeerMessage::Bitfield(bitfield);
    if socket.write_all(&bf_msg.encode()).await.is_err() {
        return;
    }

    if socket
        .write_all(&PeerMessage::Unchoke.encode())
        .await
        .is_err()
    {
        return;
    }

    loop {
        let mut len_bytes = [0u8; 4];
        if socket.read_exact(&mut len_bytes).await.is_err() {
            break;
        }
        let len = u32::from_be_bytes(len_bytes);
        if len == 0 {
            continue; // Keep-alive
        }
        if len > 131_072 {
            break;
        }
        let mut body = vec![0u8; len as usize];
        if socket.read_exact(&mut body).await.is_err() {
            break;
        }

        let msg = match PeerMessage::decode_payload(len, &body) {
            Ok(m) => m,
            Err(_) => break,
        };

        match msg {
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                let start = (index as usize * torrent.piece_size as usize) + begin as usize;
                let end = start + length as usize;
                if end <= torrent.payload.len() {
                    let block = torrent.payload[start..end].to_vec();
                    let piece_msg = PeerMessage::Piece {
                        index,
                        begin,
                        block,
                    };
                    if socket.write_all(&piece_msg.encode()).await.is_err() {
                        break;
                    }
                }
            }
            PeerMessage::Have { piece_index } => {
                if let Ok(mut rec) = have_recorded.lock() {
                    rec.push((piece_index, client_hs.info_hash));
                }
            }
            PeerMessage::Interested => {
                let _ = socket.write_all(&PeerMessage::Unchoke.encode()).await;
            }
            _ => {}
        }
    }
}

async fn wait_for_task_state(
    controller: &Arc<TaskController>,
    task_id: i64,
    expected_state: TaskState,
    timeout: Duration,
) -> Result<TaskRecord, String> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            let last_state = controller
                .database
                .lock()
                .unwrap()
                .get_task_by_id(task_id)
                .ok()
                .flatten()
                .map(|t| format!("{:?}", t.status))
                .unwrap_or_else(|| "Unknown".to_string());
            return Err(format!(
                "Task {} timed out waiting for {:?}, current status: {}",
                task_id, expected_state, last_state
            ));
        }

        let task_opt = {
            let db = controller.database.lock().unwrap();
            db.get_task_by_id(task_id).unwrap()
        };

        if let Some(task) = task_opt {
            if task.status == expected_state {
                return Ok(task);
            }
            if task.status == TaskState::FatalError {
                return Err(format!("Task {} failed with FatalError", task_id));
            }
        }

        tokio::time::sleep(Duration::from_millis(15)).await;
    }
}

#[tokio::test]
async fn test_milestone_m4_feature_complete_exit_gate() {
    let gate_start = Instant::now();
    println!("\n================================================================================");
    println!("MILESTONE M4 FEATURE COMPLETE EXIT GATE INTEGRATION TEST");
    println!("================================================================================");

    let temp = TempDirGuard::new("m4_gate");
    let dl_dir = temp.path.join("downloads");
    std::fs::create_dir_all(&dl_dir).expect("create downloads directory");

    let mock_server = MockIntegratedServer::start().await;
    println!(
        "Mock Integrated Server listening at HTTP: {}, TCP Peer: {}",
        mock_server.http_url, mock_server.peer_port
    );

    // -------------------------------------------------------------------------
    // Phase 1: Daemon Composition Root Boot & Database Initialization
    // -------------------------------------------------------------------------
    println!("\n--- [Phase 1] Daemon Composition Root Boot & Database Initialization ---");
    let boot_start = Instant::now();
    let unique_endpoint = endpoint_with_name(&format!(
        "ft_m4_gate_{}_{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let mut config = DaemonConfig::new(&temp.path).with_endpoint(&unique_endpoint);
    config.max_active_tasks = 10;
    let daemon = DaemonEngine::start(config)
        .await
        .expect("DaemonEngine::start failed");
    let boot_duration = boot_start.elapsed();

    println!(
        "Daemon engine composition root boot duration: {:?}",
        boot_duration
    );
    assert!(
        boot_duration < Duration::from_millis(1000),
        "Daemon boot latency exceeded budget: {:?}",
        boot_duration
    );

    daemon
        .quick_check()
        .expect("SQLite PRAGMA quick_check failed");
    let journal_mode: String = {
        let db = daemon.database.lock().unwrap();
        db.connection()
            .query_row("PRAGMA journal_mode;", [], |row| row.get(0))
            .expect("query journal_mode")
    };
    assert_eq!(
        journal_mode.to_lowercase(),
        "wal",
        "Database must operate in WAL journal mode"
    );
    assert!(
        daemon.memory_pool.available_slabs() > 0,
        "Slab memory pool must be initialized and ready"
    );
    println!("Phase 1 Gate PASS: WAL database initialized and memory pool ready.");

    // -------------------------------------------------------------------------
    // Phase 2: End-to-End HLS Media Stream Capture (AES-128 Decryption)
    // -------------------------------------------------------------------------
    println!("\n--- [Phase 2] End-to-End HLS Media Stream Capture (AES-128 Decryption) ---");
    let hls_key: [u8; 16] = [
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f,
    ];
    let hls_iv: [u8; 16] = [
        0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae,
        0xaf,
    ];

    const HLS_CHUNKS: usize = 4;
    const TS_PACKETS_PER_CHUNK: usize = 2;
    let mut hls_expected_plaintext = Vec::new();

    let mut media_m3u8 = format!(
        "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x{}\n",
        to_hex(&hls_iv)
    );

    for i in 0..HLS_CHUNKS {
        let seg_name = format!("seg_{i}.ts");
        media_m3u8.push_str(&format!("#EXTINF:6.0,\n{seg_name}\n"));

        let mut pt = Vec::with_capacity(TS_PACKETS_PER_CHUNK * 188);
        for p in 0..TS_PACKETS_PER_CHUNK {
            let pkt = create_valid_ts_packet(((i * TS_PACKETS_PER_CHUNK + p) % 256) as u8);
            pt.extend_from_slice(&pkt);
            hls_expected_plaintext.extend_from_slice(&pkt);
        }

        let ct = encrypt_aes128_cbc(&pt, &hls_key, &hls_iv);
        mock_server
            .register_route(format!("hls/{seg_name}"), "video/mp2t", ct, false)
            .await;
    }
    media_m3u8.push_str("#EXT-X-ENDLIST\n");

    let master_m3u8 = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360\nlow.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1280x720\nmedia.m3u8\n";
    mock_server
        .register_route(
            "hls/master.m3u8",
            "application/vnd.apple.mpegurl",
            master_m3u8.as_bytes().to_vec(),
            false,
        )
        .await;
    mock_server
        .register_route(
            "hls/low.m3u8",
            "application/vnd.apple.mpegurl",
            media_m3u8.as_bytes().to_vec(),
            false,
        )
        .await;
    mock_server
        .register_route(
            "hls/media.m3u8",
            "application/vnd.apple.mpegurl",
            media_m3u8.as_bytes().to_vec(),
            false,
        )
        .await;
    mock_server
        .register_route(
            "hls/key.bin",
            "application/octet-stream",
            hls_key.to_vec(),
            false,
        )
        .await;

    let hls_expected_sha256 = sha256(&hls_expected_plaintext);
    println!(
        "HLS Expected Plaintext Bytes: {}, SHA-256: {}",
        hls_expected_plaintext.len(),
        to_hex(&hls_expected_sha256)
    );

    let hls_dest = dl_dir.join("hls_capture.mp4");
    let hls_master_url = format!("{}/hls/master.m3u8", mock_server.http_url);

    let hls_task = daemon
        .controller
        .add_task(
            &hls_master_url,
            Some(&hls_dest.to_string_lossy()),
            Some("hls_capture.mp4"),
            Some(4),
            None,
        )
        .await
        .expect("add hls task failed");

    assert_eq!(hls_task.protocol, "hls");

    wait_for_task_state(
        &daemon.controller,
        hls_task.id,
        TaskState::Completed,
        Duration::from_secs(5),
    )
    .await
    .expect("hls task failed to complete");

    assert!(hls_dest.is_file(), "HLS output file must exist");
    let hls_actual_bytes = std::fs::read(&hls_dest).expect("read hls output file");
    let hls_actual_sha256 = sha256(&hls_actual_bytes);
    assert_eq!(
        hls_actual_sha256, hls_expected_sha256,
        "HLS decrypted stream SHA-256 parity mismatch"
    );

    let hls_staging_dir = PathBuf::from(format!(
        "{}.fetchtrek_parts_{}",
        hls_dest.display(),
        hls_task.id
    ));
    assert!(
        !hls_staging_dir.exists(),
        "HLS staging directory must be purged on completion"
    );
    println!("Phase 2 Gate PASS: 100% SHA-256 match on AES-128 decrypted HLS stream.");

    // -------------------------------------------------------------------------
    // Phase 3: End-to-End MPEG-DASH Stream Capture (fMP4 Box Assembly)
    // -------------------------------------------------------------------------
    println!("\n--- [Phase 3] End-to-End MPEG-DASH Stream Capture (fMP4 Box Assembly) ---");
    let fmp4_init = create_synthetic_fmp4_init();
    let fmp4_frag1 = create_synthetic_fmp4_fragment(1, 256);
    let fmp4_frag2 = create_synthetic_fmp4_fragment(2, 256);
    let fmp4_frag3 = create_synthetic_fmp4_fragment(3, 256);

    let mut dash_expected_bytes = Vec::new();
    dash_expected_bytes.extend_from_slice(&fmp4_init);
    dash_expected_bytes.extend_from_slice(&fmp4_frag1);
    dash_expected_bytes.extend_from_slice(&fmp4_frag2);
    dash_expected_bytes.extend_from_slice(&fmp4_frag3);
    let dash_expected_sha256 = sha256(&dash_expected_bytes);

    let mpd_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"
     mediaPresentationDuration="PT0H0M6.00S"
     type="static">
  <Period id="1">
    <AdaptationSet contentType="video" mimeType="video/mp4">
      <SegmentTemplate initialization="init_$RepresentationID$.mp4"
                       media="chunk_$RepresentationID$_$Number$.m4s"
                       startNumber="1"
                       timescale="1000"
                       duration="2000"/>
      <Representation id="1080p" bandwidth="5000000" width="1920" height="1080" codecs="avc1.640028"/>
    </AdaptationSet>
  </Period>
</MPD>
"#;

    mock_server
        .register_route(
            "dash/manifest.mpd",
            "application/dash+xml",
            mpd_content.as_bytes().to_vec(),
            false,
        )
        .await;
    mock_server
        .register_route("dash/init_1080p.mp4", "video/mp4", fmp4_init, false)
        .await;
    mock_server
        .register_route("dash/chunk_1080p_1.m4s", "video/mp4", fmp4_frag1, false)
        .await;
    mock_server
        .register_route("dash/chunk_1080p_2.m4s", "video/mp4", fmp4_frag2, false)
        .await;
    mock_server
        .register_route("dash/chunk_1080p_3.m4s", "video/mp4", fmp4_frag3, false)
        .await;

    let dash_dest = dl_dir.join("dash_capture.mp4");
    let dash_manifest_url = format!("{}/dash/manifest.mpd", mock_server.http_url);

    let dash_task = daemon
        .controller
        .add_task(
            &dash_manifest_url,
            Some(&dash_dest.to_string_lossy()),
            Some("dash_capture.mp4"),
            Some(3),
            None,
        )
        .await
        .expect("add dash task failed");

    assert_eq!(dash_task.protocol, "dash");

    wait_for_task_state(
        &daemon.controller,
        dash_task.id,
        TaskState::Completed,
        Duration::from_secs(5),
    )
    .await
    .expect("dash task failed to complete");

    assert!(dash_dest.is_file(), "DASH output file must exist");
    let dash_actual_bytes = std::fs::read(&dash_dest).expect("read dash output file");
    let dash_actual_sha256 = sha256(&dash_actual_bytes);
    assert_eq!(
        dash_actual_sha256, dash_expected_sha256,
        "DASH assembled fMP4 container SHA-256 parity mismatch"
    );

    let dash_staging_dir = PathBuf::from(format!(
        "{}.fetchtrek_parts_{}",
        dash_dest.display(),
        dash_task.id
    ));
    assert!(
        !dash_staging_dir.exists(),
        "DASH staging directory must be purged on completion"
    );
    println!("Phase 3 Gate PASS: 100% SHA-256 match on assembled fMP4 DASH stream.");

    // -------------------------------------------------------------------------
    // Phase 4: End-to-End BitTorrent Swarm Transfer & Seeding Handshake
    // -------------------------------------------------------------------------
    println!("\n--- [Phase 4] End-to-End BitTorrent Swarm Transfer & Seeding Handshake ---");
    const PIECE_SIZE: u32 = 32_768;
    const PIECE_COUNT: usize = 4;
    const TORRENT_TOTAL_SIZE: u64 = (PIECE_SIZE as u64) * (PIECE_COUNT as u64); // 128 KiB

    let mut torrent_payload = vec![0u8; TORRENT_TOTAL_SIZE as usize];
    for (i, b) in torrent_payload.iter_mut().enumerate() {
        *b = (((i.wrapping_mul(37)).wrapping_add(19)) % 256) as u8;
    }
    let torrent_expected_sha256 = sha256(&torrent_payload);

    let mut piece_hashes = Vec::with_capacity(PIECE_COUNT);
    for p in 0..PIECE_COUNT {
        let start = p * (PIECE_SIZE as usize);
        let end = start + (PIECE_SIZE as usize);
        piece_hashes.push(sha1(&torrent_payload[start..end]));
    }

    let torrent_file_path = temp.path.join("p4_sample.torrent");
    let tracker_announce_url = format!("{}/announce", mock_server.http_url);
    let p4_info_hash = create_test_torrent_file(
        &torrent_file_path,
        "p4_torrent_output.bin",
        &tracker_announce_url,
        PIECE_SIZE,
        &piece_hashes,
        TORRENT_TOTAL_SIZE,
    );

    mock_server
        .register_torrent(
            p4_info_hash,
            PIECE_COUNT,
            PIECE_SIZE,
            Arc::new(torrent_payload.clone()),
        )
        .await;

    let torrent_dest = dl_dir.join("p4_torrent_output.bin");
    let torrent_task = daemon
        .controller
        .add_task(
            &torrent_file_path.to_string_lossy(),
            Some(&torrent_dest.to_string_lossy()),
            Some("p4_torrent_output.bin"),
            Some(4),
            None,
        )
        .await
        .expect("add torrent task failed");

    assert_eq!(torrent_task.protocol, "bittorrent");

    wait_for_task_state(
        &daemon.controller,
        torrent_task.id,
        TaskState::Completed,
        Duration::from_secs(5),
    )
    .await
    .expect("torrent task failed to complete");

    assert!(torrent_dest.is_file(), "BitTorrent output file must exist");
    let torrent_actual_bytes = std::fs::read(&torrent_dest).expect("read torrent output file");
    let torrent_actual_sha256 = sha256(&torrent_actual_bytes);
    assert_eq!(
        torrent_actual_sha256, torrent_expected_sha256,
        "BitTorrent payload SHA-256 parity mismatch"
    );

    let recorded_haves = mock_server.get_recorded_haves_for(p4_info_hash);
    for p in 0..PIECE_COUNT as u32 {
        assert!(
            recorded_haves.contains(&p),
            "Mock peer must have received Have message for piece {}",
            p
        );
    }
    println!("Phase 4 Gate PASS: 100% SHA-256 match on 128 KiB BitTorrent swarm transfer with all 4 pieces verified.");

    // -------------------------------------------------------------------------
    // Phase 5: Mixed Multi-Protocol Concurrency & 10 Hz Telemetry Stress
    // -------------------------------------------------------------------------
    println!("\n--- [Phase 5] Mixed Multi-Protocol Concurrency & 10 Hz Telemetry Stress ---");
    // 1. HTTP payload (128 KiB)
    const HTTP_PAYLOAD_SIZE: usize = 128 * 1024;
    let mut http_payload = vec![0u8; HTTP_PAYLOAD_SIZE];
    for (i, b) in http_payload.iter_mut().enumerate() {
        *b = (((i.wrapping_mul(71)).wrapping_add(53)) % 256) as u8;
    }
    let p5_http_expected_sha256 = sha256(&http_payload);
    mock_server
        .register_route(
            "http/file_p5.bin",
            "application/octet-stream",
            http_payload.clone(),
            true,
        )
        .await;

    // 2. HLS stream (4 TS chunks)
    let p5_hls_key: [u8; 16] = [0x55; 16];
    let p5_hls_iv: [u8; 16] = [0x77; 16];
    let mut p5_hls_plaintext = Vec::new();
    let mut p5_media_m3u8 = format!(
        "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x{}\n",
        to_hex(&p5_hls_iv)
    );
    for i in 0..4 {
        let seg_name = format!("seg_p5_{i}.ts");
        p5_media_m3u8.push_str(&format!("#EXTINF:6.0,\n{seg_name}\n"));
        let mut pt = Vec::with_capacity(376);
        pt.extend_from_slice(&create_valid_ts_packet((i * 2 + 10) as u8));
        pt.extend_from_slice(&create_valid_ts_packet((i * 2 + 11) as u8));
        p5_hls_plaintext.extend_from_slice(&pt);
        let ct = encrypt_aes128_cbc(&pt, &p5_hls_key, &p5_hls_iv);
        mock_server
            .register_route(format!("hls_p5/{seg_name}"), "video/mp2t", ct, false)
            .await;
    }
    p5_media_m3u8.push_str("#EXT-X-ENDLIST\n");
    let p5_hls_expected_sha256 = sha256(&p5_hls_plaintext);

    mock_server
        .register_route(
            "hls_p5/master.m3u8",
            "application/vnd.apple.mpegurl",
            p5_media_m3u8.as_bytes().to_vec(),
            false,
        )
        .await;
    mock_server
        .register_route(
            "hls_p5/key.bin",
            "application/octet-stream",
            p5_hls_key.to_vec(),
            false,
        )
        .await;

    // 3. BitTorrent payload (128 KiB)
    let mut p5_torrent_payload = vec![0u8; TORRENT_TOTAL_SIZE as usize];
    for (i, b) in p5_torrent_payload.iter_mut().enumerate() {
        *b = (((i.wrapping_mul(83)).wrapping_add(11)) % 256) as u8;
    }
    let p5_torrent_expected_sha256 = sha256(&p5_torrent_payload);
    let mut p5_piece_hashes = Vec::with_capacity(PIECE_COUNT);
    for p in 0..PIECE_COUNT {
        let start = p * (PIECE_SIZE as usize);
        let end = start + (PIECE_SIZE as usize);
        p5_piece_hashes.push(sha1(&p5_torrent_payload[start..end]));
    }
    let p5_torrent_file = temp.path.join("p5_sample.torrent");
    let p5_info_hash = create_test_torrent_file(
        &p5_torrent_file,
        "p5_torrent_output.bin",
        &tracker_announce_url,
        PIECE_SIZE,
        &p5_piece_hashes,
        TORRENT_TOTAL_SIZE,
    );
    mock_server
        .register_torrent(
            p5_info_hash,
            PIECE_COUNT,
            PIECE_SIZE,
            Arc::new(p5_torrent_payload.clone()),
        )
        .await;

    let p5_http_dest = dl_dir.join("p5_http_file.bin");
    let p5_hls_dest = dl_dir.join("p5_hls_stream.mp4");
    let p5_torrent_dest = dl_dir.join("p5_torrent_output.bin");

    let t_http = daemon
        .controller
        .add_task(
            &format!("{}/http/file_p5.bin", mock_server.http_url),
            Some(&p5_http_dest.to_string_lossy()),
            Some("p5_http_file.bin"),
            Some(2),
            None,
        )
        .await
        .expect("add concurrent http task failed");

    let t_hls = daemon
        .controller
        .add_task(
            &format!("{}/hls_p5/master.m3u8", mock_server.http_url),
            Some(&p5_hls_dest.to_string_lossy()),
            Some("p5_hls_stream.mp4"),
            Some(2),
            None,
        )
        .await
        .expect("add concurrent hls task failed");

    let t_torrent = daemon
        .controller
        .add_task(
            &p5_torrent_file.to_string_lossy(),
            Some(&p5_torrent_dest.to_string_lossy()),
            Some("p5_torrent_output.bin"),
            Some(4),
            None,
        )
        .await
        .expect("add concurrent torrent task failed");

    // Monitor 10 Hz telemetry loop while all 3 run concurrently
    let stress_start = Instant::now();
    let mut observed_telemetry = false;
    let mut observed_active_conns = false;

    loop {
        if stress_start.elapsed() > Duration::from_secs(5) {
            panic!("Concurrency stress test timed out after 5s");
        }

        if let Ok(snapshot) = daemon.controller.collect_telemetry().await {
            if snapshot.active_task_count > 0 {
                observed_telemetry = true;
            }
            if snapshot.tasks.iter().any(|t| t.active_connections > 0) {
                observed_active_conns = true;
            }
        }

        let (s1, s2, s3) = {
            let db = daemon.database.lock().unwrap();
            let s1 = db.get_task_by_id(t_http.id).unwrap().map(|t| t.status);
            let s2 = db.get_task_by_id(t_hls.id).unwrap().map(|t| t.status);
            let s3 = db.get_task_by_id(t_torrent.id).unwrap().map(|t| t.status);
            (s1, s2, s3)
        };

        if s1 == Some(TaskState::FatalError)
            || s2 == Some(TaskState::FatalError)
            || s3 == Some(TaskState::FatalError)
        {
            panic!(
                "A concurrent task failed with FatalError: HTTP={:?}, HLS={:?}, Torrent={:?}",
                s1, s2, s3
            );
        }

        if s1 == Some(TaskState::Completed)
            && s2 == Some(TaskState::Completed)
            && s3 == Some(TaskState::Completed)
        {
            break;
        }

        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    assert!(
        observed_telemetry,
        "Telemetry snapshot must reflect active tasks during concurrent execution"
    );
    assert!(
        observed_active_conns,
        "Telemetry must observe active connections across worker pools"
    );

    let actual_http = std::fs::read(&p5_http_dest).expect("read concurrent http output");
    assert_eq!(
        sha256(&actual_http),
        p5_http_expected_sha256,
        "Concurrent HTTP SHA-256 parity mismatch"
    );

    let actual_hls = std::fs::read(&p5_hls_dest).expect("read concurrent hls output");
    assert_eq!(
        sha256(&actual_hls),
        p5_hls_expected_sha256,
        "Concurrent HLS SHA-256 parity mismatch"
    );

    let actual_torrent = std::fs::read(&p5_torrent_dest).expect("read concurrent torrent output");
    assert_eq!(
        sha256(&actual_torrent),
        p5_torrent_expected_sha256,
        "Concurrent BitTorrent SHA-256 parity mismatch"
    );

    println!("Phase 5 Gate PASS: HTTP, HLS and BitTorrent completed concurrently with verified telemetry and 100% SHA-256 parity.");

    // -------------------------------------------------------------------------
    // Phase 6: Session Teardown, Staging Cleanup & Clean WAL Checkpoint
    // -------------------------------------------------------------------------
    println!("\n--- [Phase 6] Session Teardown, Staging Cleanup & Clean WAL Checkpoint ---");
    daemon
        .shutdown()
        .await
        .expect("graceful daemon shutdown failed");

    // Verify all media staging directories are cleanly purged
    if let Ok(entries) = std::fs::read_dir(&dl_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            assert!(
                !name.contains(".fetchtrek_parts_"),
                "Staging directory {} was not purged",
                name
            );
        }
    }

    daemon
        .quick_check()
        .expect("SQLite clean checkpoint check failed");

    let total_elapsed = gate_start.elapsed();
    println!("Total Milestone M4 Gate Duration: {:?}", total_elapsed);
    assert!(
        total_elapsed < Duration::from_secs(4),
        "Milestone M4 gate exceeded 4.0s budget: {:?}",
        total_elapsed
    );
    println!(
        "Phase 6 Gate PASS: Staging purged, database cleanly checkpointed, total duration {:?} < 4.0s.",
        total_elapsed
    );
    println!("\n================================================================================");
    println!("ALL 6 PHASES PASSED - MILESTONE M4 FEATURE COMPLETE GATE PROVEN");
    println!("================================================================================\n");
}
