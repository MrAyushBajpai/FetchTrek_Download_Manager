use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_core::BandwidthLimiter;
use fetchtrek_daemon::controller::detect_stream_protocol;
use fetchtrek_daemon::{start_torrent_download, TaskController};
use fetchtrek_persistence::Database;
use fetchtrek_protocols::{sha1, BencodeValue, PeerHandshake, PeerMessage};
use fetchtrek_storage::{PositionalStorage, SlabPool};
use fetchtrek_types::TaskState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};

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
            "fetchtrek_torrent_ctrl_{}_{}_{}",
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

/// Creates a valid single-file BEP 0003 .torrent file on disk.
fn create_test_torrent_file(
    path: &Path,
    name: &str,
    tracker_url: &str,
    piece_length: u32,
    pieces: &[[u8; 20]],
    total_length: u64,
) {
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

    let mut root = BTreeMap::new();
    root.insert(
        b"announce".to_vec(),
        BencodeValue::Bytes(tracker_url.as_bytes().to_vec()),
    );
    root.insert(b"info".to_vec(), BencodeValue::Dict(info));

    let encoded = BencodeValue::Dict(root).encode();
    std::fs::write(path, encoded).expect("write torrent file");
}

/// Spawns a mock HTTP tracker that serves the given peer port in BEP 0023 compact format.
async fn spawn_mock_tracker(peer_port: u16) -> (String, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tracker listener");
    let port = listener.local_addr().expect("local addr").port();
    let tracker_url = format!("http://127.0.0.1:{port}/announce");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                res = listener.accept() => {
                    let (mut socket, _) = match res {
                        Ok(s) => s,
                        Err(_) => break,
                    };

                    let mut buf = vec![0u8; 2048];
                    let _ = socket.read(&mut buf).await;

                    // Response with compact peer 127.0.0.1:peer_port
                    let mut peer_bytes = Vec::new();
                    peer_bytes.extend_from_slice(&[127, 0, 0, 1]);
                    peer_bytes.extend_from_slice(&peer_port.to_be_bytes());

                    let mut resp_dict = BTreeMap::new();
                    resp_dict.insert(b"interval".to_vec(), BencodeValue::Integer(1800));
                    resp_dict.insert(b"peers".to_vec(), BencodeValue::Bytes(peer_bytes));
                    let body = BencodeValue::Dict(resp_dict).encode();

                    let http_resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(http_resp.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                    let _ = socket.shutdown().await;
                }
            }
        }
    });

    (tracker_url, shutdown_tx)
}

/// Test 1: BitTorrent protocol auto-detection.
#[test]
fn test_torrent_protocol_detection() {
    assert_eq!(
        detect_stream_protocol(
            "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
            None
        ),
        Some("bittorrent")
    );
    assert_eq!(
        detect_stream_protocol(
            "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335de7ece74f6d2&dn=Ubuntu",
            None
        ),
        Some("bittorrent")
    );
    assert_eq!(
        detect_stream_protocol("http://example.com/archive.torrent", None),
        Some("bittorrent")
    );
    assert_eq!(
        detect_stream_protocol("C:\\downloads\\linux_iso.torrent", None),
        Some("bittorrent")
    );
    assert_eq!(
        detect_stream_protocol("/var/data/dataset.torrent", None),
        Some("bittorrent")
    );
    assert_eq!(
        detect_stream_protocol(
            "https://example.com/download/file",
            Some("application/x-bittorrent")
        ),
        Some("bittorrent")
    );

    // Standard media and HTTP remains unaffected
    assert_eq!(
        detect_stream_protocol("https://example.com/live/stream.m3u8", None),
        Some("hls")
    );
    assert_eq!(
        detect_stream_protocol("https://example.com/dash/manifest.mpd", None),
        Some("dash")
    );
    assert_eq!(
        detect_stream_protocol("https://example.com/archive.zip", None),
        None
    );
}

/// Test 2: End-to-end BitTorrent session with mock tracker and mock peer.
#[tokio::test]
async fn test_torrent_session_mock_swarm_e2e() {
    let temp = TempDirGuard::new("e2e_swarm");
    let db_path = temp.path.join("test.db");
    let download_dir = temp.path.join("downloads");
    std::fs::create_dir_all(&download_dir).expect("create download dir");

    let db = Database::open(&db_path).expect("open db");
    let database = Arc::new(std::sync::Mutex::new(db));
    let storage = Arc::new(PositionalStorage::new());
    let memory_pool = SlabPool::new();
    let limiter = BandwidthLimiter::unlimited();
    let transport = Arc::new(fetchtrek_protocols::HttpTransport::new());

    let controller = Arc::new(TaskController::new(
        database.clone(),
        storage,
        memory_pool,
        limiter,
        transport,
        5,
        download_dir.clone(),
    ));

    // 2 pieces of 32 KiB each = 64 KiB total
    const PIECE_SIZE: u32 = 32_768;
    const TOTAL_SIZE: u64 = 65_536;

    let mut payload = vec![0u8; TOTAL_SIZE as usize];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }

    let p0 = &payload[0..32_768];
    let p1 = &payload[32_768..65_536];
    let h0 = sha1(p0);
    let h1 = sha1(p1);

    // Bind mock peer TCP listener
    let peer_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind peer listener");
    let peer_port = peer_listener.local_addr().expect("peer addr").port();

    // Spawn mock tracker pointing to mock peer
    let (tracker_url, _tracker_shutdown) = spawn_mock_tracker(peer_port).await;

    // Create .torrent file
    let torrent_file = temp.path.join("sample.torrent");
    create_test_torrent_file(
        &torrent_file,
        "sample_output.bin",
        &tracker_url,
        PIECE_SIZE,
        &[h0, h1],
        TOTAL_SIZE,
    );

    let output_file = download_dir.join("sample_output.bin");

    // Spawn mock peer worker
    let payload_clone = payload.clone();
    let mock_peer_handle = tokio::spawn(async move {
        let (mut socket, _) = peer_listener.accept().await.expect("peer accept");

        // Handshake
        let client_hs = PeerHandshake::read_from(&mut socket, None)
            .await
            .expect("read client handshake");
        let server_hs = PeerHandshake::new(client_hs.info_hash, *b"-FT0001-MOCKPEER0001");
        server_hs
            .write_to(&mut socket)
            .await
            .expect("write server handshake");

        // Send bitfield (2 pieces available: byte 0 = 0xC0)
        let bf_msg = PeerMessage::Bitfield(vec![0xC0]);
        socket
            .write_all(&bf_msg.encode())
            .await
            .expect("send bitfield");

        // Send unchoke
        socket
            .write_all(&PeerMessage::Unchoke.encode())
            .await
            .expect("send unchoke");

        let mut received_have = Vec::new();

        loop {
            let mut len_bytes = [0u8; 4];
            if socket.read_exact(&mut len_bytes).await.is_err() {
                break;
            }
            let len = u32::from_be_bytes(len_bytes);
            if len == 0 {
                continue;
            }
            let mut body = vec![0u8; len as usize];
            socket
                .read_exact(&mut body)
                .await
                .expect("read message body");

            let msg = PeerMessage::decode_payload(len, &body).expect("decode message");
            match msg {
                PeerMessage::Request {
                    index,
                    begin,
                    length,
                } => {
                    let start = (index as usize * PIECE_SIZE as usize) + begin as usize;
                    let end = start + length as usize;
                    let block = payload_clone[start..end].to_vec();
                    let piece_msg = PeerMessage::Piece {
                        index,
                        begin,
                        block,
                    };
                    socket
                        .write_all(&piece_msg.encode())
                        .await
                        .expect("send piece");
                }
                PeerMessage::Have { piece_index } => {
                    received_have.push(piece_index);
                    if received_have.len() >= 2 {
                        // Received Have for all pieces
                        break;
                    }
                }
                _ => {}
            }
        }

        received_have
    });

    // Add and start task in controller
    let task = controller
        .add_task(
            torrent_file.to_str().unwrap(),
            Some(output_file.to_str().unwrap()),
            Some("sample_output.bin"),
            Some(4),
            None,
        )
        .await
        .expect("add task");

    let downloaded_bytes = Arc::new(AtomicU64::new(0));
    let speed_bps = Arc::new(AtomicU64::new(0));
    let active_connections = Arc::new(AtomicU16::new(0));
    let (_cancel_tx, cancel_rx) = watch::channel(false);

    let session_handle = start_torrent_download(
        &controller,
        task.clone(),
        cancel_rx,
        downloaded_bytes,
        speed_bps,
        active_connections,
    );

    // Wait for session and peer to finish
    let _ = session_handle.await;
    let haves = mock_peer_handle.await.expect("join mock peer");

    assert!(haves.contains(&0));
    assert!(haves.contains(&1));

    // Verify task completion state in database
    let db_task = database
        .lock()
        .unwrap()
        .get_task_by_id(task.id)
        .unwrap()
        .expect("task in db");
    assert_eq!(db_task.status, TaskState::Completed);

    // Verify written file bit parity
    let on_disk = std::fs::read(&output_file).expect("read on-disk output file");
    assert_eq!(on_disk.len(), TOTAL_SIZE as usize);
    assert_eq!(on_disk, payload);
}

/// Test 3: Pause and idempotent resumption from SQLite piece records.
#[tokio::test]
async fn test_torrent_session_pause_and_resume() {
    let temp = TempDirGuard::new("pause_resume");
    let db_path = temp.path.join("test.db");
    let download_dir = temp.path.join("downloads");
    std::fs::create_dir_all(&download_dir).expect("create download dir");

    let db = Database::open(&db_path).expect("open db");
    let database = Arc::new(std::sync::Mutex::new(db));
    let storage = Arc::new(PositionalStorage::new());
    let memory_pool = SlabPool::new();
    let limiter = BandwidthLimiter::unlimited();
    let transport = Arc::new(fetchtrek_protocols::HttpTransport::new());

    let controller = Arc::new(TaskController::new(
        database.clone(),
        storage,
        memory_pool,
        limiter,
        transport,
        5,
        download_dir.clone(),
    ));

    const PIECE_SIZE: u32 = 32_768;
    const TOTAL_SIZE: u64 = 65_536;

    let payload = vec![0x33u8; TOTAL_SIZE as usize];
    let h0 = sha1(&payload[0..32_768]);
    let h1 = sha1(&payload[32_768..65_536]);

    let peer_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind peer listener");
    let peer_port = peer_listener.local_addr().expect("peer addr").port();
    let (tracker_url, _tracker_shutdown) = spawn_mock_tracker(peer_port).await;

    let torrent_file = temp.path.join("pause.torrent");
    create_test_torrent_file(
        &torrent_file,
        "pause_output.bin",
        &tracker_url,
        PIECE_SIZE,
        &[h0, h1],
        TOTAL_SIZE,
    );

    let output_file = download_dir.join("pause_output.bin");

    let (pause_signal_tx, pause_signal_rx) = oneshot::channel();
    let mut pause_tx_opt = Some(pause_signal_tx);
    let payload_clone = payload.clone();

    // Mock peer serves Piece 0, then triggers pause signal
    tokio::spawn(async move {
        let (mut socket, _) = peer_listener.accept().await.expect("peer accept");
        let client_hs = PeerHandshake::read_from(&mut socket, None).await.unwrap();
        let server_hs = PeerHandshake::new(client_hs.info_hash, *b"-FT0001-MOCKPEER0001");
        server_hs.write_to(&mut socket).await.unwrap();

        socket
            .write_all(&PeerMessage::Bitfield(vec![0xC0]).encode())
            .await
            .unwrap();
        socket
            .write_all(&PeerMessage::Unchoke.encode())
            .await
            .unwrap();

        let mut sent_pause = false;

        loop {
            let mut len_bytes = [0u8; 4];
            if socket.read_exact(&mut len_bytes).await.is_err() {
                break;
            }
            let len = u32::from_be_bytes(len_bytes);
            if len == 0 {
                continue;
            }
            let mut body = vec![0u8; len as usize];
            socket.read_exact(&mut body).await.unwrap();

            let msg = PeerMessage::decode_payload(len, &body).unwrap();
            match msg {
                PeerMessage::Request {
                    index,
                    begin,
                    length,
                } => {
                    if index != 0 {
                        continue;
                    }
                    let start = (index as usize * PIECE_SIZE as usize) + begin as usize;
                    let end = start + length as usize;
                    let block = payload_clone[start..end].to_vec();
                    socket
                        .write_all(
                            &PeerMessage::Piece {
                                index,
                                begin,
                                block,
                            }
                            .encode(),
                        )
                        .await
                        .unwrap();
                }
                PeerMessage::Have { piece_index: 0 } if !sent_pause => {
                    sent_pause = true;
                    if let Some(tx) = pause_tx_opt.take() {
                        let _ = tx.send(());
                    }
                }
                _ => {}
            }
        }
    });

    let task = controller
        .add_task(
            torrent_file.to_str().unwrap(),
            Some(output_file.to_str().unwrap()),
            Some("pause_output.bin"),
            Some(4),
            None,
        )
        .await
        .expect("add task");

    let downloaded_bytes = Arc::new(AtomicU64::new(0));
    let speed_bps = Arc::new(AtomicU64::new(0));
    let active_connections = Arc::new(AtomicU16::new(0));
    let (cancel_tx, cancel_rx) = watch::channel(false);

    let session_handle = start_torrent_download(
        &controller,
        task.clone(),
        cancel_rx,
        downloaded_bytes.clone(),
        speed_bps.clone(),
        active_connections.clone(),
    );

    // Wait for Piece 0 to complete, then signal cancel/pause
    let _ = pause_signal_rx.await;
    cancel_tx.send(true).expect("send cancel");
    let _ = session_handle.await;

    // Verify task state is Paused in DB and downloaded_bytes >= 32 KiB
    let db_task = database
        .lock()
        .unwrap()
        .get_task_by_id(task.id)
        .unwrap()
        .unwrap();
    assert_eq!(db_task.status, TaskState::Paused);
    assert!(db_task.downloaded_bytes >= 32_768);

    // Second stage: Resume download with a peer that only serves Piece 1
    let peer_listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_port2 = peer_listener2.local_addr().unwrap().port();
    let (tracker_url2, _ts2) = spawn_mock_tracker(peer_port2).await;

    // Overwrite torrent announce to point to tracker 2
    create_test_torrent_file(
        &torrent_file,
        "pause_output.bin",
        &tracker_url2,
        PIECE_SIZE,
        &[h0, h1],
        TOTAL_SIZE,
    );

    let payload_clone2 = payload.clone();
    let peer2_handle = tokio::spawn(async move {
        let (mut socket, _) = peer_listener2.accept().await.unwrap();
        let client_hs = PeerHandshake::read_from(&mut socket, None).await.unwrap();
        let server_hs = PeerHandshake::new(client_hs.info_hash, *b"-FT0001-MOCKPEER0002");
        server_hs.write_to(&mut socket).await.unwrap();

        socket
            .write_all(&PeerMessage::Bitfield(vec![0xC0]).encode())
            .await
            .unwrap();
        socket
            .write_all(&PeerMessage::Unchoke.encode())
            .await
            .unwrap();

        let mut requested_indices = Vec::new();

        loop {
            let mut len_bytes = [0u8; 4];
            if socket.read_exact(&mut len_bytes).await.is_err() {
                break;
            }
            let len = u32::from_be_bytes(len_bytes);
            if len == 0 {
                continue;
            }
            let mut body = vec![0u8; len as usize];
            socket.read_exact(&mut body).await.unwrap();

            let msg = PeerMessage::decode_payload(len, &body).unwrap();
            match msg {
                PeerMessage::Request {
                    index,
                    begin,
                    length,
                } => {
                    requested_indices.push(index);
                    let start = (index as usize * PIECE_SIZE as usize) + begin as usize;
                    let end = start + length as usize;
                    let block = payload_clone2[start..end].to_vec();
                    socket
                        .write_all(
                            &PeerMessage::Piece {
                                index,
                                begin,
                                block,
                            }
                            .encode(),
                        )
                        .await
                        .unwrap();
                }
                PeerMessage::Have { piece_index: 1 } => {
                    break;
                }
                _ => {}
            }
        }

        requested_indices
    });

    let (_cancel_tx2, cancel_rx2) = watch::channel(false);
    let session_handle2 = start_torrent_download(
        &controller,
        db_task,
        cancel_rx2,
        downloaded_bytes,
        speed_bps,
        active_connections,
    );

    let _ = session_handle2.await;
    let requests = peer2_handle.await.expect("join peer 2");

    // Invariant: Resumed session must NEVER re-request Piece 0!
    assert!(
        requests.iter().all(|&idx| idx == 1),
        "Resumed session should only request Piece 1, but requested: {requests:?}"
    );

    let on_disk = std::fs::read(&output_file).expect("read resumed file");
    assert_eq!(on_disk, payload);
}

/// Test 4: Corrupt block detection and transparent recovery.
#[tokio::test]
async fn test_torrent_session_corrupt_block_recovery() {
    let temp = TempDirGuard::new("corrupt_recovery");
    let db_path = temp.path.join("test.db");
    let download_dir = temp.path.join("downloads");
    std::fs::create_dir_all(&download_dir).expect("create download dir");

    let db = Database::open(&db_path).expect("open db");
    let database = Arc::new(std::sync::Mutex::new(db));
    let storage = Arc::new(PositionalStorage::new());
    let memory_pool = SlabPool::new();
    let limiter = BandwidthLimiter::unlimited();
    let transport = Arc::new(fetchtrek_protocols::HttpTransport::new());

    let controller = Arc::new(TaskController::new(
        database.clone(),
        storage,
        memory_pool,
        limiter,
        transport,
        5,
        download_dir.clone(),
    ));

    const PIECE_SIZE: u32 = 16_384;
    const TOTAL_SIZE: u64 = 16_384;

    let payload = vec![0xAAu8; TOTAL_SIZE as usize];
    let h0 = sha1(&payload);

    let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_port = peer_listener.local_addr().unwrap().port();
    let (tracker_url, _ts) = spawn_mock_tracker(peer_port).await;

    let torrent_file = temp.path.join("corrupt.torrent");
    create_test_torrent_file(
        &torrent_file,
        "corrupt_output.bin",
        &tracker_url,
        PIECE_SIZE,
        &[h0],
        TOTAL_SIZE,
    );

    let output_file = download_dir.join("corrupt_output.bin");

    // Mock peer serves poisoned block on first attempt, then valid block on second attempt
    let payload_clone = payload.clone();
    tokio::spawn(async move {
        let (mut socket, _) = peer_listener.accept().await.unwrap();
        let client_hs = PeerHandshake::read_from(&mut socket, None).await.unwrap();
        let server_hs = PeerHandshake::new(client_hs.info_hash, *b"-FT0001-MOCKPEER0001");
        server_hs.write_to(&mut socket).await.unwrap();

        socket
            .write_all(&PeerMessage::Bitfield(vec![0x80]).encode())
            .await
            .unwrap();
        socket
            .write_all(&PeerMessage::Unchoke.encode())
            .await
            .unwrap();

        let mut attempt = 0;

        loop {
            let mut len_bytes = [0u8; 4];
            if socket.read_exact(&mut len_bytes).await.is_err() {
                break;
            }
            let len = u32::from_be_bytes(len_bytes);
            if len == 0 {
                continue;
            }
            let mut body = vec![0u8; len as usize];
            socket.read_exact(&mut body).await.unwrap();

            let msg = PeerMessage::decode_payload(len, &body).unwrap();
            match msg {
                PeerMessage::Request {
                    index,
                    begin,
                    length: _,
                } => {
                    attempt += 1;
                    let mut block = payload_clone.clone();
                    if attempt == 1 {
                        // Poison first attempt
                        block[0] ^= 0xFF;
                    }
                    socket
                        .write_all(
                            &PeerMessage::Piece {
                                index,
                                begin,
                                block,
                            }
                            .encode(),
                        )
                        .await
                        .unwrap();
                }
                PeerMessage::Have { piece_index: 0 } => {
                    break;
                }
                _ => {}
            }
        }
    });

    let task = controller
        .add_task(
            torrent_file.to_str().unwrap(),
            Some(output_file.to_str().unwrap()),
            Some("corrupt_output.bin"),
            Some(4),
            None,
        )
        .await
        .expect("add task");

    let downloaded_bytes = Arc::new(AtomicU64::new(0));
    let speed_bps = Arc::new(AtomicU64::new(0));
    let active_connections = Arc::new(AtomicU16::new(0));
    let (_cancel_tx, cancel_rx) = watch::channel(false);

    let session_handle = start_torrent_download(
        &controller,
        task.clone(),
        cancel_rx,
        downloaded_bytes,
        speed_bps,
        active_connections,
    );

    let _ = session_handle.await;

    // Verify recovery succeeded and disk file has valid data
    let on_disk = std::fs::read(&output_file).expect("read output file");
    assert_eq!(on_disk, payload);
}

/// Test 5: Swarm session cancellation terminates within 50ms.
#[tokio::test]
async fn test_torrent_session_cancellation_latency() {
    let temp = TempDirGuard::new("cancel_latency");
    let db_path = temp.path.join("test.db");
    let download_dir = temp.path.join("downloads");
    std::fs::create_dir_all(&download_dir).expect("create download dir");

    let db = Database::open(&db_path).expect("open db");
    let database = Arc::new(std::sync::Mutex::new(db));
    let storage = Arc::new(PositionalStorage::new());
    let memory_pool = SlabPool::new();
    let limiter = BandwidthLimiter::unlimited();
    let transport = Arc::new(fetchtrek_protocols::HttpTransport::new());

    let controller = Arc::new(TaskController::new(
        database.clone(),
        storage,
        memory_pool,
        limiter,
        transport,
        5,
        download_dir.clone(),
    ));

    const PIECE_SIZE: u32 = 32_768;
    const TOTAL_SIZE: u64 = 65_536;

    let payload = vec![0x55u8; TOTAL_SIZE as usize];
    let h0 = sha1(&payload[0..32_768]);
    let h1 = sha1(&payload[32_768..65_536]);

    let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_port = peer_listener.local_addr().unwrap().port();
    let (tracker_url, _ts) = spawn_mock_tracker(peer_port).await;

    let torrent_file = temp.path.join("latency.torrent");
    create_test_torrent_file(
        &torrent_file,
        "latency_output.bin",
        &tracker_url,
        PIECE_SIZE,
        &[h0, h1],
        TOTAL_SIZE,
    );

    let output_file = download_dir.join("latency_output.bin");

    // Mock peer keeps connection open indefinitely
    tokio::spawn(async move {
        let (mut socket, _) = peer_listener.accept().await.unwrap();
        let client_hs = PeerHandshake::read_from(&mut socket, None).await.unwrap();
        let server_hs = PeerHandshake::new(client_hs.info_hash, *b"-FT0001-MOCKPEER0001");
        server_hs.write_to(&mut socket).await.unwrap();
        // Never unchoke, keep hanging
        let mut buf = [0u8; 1024];
        while socket.read(&mut buf).await.is_ok() {}
    });

    let task = controller
        .add_task(
            torrent_file.to_str().unwrap(),
            Some(output_file.to_str().unwrap()),
            Some("latency_output.bin"),
            Some(4),
            None,
        )
        .await
        .expect("add task");

    let downloaded_bytes = Arc::new(AtomicU64::new(0));
    let speed_bps = Arc::new(AtomicU64::new(0));
    let active_connections = Arc::new(AtomicU16::new(0));
    let (cancel_tx, cancel_rx) = watch::channel(false);

    let session_handle = start_torrent_download(
        &controller,
        task.clone(),
        cancel_rx,
        downloaded_bytes,
        speed_bps,
        active_connections,
    );

    // Let connection establish
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Measure cancellation latency
    let start_time = Instant::now();
    cancel_tx.send(true).expect("send cancel signal");
    let _ = session_handle.await;
    let elapsed = start_time.elapsed();

    assert!(
        elapsed < Duration::from_millis(500),
        "Cancellation latency must be under 500ms, took: {elapsed:?}"
    );
}
