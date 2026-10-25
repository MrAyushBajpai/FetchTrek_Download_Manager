use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fetchtrek_persistence::{rusqlite, TaskRecord, TaskSegmentRecord};
use fetchtrek_protocols::{
    AnnounceEvent, AnnounceRequest, MagnetLink, PeerConnection, PeerMessage, PickerStrategy,
    PiecePicker, TorrentDiskStorage, TorrentMeta, TrackerCoordinator,
};
use fetchtrek_types::TaskState;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio::task::JoinHandle;

use crate::controller::TaskController;
use crate::error::DaemonError;

const DAEMON_PEER_ID: [u8; 20] = *b"-FT0001-DAEMONPEER01";
const MAX_CONCURRENT_SWARM_PEERS: usize = 30;
const MAX_BLOCKS_PIPELINED_PER_PEER: usize = 8;
const BLOCK_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const TELEMETRY_TICK_INTERVAL: Duration = Duration::from_millis(100);

/// Resolves `TorrentMeta` from local file, HTTP URL, or existing metadata on disk.
async fn resolve_torrent_meta(
    url: &str,
    destination_path: &str,
    controller: &TaskController,
) -> Result<TorrentMeta, DaemonError> {
    if url.starts_with("magnet:?") {
        let magnet = MagnetLink::parse(url)
            .map_err(|e| DaemonError::Internal(format!("Invalid magnet URI: {e}")))?;

        let dest = Path::new(destination_path);
        let possible_meta_paths = [
            dest.to_path_buf(),
            dest.with_extension("torrent"),
            controller.default_download_dir.join(format!(
                "{}.torrent",
                magnet.display_name.as_deref().unwrap_or("download")
            )),
        ];

        for path in &possible_meta_paths {
            if path.is_file() {
                if let Ok(bytes) = tokio::fs::read(path).await {
                    if let Ok(meta) = TorrentMeta::from_bytes(&bytes) {
                        return Ok(meta);
                    }
                }
            }
        }

        return Err(DaemonError::Internal(format!(
            "Torrent metadata not found for magnet info_hash: {}",
            hex::encode(magnet.info_hash)
        )));
    }

    if url.starts_with("http://") || url.starts_with("https://") {
        let mut stream = controller
            .transport
            .open_stream(url, &fetchtrek_core::SessionContext::default())
            .await
            .map_err(|e| DaemonError::Internal(format!("Failed to open .torrent stream: {e}")))?;
        let mut bytes = Vec::new();
        stream
            .read_to_end(&mut bytes)
            .await
            .map_err(|e| DaemonError::Internal(format!("Failed to read .torrent stream: {e}")))?;
        return TorrentMeta::from_bytes(&bytes)
            .map_err(|e| DaemonError::Internal(format!("Failed to parse fetched .torrent: {e}")));
    }

    // Local file path
    let bytes = tokio::fs::read(url).await.map_err(|e| {
        DaemonError::Internal(format!("Failed to read local torrent file '{url}': {e}"))
    })?;
    TorrentMeta::from_bytes(&bytes)
        .map_err(|e| DaemonError::Internal(format!("Failed to parse torrent metainfo: {e}")))
}

/// Helper hex encoder avoiding extra external crate dependencies.
mod hex {
    pub fn encode(bytes: [u8; 20]) -> String {
        let mut s = String::with_capacity(40);
        for b in bytes {
            use std::fmt::Write;
            let _ = write!(s, "{:02x}", b);
        }
        s
    }
}

/// Orchestrates an asynchronous BitTorrent session including tracker announce,
/// swarm connection pool, piece picker, verification, and disk mapping.
pub fn start_torrent_download(
    controller: &Arc<TaskController>,
    task: TaskRecord,
    cancel_rx: watch::Receiver<bool>,
    downloaded_bytes: Arc<AtomicU64>,
    speed_bps: Arc<AtomicU64>,
    active_connections: Arc<AtomicU16>,
) -> JoinHandle<Result<(), DaemonError>> {
    let controller_clone = Arc::clone(controller);

    tokio::spawn(async move {
        if *cancel_rx.borrow() {
            return Ok(());
        }

        // 1. Resolve Metainfo
        let meta =
            resolve_torrent_meta(&task.url, &task.destination_path, &controller_clone).await?;

        // 2. Determine base directory and update total length in DB
        let dest_path = PathBuf::from(&task.destination_path);
        let base_dir = if dest_path.is_file() || dest_path.extension().is_some() {
            dest_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        } else {
            dest_path.clone()
        };

        {
            let db = controller_clone
                .database
                .lock()
                .map_err(|e| DaemonError::Internal(e.to_string()))?;
            let _ = db.connection().execute(
                "UPDATE tasks SET total_bytes = ?1 WHERE id = ?2",
                rusqlite::params![meta.total_length as i64, task.id],
            );
        }

        // 3. Initialize Storage & Pre-allocate files
        let storage = Arc::new(TorrentDiskStorage::new(base_dir, meta.clone()));
        storage.initialize_files().await.map_err(|e| {
            DaemonError::Internal(format!("Failed to initialize files on disk: {e}"))
        })?;

        // 4. PiecePicker & Persistence Resumption
        let num_pieces = meta.pieces.len();
        let mut picker = PiecePicker::new(
            num_pieces,
            meta.piece_length,
            meta.total_length,
            PickerStrategy::RarestFirst,
        );

        let existing_segments = {
            let db = controller_clone
                .database
                .lock()
                .map_err(|e| DaemonError::Internal(e.to_string()))?;
            db.get_segments(task.id)?
        };

        let mut already_downloaded = 0u64;
        for seg in existing_segments {
            // A segment where current_cursor > range_end represents a fully verified piece
            if seg.current_cursor > seg.range_end && (seg.segment_index as usize) < num_pieces {
                let idx = seg.segment_index;
                picker.mark_completed(idx);
                let p_size = picker.piece_size(idx) as u64;
                already_downloaded += p_size;
            }
        }
        downloaded_bytes.store(already_downloaded, Ordering::Release);

        if picker.is_all_completed() {
            let db = controller_clone
                .database
                .lock()
                .map_err(|e| DaemonError::Internal(e.to_string()))?;
            let _ = db.update_task_status(task.id, TaskState::Completed);
            return Ok(());
        }

        let picker = Arc::new(Mutex::new(picker));
        let connected_peers: Arc<RwLock<HashMap<SocketAddr, mpsc::Sender<PeerMessage>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // 5. Tracker Coordinator & Initial Announce
        let mut tracker_coord =
            TrackerCoordinator::new(meta.announce.as_deref(), &meta.announce_list);

        let announce_req = AnnounceRequest {
            info_hash: meta.info_hash,
            peer_id: DAEMON_PEER_ID,
            port: 6881,
            uploaded: 0,
            downloaded: downloaded_bytes.load(Ordering::Acquire),
            left: meta
                .total_length
                .saturating_sub(downloaded_bytes.load(Ordering::Acquire)),
            event: AnnounceEvent::Started,
            num_want: Some(50),
        };

        // Network error on announce triggers warning without session crash
        let peer_endpoints = match tracker_coord.announce(&announce_req).await {
            Ok(response) => response.peers,
            Err(e) => {
                eprintln!("[Daemon Torrent] Initial tracker announce error: {e}");
                Vec::new()
            }
        };

        // 6. Spawn Swarm Workers
        let mut worker_handles = Vec::new();
        let peer_limit = peer_endpoints.len().min(MAX_CONCURRENT_SWARM_PEERS);

        for endpoint in peer_endpoints.into_iter().take(peer_limit) {
            let peer_addr = SocketAddr::new(endpoint.ip, endpoint.port);
            let picker_clone = Arc::clone(&picker);
            let storage_clone = Arc::clone(&storage);
            let connected_peers_clone = Arc::clone(&connected_peers);
            let active_conns_clone = Arc::clone(&active_connections);
            let downloaded_bytes_clone = Arc::clone(&downloaded_bytes);
            let controller_for_worker = Arc::clone(&controller_clone);
            let meta_clone = meta.clone();
            let mut cancel_rx_worker = cancel_rx.clone();
            let task_id = task.id;

            let handle = tokio::spawn(async move {
                let mut conn = match PeerConnection::connect(
                    peer_addr,
                    meta_clone.info_hash,
                    DAEMON_PEER_ID,
                    num_pieces,
                    Duration::from_secs(5),
                )
                .await
                {
                    Ok(c) => c,
                    Err(_) => return,
                };

                active_conns_clone.fetch_add(1, Ordering::SeqCst);

                // Initial messages: announce available pieces via Bitfield and Interest
                {
                    let p_guard = picker_clone.lock().await;
                    let mut bf_bytes = vec![0u8; num_pieces.div_ceil(8)];
                    let mut has_pieces = false;
                    for i in 0..num_pieces {
                        if p_guard.is_completed(i as u32) {
                            bf_bytes[i / 8] |= 1 << (7 - (i % 8));
                            has_pieces = true;
                        }
                    }
                    if has_pieces {
                        let _ = conn.send(&PeerMessage::Bitfield(bf_bytes)).await;
                    }
                }
                let _ = conn.send(&PeerMessage::Interested).await;

                let (outbound_tx, mut outbound_rx) = mpsc::channel::<PeerMessage>(32);
                connected_peers_clone
                    .write()
                    .await
                    .insert(peer_addr, outbound_tx);

                let mut in_flight_count = 0usize;

                loop {
                    tokio::select! {
                        biased;

                        // Immediate sub-50ms cancellation
                        _ = cancel_rx_worker.changed() => {
                            break;
                        }

                        // Outbound message to this peer
                        Some(out_msg) = outbound_rx.recv() => {
                            if conn.send(&out_msg).await.is_err() {
                                break;
                            }
                        }

                        // Inbound message from this peer
                        msg_res = conn.recv() => {
                            match msg_res {
                                Ok(msg) => {
                                    match msg {
                                        PeerMessage::Piece { index, begin, block } => {
                                            in_flight_count = in_flight_count.saturating_sub(1);
                                            downloaded_bytes_clone.fetch_add(block.len() as u64, Ordering::Release);

                                            let complete_payload = {
                                                let mut p_guard = picker_clone.lock().await;
                                                p_guard.ingest_block_from_peer(index, begin, &block, Some(&conn.remote_peer_id))
                                            };

                                            if let Ok(Some(full_piece)) = complete_payload {
                                                match storage_clone.verify_and_write_piece(index, &full_piece).await {
                                                    Ok(true) => {
                                                        let is_done = {
                                                            let mut p_guard = picker_clone.lock().await;
                                                            p_guard.mark_completed(index);
                                                            p_guard.is_all_completed()
                                                        };

                                                        // Broadcast Have to peers
                                                        let have_msg = PeerMessage::Have { piece_index: index };
                                                        let peers = connected_peers_clone.read().await;
                                                        for tx in peers.values() {
                                                            let _ = tx.send(have_msg.clone()).await;
                                                        }

                                                        // Persist completed segment in DB
                                                        let p_offset = index as u64 * meta_clone.piece_length as u64;
                                                        let p_len = full_piece.len() as u64;
                                                        let seg = TaskSegmentRecord::new(
                                                            task_id,
                                                            index,
                                                            p_offset,
                                                            p_offset + p_len - 1,
                                                            p_offset + p_len, // cursor > range_end marks completed
                                                            0,
                                                        );

                                                        if let Ok(db) = controller_for_worker.database.lock() {
                                                            let _ = db.save_segments(task_id, &[seg]);
                                                            let _ = db.connection().execute(
                                                                "UPDATE tasks SET downloaded_bytes = ?1 WHERE id = ?2",
                                                                rusqlite::params![
                                                                    downloaded_bytes_clone.load(Ordering::Acquire) as i64,
                                                                    task_id
                                                                ],
                                                            );
                                                            if is_done {
                                                                let _ = db.update_task_status(task_id, TaskState::Completed);
                                                            }
                                                        }
                                                    }
                                                    Ok(false) => {
                                                        // Corrupt piece rejected; block buffer discarded for re-request
                                                        let mut p_guard = picker_clone.lock().await;
                                                        p_guard.reject_piece(index);
                                                    }
                                                    Err(_) => {}
                                                }
                                            }
                                        }
                                        PeerMessage::Have { piece_index } => {
                                            let mut p_guard = picker_clone.lock().await;
                                            p_guard.record_peer_have(piece_index);
                                        }
                                        PeerMessage::Bitfield(ref data) => {
                                            let mut p_guard = picker_clone.lock().await;
                                            p_guard.record_peer_bitfield(data);
                                        }
                                        PeerMessage::Request { index, begin, length } => {
                                            // Seeding support: serve blocks if piece is complete
                                            if let Ok(piece_data) = storage_clone.read_piece(index).await {
                                                let start = begin as usize;
                                                let end = start + length as usize;
                                                if end <= piece_data.len() {
                                                    let block = piece_data[start..end].to_vec();
                                                    let _ = conn.send(&PeerMessage::Piece { index, begin, block }).await;
                                                }
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                Err(_) => break,
                            }
                        }

                        // Block request pipeline loop (while peer is unchoking and pipeline isn't full)
                        _ = tokio::time::sleep(Duration::from_millis(5)), if !conn.state.peer_choking && in_flight_count < MAX_BLOCKS_PIPELINED_PER_PEER => {
                            let next_req = {
                                let mut p_guard = picker_clone.lock().await;
                                if p_guard.is_all_completed() {
                                    None
                                } else {
                                    p_guard.pick_block(&conn.remote_peer_id, |p| conn.has_piece(p as usize))
                                }
                            };

                            if let Some(req) = next_req {
                                let wire_req = PeerMessage::Request {
                                    index: req.piece_index,
                                    begin: req.block_offset,
                                    length: req.block_length,
                                };
                                if conn.send(&wire_req).await.is_ok() {
                                    in_flight_count += 1;
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }

                active_conns_clone.fetch_sub(1, Ordering::SeqCst);
                connected_peers_clone.write().await.remove(&peer_addr);

                let mut p_guard = picker_clone.lock().await;
                p_guard.cancel_peer_requests(&conn.remote_peer_id);
            });

            worker_handles.push(handle);
        }

        // 7. Telemetry, Timeout Reclamation & Completion Coordinator Loop
        let mut last_bytes = downloaded_bytes.load(Ordering::Acquire);
        let mut ticker = tokio::time::interval(TELEMETRY_TICK_INTERVAL);
        let mut cancel_rx_coord = cancel_rx.clone();

        loop {
            tokio::select! {
                biased;

                _ = cancel_rx_coord.changed() => {
                    break;
                }

                _ = ticker.tick() => {
                    let current_bytes = downloaded_bytes.load(Ordering::Acquire);
                    let delta = current_bytes.saturating_sub(last_bytes);
                    last_bytes = current_bytes;

                    // 10 Hz delta speed calculation: (delta bytes in 100ms) * 10 = bytes/sec
                    speed_bps.store(delta * 10 * 8, Ordering::Release);

                    let (is_complete, in_flight) = {
                        let mut p_guard = picker.lock().await;
                        let _reclaimed = p_guard.reclaim_timed_out_requests(BLOCK_REQUEST_TIMEOUT);
                        (p_guard.is_all_completed(), p_guard.in_flight_count())
                    };

                    if is_complete {
                        {
                            let db = controller_clone
                                .database
                                .lock()
                                .map_err(|e| DaemonError::Internal(e.to_string()))?;
                            let _ = db.update_task_status(task.id, TaskState::Completed);
                        }

                        // Announce completed event to trackers
                        let complete_req = AnnounceRequest {
                            info_hash: meta.info_hash,
                            peer_id: DAEMON_PEER_ID,
                            port: 6881,
                            uploaded: 0,
                            downloaded: meta.total_length,
                            left: 0,
                            event: AnnounceEvent::Completed,
                            num_want: Some(0),
                        };
                        let _ = tracker_coord.announce(&complete_req).await;
                        speed_bps.store(0, Ordering::Release);
                        break;
                    }

                    // If all connected peers finished and no blocks are left in flight
                    if active_connections.load(Ordering::Acquire) == 0 && in_flight == 0 && worker_handles.iter().all(|h| h.is_finished()) {
                        break;
                    }
                }
            }
        }

        // 8. Clean Shutdown & Stopped Announce
        if *cancel_rx.borrow() {
            let stop_req = AnnounceRequest {
                info_hash: meta.info_hash,
                peer_id: DAEMON_PEER_ID,
                port: 6881,
                uploaded: 0,
                downloaded: downloaded_bytes.load(Ordering::Acquire),
                left: meta
                    .total_length
                    .saturating_sub(downloaded_bytes.load(Ordering::Acquire)),
                event: AnnounceEvent::Stopped,
                num_want: Some(0),
            };
            let _ = tracker_coord.announce(&stop_req).await;

            let db = controller_clone
                .database
                .lock()
                .map_err(|e| DaemonError::Internal(e.to_string()))?;
            let _ = db.update_task_status(task.id, TaskState::Paused);
        }

        speed_bps.store(0, Ordering::Release);
        for h in worker_handles {
            h.abort();
        }

        Ok(())
    })
}
