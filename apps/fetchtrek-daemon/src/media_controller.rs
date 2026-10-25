use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fetchtrek_core::media::{
    MediaChunk, MediaChunkScheduler, MediaDownloadConfig, MediaIngestionCoordinator,
};
use fetchtrek_core::SessionContext;
use fetchtrek_persistence::TaskRecord;
use fetchtrek_protocols::media::{
    assemble_stream, parse_media_manifest, AssemblyConfig, MediaManifest, StreamContainer,
};
use fetchtrek_types::TaskState;
use tokio::io::AsyncReadExt;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::controller::TaskController;
use crate::error::DaemonError;

/// Spawns an asynchronous task orchestrating parallel media chunk ingestion and stream assembly.
pub fn start_media_stream_download(
    controller: &Arc<TaskController>,
    task: TaskRecord,
    session_context: Arc<SessionContext>,
    cancel_rx: watch::Receiver<bool>,
    downloaded_bytes: Arc<AtomicU64>,
    speed_bps: Arc<AtomicU64>,
    active_connections: Arc<AtomicU16>,
) -> JoinHandle<Result<(), DaemonError>> {
    let controller_clone = Arc::clone(controller);

    tokio::spawn(async move {
        let task_id = task.id;
        let staging_dir = PathBuf::from(format!(
            "{}.fetchtrek_parts_{}",
            task.destination_path, task_id
        ));

        tokio::fs::create_dir_all(&staging_dir).await.map_err(|e| {
            DaemonError::Internal(format!("Failed to create staging directory: {e}"))
        })?;

        if *cancel_rx.borrow() {
            return Ok(());
        }

        // 1. Fetch manifest body
        let mut manifest_stream = controller_clone
            .transport
            .open_stream(&task.url, &session_context)
            .await
            .map_err(|e| DaemonError::Internal(format!("Failed to open manifest stream: {e}")))?;

        let mut manifest_content = String::new();
        manifest_stream
            .read_to_string(&mut manifest_content)
            .await
            .map_err(|e| DaemonError::Internal(format!("Failed to read manifest: {e}")))?;

        // 2. Parse manifest (auto-detect HLS / DASH)
        let parsed = parse_media_manifest(&manifest_content, &task.url)
            .map_err(|e| DaemonError::Internal(format!("Failed to parse manifest: {e}")))?;

        let media_playlist = match parsed {
            MediaManifest::Master(master) => {
                // Select variant with highest bandwidth
                let best_variant = master
                    .variants
                    .iter()
                    .max_by_key(|v| v.bandwidth)
                    .ok_or_else(|| {
                        DaemonError::Internal("Master playlist has no variants".into())
                    })?;

                let mut var_stream = controller_clone
                    .transport
                    .open_stream(&best_variant.url, &session_context)
                    .await
                    .map_err(|e| {
                        DaemonError::Internal(format!("Failed to open variant stream: {e}"))
                    })?;

                let mut var_content = String::new();
                var_stream
                    .read_to_string(&mut var_content)
                    .await
                    .map_err(|e| DaemonError::Internal(format!("Failed to read variant: {e}")))?;

                match parse_media_manifest(&var_content, &best_variant.url)
                    .map_err(|e| DaemonError::Internal(format!("Variant manifest error: {e}")))?
                {
                    MediaManifest::Media(p) => p,
                    _ => {
                        return Err(DaemonError::Internal(
                            "Nested master playlist unsupported".into(),
                        ))
                    }
                }
            }
            MediaManifest::Media(p) => p,
        };

        let total_chunks = media_playlist.segments.len();
        if total_chunks == 0 {
            return Err(DaemonError::Internal(
                "Media playlist contains 0 segments".into(),
            ));
        }

        // 3. Download init segment if present (fMP4)
        let has_init = media_playlist.init_segment.is_some();
        if let Some(ref init_seg) = media_playlist.init_segment {
            let init_path = staging_dir.join("init.mp4");
            if !init_path.is_file() {
                let mut init_stream = controller_clone
                    .transport
                    .open_stream(&init_seg.url, &session_context)
                    .await
                    .map_err(|e| {
                        DaemonError::Internal(format!("Failed to open init stream: {e}"))
                    })?;

                let mut init_file = tokio::fs::File::create(&init_path)
                    .await
                    .map_err(|e| DaemonError::Internal(e.to_string()))?;

                tokio::io::copy(&mut init_stream, &mut init_file)
                    .await
                    .map_err(|e| DaemonError::Internal(e.to_string()))?;
            }
        }

        // 4. Download decryption key if declared
        let mut key_data: Option<([u8; 16], Option<[u8; 16]>)> = None;
        if let Some(key_meta) = media_playlist.segments.iter().find_map(|s| s.key.as_ref()) {
            if !key_meta.url.is_empty() {
                let mut key_stream = controller_clone
                    .transport
                    .open_stream(&key_meta.url, &session_context)
                    .await
                    .map_err(|e| {
                        DaemonError::Internal(format!("Failed to open key stream: {e}"))
                    })?;

                let mut key_bytes = Vec::new();
                key_stream
                    .read_to_end(&mut key_bytes)
                    .await
                    .map_err(|e| DaemonError::Internal(format!("Failed to read key: {e}")))?;

                if key_bytes.len() == 16 {
                    let mut k = [0u8; 16];
                    k.copy_from_slice(&key_bytes);
                    key_data = Some((k, key_meta.iv));
                }
            }
        }

        // 5. Convert to MediaChunk descriptors
        let chunks: Vec<MediaChunk> = media_playlist
            .segments
            .iter()
            .enumerate()
            .map(|(idx, s)| {
                let mut c = MediaChunk::new(idx, &s.url, s.duration_secs);
                if let Some(r) = s.byte_range {
                    c = c.with_byte_range(r);
                }
                c
            })
            .collect();

        // 6. Idempotent Resumption: Inspect existing staged .part files
        let scheduler = Arc::new(MediaChunkScheduler::new(chunks.clone(), 3));
        let mut preloaded_bytes = 0u64;

        for idx in 0..total_chunks {
            let part_path = staging_dir.join(format!("part_{idx:06}.part"));
            if part_path.is_file() {
                if let Ok(meta) = std::fs::metadata(&part_path) {
                    if meta.len() > 0 {
                        scheduler.mark_completed(idx, meta.len());
                        preloaded_bytes += meta.len();
                    }
                }
            }
        }
        downloaded_bytes.store(preloaded_bytes, Ordering::Release);

        // 7. Parallel Media Ingestion
        let media_config = MediaDownloadConfig::new(chunks, staging_dir.clone())
            .with_connections(4)
            .with_context((*session_context).clone())
            .with_rate_limiter(Arc::clone(&controller_clone.limiter));

        let media_coordinator = Arc::new(MediaIngestionCoordinator::with_scheduler(
            media_config,
            Arc::clone(&controller_clone.transport),
            Arc::clone(&controller_clone.memory_pool),
            Arc::clone(&scheduler),
        ));

        active_connections.store(4, Ordering::Release);
        let coord_clone = Arc::clone(&media_coordinator);
        let d_bytes = Arc::clone(&downloaded_bytes);
        let s_bps = Arc::clone(&speed_bps);
        let mut tele_cancel = cancel_rx.clone();

        let tele_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tele_cancel.changed() => break,
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        let snap = coord_clone.scheduler.snapshot();
                        d_bytes.store(snap.downloaded_bytes, Ordering::Release);
                        s_bps.store(coord_clone.speed_bps.load(Ordering::Acquire), Ordering::Release);
                    }
                }
            }
        });

        let run_result = media_coordinator.run(cancel_rx.clone()).await;
        tele_task.abort();
        active_connections.store(0, Ordering::Release);

        // 8. Handle Cancellation vs Error vs Success
        if *cancel_rx.borrow() {
            if let Ok(db) = controller_clone.database.lock() {
                let snap = scheduler.snapshot();
                let _ = db.update_task_status(task_id, TaskState::Paused);
                let _ = db.checkpoint_progress(task_id, snap.downloaded_bytes, &[]);
            }
            return Ok(());
        }

        if let Err(e) = run_result {
            if let Ok(db) = controller_clone.database.lock() {
                let _ = db.update_task_status(task_id, TaskState::FatalError);
            }
            return Err(DaemonError::Internal(format!(
                "Media ingestion failed: {e}"
            )));
        }

        // 9. Container Assembly
        let is_fmp4 = has_init
            || media_playlist
                .segments
                .first()
                .map(|s| s.url.ends_with(".m4s"))
                .unwrap_or(false);

        let container_type = if is_fmp4 {
            StreamContainer::FragmentedMp4
        } else {
            StreamContainer::MpegTs
        };

        let mut assembly_config = AssemblyConfig::new(
            container_type,
            &staging_dir,
            total_chunks,
            PathBuf::from(&task.destination_path),
        );

        if has_init {
            assembly_config = assembly_config.with_init_segment(staging_dir.join("init.mp4"));
        }
        if let Some((key, iv)) = key_data {
            assembly_config = assembly_config.with_decryption(key, iv);
        }

        let summary = assemble_stream(&assembly_config)
            .map_err(|e| DaemonError::Internal(format!("Stream assembly failed: {e}")))?;

        // 10. Atomic Finalization
        downloaded_bytes.store(summary.total_bytes, Ordering::Release);
        speed_bps.store(0, Ordering::Release);

        if let Ok(db) = controller_clone.database.lock() {
            let _ = db.checkpoint_progress(task_id, summary.total_bytes, &[]);
            let _ = db.update_task_status(task_id, TaskState::Completed);
        }

        controller_clone
            .active_sessions
            .write()
            .await
            .remove(&task_id);
        controller_clone.check_and_start_next_queued().await;

        Ok(())
    })
}
