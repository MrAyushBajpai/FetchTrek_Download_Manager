use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use fetchtrek_core::media::{
    ChunkState, MediaChunk, MediaChunkScheduler, MediaDownloadConfig, MediaIngestionCoordinator,
};
use fetchtrek_core::{ByteStream, NetworkTransport, ProbeResult, SessionContext};
use fetchtrek_storage::SlabPool;
use fetchtrek_types::{ByteRange, NetworkError};
use tokio::sync::watch;

struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(prefix: &str) -> Self {
        let unique = format!(
            "fetchtrek_media_test_{}_{}_{}",
            prefix,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(unique);
        let _ = std::fs::create_dir_all(&path);
        Self { path }
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct MockMediaTransport {
    data_map: HashMap<String, Vec<u8>>,
    received_contexts: Arc<Mutex<Vec<SessionContext>>>,
    read_delay: Option<Duration>,
}

impl MockMediaTransport {
    fn new(data_map: HashMap<String, Vec<u8>>) -> Self {
        Self {
            data_map,
            received_contexts: Arc::new(Mutex::new(Vec::new())),
            read_delay: None,
        }
    }

    fn with_delay(mut self, delay: Duration) -> Self {
        self.read_delay = Some(delay);
        self
    }
}

#[async_trait]
impl NetworkTransport for MockMediaTransport {
    async fn probe(
        &self,
        url: &str,
        context: &SessionContext,
    ) -> Result<ProbeResult, NetworkError> {
        self.received_contexts.lock().unwrap().push(context.clone());
        if let Some(bytes) = self.data_map.get(url) {
            Ok(ProbeResult {
                total_bytes: Some(bytes.len() as u64),
                supports_ranges: true,
                etag: Some("test-etag".to_string()),
                mime_type: Some("video/mp2t".to_string()),
                suggested_filename: None,
            })
        } else {
            Err(NetworkError::AccessDenied)
        }
    }

    async fn open_range_stream(
        &self,
        url: &str,
        range: ByteRange,
        context: &SessionContext,
    ) -> Result<Pin<Box<dyn ByteStream>>, NetworkError> {
        self.received_contexts.lock().unwrap().push(context.clone());
        if let Some(delay) = self.read_delay {
            tokio::time::sleep(delay).await;
        }

        if let Some(bytes) = self.data_map.get(url) {
            let start = range.start as usize;
            let end = (range.end as usize + 1).min(bytes.len());
            if start >= bytes.len() {
                return Err(NetworkError::NoRangeSupport);
            }
            let slice = bytes[start..end].to_vec();
            Ok(Box::pin(Cursor::new(slice)))
        } else {
            Err(NetworkError::AccessDenied)
        }
    }

    async fn open_stream(
        &self,
        url: &str,
        context: &SessionContext,
    ) -> Result<Pin<Box<dyn ByteStream>>, NetworkError> {
        self.received_contexts.lock().unwrap().push(context.clone());
        if let Some(delay) = self.read_delay {
            tokio::time::sleep(delay).await;
        }

        if let Some(bytes) = self.data_map.get(url) {
            Ok(Box::pin(Cursor::new(bytes.clone())))
        } else {
            Err(NetworkError::AccessDenied)
        }
    }
}

#[test]
fn test_scheduler_sequential_claim() {
    let chunks: Vec<MediaChunk> = (0..10)
        .map(|i| MediaChunk::new(i, format!("https://cdn.example.com/chunk_{i}.ts"), 6.0))
        .collect();

    let scheduler = MediaChunkScheduler::new(chunks, 3);
    assert!(!scheduler.is_finished());
    assert!(!scheduler.has_terminal_failure());

    // Claim sequentially across alternating workers
    for expected_idx in 0..10 {
        let worker_id = expected_idx % 3;
        let claimed = scheduler
            .claim_next_chunk(worker_id)
            .expect("must claim chunk");
        assert_eq!(claimed.index, expected_idx);
        assert_eq!(
            scheduler.chunk_state(expected_idx),
            Some(ChunkState::Downloading {
                worker_id,
                attempt: 1
            })
        );
    }

    // After all 10 claimed, claim_next_chunk returns None
    assert!(scheduler.claim_next_chunk(0).is_none());
    assert!(!scheduler.is_finished());

    // Mark all completed
    for i in 0..10 {
        scheduler.mark_completed(i, 1024);
        assert_eq!(
            scheduler.chunk_state(i),
            Some(ChunkState::Completed { bytes: 1024 })
        );
    }

    assert!(scheduler.is_finished());
    assert!(!scheduler.has_terminal_failure());
    let snap = scheduler.snapshot();
    assert_eq!(snap.completed_chunks, 10);
    assert_eq!(snap.downloaded_bytes, 10 * 1024);
    assert_eq!(snap.progress_percent, 100.0);
}

#[test]
fn test_scheduler_transient_failure_and_work_stealing() {
    let chunks: Vec<MediaChunk> = (0..3)
        .map(|i| MediaChunk::new(i, format!("https://cdn.example.com/chunk_{i}.ts"), 6.0))
        .collect();

    let scheduler = MediaChunkScheduler::new(chunks, 3);

    // Worker 0 claims chunk 0
    let chunk0 = scheduler.claim_next_chunk(0).unwrap();
    assert_eq!(chunk0.index, 0);
    assert_eq!(scheduler.chunk_attempts(0), Some(1));

    // Worker 0 reports transient network failure
    let retryable = scheduler.mark_failed(0, "socket timeout");
    assert!(retryable);
    assert_eq!(scheduler.chunk_state(0), Some(ChunkState::Pending));
    assert_eq!(scheduler.chunk_attempts(0), Some(1));

    // Worker 1 now steals chunk 0
    let stolen = scheduler.claim_next_chunk(1).unwrap();
    assert_eq!(stolen.index, 0);
    assert_eq!(scheduler.chunk_attempts(0), Some(2));
    assert_eq!(
        scheduler.chunk_state(0),
        Some(ChunkState::Downloading {
            worker_id: 1,
            attempt: 2
        })
    );

    // Worker 1 succeeds
    scheduler.mark_completed(0, 2048);
    assert_eq!(
        scheduler.chunk_state(0),
        Some(ChunkState::Completed { bytes: 2048 })
    );
}

#[test]
fn test_scheduler_terminal_failure_exhaustion() {
    let chunks = vec![MediaChunk::new(0, "https://cdn.example.com/broken.ts", 6.0)];
    let scheduler = MediaChunkScheduler::new(chunks, 3);

    // Attempt 1 fails (retryable)
    let _ = scheduler.claim_next_chunk(0).unwrap();
    assert!(scheduler.mark_failed(0, "err 1"));
    assert!(!scheduler.has_terminal_failure());

    // Attempt 2 fails (retryable)
    let _ = scheduler.claim_next_chunk(1).unwrap();
    assert!(scheduler.mark_failed(0, "err 2"));
    assert!(!scheduler.has_terminal_failure());

    // Attempt 3 fails (terminal exhaustion)
    let _ = scheduler.claim_next_chunk(2).unwrap();
    let retryable = scheduler.mark_failed(0, "err 3 terminal");
    assert!(!retryable);
    assert!(scheduler.has_terminal_failure());

    // Verify terminal failure state and rejection of future claims
    assert_eq!(
        scheduler.chunk_state(0),
        Some(ChunkState::Failed {
            error: "err 3 terminal".to_string()
        })
    );
    assert!(scheduler.claim_next_chunk(0).is_none());
    assert!(!scheduler.is_finished());

    let snap = scheduler.snapshot();
    assert_eq!(snap.failed_chunks, 1);
    assert_eq!(snap.completed_chunks, 0);
    assert_eq!(snap.in_flight_chunks, 0);
}

#[test]
fn test_scheduler_progress_snapshot() {
    let chunks: Vec<MediaChunk> = (0..10)
        .map(|i| MediaChunk::new(i, format!("https://cdn.example.com/chunk_{i}.ts"), 5.0))
        .collect();

    let scheduler = MediaChunkScheduler::new(chunks, 3);

    // Complete 3 chunks (1,000 bytes each)
    for i in 0..3 {
        let _ = scheduler.claim_next_chunk(0).unwrap();
        scheduler.mark_completed(i, 1000);
    }

    // Leave 2 chunks Downloading (in-flight)
    let _ = scheduler.claim_next_chunk(1).unwrap();
    let _ = scheduler.claim_next_chunk(2).unwrap();

    let snap = scheduler.snapshot();
    assert_eq!(snap.total_chunks, 10);
    assert_eq!(snap.completed_chunks, 3);
    assert_eq!(snap.in_flight_chunks, 2);
    assert_eq!(snap.failed_chunks, 0);
    assert_eq!(snap.downloaded_bytes, 3000);
    assert!((snap.progress_percent - 30.0).abs() < 1e-6);
}

#[tokio::test]
async fn test_concurrent_multi_worker_stress() {
    const TOTAL_CHUNKS: usize = 100;
    const NUM_WORKERS: usize = 8;
    const CHUNK_SIZE: u64 = 512;

    let chunks: Vec<MediaChunk> = (0..TOTAL_CHUNKS)
        .map(|i| MediaChunk::new(i, format!("https://cdn.example.com/segment_{i:04}.ts"), 4.0))
        .collect();

    let scheduler = Arc::new(MediaChunkScheduler::new(chunks, 10));
    let transient_fail_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    for worker_id in 0..NUM_WORKERS {
        let scheduler = Arc::clone(&scheduler);
        let transient_fail_count = Arc::clone(&transient_fail_count);

        let handle = tokio::spawn(async move {
            loop {
                let chunk = match scheduler.claim_next_chunk(worker_id) {
                    Some(c) => c,
                    None => {
                        if scheduler.is_finished() || scheduler.has_terminal_failure() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        continue;
                    }
                };

                let attempt = scheduler.chunk_attempts(chunk.index).unwrap_or(1);

                // Simulate ~10% deterministic transient failures on first attempt
                if chunk.index % 10 == 0 && attempt == 1 {
                    transient_fail_count.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_micros(100)).await;
                    scheduler.mark_failed(chunk.index, "transient reset");
                } else {
                    tokio::time::sleep(Duration::from_micros(200)).await;
                    scheduler.mark_completed(chunk.index, CHUNK_SIZE);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("worker task must not panic");
    }

    assert!(scheduler.is_finished());
    assert!(!scheduler.has_terminal_failure());
    assert!(transient_fail_count.load(Ordering::Relaxed) > 0);

    let snap = scheduler.snapshot();
    assert_eq!(snap.total_chunks, TOTAL_CHUNKS);
    assert_eq!(snap.completed_chunks, TOTAL_CHUNKS);
    assert_eq!(snap.failed_chunks, 0);
    assert_eq!(snap.downloaded_bytes, TOTAL_CHUNKS as u64 * CHUNK_SIZE);
    assert_eq!(snap.progress_percent, 100.0);
}

#[tokio::test]
async fn test_coordinator_cancellation_token() {
    let temp_dir = TempDirGuard::new("cancel");
    let chunks: Vec<MediaChunk> = (0..20)
        .map(|i| MediaChunk::new(i, format!("https://cdn.example.com/part_{i}.ts"), 6.0))
        .collect();

    let mut data_map = HashMap::new();
    for c in &chunks {
        data_map.insert(c.url.clone(), vec![0xAA; 4096]);
    }

    // Mock transport with 50ms read delay to allow clean mid-flight cancellation
    let transport =
        Arc::new(MockMediaTransport::new(data_map).with_delay(Duration::from_millis(50)));
    let pool = SlabPool::new();

    let config = MediaDownloadConfig::new(chunks, temp_dir.path.clone()).with_connections(4);
    let coordinator = Arc::new(MediaIngestionCoordinator::new(config, transport, pool));

    let (cancel_tx, cancel_rx) = watch::channel(false);
    let handle = coordinator.start(cancel_rx);

    // Let workers spin up and start downloading
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Trigger cancellation
    cancel_tx.send(true).expect("cancel send must succeed");

    let result = handle.await.expect("handle join must succeed");
    assert!(
        result.is_err(),
        "coordinator must return error on cancellation"
    );

    // Must have aborted promptly without completing all 20 chunks
    let snap = coordinator.scheduler.snapshot();
    assert!(snap.completed_chunks < 20);
}

#[tokio::test]
async fn test_coordinator_end_to_end_parallel_download() {
    let temp_dir = TempDirGuard::new("e2e");
    const CHUNK_COUNT: usize = 6;
    let mut chunks = Vec::new();
    let mut data_map = HashMap::new();

    for i in 0..CHUNK_COUNT {
        let url = format!("https://cdn.example.com/video_chunk_{i}.ts");
        let payload = vec![(i as u8) + 1; 2048];
        data_map.insert(url.clone(), payload);
        chunks.push(MediaChunk::new(i, url, 6.0));
    }

    let transport = Arc::new(MockMediaTransport::new(data_map));
    let pool = SlabPool::new();

    let context = SessionContext::new()
        .with_user_agent("FetchTrek/1.0")
        .with_cookies("session_id=secret123")
        .with_header("X-Custom-Auth", "token-xyz");

    let config = MediaDownloadConfig::new(chunks, temp_dir.path.clone())
        .with_connections(3)
        .with_context(context.clone());

    let coordinator = Arc::new(MediaIngestionCoordinator::new(
        config,
        transport.clone(),
        pool,
    ));
    let (_cancel_tx, cancel_rx) = watch::channel(false);

    let summary = coordinator
        .start(cancel_rx)
        .await
        .expect("join succeeded")
        .expect("download must succeed");

    assert_eq!(summary.total_chunks, CHUNK_COUNT);
    assert_eq!(summary.total_bytes, (CHUNK_COUNT * 2048) as u64);
    assert_eq!(summary.staging_dir, temp_dir.path);

    // Verify all part files exist on disk with expected contents
    for i in 0..CHUNK_COUNT {
        let part_file = temp_dir.path.join(format!("part_{i:06}.part"));
        assert!(part_file.exists(), "part file must exist: {:?}", part_file);
        let contents = std::fs::read(&part_file).expect("read part file");
        assert_eq!(contents.len(), 2048);
        assert_eq!(contents[0], (i as u8) + 1);
    }

    // Verify session context was forwarded on all chunk network calls
    let recorded = transport.received_contexts.lock().unwrap();
    assert!(!recorded.is_empty());
    for ctx in recorded.iter() {
        assert_eq!(ctx.user_agent, Some("FetchTrek/1.0".to_string()));
        assert_eq!(ctx.cookies, Some("session_id=secret123".to_string()));
        assert!(ctx
            .headers
            .iter()
            .any(|(k, v)| k == "X-Custom-Auth" && v == "token-xyz"));
    }
}
