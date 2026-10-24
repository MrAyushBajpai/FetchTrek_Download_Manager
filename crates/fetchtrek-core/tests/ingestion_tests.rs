use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use fetchtrek_core::{
    ByteStream, DownloadConfig, DownloadCoordinator, NetworkTransport, ProbeResult, SessionContext,
};
use fetchtrek_storage::{AsyncStorage, PositionalStorage, SlabPool};
use fetchtrek_types::{ByteRange, NetworkError, TaskState};
use tokio::io::AsyncWriteExt;

struct TempFileGuard {
    path: PathBuf,
}

impl TempFileGuard {
    fn new(name: &str) -> Self {
        let mut path = std::env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique = format!(
            "fetchtrek_ingest_{}_{}_{}.bin",
            std::process::id(),
            timestamp,
            name
        );
        path.push(unique);
        Self { path }
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct MockTransport {
    payload: Arc<Vec<u8>>,
    slow_threshold: Option<u64>,
    slow_chunk_delay: Duration,
    slow_chunk_size: usize,
}

impl MockTransport {
    fn new_fast(payload: Arc<Vec<u8>>) -> Self {
        Self {
            payload,
            slow_threshold: None,
            slow_chunk_delay: Duration::ZERO,
            slow_chunk_size: 64 * 1024,
        }
    }

    fn new_unbalanced(
        payload: Arc<Vec<u8>>,
        slow_threshold: u64,
        slow_chunk_delay: Duration,
        slow_chunk_size: usize,
    ) -> Self {
        Self {
            payload,
            slow_threshold: Some(slow_threshold),
            slow_chunk_delay,
            slow_chunk_size,
        }
    }
}

#[async_trait]
impl NetworkTransport for MockTransport {
    async fn probe(
        &self,
        _url: &str,
        _context: &SessionContext,
    ) -> Result<ProbeResult, NetworkError> {
        Ok(ProbeResult {
            total_bytes: Some(self.payload.len() as u64),
            supports_ranges: true,
            etag: Some("\"mock-etag-parity\"".to_string()),
            mime_type: Some("application/octet-stream".to_string()),
            suggested_filename: Some("payload.bin".to_string()),
        })
    }

    async fn open_range_stream(
        &self,
        _url: &str,
        range: ByteRange,
        _context: &SessionContext,
    ) -> Result<Pin<Box<dyn ByteStream>>, NetworkError> {
        let start = range.start as usize;
        let end = range.end as usize;

        if start >= self.payload.len() || start > end {
            return Err(NetworkError::NoRangeSupport);
        }

        let slice_end = (end + 1).min(self.payload.len());
        let data = self.payload[start..slice_end].to_vec();

        // Slow down only initial segments beyond threshold; stolen segments run fast
        let is_slow = self
            .slow_threshold
            .map(|thresh| range.start >= thresh && range.len() >= 1024 * 1024)
            .unwrap_or(false);

        if is_slow && self.slow_chunk_delay > Duration::ZERO {
            let (mut writer, reader) = tokio::io::duplex(64 * 1024);
            let delay = self.slow_chunk_delay;
            let chunk_size = self.slow_chunk_size;

            tokio::spawn(async move {
                let mut offset = 0;
                while offset < data.len() {
                    tokio::time::sleep(delay).await;
                    let len = (data.len() - offset).min(chunk_size);
                    if writer.write_all(&data[offset..offset + len]).await.is_err() {
                        break;
                    }
                    offset += len;
                }
            });

            Ok(Box::pin(reader))
        } else {
            Ok(Box::pin(std::io::Cursor::new(data)))
        }
    }
}

#[tokio::test]
async fn test_multi_connection_parallel_ingestion() {
    const PAYLOAD_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
    let payload: Arc<Vec<u8>> = Arc::new(
        (0..PAYLOAD_SIZE)
            .map(|i| ((i * 31 + 7) & 0xFF) as u8)
            .collect(),
    );

    let transport = Arc::new(MockTransport::new_fast(Arc::clone(&payload)));
    let storage: Arc<dyn AsyncStorage> = Arc::new(PositionalStorage::new());
    let slab_pool = SlabPool::new();

    let coordinator = DownloadCoordinator::new(transport, storage, slab_pool);
    let temp_guard = TempFileGuard::new("parallel_4conn");

    let config = DownloadConfig::new("https://fetchtrek.local/test.bin", &temp_guard.path)
        .with_connections(4)
        .with_min_split_size(64 * 1024);

    let summary = coordinator
        .download(config)
        .await
        .expect("download must succeed");

    assert_eq!(summary.total_bytes, PAYLOAD_SIZE as u64);
    assert_eq!(summary.final_state, TaskState::Completed);
    assert_eq!(coordinator.state(), TaskState::Completed);
    assert_eq!(coordinator.bytes_ingested(), PAYLOAD_SIZE as u64);

    // Verify bit-for-bit parity
    let file_data = std::fs::read(&temp_guard.path).expect("downloaded file must exist");
    assert_eq!(file_data.len(), PAYLOAD_SIZE);
    assert_eq!(file_data, *payload, "Contents must match byte-for-byte");
}

#[tokio::test]
async fn test_backpressure_and_work_stealing_integration() {
    const PAYLOAD_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
    let payload: Arc<Vec<u8>> = Arc::new(
        (0..PAYLOAD_SIZE)
            .map(|i| (((i.wrapping_mul(17)) ^ 0xAA) & 0xFF) as u8)
            .collect(),
    );

    // Connection 0 is fast (< 1 MiB); connections 1..4 (>= 1 MiB) are throttled with 5ms delays
    let transport = Arc::new(MockTransport::new_unbalanced(
        Arc::clone(&payload),
        1024 * 1024,
        Duration::from_millis(5),
        16 * 1024,
    ));
    let storage: Arc<dyn AsyncStorage> = Arc::new(PositionalStorage::new());
    let slab_pool = SlabPool::new();

    let coordinator = DownloadCoordinator::new(transport, storage, slab_pool);
    let temp_guard = TempFileGuard::new("stealing_unbalanced");

    let config = DownloadConfig::new("https://fetchtrek.local/unbalanced.bin", &temp_guard.path)
        .with_connections(4)
        .with_min_split_size(64 * 1024);

    let summary = coordinator
        .download(config)
        .await
        .expect("unbalanced download must succeed with work-stealing");

    assert_eq!(summary.total_bytes, PAYLOAD_SIZE as u64);
    assert_eq!(summary.final_state, TaskState::Completed);
    assert_eq!(coordinator.state(), TaskState::Completed);

    // Verify zero missing ranges and exact byte-for-byte parity
    let file_data = std::fs::read(&temp_guard.path).expect("downloaded file must exist");
    assert_eq!(file_data.len(), PAYLOAD_SIZE);
    assert_eq!(
        file_data, *payload,
        "Zero corrupted offsets and bit-for-bit parity under dynamic work stealing"
    );
}

#[tokio::test]
async fn test_abort_cleanly_on_probe_failure() {
    struct FailingTransport;
    #[async_trait]
    impl NetworkTransport for FailingTransport {
        async fn probe(
            &self,
            _url: &str,
            _context: &SessionContext,
        ) -> Result<ProbeResult, NetworkError> {
            Err(NetworkError::AccessDenied)
        }
        async fn open_range_stream(
            &self,
            _url: &str,
            _range: ByteRange,
            _context: &SessionContext,
        ) -> Result<Pin<Box<dyn ByteStream>>, NetworkError> {
            Err(NetworkError::AccessDenied)
        }
    }

    let storage: Arc<dyn AsyncStorage> = Arc::new(PositionalStorage::new());
    let slab_pool = SlabPool::new();
    let coordinator = DownloadCoordinator::new(Arc::new(FailingTransport), storage, slab_pool);
    let temp_guard = TempFileGuard::new("probe_fail");

    let config = DownloadConfig::new("https://fetchtrek.local/403.bin", &temp_guard.path);
    let err = coordinator.download(config).await.unwrap_err();

    assert_eq!(coordinator.state(), TaskState::FatalError);
    assert!(
        !temp_guard.path.exists(),
        "File must not be created on probe failure"
    );
    assert!(matches!(
        err,
        fetchtrek_types::FetchTrekError::Network(NetworkError::AccessDenied)
    ));
}
