use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use fetchtrek_core::{
    ByteStream, DownloadConfig, DownloadCoordinator, DownloadSession, NetworkTransport,
    ProbeResult, SessionContext,
};
use fetchtrek_storage::{AsyncStorage, PositionalStorage, SlabPool};
use fetchtrek_types::{ByteRange, FetchTrekError, NetworkError, TaskEvent, TaskState};

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
            "fetchtrek_concurrency_{}_{}_{}.bin",
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

struct MultiUrlMockTransport {
    payload_a: Arc<Vec<u8>>,
    payload_b: Arc<Vec<u8>>,
}

#[async_trait]
impl NetworkTransport for MultiUrlMockTransport {
    async fn probe(
        &self,
        url: &str,
        _context: &SessionContext,
    ) -> Result<ProbeResult, NetworkError> {
        let total_bytes = if url.contains("item_a") {
            self.payload_a.len() as u64
        } else {
            self.payload_b.len() as u64
        };

        Ok(ProbeResult {
            total_bytes: Some(total_bytes),
            supports_ranges: true,
            etag: Some("etag-concurrent-test".to_string()),
            mime_type: Some("application/octet-stream".to_string()),
            suggested_filename: None,
        })
    }

    async fn open_range_stream(
        &self,
        url: &str,
        range: ByteRange,
        _context: &SessionContext,
    ) -> Result<Pin<Box<dyn ByteStream>>, NetworkError> {
        let payload = if url.contains("item_a") {
            Arc::clone(&self.payload_a)
        } else {
            Arc::clone(&self.payload_b)
        };

        let start = range.start as usize;
        let end = (range.end + 1) as usize;
        let slice = payload[start..end].to_vec();

        Ok(Box::pin(std::io::Cursor::new(slice)))
    }

    async fn open_stream(
        &self,
        url: &str,
        context: &SessionContext,
    ) -> Result<Pin<Box<dyn ByteStream>>, NetworkError> {
        let total = if url.contains("item_a") {
            self.payload_a.len() as u64
        } else {
            self.payload_b.len() as u64
        };
        self.open_range_stream(url, ByteRange::new(0, total - 1), context)
            .await
    }
}

#[tokio::test]
async fn test_coordinator_concurrent_downloads() {
    const SIZE_A: usize = 512 * 1024; // 512 KiB
    const SIZE_B: usize = 1024 * 1024; // 1 MiB

    let payload_a = Arc::new(vec![0xAAu8; SIZE_A]);
    let payload_b = Arc::new(vec![0xBBu8; SIZE_B]);

    let transport: Arc<dyn NetworkTransport> = Arc::new(MultiUrlMockTransport {
        payload_a: Arc::clone(&payload_a),
        payload_b: Arc::clone(&payload_b),
    });

    let storage: Arc<dyn AsyncStorage> = Arc::new(PositionalStorage::new());
    let slab_pool = SlabPool::new();

    // Single shared DownloadCoordinator instance
    let coordinator = Arc::new(DownloadCoordinator::new(transport, storage, slab_pool));

    let guard_a = TempFileGuard::new("dest_a");
    let guard_b = TempFileGuard::new("dest_b");

    let config_a = DownloadConfig::new("https://fetchtrek.local/item_a.bin", &guard_a.path)
        .with_connections(4)
        .with_min_split_size(64 * 1024);

    let config_b = DownloadConfig::new("https://fetchtrek.local/item_b.bin", &guard_b.path)
        .with_connections(4)
        .with_min_split_size(64 * 1024);

    let session_a = Arc::new(DownloadSession::new());
    let session_b = Arc::new(DownloadSession::new());

    let coord_a = Arc::clone(&coordinator);
    let s_a = Arc::clone(&session_a);
    let handle_a = tokio::spawn(async move { coord_a.download_with_session(config_a, s_a).await });

    let coord_b = Arc::clone(&coordinator);
    let s_b = Arc::clone(&session_b);
    let handle_b = tokio::spawn(async move { coord_b.download_with_session(config_b, s_b).await });

    let (res_a, res_b) = tokio::join!(handle_a, handle_b);

    let summary_a = res_a.expect("task_a join").expect("download a succeed");
    let summary_b = res_b.expect("task_b join").expect("download b succeed");

    // 1. Verify independent completion summaries
    assert_eq!(summary_a.total_bytes, SIZE_A as u64);
    assert_eq!(summary_a.final_state, TaskState::Completed);

    assert_eq!(summary_b.total_bytes, SIZE_B as u64);
    assert_eq!(summary_b.final_state, TaskState::Completed);

    // 2. Verify independent session counter isolations
    assert_eq!(session_a.bytes_ingested(), SIZE_A as u64);
    assert_eq!(session_a.state(), TaskState::Completed);

    assert_eq!(session_b.bytes_ingested(), SIZE_B as u64);
    assert_eq!(session_b.state(), TaskState::Completed);

    // 3. Verify exact byte parity on disk
    let data_a = std::fs::read(&guard_a.path).expect("read dest_a");
    assert_eq!(data_a.len(), SIZE_A);
    assert_eq!(data_a, *payload_a, "Destination A contents mismatch");

    let data_b = std::fs::read(&guard_b.path).expect("read dest_b");
    assert_eq!(data_b.len(), SIZE_B);
    assert_eq!(data_b, *payload_b, "Destination B contents mismatch");
}

#[test]
fn test_coordinator_invalid_transition_error() {
    let session = DownloadSession::new();
    assert_eq!(session.state(), TaskState::Queued);

    // Valid transition Queued -> Resolving
    assert!(session.apply_event(TaskEvent::StartGranted).is_ok());
    assert_eq!(session.state(), TaskState::Resolving);

    // Force terminal state
    session.set_state(TaskState::FatalError);
    assert_eq!(session.state(), TaskState::FatalError);

    // Invalid transition: UserPause from FatalError must return FetchTrekError::InvalidStateTransition
    let err = session
        .apply_event(TaskEvent::UserPause)
        .expect_err("UserPause from FatalError must fail");

    match err {
        FetchTrekError::InvalidStateTransition(trans_err) => {
            assert_eq!(trans_err.current, TaskState::FatalError);
            assert_eq!(trans_err.event, TaskEvent::UserPause);
        }
        other => panic!("Expected InvalidStateTransition, got {:?}", other),
    }

    // State remains unchanged
    assert_eq!(session.state(), TaskState::FatalError);
}
