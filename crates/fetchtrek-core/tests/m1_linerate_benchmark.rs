use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use fetchtrek_core::{
    ByteStream, DownloadConfig, DownloadCoordinator, NetworkTransport, ProbeResult, SessionContext,
};
use fetchtrek_storage::{AsyncStorage, PositionalStorage, SlabPool, TOTAL_SLABS};
use fetchtrek_types::{ByteRange, NetworkError, TaskState};
use tokio::io::AsyncWriteExt;

struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(prefix: &str) -> Self {
        let mut path = std::env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique = format!(
            "fetchtrek_m1_{}_{}_{}",
            std::process::id(),
            timestamp,
            prefix
        );
        path.push(unique);
        std::fs::create_dir_all(&path).expect("failed to create temp benchmark dir");
        Self { path }
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct FastRangeTransport {
    payload: Arc<Vec<u8>>,
    throttle_threshold: Option<u64>,
    throttle_delay: Duration,
    throttle_chunk_size: usize,
}

impl FastRangeTransport {
    fn new_line_rate(payload: Arc<Vec<u8>>) -> Self {
        Self {
            payload,
            throttle_threshold: None,
            throttle_delay: Duration::ZERO,
            throttle_chunk_size: 64 * 1024,
        }
    }

    fn new_imbalanced(
        payload: Arc<Vec<u8>>,
        throttle_threshold: u64,
        throttle_delay: Duration,
        throttle_chunk_size: usize,
    ) -> Self {
        Self {
            payload,
            throttle_threshold: Some(throttle_threshold),
            throttle_delay,
            throttle_chunk_size,
        }
    }
}

#[async_trait]
impl NetworkTransport for FastRangeTransport {
    async fn probe(
        &self,
        _url: &str,
        _context: &SessionContext,
    ) -> Result<ProbeResult, NetworkError> {
        Ok(ProbeResult {
            total_bytes: Some(self.payload.len() as u64),
            supports_ranges: true,
            etag: Some("\"m1-benchmark-etag\"".to_string()),
            mime_type: Some("application/octet-stream".to_string()),
            suggested_filename: Some("m1_target.bin".to_string()),
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

        // Slow down only original unbisected segments beyond the threshold
        let is_slow = self
            .throttle_threshold
            .map(|thresh| range.start >= thresh && range.len() >= 4 * 1024 * 1024)
            .unwrap_or(false);

        if is_slow && self.throttle_delay > Duration::ZERO {
            let (mut writer, reader) = tokio::io::duplex(64 * 1024);
            let delay = self.throttle_delay;
            let chunk_size = self.throttle_chunk_size;

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
async fn test_m1_line_rate_parallel_transfer_and_zero_second_merge() {
    const PAYLOAD_SIZE: usize = 32 * 1024 * 1024; // 32 MiB
    let payload: Arc<Vec<u8>> = Arc::new(
        (0..PAYLOAD_SIZE)
            .map(|i| (((i.wrapping_mul(41)).wrapping_add(13)) & 0xFF) as u8)
            .collect(),
    );

    let transport = Arc::new(FastRangeTransport::new_line_rate(Arc::clone(&payload)));
    let storage: Arc<dyn AsyncStorage> = Arc::new(PositionalStorage::new());
    let slab_pool = SlabPool::new();

    let temp_dir = TempDirGuard::new("linerate");
    let target_file = temp_dir.path.join("m1_target.bin");

    let coordinator = DownloadCoordinator::new(transport, storage, Arc::clone(&slab_pool));

    let config = DownloadConfig::new("https://fetchtrek.local/m1_linerate.bin", &target_file)
        .with_connections(8)
        .with_min_split_size(64 * 1024);

    let transfer_start = Instant::now();
    let summary = coordinator
        .download(config)
        .await
        .expect("M1 line-rate download must complete successfully");
    let total_elapsed = transfer_start.elapsed();

    // 1. Throughput verification (>= 115 MB/s on loopback)
    let total_mb = (PAYLOAD_SIZE as f64) / (1024.0 * 1024.0);
    let throughput_mb_s = total_mb / total_elapsed.as_secs_f64();
    assert!(
        throughput_mb_s >= 115.0,
        "Throughput must saturate line rate (>= 115 MB/s), measured: {:.2} MB/s in {:?}",
        throughput_mb_s,
        total_elapsed
    );

    // 2. 0.00s Merge Invariant Gate (< 15ms wall-clock duration)
    assert!(
        summary.finalization_duration < Duration::from_millis(15),
        "Merge/finalization delay must strictly be < 15ms, measured: {:?}",
        summary.finalization_duration
    );

    // 3. Bit-for-Bit Cryptographic Parity Proof
    let file_data = std::fs::read(&target_file).expect("target file must be readable");
    assert_eq!(
        file_data.len(),
        PAYLOAD_SIZE,
        "File size must match exactly"
    );
    assert_eq!(
        file_data, *payload,
        "Target file must have exact bit-for-bit parity with source payload"
    );

    // 4. Write Amplification Factor (WAF = 1.00x) Verification
    let dir_entries: Vec<_> = std::fs::read_dir(&temp_dir.path)
        .expect("must read temp dir")
        .filter_map(|e| e.ok())
        .collect();

    // Verify only the single target file was written; zero .part, .tmp, or chunk files
    assert_eq!(
        dir_entries.len(),
        1,
        "WAF = 1.00x: exactly one file must exist on disk, found: {:?}",
        dir_entries
            .iter()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        dir_entries[0].file_name(),
        "m1_target.bin",
        "The single file must be the target output"
    );

    // 5. Memory Reclamation Verification
    assert_eq!(
        slab_pool.available_slabs(),
        TOTAL_SLABS,
        "All 256 slabs must be reclaimed into pool without leaks"
    );

    assert_eq!(summary.final_state, TaskState::Completed);
    assert_eq!(coordinator.state(), TaskState::Completed);
}

#[tokio::test]
async fn test_m1_dynamic_bisection_under_connection_imbalance() {
    const PAYLOAD_SIZE: usize = 32 * 1024 * 1024; // 32 MiB
    let payload: Arc<Vec<u8>> = Arc::new(
        (0..PAYLOAD_SIZE)
            .map(|i| (((i.wrapping_mul(73)) ^ 0x5C) & 0xFF) as u8)
            .collect(),
    );

    // 8 connections: workers 0..4 (< 16 MiB) run fast; workers 4..8 (>= 16 MiB) are throttled with 5ms delays
    let transport = Arc::new(FastRangeTransport::new_imbalanced(
        Arc::clone(&payload),
        16 * 1024 * 1024,
        Duration::from_millis(5),
        16 * 1024,
    ));
    let storage: Arc<dyn AsyncStorage> = Arc::new(PositionalStorage::new());
    let slab_pool = SlabPool::new();

    let temp_dir = TempDirGuard::new("imbalance");
    let target_file = temp_dir.path.join("m1_imbalance_target.bin");

    let coordinator = DownloadCoordinator::new(transport, storage, Arc::clone(&slab_pool));

    let config = DownloadConfig::new("https://fetchtrek.local/m1_imbalanced.bin", &target_file)
        .with_connections(8)
        .with_min_split_size(64 * 1024);

    let summary = coordinator
        .download(config)
        .await
        .expect("imbalanced M1 download must complete via work-stealing bisection");

    // Verify 0.00s merge delay under work-stealing
    assert!(
        summary.finalization_duration < Duration::from_millis(15),
        "Finalization delay must be < 15ms under work stealing: {:?}",
        summary.finalization_duration
    );

    // Verify bit-for-bit parity
    let file_data = std::fs::read(&target_file).expect("target file must be readable");
    assert_eq!(file_data.len(), PAYLOAD_SIZE);
    assert_eq!(
        file_data, *payload,
        "Bit-for-bit parity must be strictly preserved under dynamic CAS work-stealing"
    );

    // Verify complete memory arena reclamation
    assert_eq!(
        slab_pool.available_slabs(),
        TOTAL_SLABS,
        "All slabs must be reclaimed after work stealing completes"
    );
}
