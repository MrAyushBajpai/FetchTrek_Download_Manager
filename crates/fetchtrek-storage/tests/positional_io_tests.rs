use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fetchtrek_storage::{AsyncStorage, PositionalStorage, SlabPool, SLAB_SIZE};
use fetchtrek_types::StorageError;

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
            "fetchtrek_pos_io_{}_{}_{}",
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
        let _ = fs::remove_file(&self.path);
    }
}

#[tokio::test]
async fn test_concurrent_multi_segment_positional_writes() {
    const TOTAL_FILE_BYTES: u64 = 4 * 1024 * 1024; // 4 MiB
    const SEGMENT_BYTES: u64 = 1024 * 1024; // 1 MiB per segment
    const NUM_SEGMENTS: usize = 4;
    const CHUNKS_PER_SEGMENT: usize = (SEGMENT_BYTES as usize) / SLAB_SIZE; // 16 slabs

    let guard = TempFileGuard::new("multi_segment.bin");
    let storage = Arc::new(PositionalStorage::new());
    let pool = SlabPool::new();

    // 1. Allocate sparse backing file
    storage
        .allocate_sparse(&guard.path, TOTAL_FILE_BYTES)
        .await
        .expect("sparse allocation should succeed");

    // 2. Prepare reference buffer for bit-for-bit verification
    let mut expected = vec![0u8; TOTAL_FILE_BYTES as usize];
    for seg in 0..NUM_SEGMENTS {
        for chunk in 0..CHUNKS_PER_SEGMENT {
            let offset = (seg as u64 * SEGMENT_BYTES) + (chunk as u64 * SLAB_SIZE as u64);
            let pattern_byte = ((seg * 53 + chunk * 17) & 0xFF) as u8;
            for i in 0..SLAB_SIZE {
                expected[offset as usize + i] = pattern_byte ^ (i as u8);
            }
        }
    }

    // 3. Spawn 4 concurrent tasks writing segments out-of-order
    let mut join_handles = Vec::with_capacity(NUM_SEGMENTS);
    // Write order: segment 2, 0, 3, 1
    let execution_order = [2, 0, 3, 1];

    for &seg in &execution_order {
        let task_storage = Arc::clone(&storage);
        let task_pool = Arc::clone(&pool);
        let dest = guard.path.clone();

        let handle = tokio::spawn(async move {
            for chunk in 0..CHUNKS_PER_SEGMENT {
                let offset = (seg as u64 * SEGMENT_BYTES) + (chunk as u64 * SLAB_SIZE as u64);
                let pattern_byte = ((seg * 53 + chunk * 17) & 0xFF) as u8;

                // Lease slab from bounded pool
                let mut lease = loop {
                    if let Some(l) = task_pool.acquire() {
                        break l;
                    }
                    tokio::task::yield_now().await;
                };

                let slice = lease.as_mut_slice();
                for (i, byte) in slice.iter_mut().enumerate().take(SLAB_SIZE) {
                    *byte = pattern_byte ^ (i as u8);
                }
                lease.valid_bytes = SLAB_SIZE;

                let written = task_storage
                    .write_positional(&dest, offset, lease)
                    .await
                    .expect("positional write must succeed");
                assert_eq!(written, SLAB_SIZE);
            }
        });
        join_handles.push(handle);
    }

    for handle in join_handles {
        handle.await.expect("task join failed");
    }

    // 4. Finalize file
    storage
        .finalize_file(&guard.path, TOTAL_FILE_BYTES)
        .await
        .expect("file finalization should succeed");

    // 5. Read back entire file and assert cryptographic bit-for-bit parity
    let actual = fs::read(&guard.path).expect("file should be readable");
    assert_eq!(actual.len(), TOTAL_FILE_BYTES as usize);
    assert_eq!(actual, expected, "bit-for-bit parity verification failed");
}

#[tokio::test]
async fn test_out_of_order_writes_and_seek_accuracy() {
    const TOTAL_BYTES: u64 = 3 * 1024 * 1024; // 3 MiB
    let guard = TempFileGuard::new("out_of_order.bin");
    let storage = PositionalStorage::new();
    let pool = SlabPool::new();

    storage
        .allocate_sparse(&guard.path, TOTAL_BYTES)
        .await
        .expect("allocate sparse");

    // Write chunk at offset 2 MiB
    let mut slab2 = pool.acquire().unwrap();
    slab2.as_mut_slice()[0..8].copy_from_slice(b"OFFSET2M");
    slab2.valid_bytes = 8;
    storage
        .write_positional(&guard.path, 2 * 1024 * 1024, slab2)
        .await
        .expect("write at 2MB");

    // Write chunk at offset 0
    let mut slab0 = pool.acquire().unwrap();
    slab0.as_mut_slice()[0..8].copy_from_slice(b"OFFSET00");
    slab0.valid_bytes = 8;
    storage
        .write_positional(&guard.path, 0, slab0)
        .await
        .expect("write at 0");

    // Write chunk at offset 1 MiB
    let mut slab1 = pool.acquire().unwrap();
    slab1.as_mut_slice()[0..8].copy_from_slice(b"OFFSET1M");
    slab1.valid_bytes = 8;
    storage
        .write_positional(&guard.path, 1024 * 1024, slab1)
        .await
        .expect("write at 1MB");

    storage
        .finalize_file(&guard.path, TOTAL_BYTES)
        .await
        .expect("finalize");

    let bytes = fs::read(&guard.path).expect("read file");
    assert_eq!(bytes.len(), TOTAL_BYTES as usize);

    assert_eq!(&bytes[0..8], b"OFFSET00");
    assert_eq!(&bytes[1024 * 1024..1024 * 1024 + 8], b"OFFSET1M");
    assert_eq!(&bytes[2 * 1024 * 1024..2 * 1024 * 1024 + 8], b"OFFSET2M");

    // Check that untouched sparse gap is zeroes
    assert_eq!(&bytes[8..16], &[0u8; 8]);
}

#[tokio::test]
async fn test_finalize_size_mismatch_fails() {
    const REAL_SIZE: u64 = 4 * 1024 * 1024;
    let guard = TempFileGuard::new("mismatch.bin");
    let storage = PositionalStorage::new();

    storage
        .allocate_sparse(&guard.path, REAL_SIZE)
        .await
        .expect("allocate");

    // Attempt finalize with expected 8,000,000 bytes
    let result = storage.finalize_file(&guard.path, 8_000_000).await;
    assert_eq!(result, Err(StorageError::AllocationFailed));
}
