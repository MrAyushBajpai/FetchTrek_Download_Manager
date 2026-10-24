use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_storage::allocate_sparse;

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
            "fetchtrek_sparse_{}_{}_{}",
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

#[test]
fn test_sparse_preallocation_latency() {
    let guard = TempFileGuard::new("1gib.bin");
    const ONE_GIB: u64 = 1_073_741_824;

    {
        let start = Instant::now();
        let file = allocate_sparse(&guard.path, ONE_GIB).expect("sparse allocation should succeed");
        let elapsed = start.elapsed();

        assert_eq!(
            file.metadata().expect("metadata should be readable").len(),
            ONE_GIB
        );
        assert!(
            elapsed < Duration::from_millis(15),
            "Sparse pre-allocation of 1 GiB took too long: {elapsed:?} (must be < 15ms)"
        );
    }
}

#[test]
fn test_sparse_zero_byte() {
    let guard = TempFileGuard::new("zero.bin");

    {
        let file =
            allocate_sparse(&guard.path, 0).expect("zero-byte sparse allocation should succeed");
        assert_eq!(file.metadata().expect("metadata").len(), 0);
    }
}

#[test]
fn test_sparse_invalid_path() {
    #[cfg(windows)]
    let invalid_path = PathBuf::from("Z:\\nonexistent_dir_12345\\uncreatable.bin");
    #[cfg(not(windows))]
    let invalid_path = PathBuf::from("/nonexistent_dir_12345/uncreatable.bin");

    let result = allocate_sparse(&invalid_path, 1024);
    assert!(
        result.is_err(),
        "allocating to invalid path must return error"
    );
}
