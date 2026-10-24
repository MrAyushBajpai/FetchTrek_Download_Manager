use std::fs::OpenOptions;
use std::path::Path;

use async_trait::async_trait;
use fetchtrek_types::StorageError;

use crate::memory::LeasedSlab;

#[async_trait]
pub trait AsyncStorage: Send + Sync + 'static {
    async fn allocate_sparse(
        &self,
        destination: &Path,
        total_bytes: u64,
    ) -> Result<(), StorageError>;
    async fn write_positional(
        &self,
        destination: &Path,
        offset: u64,
        buffer: LeasedSlab,
    ) -> Result<usize, StorageError>;
    async fn sync_data(&self, destination: &Path) -> Result<(), StorageError>;
    async fn finalize_file(
        &self,
        destination: &Path,
        expected_bytes: u64,
    ) -> Result<(), StorageError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PositionalStorage;

impl PositionalStorage {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AsyncStorage for PositionalStorage {
    async fn allocate_sparse(
        &self,
        destination: &Path,
        total_bytes: u64,
    ) -> Result<(), StorageError> {
        crate::sparse::allocate_sparse(destination, total_bytes).map(|_| ())
    }

    async fn write_positional(
        &self,
        destination: &Path,
        offset: u64,
        buffer: LeasedSlab,
    ) -> Result<usize, StorageError> {
        let dest = destination.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .write(true)
                .open(&dest)
                .map_err(map_io_error)?;

            let slice = buffer.valid_slice();
            write_at_offset(&file, slice, offset).map_err(map_io_error)
        })
        .await
        .map_err(|_| StorageError::IoFailure)?
    }

    async fn sync_data(&self, destination: &Path) -> Result<(), StorageError> {
        let dest = destination.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .write(true)
                .open(&dest)
                .map_err(map_io_error)?;
            file.sync_data().map_err(map_io_error)?;
            Ok(())
        })
        .await
        .map_err(|_| StorageError::IoFailure)?
    }

    async fn finalize_file(
        &self,
        destination: &Path,
        expected_bytes: u64,
    ) -> Result<(), StorageError> {
        let dest = destination.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .write(true)
                .open(&dest)
                .map_err(map_io_error)?;

            let actual_len = file.metadata().map_err(map_io_error)?.len();
            if actual_len != expected_bytes {
                return Err(StorageError::AllocationFailed);
            }

            file.sync_data().map_err(map_io_error)?;
            Ok(())
        })
        .await
        .map_err(|_| StorageError::IoFailure)?
    }
}

#[cfg(windows)]
fn write_at_offset(
    file: &std::fs::File,
    mut slice: &[u8],
    mut offset: u64,
) -> std::io::Result<usize> {
    use std::os::windows::fs::FileExt;
    let total = slice.len();
    while !slice.is_empty() {
        let written = file.seek_write(slice, offset)?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write whole slice",
            ));
        }
        slice = &slice[written..];
        offset += written as u64;
    }
    Ok(total)
}

#[cfg(unix)]
fn write_at_offset(file: &std::fs::File, slice: &[u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(slice, offset)?;
    Ok(slice.len())
}

#[cfg(not(any(windows, unix)))]
fn write_at_offset(_file: &std::fs::File, _slice: &[u8], _offset: u64) -> std::io::Result<usize> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "positional write unsupported on this platform",
    ))
}

fn map_io_error(err: std::io::Error) -> StorageError {
    match err.kind() {
        std::io::ErrorKind::PermissionDenied => StorageError::AccessDenied,
        std::io::ErrorKind::NotFound => StorageError::PathNotFound,
        std::io::ErrorKind::StorageFull => StorageError::DiskFull,
        _ => {
            if let Some(code) = err.raw_os_error() {
                #[cfg(windows)]
                if code == 112 || code == 39 {
                    return StorageError::DiskFull;
                }
                #[cfg(unix)]
                if code == libc::ENOSPC {
                    return StorageError::DiskFull;
                }
            }
            StorageError::IoFailure
        }
    }
}
