use std::fs::{File, OpenOptions};
use std::path::Path;

use fetchtrek_types::StorageError;

pub fn allocate_sparse(destination: &Path, total_bytes: u64) -> Result<File, StorageError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(destination)
        .map_err(map_io_error)?;

    apply_sparse_hint(&file, total_bytes);

    file.set_len(total_bytes).map_err(map_io_error)?;

    Ok(file)
}

#[cfg(windows)]
fn apply_sparse_hint(file: &File, _total_bytes: u64) {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE;
    use windows_sys::Win32::System::IO::DeviceIoControl;

    let handle = file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
    let mut bytes_returned: u32 = 0;

    // Issue FSCTL_SET_SPARSE. If the filesystem does not support sparse files (e.g. FAT32),
    // we silently ignore the error and fall back to standard file extension via set_len.
    unsafe {
        DeviceIoControl(
            handle,
            FSCTL_SET_SPARSE,
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            0,
            &mut bytes_returned,
            std::ptr::null_mut(),
        );
    }
}

#[cfg(not(windows))]
fn apply_sparse_hint(_file: &File, _total_bytes: u64) {
    // On Unix platforms (Linux, macOS), standard POSIX ftruncate (invoked via File::set_len)
    // natively creates sparse file holes in O(1) time without allocating physical disk extents.
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
