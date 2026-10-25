#[cfg(target_os = "windows")]
pub fn read_clipboard_text() -> Option<String> {
    use std::ptr;
    use windows_sys::Win32::System::DataExchange::{
        CloseClipboard, GetClipboardData, OpenClipboard,
    };
    use windows_sys::Win32::System::Memory::{GlobalLock, GlobalUnlock};

    const CF_UNICODETEXT: u32 = 13;

    unsafe {
        if OpenClipboard(ptr::null_mut()) == 0 {
            return None;
        }
        let handle = GetClipboardData(CF_UNICODETEXT);
        if handle.is_null() {
            CloseClipboard();
            return None;
        }
        let ptr = GlobalLock(handle) as *const u16;
        if ptr.is_null() {
            CloseClipboard();
            return None;
        }

        let mut len = 0;
        while *ptr.add(len) != 0 {
            len += 1;
        }

        let slice = std::slice::from_raw_parts(ptr, len);
        let s = String::from_utf16_lossy(slice);

        GlobalUnlock(handle);
        CloseClipboard();
        Some(s)
    }
}

#[cfg(not(target_os = "windows"))]
pub fn read_clipboard_text() -> Option<String> {
    None
}

/// Privacy-filtered clipboard URL inspector
pub fn inspect_clipboard_for_url() -> Option<String> {
    let text = read_clipboard_text()?;
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.len() > 2048 {
        return None;
    }
    if trimmed.starts_with("http://")
        || trimmed.starts_with("https://")
        || trimmed.starts_with("magnet:")
    {
        Some(trimmed.to_string())
    } else {
        None
    }
}
