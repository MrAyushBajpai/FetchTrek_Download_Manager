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
    use std::process::Command;

    #[cfg(target_os = "macos")]
    let tools = ["pbpaste"];
    #[cfg(not(target_os = "macos"))]
    let tools = [
        "wl-paste",
        "xclip -selection clipboard -o",
        "xsel --clipboard --output",
    ];

    for tool in tools {
        let parts: Vec<&str> = tool.split_whitespace().collect();
        if let Some((cmd, args)) = parts.split_first() {
            if let Ok(output) = Command::new(cmd).args(args).output() {
                if output.status.success() {
                    if let Ok(s) = String::from_utf8(output.stdout) {
                        return Some(s);
                    }
                }
            }
        }
    }
    None
}

/// Writes text to the system clipboard using native platform mechanisms (FT-QA-019).
#[cfg(target_os = "windows")]
pub fn write_clipboard_text(text: &str) -> bool {
    use std::ptr;
    use windows_sys::Win32::System::DataExchange::{
        CloseClipboard, EmptyClipboard, OpenClipboard, SetClipboardData,
    };
    use windows_sys::Win32::System::Memory::{
        GlobalAlloc, GlobalLock, GlobalUnlock, GMEM_MOVEABLE,
    };

    const CF_UNICODETEXT: u32 = 13;

    let utf16: Vec<u16> = text.encode_utf16().chain(std::iter::once(0)).collect();
    let num_bytes = utf16.len() * std::mem::size_of::<u16>();

    unsafe {
        if OpenClipboard(ptr::null_mut()) == 0 {
            return false;
        }
        EmptyClipboard();

        let handle = GlobalAlloc(GMEM_MOVEABLE, num_bytes);
        if handle.is_null() {
            CloseClipboard();
            return false;
        }

        let ptr = GlobalLock(handle) as *mut u16;
        if ptr.is_null() {
            CloseClipboard();
            return false;
        }

        ptr::copy_nonoverlapping(utf16.as_ptr(), ptr, utf16.len());
        GlobalUnlock(handle);

        let res = SetClipboardData(CF_UNICODETEXT, handle);
        CloseClipboard();
        !res.is_null()
    }
}

/// Writes text to the system clipboard on non-Windows platforms.
#[cfg(not(target_os = "windows"))]
pub fn write_clipboard_text(text: &str) -> bool {
    use std::io::Write;
    use std::process::{Command, Stdio};

    #[cfg(target_os = "macos")]
    let tools = ["pbcopy"];
    #[cfg(not(target_os = "macos"))]
    let tools = [
        "wl-copy",
        "xclip -selection clipboard",
        "xsel --clipboard --input",
    ];

    for tool in tools {
        let parts: Vec<&str> = tool.split_whitespace().collect();
        if let Some((cmd, args)) = parts.split_first() {
            if let Ok(mut child) = Command::new(cmd)
                .args(args)
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
            {
                if let Some(mut stdin) = child.stdin.take() {
                    let _ = stdin.write_all(text.as_bytes());
                }
                if let Ok(status) = child.wait() {
                    if status.success() {
                        return true;
                    }
                }
            }
        }
    }
    false
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
