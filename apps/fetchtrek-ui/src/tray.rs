use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrayAction {
    ShowWindow,
    PauseAll,
    ResumeAll,
    Quit,
}

static TRAY_CALLBACK: OnceLock<Box<dyn Fn(TrayAction) + Send + Sync>> = OnceLock::new();

#[cfg(target_os = "windows")]
mod windows_impl {
    use super::*;
    use std::ptr;
    use std::thread;
    use windows_sys::Win32::Foundation::{HWND, LPARAM, LRESULT, POINT, WPARAM};
    use windows_sys::Win32::UI::Shell::{
        Shell_NotifyIconW, NIF_ICON, NIF_MESSAGE, NIF_TIP, NIM_ADD, NIM_DELETE, NOTIFYICONDATAW,
    };
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        AppendMenuW, CreatePopupMenu, CreateWindowExW, DefWindowProcW, DestroyMenu, DestroyWindow,
        DispatchMessageW, GetCursorPos, GetMessageW, LoadIconW, PostQuitMessage, RegisterClassW,
        SetForegroundWindow, TrackPopupMenu, TranslateMessage, IDI_APPLICATION, MF_GRAYED,
        MF_SEPARATOR, MF_STRING, MSG, TPM_RETURNCMD, TPM_RIGHTBUTTON, WM_APP, WM_CONTEXTMENU,
        WM_DESTROY, WM_LBUTTONDBLCLK, WM_LBUTTONUP, WM_RBUTTONUP, WNDCLASSW, WS_OVERLAPPEDWINDOW,
    };

    const WM_TRAYICON: u32 = WM_APP + 42;
    const MENU_HEADER: usize = 0;
    const MENU_SHOW: usize = 101;
    const MENU_PAUSE_ALL: usize = 102;
    const MENU_RESUME_ALL: usize = 103;
    const MENU_QUIT: usize = 104;

    fn to_wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    unsafe extern "system" fn tray_wnd_proc(
        hwnd: HWND,
        msg: u32,
        wparam: WPARAM,
        lparam: LPARAM,
    ) -> LRESULT {
        if msg == WM_TRAYICON {
            let event = lparam as u32;
            if event == WM_LBUTTONUP || event == WM_LBUTTONDBLCLK {
                if let Some(cb) = TRAY_CALLBACK.get() {
                    cb(TrayAction::ShowWindow);
                }
                return 0;
            } else if event == WM_RBUTTONUP || event == WM_CONTEXTMENU {
                SetForegroundWindow(hwnd);
                let menu = CreatePopupMenu();
                if !menu.is_null() {
                    let title = to_wide("FetchTrek Download Manager");
                    let show = to_wide("Show Window");
                    let pause = to_wide("Pause All Downloads");
                    let resume = to_wide("Resume All Downloads");
                    let quit = to_wide("Quit FetchTrek");

                    AppendMenuW(menu, MF_STRING | MF_GRAYED, MENU_HEADER, title.as_ptr());
                    AppendMenuW(menu, MF_SEPARATOR, 0, ptr::null());
                    AppendMenuW(menu, MF_STRING, MENU_SHOW, show.as_ptr());
                    AppendMenuW(menu, MF_STRING, MENU_PAUSE_ALL, pause.as_ptr());
                    AppendMenuW(menu, MF_STRING, MENU_RESUME_ALL, resume.as_ptr());
                    AppendMenuW(menu, MF_SEPARATOR, 0, ptr::null());
                    AppendMenuW(menu, MF_STRING, MENU_QUIT, quit.as_ptr());

                    let mut pt: POINT = std::mem::zeroed();
                    GetCursorPos(&mut pt);

                    let selected = TrackPopupMenu(
                        menu,
                        TPM_RIGHTBUTTON | TPM_RETURNCMD,
                        pt.x,
                        pt.y,
                        0,
                        hwnd,
                        ptr::null(),
                    );
                    DestroyMenu(menu);

                    if let Some(cb) = TRAY_CALLBACK.get() {
                        match selected as usize {
                            MENU_SHOW => cb(TrayAction::ShowWindow),
                            MENU_PAUSE_ALL => cb(TrayAction::PauseAll),
                            MENU_RESUME_ALL => cb(TrayAction::ResumeAll),
                            MENU_QUIT => cb(TrayAction::Quit),
                            _ => {}
                        }
                    }
                }
                return 0;
            }
        } else if msg == WM_DESTROY {
            PostQuitMessage(0);
            return 0;
        }

        DefWindowProcW(hwnd, msg, wparam, lparam)
    }

    pub fn start_tray_thread() {
        thread::Builder::new()
            .name("fetchtrek-tray".to_string())
            .spawn(move || unsafe {
                let class_name = to_wide("FetchTrekTrayWindowClass");
                let icon = LoadIconW(ptr::null_mut(), IDI_APPLICATION);

                let mut wc: WNDCLASSW = std::mem::zeroed();
                wc.lpfnWndProc = Some(tray_wnd_proc);
                wc.hInstance = ptr::null_mut();
                wc.lpszClassName = class_name.as_ptr();
                RegisterClassW(&wc);

                let title = to_wide("FetchTrek Tray Window");
                let hwnd = CreateWindowExW(
                    0,
                    class_name.as_ptr(),
                    title.as_ptr(),
                    WS_OVERLAPPEDWINDOW,
                    0,
                    0,
                    0,
                    0,
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null(),
                );

                if hwnd.is_null() {
                    return;
                }

                let mut nid: NOTIFYICONDATAW = std::mem::zeroed();
                nid.cbSize = std::mem::size_of::<NOTIFYICONDATAW>() as u32;
                nid.hWnd = hwnd;
                nid.uID = 1;
                nid.uFlags = NIF_MESSAGE | NIF_ICON | NIF_TIP;
                nid.uCallbackMessage = WM_TRAYICON;
                nid.hIcon = icon;

                let tip = "FetchTrek Download Manager";
                let tip_w: Vec<u16> = tip.encode_utf16().collect();
                let copy_len = tip_w.len().min(nid.szTip.len() - 1);
                nid.szTip[..copy_len].copy_from_slice(&tip_w[..copy_len]);

                Shell_NotifyIconW(NIM_ADD, &nid);

                let mut msg: MSG = std::mem::zeroed();
                while GetMessageW(&mut msg, ptr::null_mut(), 0, 0) > 0 {
                    TranslateMessage(&msg);
                    DispatchMessageW(&msg);
                }

                Shell_NotifyIconW(NIM_DELETE, &nid);
                DestroyWindow(hwnd);
            })
            .expect("Failed to start tray thread");
    }
}

pub fn init_tray<F>(on_action: F)
where
    F: Fn(TrayAction) + Send + Sync + 'static,
{
    let _ = TRAY_CALLBACK.set(Box::new(on_action));

    #[cfg(target_os = "windows")]
    {
        windows_impl::start_tray_thread();
    }
}
