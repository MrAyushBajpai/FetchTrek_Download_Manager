use std::rc::Rc;
use std::sync::{Arc, RwLock};

use fetchtrek_ipc::TaskSummary;
use fetchtrek_types::TaskState;
use slint::VecModel;

mod clipboard;
mod config;
mod ipc_bridge;
mod store;
mod tray;

use ipc_bridge::UiCommand;
use store::{init_store, with_store, with_store_mut};
use tray::TrayAction;

slint::include_modules!();

fn create_initial_mock_summaries() -> Vec<TaskSummary> {
    vec![
        TaskSummary {
            id: 1,
            uuid: "mock-1".to_string(),
            url: "https://releases.ubuntu.com/24.04/ubuntu-24.04-desktop-amd64.iso".to_string(),
            destination_path: "C:\\Downloads\\ubuntu-24.04-desktop-amd64.iso".to_string(),
            filename: "ubuntu-24.04-desktop-amd64.iso".to_string(),
            total_bytes: 4_800_000_000,
            downloaded_bytes: 3_129_600_000,
            speed_bps: 18_400_000,
            status: TaskState::Transferring,
            progress_percent: 65.2,
        },
        TaskSummary {
            id: 2,
            uuid: "mock-2".to_string(),
            url: "https://download.jetbrains.com/rustrover/RustRover-2024.1.tar.gz".to_string(),
            destination_path: "C:\\Downloads\\RustRover-2024.1.tar.gz".to_string(),
            filename: "RustRover-2024.1.tar.gz".to_string(),
            total_bytes: 850_000_000,
            downloaded_bytes: 238_000_000,
            speed_bps: 12_100_000,
            status: TaskState::Transferring,
            progress_percent: 28.0,
        },
        TaskSummary {
            id: 3,
            uuid: "mock-3".to_string(),
            url: "https://storage.googleapis.com/models/TensorFlow-Models-v2.zip".to_string(),
            destination_path: "C:\\Downloads\\TensorFlow-Models-v2.zip".to_string(),
            filename: "TensorFlow-Models-v2.zip".to_string(),
            total_bytes: 1_200_000_000,
            downloaded_bytes: 540_000_000,
            speed_bps: 0,
            status: TaskState::Paused,
            progress_percent: 45.0,
        },
        TaskSummary {
            id: 4,
            uuid: "mock-4".to_string(),
            url: "https://geo.mirror.pkgbuild.com/iso/latest/archlinux-x86_64.iso".to_string(),
            destination_path: "C:\\Downloads\\archlinux-x86_64.iso".to_string(),
            filename: "archlinux-x86_64.iso".to_string(),
            total_bytes: 980_000_000,
            downloaded_bytes: 0,
            speed_bps: 0,
            status: TaskState::Queued,
            progress_percent: 0.0,
        },
        TaskSummary {
            id: 5,
            uuid: "mock-5".to_string(),
            url: "https://github.com/FetchTrek/cli/releases/download/v0.1.0/FetchTrek-CLI.msi"
                .to_string(),
            destination_path: "C:\\Downloads\\FetchTrek-CLI.msi".to_string(),
            filename: "FetchTrek-CLI.msi".to_string(),
            total_bytes: 14_200_000,
            downloaded_bytes: 14_200_000,
            speed_bps: 0,
            status: TaskState::Completed,
            progress_percent: 100.0,
        },
    ]
}

fn reveal_in_file_manager(path: &str) {
    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("explorer.exe")
            .arg("/select,")
            .arg(path)
            .spawn();
    }

    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("open")
            .arg("-R")
            .arg(path)
            .spawn();
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        let p = std::path::Path::new(path);
        let dir = p.parent().unwrap_or(p);
        let _ = std::process::Command::new("xdg-open").arg(dir).spawn();
    }
}

fn main() -> Result<(), slint::PlatformError> {
    let main_window = MainWindow::new()?;
    let tasks_model = Rc::new(VecModel::<TaskRowData>::default());

    main_window.set_tasks(tasks_model.clone().into());
    main_window.set_daemon_status("Connecting...".into());
    init_store(tasks_model);

    // Load persisted settings and synchronize Slint model
    let initial_settings = config::load_settings();
    let current_settings = Arc::new(RwLock::new(initial_settings.clone()));

    let slint_settings = UserSettingsData {
        theme_mode: initial_settings.theme_mode,
        start_on_boot: initial_settings.start_on_boot,
        minimize_to_tray: initial_settings.minimize_to_tray,
        show_notifications: initial_settings.show_notifications,
        default_download_path: initial_settings.default_download_path.clone().into(),
        sparse_preallocation: initial_settings.sparse_preallocation,
        cursor_rewind_kb: initial_settings.cursor_rewind_kb,
        global_speed_limit_mb: initial_settings.global_speed_limit_mb,
        max_active_tasks: initial_settings.max_active_tasks,
        default_connections: initial_settings.default_connections,
    };
    main_window.set_settings_data(slint_settings);

    // Intercept window close request for minimize-to-tray lifecycle
    {
        let settings_for_close = current_settings.clone();
        main_window.window().on_close_requested(move || {
            let minimize = settings_for_close
                .read()
                .map(|s| s.minimize_to_tray)
                .unwrap_or(true);
            if minimize {
                slint::CloseRequestResponse::HideWindow
            } else {
                std::process::exit(0);
            }
        });
    }

    // Seed mock data for immediate standalone visualization
    with_store_mut(|st| {
        st.set_initial_tasks(create_initial_mock_summaries(), &main_window);
        if let Some(record) = st.tasks.get_mut(&1) {
            for i in 0..60 {
                let speed = 12_000_000 + ((i * 350_000) % 8_000_000);
                record.push_speed(speed);
            }
        }
        st.update_inspector(&main_window);
    });

    let bridge = ipc_bridge::start_ipc_bridge(main_window.as_weak());

    // Initialize System Tray
    {
        let weak_win = main_window.as_weak();
        let tray_cmd_tx = bridge.command_tx.clone();
        tray::init_tray(move |action| match action {
            TrayAction::ShowWindow => {
                let weak = weak_win.clone();
                let _ = slint::invoke_from_event_loop(move || {
                    if let Some(win) = weak.upgrade() {
                        let _ = win.show();
                    }
                });
            }
            TrayAction::PauseAll => {
                let tx = tray_cmd_tx.clone();
                let _ = slint::invoke_from_event_loop(move || {
                    let active = with_store(|st| st.get_active_ids()).unwrap_or_default();
                    for id in active {
                        let _ = tx.try_send(UiCommand::PauseTask(id));
                    }
                });
            }
            TrayAction::ResumeAll => {
                let tx = tray_cmd_tx.clone();
                let _ = slint::invoke_from_event_loop(move || {
                    let paused = with_store(|st| st.get_paused_or_queued_ids()).unwrap_or_default();
                    for id in paused {
                        let _ = tx.try_send(UiCommand::ResumeTask(id));
                    }
                });
            }
            TrayAction::Quit => {
                std::process::exit(0);
            }
        });
    }

    // Row selection toggle & inspector focus
    {
        let weak_win = main_window.as_weak();
        main_window.on_row_selected(move |id_str| {
            if let Ok(id) = id_str.parse::<i64>() {
                if let Some(win) = weak_win.upgrade() {
                    with_store_mut(|st| {
                        st.toggle_selection(id, &win);
                    });
                }
            }
        });
    }

    // Row toggle pause / resume
    {
        let cmd_tx = bridge.command_tx.clone();
        main_window.on_row_toggle_pause_resume(move |id_str| {
            if let Ok(id) = id_str.parse::<i64>() {
                let is_active = with_store(|st| {
                    st.tasks
                        .get(&id)
                        .map(|t| t.status == fetchtrek_types::TaskState::Transferring)
                        .unwrap_or(false)
                })
                .unwrap_or(false);

                let cmd = if is_active {
                    UiCommand::PauseTask(id)
                } else {
                    UiCommand::ResumeTask(id)
                };
                let _ = cmd_tx.try_send(cmd);
            }
        });
    }

    // Row delete
    {
        let cmd_tx = bridge.command_tx.clone();
        let weak_win = main_window.as_weak();
        main_window.on_row_delete(move |id_str| {
            if let Ok(id) = id_str.parse::<i64>() {
                let _ = cmd_tx.try_send(UiCommand::CancelTask {
                    task_id: id,
                    delete_file: false,
                });
                if let Some(win) = weak_win.upgrade() {
                    with_store_mut(|st| {
                        st.remove_task(id, &win);
                    });
                }
            }
        });
    }

    // Row open folder
    {
        main_window.on_row_open_folder(move |id_str| {
            if let Ok(id) = id_str.parse::<i64>() {
                if let Some(path) = with_store(|st| {
                    st.tasks
                        .get(&id)
                        .map(|t| t.destination_path.clone())
                        .unwrap_or_default()
                }) {
                    reveal_in_file_manager(&path);
                }
            }
        });
    }

    // Inspector open folder
    main_window.on_open_folder_clicked(move |path| {
        reveal_in_file_manager(&path);
    });

    // Inspector copy URL
    main_window.on_copy_url_clicked(move |url| {
        println!("Copy URL requested: {}", url);
    });

    // Batch resume
    {
        let cmd_tx = bridge.command_tx.clone();
        main_window.on_batch_resume_clicked(move || {
            let selected = with_store(|st| st.get_selected_ids()).unwrap_or_default();
            for id in selected {
                let _ = cmd_tx.try_send(UiCommand::ResumeTask(id));
            }
        });
    }

    // Batch pause
    {
        let cmd_tx = bridge.command_tx.clone();
        main_window.on_batch_pause_clicked(move || {
            let selected = with_store(|st| st.get_selected_ids()).unwrap_or_default();
            for id in selected {
                let _ = cmd_tx.try_send(UiCommand::PauseTask(id));
            }
        });
    }

    // Batch delete
    {
        let cmd_tx = bridge.command_tx.clone();
        let weak_win = main_window.as_weak();
        main_window.on_batch_delete_clicked(move || {
            let selected = with_store(|st| st.get_selected_ids()).unwrap_or_default();
            for id in selected {
                let _ = cmd_tx.try_send(UiCommand::CancelTask {
                    task_id: id,
                    delete_file: false,
                });
                if let Some(win) = weak_win.upgrade() {
                    with_store_mut(|st| {
                        st.remove_task(id, &win);
                    });
                }
            }
        });
    }

    // Deselect all
    {
        let weak_win = main_window.as_weak();
        main_window.on_deselect_all_clicked(move || {
            if let Some(win) = weak_win.upgrade() {
                with_store_mut(|st| {
                    st.deselect_all(&win);
                });
            }
        });
    }

    // Open Add Download Modal
    {
        let weak_win = main_window.as_weak();
        let settings_ref = current_settings.clone();
        main_window.on_add_download_clicked(move || {
            if let Some(win) = weak_win.upgrade() {
                let cur_dest = settings_ref
                    .read()
                    .map(|s| s.default_download_path.clone())
                    .unwrap_or_else(|_| "C:\\Downloads".to_string());
                let cur_conns = settings_ref
                    .read()
                    .map(|s| s.default_connections)
                    .unwrap_or(4);
                let detected_url = clipboard::inspect_clipboard_for_url().unwrap_or_default();
                let form = AddTaskFormData {
                    url: detected_url.into(),
                    destination_path: cur_dest.into(),
                    filename: "".into(),
                    connections: cur_conns,
                    referer: "".into(),
                    user_agent: "".into(),
                    cookies: "".into(),
                    store_in_vault: true,
                };
                win.set_add_form_data(form);
                win.set_show_add_modal(true);
            }
        });
    }

    // Modal Paste Clipboard
    {
        let weak_win = main_window.as_weak();
        main_window.on_add_modal_paste_clipboard(move || {
            if let Some(win) = weak_win.upgrade() {
                if let Some(text) = clipboard::read_clipboard_text() {
                    let mut data = win.get_add_form_data();
                    data.url = text.trim().into();
                    win.set_add_form_data(data);
                }
            }
        });
    }

    // Modal Browse Path
    {
        let weak_win = main_window.as_weak();
        main_window.on_add_modal_browse_path(move || {
            if let Some(win) = weak_win.upgrade() {
                let mut data = win.get_add_form_data();
                data.destination_path = config::get_default_download_dir().into();
                win.set_add_form_data(data);
            }
        });
    }

    // Modal Cancel
    {
        let weak_win = main_window.as_weak();
        main_window.on_add_modal_cancel(move || {
            if let Some(win) = weak_win.upgrade() {
                win.set_show_add_modal(false);
            }
        });
    }

    // Modal Submit
    {
        let weak_win = main_window.as_weak();
        let cmd_tx = bridge.command_tx.clone();
        main_window.on_add_modal_submit(move |form| {
            if let Some(win) = weak_win.upgrade() {
                win.set_show_add_modal(false);
                let url = form.url.trim().to_string();
                if !url.is_empty() {
                    let dest = if form.destination_path.trim().is_empty() {
                        None
                    } else {
                        Some(form.destination_path.trim().to_string())
                    };
                    let filename = if form.filename.trim().is_empty() {
                        url.rsplit('/')
                            .next()
                            .filter(|s| !s.is_empty())
                            .map(|s| s.split('?').next().unwrap_or(s).to_string())
                    } else {
                        Some(form.filename.trim().to_string())
                    };
                    let conns = Some(form.connections as usize);

                    let _ = cmd_tx.try_send(UiCommand::AddTask {
                        url: url.clone(),
                        destination_path: dest.clone(),
                        filename: filename.clone(),
                        connections: conns,
                    });

                    let task_name = filename.unwrap_or_else(|| "download.bin".to_string());
                    let dest_path = dest.unwrap_or_else(|| "C:\\Downloads".to_string());
                    let new_id = with_store(|st| st.tasks.keys().max().copied().unwrap_or(0) + 1)
                        .unwrap_or(1);

                    let mut record = store::TaskModelRecord::new(new_id, task_name, url, dest_path);
                    record.connection_count = form.connections.max(1) as usize;
                    record.status = fetchtrek_types::TaskState::Transferring;
                    with_store_mut(|st| {
                        st.add_task(record, &win);
                    });
                }
            }
        });
    }

    // Resume All
    {
        let cmd_tx = bridge.command_tx.clone();
        main_window.on_resume_all_clicked(move || {
            let paused = with_store(|st| st.get_paused_or_queued_ids()).unwrap_or_default();
            for id in paused {
                let _ = cmd_tx.try_send(UiCommand::ResumeTask(id));
            }
        });
    }

    // Pause All
    {
        let cmd_tx = bridge.command_tx.clone();
        main_window.on_pause_all_clicked(move || {
            let active = with_store(|st| st.get_active_ids()).unwrap_or_default();
            for id in active {
                let _ = cmd_tx.try_send(UiCommand::PauseTask(id));
            }
        });
    }

    // Open Settings Modal
    {
        let weak_win = main_window.as_weak();
        main_window.on_settings_clicked(move || {
            if let Some(win) = weak_win.upgrade() {
                win.set_show_settings_modal(true);
            }
        });
    }

    // Settings Modal Cancel
    {
        let weak_win = main_window.as_weak();
        main_window.on_settings_modal_cancel(move || {
            if let Some(win) = weak_win.upgrade() {
                win.set_show_settings_modal(false);
            }
        });
    }

    // Settings Modal Save
    {
        let weak_win = main_window.as_weak();
        let settings_ref = current_settings.clone();
        main_window.on_settings_modal_save(move |data| {
            if let Some(win) = weak_win.upgrade() {
                win.set_show_settings_modal(false);
                let updated = config::UserSettingsData {
                    theme_mode: data.theme_mode,
                    start_on_boot: data.start_on_boot,
                    minimize_to_tray: data.minimize_to_tray,
                    show_notifications: data.show_notifications,
                    default_download_path: data.default_download_path.to_string(),
                    sparse_preallocation: data.sparse_preallocation,
                    cursor_rewind_kb: data.cursor_rewind_kb,
                    global_speed_limit_mb: data.global_speed_limit_mb,
                    max_active_tasks: data.max_active_tasks,
                    default_connections: data.default_connections,
                };
                let _ = config::save_settings(&updated);
                if let Ok(mut lock) = settings_ref.write() {
                    *lock = updated;
                }
            }
        });
    }

    // Settings Modal Browse Path
    {
        let weak_win = main_window.as_weak();
        main_window.on_settings_modal_browse_path(move || {
            if let Some(win) = weak_win.upgrade() {
                let mut data = win.get_settings_data();
                data.default_download_path = config::get_default_download_dir().into();
                win.set_settings_data(data);
            }
        });
    }

    main_window.on_search_query_changed(move |query| {
        println!("Search query updated: {}", query);
    });

    main_window.on_filter_category_selected(move |category| {
        println!("Sidebar filter selected: {}", category);
    });

    main_window.run()
}
