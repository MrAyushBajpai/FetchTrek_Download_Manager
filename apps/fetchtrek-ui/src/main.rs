use std::rc::Rc;

use fetchtrek_ipc::TaskSummary;
use fetchtrek_types::TaskState;
use slint::VecModel;

mod ipc_bridge;
mod store;

use ipc_bridge::UiCommand;
use store::{init_store, with_store, with_store_mut};

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

    // Seed mock data for immediate standalone visualization
    with_store_mut(|st| {
        st.set_initial_tasks(create_initial_mock_summaries(), &main_window);
        // Pre-populate some history points for task 1 to show sparkline
        if let Some(record) = st.tasks.get_mut(&1) {
            for i in 0..60 {
                let speed = 12_000_000 + ((i * 350_000) % 8_000_000);
                record.push_speed(speed);
            }
        }
        st.update_inspector(&main_window);
    });

    let bridge = ipc_bridge::start_ipc_bridge(main_window.as_weak());

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

    // Toolbar actions
    main_window.on_add_download_clicked(move || {
        println!("Add URL modal requested");
    });

    {
        let cmd_tx = bridge.command_tx.clone();
        main_window.on_resume_all_clicked(move || {
            let paused = with_store(|st| st.get_paused_or_queued_ids()).unwrap_or_default();
            for id in paused {
                let _ = cmd_tx.try_send(UiCommand::ResumeTask(id));
            }
        });
    }

    {
        let cmd_tx = bridge.command_tx;
        main_window.on_pause_all_clicked(move || {
            let active = with_store(|st| st.get_active_ids()).unwrap_or_default();
            for id in active {
                let _ = cmd_tx.try_send(UiCommand::PauseTask(id));
            }
        });
    }

    main_window.on_settings_clicked(move || {
        println!("Settings drawer requested");
    });

    main_window.on_search_query_changed(move |query| {
        println!("Search query updated: {}", query);
    });

    main_window.on_filter_category_selected(move |category| {
        println!("Sidebar filter selected: {}", category);
    });

    main_window.run()
}
