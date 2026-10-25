use std::rc::Rc;

use slint::VecModel;

mod ipc_bridge;
mod store;

use ipc_bridge::UiCommand;
use store::{init_store, with_store, with_store_mut};

slint::include_modules!();

fn main() -> Result<(), slint::PlatformError> {
    let main_window = MainWindow::new()?;
    let tasks_model = Rc::new(VecModel::<TaskRowData>::default());

    main_window.set_tasks(tasks_model.clone().into());
    main_window.set_daemon_status("Connecting...".into());
    init_store(tasks_model);

    let bridge = ipc_bridge::start_ipc_bridge(main_window.as_weak());

    // Row selection toggle
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
    main_window.on_row_open_folder(move |id_str| {
        println!("Open folder for task ID: {}", id_str);
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
