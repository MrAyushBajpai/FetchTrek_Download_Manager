use slint::{Model, SharedString, VecModel};
use std::rc::Rc;

slint::include_modules!();

fn update_counts(window: &MainWindow, tasks: &VecModel<TaskRowData>) {
    let mut all = 0;
    let mut downloading = 0;
    let mut paused = 0;
    let mut completed = 0;
    let mut failed = 0;
    let mut selected = 0;

    for i in 0..tasks.row_count() {
        if let Some(task) = tasks.row_data(i) {
            all += 1;
            if task.is_selected {
                selected += 1;
            }
            match task.status {
                TaskDisplayStatus::Downloading => downloading += 1,
                TaskDisplayStatus::Paused => paused += 1,
                TaskDisplayStatus::Completed => completed += 1,
                TaskDisplayStatus::Failed => failed += 1,
                TaskDisplayStatus::Queued => {}
            }
        }
    }

    window.set_count_all(all);
    window.set_count_downloading(downloading);
    window.set_count_paused(paused);
    window.set_count_completed(completed);
    window.set_count_failed(failed);
    window.set_selected_count(selected);
}

fn create_initial_mock_tasks() -> Vec<TaskRowData> {
    vec![
        TaskRowData {
            id: SharedString::from("1"),
            filename: SharedString::from("ubuntu-24.04-desktop-amd64.iso"),
            file_type: SharedString::from("ISO"),
            total_size_formatted: SharedString::from("4.80 GB"),
            progress: 0.652,
            progress_percent_formatted: SharedString::from("65.2%"),
            status: TaskDisplayStatus::Downloading,
            speed_formatted: SharedString::from("18.4 MB/s"),
            eta_formatted: SharedString::from("00:01:24"),
            is_selected: false,
        },
        TaskRowData {
            id: SharedString::from("2"),
            filename: SharedString::from("RustRover-2024.1.tar.gz"),
            file_type: SharedString::from("GZ"),
            total_size_formatted: SharedString::from("850.0 MB"),
            progress: 0.280,
            progress_percent_formatted: SharedString::from("28.0%"),
            status: TaskDisplayStatus::Downloading,
            speed_formatted: SharedString::from("12.1 MB/s"),
            eta_formatted: SharedString::from("00:00:52"),
            is_selected: false,
        },
        TaskRowData {
            id: SharedString::from("3"),
            filename: SharedString::from("TensorFlow-Models-v2.zip"),
            file_type: SharedString::from("ZIP"),
            total_size_formatted: SharedString::from("1.20 GB"),
            progress: 0.450,
            progress_percent_formatted: SharedString::from("45.0%"),
            status: TaskDisplayStatus::Paused,
            speed_formatted: SharedString::from("--"),
            eta_formatted: SharedString::from("--:--:--"),
            is_selected: true,
        },
        TaskRowData {
            id: SharedString::from("4"),
            filename: SharedString::from("ArchLinux-x86_64-latest.iso"),
            file_type: SharedString::from("ISO"),
            total_size_formatted: SharedString::from("980.0 MB"),
            progress: 0.000,
            progress_percent_formatted: SharedString::from("0.0%"),
            status: TaskDisplayStatus::Queued,
            speed_formatted: SharedString::from("--"),
            eta_formatted: SharedString::from("--:--:--"),
            is_selected: false,
        },
        TaskRowData {
            id: SharedString::from("5"),
            filename: SharedString::from("FetchTrek-CLI-x86_64.msi"),
            file_type: SharedString::from("MSI"),
            total_size_formatted: SharedString::from("14.2 MB"),
            progress: 1.000,
            progress_percent_formatted: SharedString::from("100.0%"),
            status: TaskDisplayStatus::Completed,
            speed_formatted: SharedString::from("--"),
            eta_formatted: SharedString::from("Done"),
            is_selected: false,
        },
        TaskRowData {
            id: SharedString::from("6"),
            filename: SharedString::from("Corrupted-Payload-Test.bin"),
            file_type: SharedString::from("BIN"),
            total_size_formatted: SharedString::from("256.0 MB"),
            progress: 0.125,
            progress_percent_formatted: SharedString::from("12.5%"),
            status: TaskDisplayStatus::Failed,
            speed_formatted: SharedString::from("--"),
            eta_formatted: SharedString::from("Error 416"),
            is_selected: false,
        },
    ]
}

fn main() -> Result<(), slint::PlatformError> {
    let main_window = MainWindow::new()?;
    let tasks_model = Rc::new(VecModel::from(create_initial_mock_tasks()));

    main_window.set_tasks(tasks_model.clone().into());
    main_window.set_aggregate_download_speed(SharedString::from("30.5 MB/s"));
    main_window.set_aggregate_upload_speed(SharedString::from("0.0 MB/s"));
    update_counts(&main_window, &tasks_model);

    // Row selection toggle
    {
        let tasks = tasks_model.clone();
        let weak_win = main_window.as_weak();
        main_window.on_row_selected(move |id| {
            for i in 0..tasks.row_count() {
                if let Some(mut task) = tasks.row_data(i) {
                    if task.id == id {
                        task.is_selected = !task.is_selected;
                        tasks.set_row_data(i, task);
                        break;
                    }
                }
            }
            if let Some(win) = weak_win.upgrade() {
                update_counts(&win, &tasks);
            }
        });
    }

    // Toggle pause / resume
    {
        let tasks = tasks_model.clone();
        let weak_win = main_window.as_weak();
        main_window.on_row_toggle_pause_resume(move |id| {
            for i in 0..tasks.row_count() {
                if let Some(mut task) = tasks.row_data(i) {
                    if task.id == id {
                        if task.status == TaskDisplayStatus::Downloading {
                            task.status = TaskDisplayStatus::Paused;
                            task.speed_formatted = SharedString::from("--");
                            task.eta_formatted = SharedString::from("--:--:--");
                        } else {
                            task.status = TaskDisplayStatus::Downloading;
                            task.speed_formatted = SharedString::from("15.2 MB/s");
                            task.eta_formatted = SharedString::from("00:01:10");
                        }
                        tasks.set_row_data(i, task);
                        break;
                    }
                }
            }
            if let Some(win) = weak_win.upgrade() {
                update_counts(&win, &tasks);
            }
        });
    }

    // Row delete
    {
        let tasks = tasks_model.clone();
        let weak_win = main_window.as_weak();
        main_window.on_row_delete(move |id| {
            for i in 0..tasks.row_count() {
                if let Some(task) = tasks.row_data(i) {
                    if task.id == id {
                        tasks.remove(i);
                        break;
                    }
                }
            }
            if let Some(win) = weak_win.upgrade() {
                update_counts(&win, &tasks);
            }
        });
    }

    // Row open folder
    main_window.on_row_open_folder(move |id| {
        println!("Open folder for task ID: {}", id);
    });

    // Batch resume
    {
        let tasks = tasks_model.clone();
        let weak_win = main_window.as_weak();
        main_window.on_batch_resume_clicked(move || {
            for i in 0..tasks.row_count() {
                if let Some(mut task) = tasks.row_data(i) {
                    if task.is_selected && task.status != TaskDisplayStatus::Completed {
                        task.status = TaskDisplayStatus::Downloading;
                        task.speed_formatted = SharedString::from("16.0 MB/s");
                        task.eta_formatted = SharedString::from("00:01:05");
                        tasks.set_row_data(i, task);
                    }
                }
            }
            if let Some(win) = weak_win.upgrade() {
                update_counts(&win, &tasks);
            }
        });
    }

    // Batch pause
    {
        let tasks = tasks_model.clone();
        let weak_win = main_window.as_weak();
        main_window.on_batch_pause_clicked(move || {
            for i in 0..tasks.row_count() {
                if let Some(mut task) = tasks.row_data(i) {
                    if task.is_selected && task.status == TaskDisplayStatus::Downloading {
                        task.status = TaskDisplayStatus::Paused;
                        task.speed_formatted = SharedString::from("--");
                        task.eta_formatted = SharedString::from("--:--:--");
                        tasks.set_row_data(i, task);
                    }
                }
            }
            if let Some(win) = weak_win.upgrade() {
                update_counts(&win, &tasks);
            }
        });
    }

    // Batch delete
    {
        let tasks = tasks_model.clone();
        let weak_win = main_window.as_weak();
        main_window.on_batch_delete_clicked(move || {
            let mut i = 0;
            while i < tasks.row_count() {
                if let Some(task) = tasks.row_data(i) {
                    if task.is_selected {
                        tasks.remove(i);
                        continue;
                    }
                }
                i += 1;
            }
            if let Some(win) = weak_win.upgrade() {
                update_counts(&win, &tasks);
            }
        });
    }

    // Deselect all
    {
        let tasks = tasks_model.clone();
        let weak_win = main_window.as_weak();
        main_window.on_deselect_all_clicked(move || {
            for i in 0..tasks.row_count() {
                if let Some(mut task) = tasks.row_data(i) {
                    if task.is_selected {
                        task.is_selected = false;
                        tasks.set_row_data(i, task);
                    }
                }
            }
            if let Some(win) = weak_win.upgrade() {
                update_counts(&win, &tasks);
            }
        });
    }

    // Toolbar actions
    main_window.on_add_download_clicked(move || {
        println!("Add URL modal requested");
    });

    {
        let tasks = tasks_model.clone();
        let weak_win = main_window.as_weak();
        main_window.on_resume_all_clicked(move || {
            for i in 0..tasks.row_count() {
                if let Some(mut task) = tasks.row_data(i) {
                    if task.status == TaskDisplayStatus::Paused
                        || task.status == TaskDisplayStatus::Queued
                    {
                        task.status = TaskDisplayStatus::Downloading;
                        task.speed_formatted = SharedString::from("14.5 MB/s");
                        task.eta_formatted = SharedString::from("00:01:15");
                        tasks.set_row_data(i, task);
                    }
                }
            }
            if let Some(win) = weak_win.upgrade() {
                update_counts(&win, &tasks);
            }
        });
    }

    {
        let tasks = tasks_model;
        let weak_win = main_window.as_weak();
        main_window.on_pause_all_clicked(move || {
            for i in 0..tasks.row_count() {
                if let Some(mut task) = tasks.row_data(i) {
                    if task.status == TaskDisplayStatus::Downloading {
                        task.status = TaskDisplayStatus::Paused;
                        task.speed_formatted = SharedString::from("--");
                        task.eta_formatted = SharedString::from("--:--:--");
                        tasks.set_row_data(i, task);
                    }
                }
            }
            if let Some(win) = weak_win.upgrade() {
                update_counts(&win, &tasks);
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
