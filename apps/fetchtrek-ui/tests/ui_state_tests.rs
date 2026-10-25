use std::rc::Rc;

use fetchtrek_ipc::TaskSummary;
use fetchtrek_types::TaskState;
use fetchtrek_ui::clipboard::{
    inspect_clipboard_for_url, read_clipboard_text, write_clipboard_text,
};
use fetchtrek_ui::store::TaskStore;
use fetchtrek_ui::{MainWindow, TaskDisplayStatus, TaskRowData};
use slint::{Model, VecModel};

fn make_test_summaries() -> Vec<TaskSummary> {
    vec![
        TaskSummary {
            id: 1,
            uuid: "task-1".to_string(),
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
            uuid: "task-2".to_string(),
            url: "https://download.jetbrains.com/rustrover/RustRover-2024.1.tar.gz".to_string(),
            destination_path: "C:\\Downloads\\RustRover-2024.1.tar.gz".to_string(),
            filename: "RustRover-2024.1.tar.gz".to_string(),
            total_bytes: 850_000_000,
            downloaded_bytes: 238_000_000,
            speed_bps: 0,
            status: TaskState::Paused,
            progress_percent: 28.0,
        },
        TaskSummary {
            id: 3,
            uuid: "task-3".to_string(),
            url: "https://archlinux.org/iso/archlinux.iso".to_string(),
            destination_path: "C:\\Downloads\\archlinux.iso".to_string(),
            filename: "archlinux.iso".to_string(),
            total_bytes: 900_000_000,
            downloaded_bytes: 900_000_000,
            speed_bps: 0,
            status: TaskState::Completed,
            progress_percent: 100.0,
        },
        TaskSummary {
            id: 4,
            uuid: "task-4".to_string(),
            url: "https://corrupt.source/broken.bin".to_string(),
            destination_path: "C:\\Downloads\\broken.bin".to_string(),
            filename: "broken.bin".to_string(),
            total_bytes: 100_000,
            downloaded_bytes: 0,
            speed_bps: 0,
            status: TaskState::FatalError,
            progress_percent: 0.0,
        },
    ]
}

#[test]
fn test_ui_store_filtering_and_selection() {
    let window = match MainWindow::new() {
        Ok(w) => w,
        Err(e) => {
            eprintln!("Skipping UI window test in headless environment without display: {e}");
            return;
        }
    };
    let model = Rc::new(VecModel::<TaskRowData>::default());
    let mut store = TaskStore::new(model.clone());

    // 1. Initial tasks verification
    store.set_initial_tasks(make_test_summaries(), &window);
    assert_eq!(model.row_count(), 4, "All tasks visible under 'all' filter");

    // 2. Filter Downloading
    store.set_filter("Downloading", &window);
    assert_eq!(model.row_count(), 1);
    assert_eq!(
        model.row_data(0).unwrap().status,
        TaskDisplayStatus::Downloading
    );

    // 3. Filter Paused
    store.set_filter("Paused", &window);
    assert_eq!(model.row_count(), 1);
    assert_eq!(model.row_data(0).unwrap().status, TaskDisplayStatus::Paused);

    // 4. Filter Completed
    store.set_filter("Completed", &window);
    assert_eq!(model.row_count(), 1);
    assert_eq!(
        model.row_data(0).unwrap().status,
        TaskDisplayStatus::Completed
    );

    // 5. Filter Failed
    store.set_filter("Failed", &window);
    assert_eq!(model.row_count(), 1);
    assert_eq!(model.row_data(0).unwrap().status, TaskDisplayStatus::Failed);

    // 6. Reset filter
    store.set_filter("All Tasks", &window);
    assert_eq!(model.row_count(), 4);

    // 7. Case-insensitive filename search
    store.set_search_query("UBUNTU", &window);
    assert_eq!(model.row_count(), 1);
    assert_eq!(model.row_data(0).unwrap().id.as_str(), "1");

    // 8. URL substring search
    store.set_search_query("jetbrains", &window);
    assert_eq!(model.row_count(), 1);
    assert_eq!(model.row_data(0).unwrap().id.as_str(), "2");

    // 9. No match
    store.set_search_query("nonexistent-file-query", &window);
    assert_eq!(model.row_count(), 0);

    // 10. Clear search
    store.set_search_query("", &window);
    assert_eq!(model.row_count(), 4);

    // 11. Selection preservation
    store.toggle_selection(2, &window);
    assert_eq!(store.selected_task_id, Some(2));

    // When filter is changed to Paused, Task 2 remains selected
    store.set_filter("Paused", &window);
    assert_eq!(store.selected_task_id, Some(2));

    // When filter excludes Task 2 (e.g. Completed), selection moves to visible Task 3
    store.set_filter("Completed", &window);
    assert_eq!(store.selected_task_id, Some(3));
}

#[test]
fn test_clipboard_read_write_roundtrip() {
    let sample_url = "https://fetchtrek.download/releases/v1.0.0/fetchtrek-setup.exe";
    let wrote = write_clipboard_text(sample_url);
    if wrote {
        let read = read_clipboard_text();
        assert_eq!(read.as_deref(), Some(sample_url));

        let detected = inspect_clipboard_for_url();
        assert_eq!(detected.as_deref(), Some(sample_url));
    }
}
