use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use fetchtrek_ipc::{TaskProgress, TaskSummary, TelemetrySnapshot};
use fetchtrek_types::TaskState;
use slint::{Model, SharedString, VecModel};

use crate::{MainWindow, TaskDisplayStatus, TaskRowData};

thread_local! {
    static STORE: RefCell<Option<TaskStore>> = const { RefCell::new(None) };
}

pub fn init_store(slint_model: Rc<VecModel<TaskRowData>>) {
    STORE.with(|s| {
        *s.borrow_mut() = Some(TaskStore::new(slint_model));
    });
}

pub fn with_store_mut<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&mut TaskStore) -> R,
{
    STORE.with(|s| s.borrow_mut().as_mut().map(f))
}

pub fn with_store<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&TaskStore) -> R,
{
    STORE.with(|s| s.borrow().as_ref().map(f))
}

pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

pub fn format_speed(bps: u64) -> String {
    if bps == 0 {
        "--".to_string()
    } else if bps >= 1024 * 1024 {
        format!("{:.1} MB/s", bps as f64 / (1024.0 * 1024.0))
    } else if bps >= 1024 {
        format!("{:.1} KB/s", bps as f64 / 1024.0)
    } else {
        format!("{} B/s", bps)
    }
}

pub fn format_eta(total_bytes: u64, downloaded_bytes: u64, speed_bps: u64) -> String {
    if total_bytes > 0 && downloaded_bytes >= total_bytes {
        return "Done".to_string();
    }
    if speed_bps == 0 {
        return "--:--:--".to_string();
    }

    let remaining = total_bytes.saturating_sub(downloaded_bytes);
    let total_secs = remaining / speed_bps;
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    if hours > 99 {
        "> 99 hrs".to_string()
    } else {
        format!("{:02}:{:02}:{:02}", hours, mins, secs)
    }
}

pub fn extract_file_type(filename: &str) -> String {
    filename
        .rsplit('.')
        .next()
        .map(|ext| ext.to_ascii_uppercase())
        .unwrap_or_else(|| "FILE".to_string())
}

pub fn map_task_state_to_display(state: TaskState) -> TaskDisplayStatus {
    match state {
        TaskState::Transferring
        | TaskState::Resolving
        | TaskState::Allocating
        | TaskState::PostProcessing => TaskDisplayStatus::Downloading,
        TaskState::Paused => TaskDisplayStatus::Paused,
        TaskState::Queued | TaskState::Retrying => TaskDisplayStatus::Queued,
        TaskState::Completed => TaskDisplayStatus::Completed,
        TaskState::FatalError => TaskDisplayStatus::Failed,
    }
}

#[derive(Clone, Debug)]
pub struct TaskModelRecord {
    pub id: i64,
    pub filename: String,
    pub file_type: String,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub speed_bps: u64,
    pub status: TaskState,
    pub is_selected: bool,
}

impl TaskModelRecord {
    pub fn to_row_data(&self) -> TaskRowData {
        let progress = if self.total_bytes > 0 {
            (self.downloaded_bytes as f32 / self.total_bytes as f32).clamp(0.0, 1.0)
        } else {
            0.0
        };

        let progress_percent_formatted = format!("{:.1}%", progress * 100.0);
        let speed_formatted = format_speed(self.speed_bps);
        let eta_formatted = format_eta(self.total_bytes, self.downloaded_bytes, self.speed_bps);

        TaskRowData {
            id: SharedString::from(self.id.to_string()),
            filename: SharedString::from(&self.filename),
            file_type: SharedString::from(&self.file_type),
            total_size_formatted: SharedString::from(format_bytes(self.total_bytes)),
            progress,
            progress_percent_formatted: SharedString::from(progress_percent_formatted),
            status: map_task_state_to_display(self.status),
            speed_formatted: SharedString::from(speed_formatted),
            eta_formatted: SharedString::from(eta_formatted),
            is_selected: self.is_selected,
        }
    }
}

pub struct TaskStore {
    pub tasks: HashMap<i64, TaskModelRecord>,
    pub id_order: Vec<i64>,
    pub slint_model: Rc<VecModel<TaskRowData>>,
}

impl TaskStore {
    pub fn new(slint_model: Rc<VecModel<TaskRowData>>) -> Self {
        Self {
            tasks: HashMap::new(),
            id_order: Vec::new(),
            slint_model,
        }
    }

    pub fn set_initial_tasks(&mut self, summaries: Vec<TaskSummary>, window: &MainWindow) {
        self.tasks.clear();
        self.id_order.clear();

        let mut row_items = Vec::with_capacity(summaries.len());
        for summary in summaries {
            let file_type = extract_file_type(&summary.filename);
            let record = TaskModelRecord {
                id: summary.id,
                filename: summary.filename,
                file_type,
                total_bytes: summary.total_bytes,
                downloaded_bytes: summary.downloaded_bytes,
                speed_bps: summary.speed_bps,
                status: summary.status,
                is_selected: false,
            };
            row_items.push(record.to_row_data());
            self.id_order.push(summary.id);
            self.tasks.insert(summary.id, record);
        }

        self.slint_model.set_vec(row_items);
        self.refresh_counts(window);
    }

    pub fn apply_telemetry(&mut self, snapshot: &TelemetrySnapshot, window: &MainWindow) {
        for p in &snapshot.tasks {
            if let Some(record) = self.tasks.get_mut(&p.task_id) {
                record.downloaded_bytes = p.downloaded_bytes;
                record.speed_bps = p.speed_bps;
                record.status = p.status;

                if let Some(pos) = self.id_order.iter().position(|id| *id == p.task_id) {
                    self.slint_model.set_row_data(pos, record.to_row_data());
                }
            } else {
                self.add_or_update_from_progress(p);
            }
        }

        window.set_aggregate_download_speed(SharedString::from(format_speed(
            snapshot.global_download_speed_bps,
        )));
        window.set_aggregate_upload_speed(SharedString::from("0.0 B/s"));

        self.refresh_counts(window);
    }

    fn add_or_update_from_progress(&mut self, p: &TaskProgress) {
        let filename = format!("Task-{}", p.task_id);
        let file_type = "BIN".to_string();
        let record = TaskModelRecord {
            id: p.task_id,
            filename,
            file_type,
            total_bytes: p.total_bytes,
            downloaded_bytes: p.downloaded_bytes,
            speed_bps: p.speed_bps,
            status: p.status,
            is_selected: false,
        };
        self.slint_model.push(record.to_row_data());
        self.id_order.push(p.task_id);
        self.tasks.insert(p.task_id, record);
    }

    pub fn toggle_selection(&mut self, id: i64, window: &MainWindow) {
        if let Some(record) = self.tasks.get_mut(&id) {
            record.is_selected = !record.is_selected;
            if let Some(pos) = self.id_order.iter().position(|item_id| *item_id == id) {
                self.slint_model.set_row_data(pos, record.to_row_data());
            }
        }
        self.refresh_counts(window);
    }

    pub fn deselect_all(&mut self, window: &MainWindow) {
        for (i, id) in self.id_order.iter().enumerate() {
            if let Some(record) = self.tasks.get_mut(id) {
                if record.is_selected {
                    record.is_selected = false;
                    self.slint_model.set_row_data(i, record.to_row_data());
                }
            }
        }
        self.refresh_counts(window);
    }

    pub fn remove_task(&mut self, id: i64, window: &MainWindow) {
        if let Some(pos) = self.id_order.iter().position(|item_id| *item_id == id) {
            self.id_order.remove(pos);
            self.tasks.remove(&id);
            self.slint_model.remove(pos);
            self.refresh_counts(window);
        }
    }

    pub fn refresh_counts(&self, window: &MainWindow) {
        let mut all = 0;
        let mut downloading = 0;
        let mut paused = 0;
        let mut completed = 0;
        let mut failed = 0;
        let mut selected = 0;

        for record in self.tasks.values() {
            all += 1;
            if record.is_selected {
                selected += 1;
            }
            match record.status {
                TaskState::Transferring
                | TaskState::Resolving
                | TaskState::Allocating
                | TaskState::PostProcessing => downloading += 1,
                TaskState::Paused => paused += 1,
                TaskState::Completed => completed += 1,
                TaskState::FatalError => failed += 1,
                TaskState::Queued | TaskState::Retrying => {}
            }
        }

        window.set_count_all(all);
        window.set_count_downloading(downloading);
        window.set_count_paused(paused);
        window.set_count_completed(completed);
        window.set_count_failed(failed);
        window.set_selected_count(selected);
    }

    pub fn get_active_ids(&self) -> Vec<i64> {
        self.tasks
            .values()
            .filter(|t| t.status == TaskState::Transferring)
            .map(|t| t.id)
            .collect()
    }

    pub fn get_paused_or_queued_ids(&self) -> Vec<i64> {
        self.tasks
            .values()
            .filter(|t| t.status == TaskState::Paused || t.status == TaskState::Queued)
            .map(|t| t.id)
            .collect()
    }

    pub fn get_selected_ids(&self) -> Vec<i64> {
        self.tasks
            .values()
            .filter(|t| t.is_selected)
            .map(|t| t.id)
            .collect()
    }
}
