use std::time::{SystemTime, UNIX_EPOCH};

use fetchtrek_types::TaskState;
use rusqlite::Row;

use crate::db::Database;
use crate::error::PersistenceError;
use crate::intervals::IntervalSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRecord {
    pub id: i64,
    pub uuid: String,
    pub url: String,
    pub destination_path: String,
    pub filename: String,
    pub protocol: String,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub status: TaskState,
    pub priority: u32,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub mime_type: Option<String>,
    pub expected_checksum: Option<String>,
    pub vault_cred_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTask {
    pub uuid: String,
    pub url: String,
    pub destination_path: String,
    pub filename: String,
    pub protocol: String,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub status: TaskState,
    pub priority: u32,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub mime_type: Option<String>,
    pub expected_checksum: Option<String>,
    pub vault_cred_id: Option<String>,
    pub completed_at: Option<i64>,
}

impl NewTask {
    pub fn new(
        uuid: impl Into<String>,
        url: impl Into<String>,
        destination_path: impl Into<String>,
        filename: impl Into<String>,
        protocol: impl Into<String>,
        total_bytes: u64,
    ) -> Self {
        Self {
            uuid: uuid.into(),
            url: url.into(),
            destination_path: destination_path.into(),
            filename: filename.into(),
            protocol: protocol.into(),
            total_bytes,
            downloaded_bytes: 0,
            status: TaskState::Queued,
            priority: 1,
            etag: None,
            last_modified: None,
            mime_type: None,
            expected_checksum: None,
            vault_cred_id: None,
            completed_at: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSegmentRecord {
    pub task_id: i64,
    pub segment_index: u32,
    pub range_start: u64,
    pub range_end: u64,
    pub current_cursor: u64,
    pub status: u32,
    pub worker_id: Option<u32>,
    pub verified_intervals: IntervalSet,
}

impl TaskSegmentRecord {
    pub fn new(
        task_id: i64,
        segment_index: u32,
        range_start: u64,
        range_end: u64,
        current_cursor: u64,
        status: u32,
    ) -> Self {
        Self {
            task_id,
            segment_index,
            range_start,
            range_end,
            current_cursor,
            status,
            worker_id: None,
            verified_intervals: IntervalSet::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentCheckpoint {
    pub segment_index: u32,
    pub current_cursor: u64,
    pub status: u32,
    pub verified_intervals: IntervalSet,
}

impl SegmentCheckpoint {
    pub fn new(
        segment_index: u32,
        current_cursor: u64,
        status: u32,
        verified_intervals: IntervalSet,
    ) -> Self {
        Self {
            segment_index,
            current_cursor,
            status,
            verified_intervals,
        }
    }
}

pub(crate) fn map_task_row(row: &Row<'_>) -> rusqlite::Result<TaskRecord> {
    let id: i64 = row.get(0)?;
    let uuid: String = row.get(1)?;
    let url: String = row.get(2)?;
    let destination_path: String = row.get(3)?;
    let filename: String = row.get(4)?;
    let protocol: String = row.get(5)?;
    let total_bytes: i64 = row.get(6)?;
    let downloaded_bytes: i64 = row.get(7)?;
    let status_raw: i32 = row.get(8)?;
    let priority: i32 = row.get(9)?;
    let etag: Option<String> = row.get(10)?;
    let last_modified: Option<String> = row.get(11)?;
    let mime_type: Option<String> = row.get(12)?;
    let expected_checksum: Option<String> = row.get(13)?;
    let vault_cred_id: Option<String> = row.get(14)?;
    let created_at: i64 = row.get(15)?;
    let updated_at: i64 = row.get(16)?;
    let completed_at: Option<i64> = row.get(17)?;

    let status = TaskState::try_from(status_raw as u8).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            8,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid TaskState variant integer: {}", status_raw),
            )),
        )
    })?;

    Ok(TaskRecord {
        id,
        uuid,
        url,
        destination_path,
        filename,
        protocol,
        total_bytes: total_bytes as u64,
        downloaded_bytes: downloaded_bytes as u64,
        status,
        priority: priority as u32,
        etag,
        last_modified,
        mime_type,
        expected_checksum,
        vault_cred_id,
        created_at,
        updated_at,
        completed_at,
    })
}

impl Database {
    pub fn repository(&self) -> TaskRepository<'_> {
        TaskRepository::new(self)
    }

    pub fn create_task(&self, task: &NewTask) -> Result<i64, PersistenceError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        self.connection().execute(
            "INSERT INTO tasks (
                uuid, url, destination_path, filename, protocol, total_bytes, downloaded_bytes,
                status, priority, etag, last_modified, mime_type, expected_checksum, vault_cred_id,
                created_at, updated_at, completed_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            rusqlite::params![
                task.uuid,
                task.url,
                task.destination_path,
                task.filename,
                task.protocol,
                task.total_bytes as i64,
                task.downloaded_bytes as i64,
                task.status.as_u8() as i32,
                task.priority as i32,
                task.etag,
                task.last_modified,
                task.mime_type,
                task.expected_checksum,
                task.vault_cred_id,
                now,
                now,
                task.completed_at,
            ],
        )?;

        Ok(self.connection().last_insert_rowid())
    }

    pub fn get_task_by_id(&self, id: i64) -> Result<Option<TaskRecord>, PersistenceError> {
        let mut stmt = self.connection().prepare(
            "SELECT id, uuid, url, destination_path, filename, protocol, total_bytes, downloaded_bytes,
                    status, priority, etag, last_modified, mime_type, expected_checksum, vault_cred_id,
                    created_at, updated_at, completed_at
             FROM tasks WHERE id = ?1",
        )?;

        let mut rows = stmt.query([id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(map_task_row(row)?))
        } else {
            Ok(None)
        }
    }

    pub fn get_task_by_uuid(&self, uuid: &str) -> Result<Option<TaskRecord>, PersistenceError> {
        let mut stmt = self.connection().prepare(
            "SELECT id, uuid, url, destination_path, filename, protocol, total_bytes, downloaded_bytes,
                    status, priority, etag, last_modified, mime_type, expected_checksum, vault_cred_id,
                    created_at, updated_at, completed_at
             FROM tasks WHERE uuid = ?1",
        )?;

        let mut rows = stmt.query([uuid])?;
        if let Some(row) = rows.next()? {
            Ok(Some(map_task_row(row)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_tasks(
        &self,
        filter_status: Option<TaskState>,
    ) -> Result<Vec<TaskRecord>, PersistenceError> {
        let mut records = Vec::new();

        if let Some(status) = filter_status {
            let mut stmt = self.connection().prepare(
                "SELECT id, uuid, url, destination_path, filename, protocol, total_bytes, downloaded_bytes,
                        status, priority, etag, last_modified, mime_type, expected_checksum, vault_cred_id,
                        created_at, updated_at, completed_at
                 FROM tasks
                 WHERE status = ?1
                 ORDER BY priority DESC, created_at ASC",
            )?;

            let rows = stmt.query_map([status.as_u8() as i32], map_task_row)?;
            for r in rows {
                records.push(r?);
            }
        } else {
            let mut stmt = self.connection().prepare(
                "SELECT id, uuid, url, destination_path, filename, protocol, total_bytes, downloaded_bytes,
                        status, priority, etag, last_modified, mime_type, expected_checksum, vault_cred_id,
                        created_at, updated_at, completed_at
                 FROM tasks
                 ORDER BY priority DESC, created_at ASC",
            )?;

            let rows = stmt.query_map([], map_task_row)?;
            for r in rows {
                records.push(r?);
            }
        }

        Ok(records)
    }

    pub fn update_task_status(&self, id: i64, status: TaskState) -> Result<(), PersistenceError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        if status == TaskState::Completed {
            self.connection().execute(
                "UPDATE tasks SET status = ?1, updated_at = ?2, completed_at = ?2 WHERE id = ?3",
                rusqlite::params![status.as_u8() as i32, now, id],
            )?;
        } else {
            self.connection().execute(
                "UPDATE tasks SET status = ?1, updated_at = ?2 WHERE id = ?3",
                rusqlite::params![status.as_u8() as i32, now, id],
            )?;
        }

        Ok(())
    }

    pub fn delete_task(&self, id: i64) -> Result<(), PersistenceError> {
        self.connection()
            .execute("DELETE FROM tasks WHERE id = ?1", [id])?;
        Ok(())
    }

    pub fn save_headers(
        &self,
        task_id: i64,
        headers: &[(&str, &str)],
    ) -> Result<(), PersistenceError> {
        self.connection()
            .execute_batch("SAVEPOINT save_headers_sp;")?;

        let res = (|| -> Result<(), PersistenceError> {
            let mut stmt = self.connection().prepare(
                "INSERT OR REPLACE INTO task_headers (task_id, header_name, header_value) VALUES (?1, ?2, ?3)",
            )?;
            for (name, value) in headers {
                stmt.execute(rusqlite::params![task_id, *name, *value])?;
            }
            Ok(())
        })();

        match res {
            Ok(()) => {
                self.connection()
                    .execute_batch("RELEASE SAVEPOINT save_headers_sp;")?;
                Ok(())
            }
            Err(e) => {
                let _ = self.connection().execute_batch(
                    "ROLLBACK TO SAVEPOINT save_headers_sp; RELEASE SAVEPOINT save_headers_sp;",
                );
                Err(e)
            }
        }
    }

    pub fn get_headers(&self, task_id: i64) -> Result<Vec<(String, String)>, PersistenceError> {
        let mut stmt = self.connection().prepare(
            "SELECT header_name, header_value FROM task_headers WHERE task_id = ?1 ORDER BY header_name ASC",
        )?;

        let rows = stmt.query_map([task_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut headers = Vec::new();
        for r in rows {
            headers.push(r?);
        }
        Ok(headers)
    }

    pub fn save_segments(
        &self,
        task_id: i64,
        segments: &[TaskSegmentRecord],
    ) -> Result<(), PersistenceError> {
        self.connection()
            .execute_batch("SAVEPOINT save_segments_sp;")?;

        let res = (|| -> Result<(), PersistenceError> {
            let mut stmt = self.connection().prepare(
                "INSERT OR REPLACE INTO task_segments (
                    task_id, segment_index, range_start, range_end, current_cursor, status, worker_id, verified_intervals
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )?;

            for seg in segments {
                stmt.execute(rusqlite::params![
                    task_id,
                    seg.segment_index as i32,
                    seg.range_start as i64,
                    seg.range_end as i64,
                    seg.current_cursor as i64,
                    seg.status as i32,
                    seg.worker_id.map(|w| w as i32),
                    &seg.verified_intervals,
                ])?;
            }
            Ok(())
        })();

        match res {
            Ok(()) => {
                self.connection()
                    .execute_batch("RELEASE SAVEPOINT save_segments_sp;")?;
                Ok(())
            }
            Err(e) => {
                let _ = self.connection().execute_batch(
                    "ROLLBACK TO SAVEPOINT save_segments_sp; RELEASE SAVEPOINT save_segments_sp;",
                );
                Err(e)
            }
        }
    }

    pub fn get_segments(&self, task_id: i64) -> Result<Vec<TaskSegmentRecord>, PersistenceError> {
        let mut stmt = self.connection().prepare(
            "SELECT task_id, segment_index, range_start, range_end, current_cursor, status, worker_id, verified_intervals
             FROM task_segments WHERE task_id = ?1 ORDER BY segment_index ASC",
        )?;

        let rows = stmt.query_map([task_id], |row| {
            let task_id: i64 = row.get(0)?;
            let segment_index: i32 = row.get(1)?;
            let range_start: i64 = row.get(2)?;
            let range_end: i64 = row.get(3)?;
            let current_cursor: i64 = row.get(4)?;
            let status: i32 = row.get(5)?;
            let worker_id: Option<i32> = row.get(6)?;
            let verified_intervals: Option<IntervalSet> = row.get(7)?;

            Ok(TaskSegmentRecord {
                task_id,
                segment_index: segment_index as u32,
                range_start: range_start as u64,
                range_end: range_end as u64,
                current_cursor: current_cursor as u64,
                status: status as u32,
                worker_id: worker_id.map(|w| w as u32),
                verified_intervals: verified_intervals.unwrap_or_default(),
            })
        })?;

        let mut segments = Vec::new();
        for r in rows {
            segments.push(r?);
        }
        Ok(segments)
    }

    pub fn checkpoint_progress(
        &self,
        task_id: i64,
        downloaded_bytes: u64,
        segments: &[SegmentCheckpoint],
    ) -> Result<(), PersistenceError> {
        self.connection()
            .execute_batch("SAVEPOINT checkpoint_sp;")?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let res = (|| -> Result<(), PersistenceError> {
            let rows_affected = self.connection().execute(
                "UPDATE tasks SET downloaded_bytes = ?1, updated_at = ?2 WHERE id = ?3",
                rusqlite::params![downloaded_bytes as i64, now, task_id],
            )?;

            if rows_affected == 0 {
                return Err(PersistenceError::Corruption(format!(
                    "Task with id {} not found for checkpoint",
                    task_id
                )));
            }

            let mut stmt = self.connection().prepare(
                "UPDATE task_segments 
                 SET current_cursor = ?1, status = ?2, verified_intervals = ?3 
                 WHERE task_id = ?4 AND segment_index = ?5",
            )?;

            for seg in segments {
                let seg_rows = stmt.execute(rusqlite::params![
                    seg.current_cursor as i64,
                    seg.status as i32,
                    &seg.verified_intervals,
                    task_id,
                    seg.segment_index as i32,
                ])?;

                if seg_rows == 0 {
                    return Err(PersistenceError::Corruption(format!(
                        "Segment index {} for task id {} not found during checkpoint",
                        seg.segment_index, task_id
                    )));
                }
            }

            Ok(())
        })();

        match res {
            Ok(()) => {
                self.connection()
                    .execute_batch("RELEASE SAVEPOINT checkpoint_sp;")?;
                Ok(())
            }
            Err(e) => {
                let _ = self.connection().execute_batch(
                    "ROLLBACK TO SAVEPOINT checkpoint_sp; RELEASE SAVEPOINT checkpoint_sp;",
                );
                Err(e)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TaskRepository<'a> {
    db: &'a Database,
}

impl<'a> TaskRepository<'a> {
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }

    pub fn create_task(&self, task: &NewTask) -> Result<i64, PersistenceError> {
        self.db.create_task(task)
    }

    pub fn get_task_by_id(&self, id: i64) -> Result<Option<TaskRecord>, PersistenceError> {
        self.db.get_task_by_id(id)
    }

    pub fn get_task_by_uuid(&self, uuid: &str) -> Result<Option<TaskRecord>, PersistenceError> {
        self.db.get_task_by_uuid(uuid)
    }

    pub fn list_tasks(
        &self,
        filter_status: Option<TaskState>,
    ) -> Result<Vec<TaskRecord>, PersistenceError> {
        self.db.list_tasks(filter_status)
    }

    pub fn update_task_status(&self, id: i64, status: TaskState) -> Result<(), PersistenceError> {
        self.db.update_task_status(id, status)
    }

    pub fn delete_task(&self, id: i64) -> Result<(), PersistenceError> {
        self.db.delete_task(id)
    }

    pub fn save_headers(
        &self,
        task_id: i64,
        headers: &[(&str, &str)],
    ) -> Result<(), PersistenceError> {
        self.db.save_headers(task_id, headers)
    }

    pub fn get_headers(&self, task_id: i64) -> Result<Vec<(String, String)>, PersistenceError> {
        self.db.get_headers(task_id)
    }

    pub fn save_segments(
        &self,
        task_id: i64,
        segments: &[TaskSegmentRecord],
    ) -> Result<(), PersistenceError> {
        self.db.save_segments(task_id, segments)
    }

    pub fn get_segments(&self, task_id: i64) -> Result<Vec<TaskSegmentRecord>, PersistenceError> {
        self.db.get_segments(task_id)
    }

    pub fn checkpoint_progress(
        &self,
        task_id: i64,
        downloaded_bytes: u64,
        segments: &[SegmentCheckpoint],
    ) -> Result<(), PersistenceError> {
        self.db
            .checkpoint_progress(task_id, downloaded_bytes, segments)
    }

    pub fn recover_interrupted_tasks(
        &self,
    ) -> Result<Vec<crate::recovery::RecoveredTask>, PersistenceError> {
        self.db.recover_interrupted_tasks()
    }
}
