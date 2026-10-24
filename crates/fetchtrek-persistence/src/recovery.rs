use std::time::{SystemTime, UNIX_EPOCH};

use fetchtrek_types::TaskState;

use crate::db::Database;
use crate::error::PersistenceError;
use crate::repository::{TaskRecord, TaskSegmentRecord};

pub const CLUSTER_SIZE: u64 = 65_536;
pub const CLUSTER_ALIGNMENT_MASK: u64 = !(CLUSTER_SIZE - 1);

#[inline]
pub fn rewind_cursor_to_cluster(current_cursor: u64, range_start: u64) -> u64 {
    let aligned = current_cursor & CLUSTER_ALIGNMENT_MASK;
    std::cmp::max(range_start, aligned)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredTask {
    pub task: TaskRecord,
    pub segments: Vec<TaskSegmentRecord>,
    pub discarded_tail_bytes: u64,
}

impl Database {
    pub fn recover_interrupted_tasks(&self) -> Result<Vec<RecoveredTask>, PersistenceError> {
        // Step 1: Ensure WAL replay completed without corruption
        self.quick_check()?;

        // Step 2: Query all active / interrupted tasks at process boot
        let mut select_stmt = self.connection().prepare(
            "SELECT id, uuid, url, destination_path, filename, protocol, total_bytes, downloaded_bytes,
                    status, priority, etag, last_modified, mime_type, expected_checksum, vault_cred_id,
                    created_at, updated_at, completed_at
             FROM tasks
             WHERE status IN (1, 2, 3, 5, 6)
             ORDER BY priority DESC, created_at ASC",
        )?;

        let tasks: Vec<TaskRecord> = select_stmt
            .query_map([], crate::repository::map_task_row)?
            .collect::<Result<Vec<_>, _>>()?;

        drop(select_stmt);

        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        // Prepare statements outside loop for high-throughput recovery
        let mut update_seg_stmt = self.connection().prepare(
            "UPDATE task_segments 
             SET current_cursor = ?1, status = 0, worker_id = NULL 
             WHERE task_id = ?2 AND segment_index = ?3",
        )?;

        let mut update_task_stmt = self.connection().prepare(
            "UPDATE tasks 
             SET status = ?1, downloaded_bytes = ?2, updated_at = ?3 
             WHERE id = ?4",
        )?;

        let mut recovered = Vec::with_capacity(tasks.len());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        for mut task in tasks {
            self.connection()
                .execute_batch("SAVEPOINT recover_task_sp;")?;

            let res = (|| -> Result<(Vec<TaskSegmentRecord>, u64), PersistenceError> {
                let mut segments = self.get_segments(task.id)?;
                let mut total_discarded = 0u64;

                for seg in segments.iter_mut() {
                    if seg.status != 3 {
                        let new_cursor =
                            rewind_cursor_to_cluster(seg.current_cursor, seg.range_start);
                        let discarded = seg.current_cursor.saturating_sub(new_cursor);
                        total_discarded += discarded;

                        seg.current_cursor = new_cursor;
                        seg.status = 0;
                        seg.worker_id = None;

                        update_seg_stmt.execute(rusqlite::params![
                            new_cursor as i64,
                            task.id,
                            seg.segment_index as i32,
                        ])?;
                    }
                }

                let new_downloaded = task.downloaded_bytes.saturating_sub(total_discarded);
                task.status = TaskState::Paused;
                task.downloaded_bytes = new_downloaded;
                task.updated_at = now;

                update_task_stmt.execute(rusqlite::params![
                    TaskState::Paused.as_u8() as i32,
                    new_downloaded as i64,
                    now,
                    task.id,
                ])?;

                Ok((segments, total_discarded))
            })();

            match res {
                Ok((segments, discarded_tail_bytes)) => {
                    self.connection()
                        .execute_batch("RELEASE SAVEPOINT recover_task_sp;")?;
                    recovered.push(RecoveredTask {
                        task,
                        segments,
                        discarded_tail_bytes,
                    });
                }
                Err(e) => {
                    let _ = self.connection().execute_batch(
                        "ROLLBACK TO SAVEPOINT recover_task_sp; RELEASE SAVEPOINT recover_task_sp;",
                    );
                    return Err(e);
                }
            }
        }

        Ok(recovered)
    }
}
