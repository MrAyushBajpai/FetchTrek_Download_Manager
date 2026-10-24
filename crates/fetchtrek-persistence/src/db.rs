use std::path::Path;

use rusqlite::Connection;

use crate::error::PersistenceError;

pub const SCHEMA_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL,
    destination_path TEXT NOT NULL,
    filename TEXT NOT NULL,
    protocol TEXT NOT NULL,
    total_bytes INTEGER NOT NULL DEFAULT 0,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    status INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 1,
    etag TEXT,
    last_modified TEXT,
    mime_type TEXT,
    expected_checksum TEXT,
    vault_cred_id TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_priority ON tasks (status, priority DESC, created_at ASC);

CREATE TABLE IF NOT EXISTS task_segments (
    task_id INTEGER NOT NULL,
    segment_index INTEGER NOT NULL,
    range_start INTEGER NOT NULL,
    range_end INTEGER NOT NULL,
    current_cursor INTEGER NOT NULL,
    status INTEGER NOT NULL DEFAULT 0,
    worker_id INTEGER,
    verified_intervals BLOB,
    PRIMARY KEY (task_id, segment_index),
    FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS task_headers (
    task_id INTEGER NOT NULL,
    header_name TEXT NOT NULL,
    header_value TEXT NOT NULL,
    PRIMARY KEY (task_id, header_name),
    FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE CASCADE
);
"#;

#[derive(Debug)]
pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, PersistenceError> {
        let path_ref = path.as_ref();
        if let Some(parent) = path_ref.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        let conn = Connection::open(path_ref)?;
        Self::initialize_connection(&conn, false)?;
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> Result<Self, PersistenceError> {
        let conn = Connection::open_in_memory()?;
        Self::initialize_connection(&conn, true)?;
        Ok(Self { conn })
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }

    pub fn quick_check(&self) -> Result<(), PersistenceError> {
        let result: String = self
            .conn
            .query_row("PRAGMA quick_check;", [], |row| row.get(0))?;
        if result.eq_ignore_ascii_case("ok") {
            Ok(())
        } else {
            Err(PersistenceError::Corruption(result))
        }
    }

    fn initialize_connection(conn: &Connection, is_memory: bool) -> Result<(), PersistenceError> {
        if !is_memory {
            let journal_mode: String =
                conn.query_row("PRAGMA journal_mode = WAL;", [], |row| row.get(0))?;
            if !journal_mode.eq_ignore_ascii_case("wal") {
                return Err(PersistenceError::PragmaFailed {
                    pragma: "journal_mode",
                    expected: "wal".to_string(),
                    actual: journal_mode,
                });
            }
        } else {
            let _ = conn.execute_batch("PRAGMA journal_mode = WAL;");
        }

        conn.execute_batch(
            "PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;",
        )?;

        let foreign_keys: i32 = conn.query_row("PRAGMA foreign_keys;", [], |row| row.get(0))?;
        if foreign_keys != 1 {
            return Err(PersistenceError::PragmaFailed {
                pragma: "foreign_keys",
                expected: "1".to_string(),
                actual: foreign_keys.to_string(),
            });
        }

        // Apply embedded DDL schema migrations idempotently
        conn.execute_batch(SCHEMA_DDL)
            .map_err(|e| PersistenceError::Migration(e.to_string()))?;

        Ok(())
    }
}
