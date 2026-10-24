use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_persistence::Database;

struct TempDbGuard {
    path: PathBuf,
}

impl TempDbGuard {
    fn new(name: &str) -> Self {
        let mut path = std::env::temp_dir();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique = format!(
            "fetchtrek_db_{}_{}_{}.sqlite3",
            std::process::id(),
            timestamp,
            name
        );
        path.push(unique);
        Self { path }
    }
}

impl Drop for TempDbGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
    }
}

#[test]
fn test_in_memory_initialization_and_pragmas() {
    let db = Database::open_in_memory().expect("in-memory db open failed");

    // Verify all 3 core tables exist
    let mut stmt = db
        .connection()
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('tasks', 'task_segments', 'task_headers')",
        )
        .expect("prepare table query");

    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .expect("query map")
        .map(|r| r.expect("row"))
        .collect();

    assert_eq!(tables.len(), 3, "Expected 3 tables, found: {:?}", tables);
    assert!(tables.contains(&"tasks".to_string()));
    assert!(tables.contains(&"task_segments".to_string()));
    assert!(tables.contains(&"task_headers".to_string()));

    // Verify foreign keys enabled
    let fk: i32 = db
        .connection()
        .query_row("PRAGMA foreign_keys;", [], |r| r.get(0))
        .expect("query foreign_keys");
    assert_eq!(fk, 1, "Foreign keys must be enabled");

    // Verify PRAGMA quick_check completes in < 15ms
    let start = Instant::now();
    db.quick_check().expect("quick_check failed");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(15),
        "quick_check must complete in < 15ms, took: {:?}",
        elapsed
    );
}

#[test]
fn test_disk_wal_mode_and_foreign_key_cascades() {
    let temp = TempDbGuard::new("wal_cascade");
    let db = Database::open(&temp.path).expect("disk db open failed");

    // Verify WAL journal mode
    let mode: String = db
        .connection()
        .query_row("PRAGMA journal_mode;", [], |r| r.get(0))
        .expect("query journal_mode");
    assert_eq!(
        mode.to_lowercase(),
        "wal",
        "Disk database must use WAL journal mode"
    );

    // Insert parent task
    db.connection()
        .execute(
            "INSERT INTO tasks (uuid, url, destination_path, filename, protocol, total_bytes, downloaded_bytes, status, priority, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            (
                "550e8400-e29b-41d4-a716-446655440000",
                "https://example.com/file.iso",
                "/downloads/file.iso",
                "file.iso",
                "https",
                1048576i64,
                0i64,
                0i32,
                1i32,
                1700000000i64,
                1700000000i64,
            ),
        )
        .expect("insert task");
    let task_id: i64 = db.connection().last_insert_rowid();

    // Insert child records in task_segments and task_headers
    db.connection()
        .execute(
            "INSERT INTO task_segments (task_id, segment_index, range_start, range_end, current_cursor, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (task_id, 0i32, 0i64, 524287i64, 0i64, 0i32),
        )
        .expect("insert segment 0");

    db.connection()
        .execute(
            "INSERT INTO task_segments (task_id, segment_index, range_start, range_end, current_cursor, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (task_id, 1i32, 524288i64, 1048575i64, 524288i64, 0i32),
        )
        .expect("insert segment 1");

    db.connection()
        .execute(
            "INSERT INTO task_headers (task_id, header_name, header_value)
             VALUES (?1, ?2, ?3)",
            (task_id, "User-Agent", "FetchTrek/1.0"),
        )
        .expect("insert header");

    let seg_count: i64 = db
        .connection()
        .query_row(
            "SELECT COUNT(*) FROM task_segments WHERE task_id = ?1",
            [task_id],
            |r| r.get(0),
        )
        .expect("count segments before delete");
    assert_eq!(seg_count, 2);

    let hdr_count: i64 = db
        .connection()
        .query_row(
            "SELECT COUNT(*) FROM task_headers WHERE task_id = ?1",
            [task_id],
            |r| r.get(0),
        )
        .expect("count headers before delete");
    assert_eq!(hdr_count, 1);

    // Delete parent task -> verify cascading deletion of child rows
    db.connection()
        .execute("DELETE FROM tasks WHERE id = ?1", [task_id])
        .expect("delete parent task");

    let seg_count_after: i64 = db
        .connection()
        .query_row(
            "SELECT COUNT(*) FROM task_segments WHERE task_id = ?1",
            [task_id],
            |r| r.get(0),
        )
        .expect("count segments after delete");
    assert_eq!(
        seg_count_after, 0,
        "task_segments child rows must be atomically deleted via cascade"
    );

    let hdr_count_after: i64 = db
        .connection()
        .query_row(
            "SELECT COUNT(*) FROM task_headers WHERE task_id = ?1",
            [task_id],
            |r| r.get(0),
        )
        .expect("count headers after delete");
    assert_eq!(
        hdr_count_after, 0,
        "task_headers child rows must be atomically deleted via cascade"
    );
}

#[test]
fn test_idempotent_reopen() {
    let temp = TempDbGuard::new("idempotent");

    // First open: creates schema, inserts record
    {
        let db1 = Database::open(&temp.path).expect("first open failed");
        db1.connection()
            .execute(
                "INSERT INTO tasks (uuid, url, destination_path, filename, protocol, created_at, updated_at)
                 VALUES ('uuid-idempotent-1', 'https://example.com/file', '/dest', 'file', 'https', 1, 1)",
                [],
            )
            .expect("insert into db1");
    }

    // Second open: runs migration on existing database, must succeed idempotently
    {
        let db2 = Database::open(&temp.path).expect("second open must succeed idempotently");
        let count: i64 = db2
            .connection()
            .query_row("SELECT COUNT(*) FROM tasks", [], |r| r.get(0))
            .expect("count tasks");
        assert_eq!(count, 1, "Existing records must be preserved across reopen");
        db2.quick_check()
            .expect("quick check after reopen must succeed");
    }
}
