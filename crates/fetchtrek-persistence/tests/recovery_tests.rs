use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_persistence::{
    rewind_cursor_to_cluster, Database, NewTask, TaskSegmentRecord, CLUSTER_SIZE,
};
use fetchtrek_types::TaskState;

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
            "fetchtrek_recovery_test_{}_{}_{}.sqlite3",
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
fn test_cursor_cluster_rewind_arithmetic() {
    assert_eq!(rewind_cursor_to_cluster(65_536 + 128, 0), 65_536);
    assert_eq!(rewind_cursor_to_cluster(131_072 + 65_535, 0), 131_072);
    assert_eq!(rewind_cursor_to_cluster(65_536, 0), 65_536);
    assert_eq!(rewind_cursor_to_cluster(150, 100), 100);

    // Additional boundary invariant assertions
    assert_eq!(rewind_cursor_to_cluster(0, 0), 0);
    assert_eq!(rewind_cursor_to_cluster(65_535, 0), 0);
    assert_eq!(rewind_cursor_to_cluster(65_536 * 10, 0), 65_536 * 10);
    assert_eq!(
        rewind_cursor_to_cluster(65_536 * 10 + 1, 65_536 * 10),
        65_536 * 10
    );

    // Range start clamping invariant: cursor >= range_start
    let range_start = 500_000;
    let rewound = rewind_cursor_to_cluster(520_000, range_start);
    // 520_000 & !(65535) = 458_752 < 500_000, so clamps to 500_000
    assert_eq!(rewound, range_start);
}

#[test]
fn test_simulated_crash_and_state_recovery() {
    let temp = TempDbGuard::new("simulated_crash");

    let initial_cursors = [100_000u64, 250_000u64, 500_000u64, 750_000u64];
    let initial_downloaded: u64 = initial_cursors.iter().sum();

    let task_id = {
        let db = Database::open(&temp.path).expect("open initial db failed");

        let mut new_task = NewTask::new(
            "uuid-crash-recovery-001",
            "https://cdn.example.com/system_image.iso",
            "/var/tmp/system_image.iso",
            "system_image.iso",
            "https",
            10_000_000,
        );
        new_task.status = TaskState::Active;
        new_task.downloaded_bytes = initial_downloaded;

        let id = db.create_task(&new_task).expect("create_task failed");

        let seg0 = TaskSegmentRecord::new(id, 0, 0, 1_999_999, initial_cursors[0], 1);
        let seg1 = TaskSegmentRecord::new(id, 1, 0, 1_999_999, initial_cursors[1], 1);
        let seg2 = TaskSegmentRecord::new(id, 2, 0, 1_999_999, initial_cursors[2], 1);
        let seg3 = TaskSegmentRecord::new(id, 3, 0, 1_999_999, initial_cursors[3], 1);

        db.save_segments(id, &[seg0, seg1, seg2, seg3])
            .expect("save_segments failed");

        id
        // db is dropped abruptly to simulate crash / process termination
    };

    // Reopen database instance at the same path
    let reopened_db = Database::open(&temp.path).expect("reopen db after crash failed");

    // Perform recovery
    let recovered_tasks = reopened_db
        .recover_interrupted_tasks()
        .expect("recover_interrupted_tasks failed");

    assert_eq!(recovered_tasks.len(), 1);
    let recovered = &recovered_tasks[0];
    assert_eq!(recovered.task.id, task_id);

    // 1. Task status transitioned to Paused
    assert_eq!(recovered.task.status, TaskState::Paused);

    // Verify task status in database is also Paused
    let db_task = reopened_db
        .get_task_by_id(task_id)
        .unwrap()
        .expect("task not found in db");
    assert_eq!(db_task.status, TaskState::Paused);

    // 2. All 4 segments have cursors rewound to nearest 64 KiB boundary (or clamped to range_start)
    assert_eq!(recovered.segments.len(), 4);
    let mut total_discarded = 0u64;

    for (i, seg) in recovered.segments.iter().enumerate() {
        let expected_rewound = rewind_cursor_to_cluster(initial_cursors[i], seg.range_start);
        assert_eq!(seg.current_cursor, expected_rewound);
        assert_eq!(seg.status, 0); // Reset to Pending/Paused
        assert!(seg.worker_id.is_none());

        // Alignment invariant
        assert!(
            seg.current_cursor % CLUSTER_SIZE == 0 || seg.current_cursor == seg.range_start,
            "Segment cursor must be 64 KiB aligned or equal to range_start"
        );

        total_discarded += initial_cursors[i] - expected_rewound;
    }

    // 3. discarded_tail_bytes accurately matches sum of rewound bytes
    assert_eq!(recovered.discarded_tail_bytes, total_discarded);

    // 4. Task downloaded_bytes decremented by exactly discarded_tail_bytes
    let expected_new_downloaded = initial_downloaded - total_discarded;
    assert_eq!(recovered.task.downloaded_bytes, expected_new_downloaded);
    assert_eq!(db_task.downloaded_bytes, expected_new_downloaded);

    // Subsequent recovery on already-paused tasks returns empty list
    let second_recovery = reopened_db.recover_interrupted_tasks().unwrap();
    assert!(second_recovery.is_empty());
}

#[test]
fn test_recovery_latency_and_quick_check_gate() {
    let temp = TempDbGuard::new("recovery_latency_gate");
    let db = Database::open(&temp.path).expect("open db failed");

    // Populate database with 50 active tasks and 200 active segments (4 segments per task)
    let num_tasks = 50;
    for i in 0..num_tasks {
        let mut new_task = NewTask::new(
            format!("uuid-perf-task-{:03}", i),
            format!("https://cdn.example.com/file_{:03}.dat", i),
            format!("/var/tmp/file_{:03}.dat", i),
            format!("file_{:03}.dat", i),
            "https",
            64 * 1024 * 1024,
        );
        new_task.status = TaskState::Active;
        new_task.downloaded_bytes = 4 * 100_000;

        let task_id = db.create_task(&new_task).unwrap();

        let segments = [
            TaskSegmentRecord::new(task_id, 0, 0, 16_777_215, 100_000, 1),
            TaskSegmentRecord::new(task_id, 1, 16_777_216, 33_554_431, 16_777_216 + 100_000, 1),
            TaskSegmentRecord::new(task_id, 2, 33_554_432, 50_331_647, 33_554_432 + 100_000, 1),
            TaskSegmentRecord::new(task_id, 3, 50_331_648, 67_108_863, 50_331_648 + 100_000, 1),
        ];

        db.save_segments(task_id, &segments).unwrap();
    }

    // Time recovery execution
    let start = Instant::now();
    let recovered = db
        .recover_interrupted_tasks()
        .expect("recover_interrupted_tasks failed");
    let elapsed = start.elapsed();

    println!(
        "Crash recovery of 50 active tasks (200 segments) took {:?}",
        elapsed
    );

    assert_eq!(recovered.len(), num_tasks as usize);

    // Acceptance requirement: total recovery finishes in < 15ms locally.
    // CI runners are virtualized and can show higher jitter under load.
    let latency_budget = if std::env::var_os("GITHUB_ACTIONS").is_some() {
        Duration::from_millis(30)
    } else {
        Duration::from_millis(15)
    };
    assert!(
        elapsed < latency_budget,
        "Total crash recovery must finish in < {:?}, took: {:?}",
        latency_budget,
        elapsed,
    );

    // Verify all 50 tasks in database were transitioned to Paused
    for rec in recovered {
        assert_eq!(rec.task.status, TaskState::Paused);
        for seg in rec.segments {
            assert_eq!(seg.status, 0);
            assert!(
                seg.current_cursor % CLUSTER_SIZE == 0 || seg.current_cursor == seg.range_start
            );
        }
    }
}

#[test]
fn test_recovery_disk_file_clamping() {
    let temp = TempDbGuard::new("recovery_disk_clamp");
    let db = Database::open(&temp.path).expect("open db failed");

    let dest_dir = std::env::temp_dir();
    let dest_file = dest_dir.join(format!(
        "fetchtrek_disk_clamp_{}.bin",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let mut new_task = NewTask::new(
        "uuid-disk-clamp-001",
        "https://cdn.example.com/truncated.bin",
        dest_file.to_str().unwrap(),
        "truncated.bin",
        "https",
        1_000_000,
    );
    new_task.status = TaskState::Active;
    new_task.downloaded_bytes = 200_000;

    let task_id = db.create_task(&new_task).expect("create_task failed");

    // Segment cursor recorded in DB as 200_000
    let seg0 = TaskSegmentRecord::new(task_id, 0, 0, 999_999, 200_000, 1);
    db.save_segments(task_id, &[seg0])
        .expect("save_segments failed");

    // Create staging file on disk with truncated size: 150_000 bytes
    let staging_path = dest_dir.join(format!(".fetchtrek_temp_{task_id}.part"));
    std::fs::write(&staging_path, vec![0u8; 150_000]).expect("write staging file");

    let recovered = db
        .recover_interrupted_tasks()
        .expect("recover_interrupted_tasks failed");

    let _ = std::fs::remove_file(&staging_path);

    assert_eq!(recovered.len(), 1);
    let rec = &recovered[0];
    assert_eq!(rec.task.status, TaskState::Paused);

    // Bounded to 150_000, then rewound to 64 KiB boundary: 2 * 65536 = 131_072
    let expected_rewound = rewind_cursor_to_cluster(150_000, 0);
    assert_eq!(expected_rewound, 131_072);
    assert_eq!(rec.segments[0].current_cursor, expected_rewound);

    // Discarded bytes: 200_000 - 131_072 = 68_928
    assert_eq!(rec.discarded_tail_bytes, 200_000 - 131_072);
    assert_eq!(rec.task.downloaded_bytes, 131_072);
}
