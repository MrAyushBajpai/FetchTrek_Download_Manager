use fetchtrek_persistence::{Database, IntervalSet, PersistenceError, FORMAT_VERSION};
use fetchtrek_types::ByteRange;

#[test]
fn test_interval_coalescing_and_merging() {
    let mut set = IntervalSet::new();
    assert!(set.is_empty());
    assert_eq!(set.len(), 0);
    assert_eq!(set.total_verified_bytes(), 0);

    // Insert [0, 99]
    set.add_range(0, 99).unwrap();
    assert_eq!(set.len(), 1);
    assert_eq!(set.ranges(), &[ByteRange::new(0, 99)]);
    assert_eq!(set.total_verified_bytes(), 100);

    // Insert disjoint [200, 299]
    set.add_range(200, 299).unwrap();
    assert_eq!(set.len(), 2);
    assert_eq!(
        set.ranges(),
        &[ByteRange::new(0, 99), ByteRange::new(200, 299)]
    );
    assert_eq!(set.total_verified_bytes(), 200);

    // Insert bridging [100, 199] touching both adjacent edges:
    // 0..=99 touches 100, 199 touches 200..=299
    set.add_range(100, 199).unwrap();
    assert_eq!(set.len(), 1);
    assert_eq!(set.ranges(), &[ByteRange::new(0, 299)]);
    assert_eq!(set.total_verified_bytes(), 300);

    // Overlapping insert within existing range
    set.add_range(50, 150).unwrap();
    assert_eq!(set.len(), 1);
    assert_eq!(set.ranges(), &[ByteRange::new(0, 299)]);
    assert_eq!(set.total_verified_bytes(), 300);

    // Partial overlap expanding to the right
    set.add_range(250, 350).unwrap();
    assert_eq!(set.len(), 1);
    assert_eq!(set.ranges(), &[ByteRange::new(0, 350)]);
    assert_eq!(set.total_verified_bytes(), 351);

    // Disjoint addition
    set.add_range(500, 599).unwrap();
    assert_eq!(set.len(), 2);
    assert_eq!(
        set.ranges(),
        &[ByteRange::new(0, 350), ByteRange::new(500, 599)]
    );
    assert_eq!(set.total_verified_bytes(), 451);

    // Range bridging with multiple overlaps
    // [300, 550] overlaps [0, 350] and [500, 599]
    set.add_range(300, 550).unwrap();
    assert_eq!(set.len(), 1);
    assert_eq!(set.ranges(), &[ByteRange::new(0, 599)]);
    assert_eq!(set.total_verified_bytes(), 600);

    // Contains checks
    assert!(set.contains_byte(0));
    assert!(set.contains_byte(300));
    assert!(set.contains_byte(599));
    assert!(!set.contains_byte(600));

    assert!(set.contains_range(10, 100));
    assert!(set.contains_range(0, 599));
    assert!(!set.contains_range(500, 601));
    assert!(!set.contains_range(600, 700));

    // Invalid bounds rejection
    let err = set.add_range(100, 50).unwrap_err();
    assert!(matches!(err, PersistenceError::Corruption(_)));
}

#[test]
fn test_simulated_multi_socket_chunks() {
    let mut set = IntervalSet::new();
    let chunk_size = 64 * 1024u64; // 64 KiB
    let total_chunks = 256u64;
    let total_bytes = chunk_size * total_chunks; // 16 MiB

    // Pseudo-random deterministic chunk arrival order
    // Using a simple LCG permutation to simulate multi-socket ingestion
    let mut chunk_indices: Vec<u64> = (0..total_chunks).collect();
    let mut seed: u64 = 0xDEADBEEF;
    for i in (1..chunk_indices.len()).rev() {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (seed % (i as u64 + 1)) as usize;
        chunk_indices.swap(i, j);
    }

    assert!(!set.is_complete(total_bytes));

    for &idx in &chunk_indices {
        let start = idx * chunk_size;
        let end = start + chunk_size - 1;
        set.add_range(start, end).unwrap();
    }

    assert_eq!(set.total_verified_bytes(), total_bytes);
    assert_eq!(set.len(), 1);
    assert_eq!(set.ranges(), &[ByteRange::new(0, total_bytes - 1)]);
    assert!(set.is_complete(total_bytes));
    assert!(!set.is_complete(total_bytes + 1));
}

#[test]
fn test_binary_round_trip_and_size_budget() {
    let mut set = IntervalSet::new();

    // 8 disjoint ranges
    for i in 0..8 {
        let start = i * 200;
        let end = start + 99;
        set.add_range(start, end).unwrap();
    }

    assert_eq!(set.len(), 8);
    let bytes = set.to_bytes();
    // Size: 1 (version) + 4 (count) + 8 * 16 (ranges) = 133 bytes
    assert_eq!(bytes.len(), 5 + 8 * 16);

    let decoded = IntervalSet::from_bytes(&bytes).unwrap();
    assert_eq!(set, decoded);
    assert_eq!(decoded.len(), 8);
    assert_eq!(decoded.total_verified_bytes(), 800);

    // Empty set serialization
    let empty_set = IntervalSet::new();
    assert_eq!(empty_set.to_bytes(), vec![FORMAT_VERSION, 0, 0, 0, 0]);
    let empty_decoded = IntervalSet::from_bytes(&empty_set.to_bytes()).unwrap();
    assert_eq!(empty_set, empty_decoded);
    assert!(empty_decoded.is_empty());

    // Single fully merged download budget test (must be <= 32 bytes)
    let mut full_download_set = IntervalSet::new();
    full_download_set
        .add_range(0, 10 * 1024 * 1024 * 1024 - 1)
        .unwrap(); // 10 GiB
    let single_range_bytes = full_download_set.to_bytes();
    // 1 + 4 + 16 = 21 bytes
    assert_eq!(single_range_bytes.len(), 21);
    assert!(
        single_range_bytes.len() <= 32,
        "Single completed range must be <= 32 bytes, got {}",
        single_range_bytes.len()
    );

    let decoded_full = IntervalSet::from_bytes(&single_range_bytes).unwrap();
    assert_eq!(full_download_set, decoded_full);
    assert!(decoded_full.is_complete(10 * 1024 * 1024 * 1024));
}

#[test]
fn test_corruption_and_malformed_binary() {
    // Empty buffer maps to empty IntervalSet
    assert!(IntervalSet::from_bytes(&[]).unwrap().is_empty());

    // Too short (< 5 bytes)
    let short_bytes = vec![1, 0, 0];
    let err = IntervalSet::from_bytes(&short_bytes).unwrap_err();
    assert!(matches!(err, PersistenceError::Corruption(_)));

    // Unsupported version header
    let invalid_version = vec![99, 0, 0, 0, 0];
    let err = IntervalSet::from_bytes(&invalid_version).unwrap_err();
    assert!(matches!(err, PersistenceError::Corruption(_)));

    // Payload length mismatch (count says 1, but payload ends)
    let count_mismatch = vec![FORMAT_VERSION, 1, 0, 0, 0];
    let err = IntervalSet::from_bytes(&count_mismatch).unwrap_err();
    assert!(matches!(err, PersistenceError::Corruption(_)));

    // Inverted bounds (start = 100, end = 50)
    let mut inverted = vec![FORMAT_VERSION, 1, 0, 0, 0];
    inverted.extend_from_slice(&100u64.to_le_bytes());
    inverted.extend_from_slice(&50u64.to_le_bytes());
    let err = IntervalSet::from_bytes(&inverted).unwrap_err();
    assert!(matches!(err, PersistenceError::Corruption(_)));

    // Unsorted ranges: [200, 300] followed by [50, 100]
    let mut unsorted = vec![FORMAT_VERSION, 2, 0, 0, 0];
    unsorted.extend_from_slice(&200u64.to_le_bytes());
    unsorted.extend_from_slice(&300u64.to_le_bytes());
    unsorted.extend_from_slice(&50u64.to_le_bytes());
    unsorted.extend_from_slice(&100u64.to_le_bytes());
    let err = IntervalSet::from_bytes(&unsorted).unwrap_err();
    assert!(matches!(err, PersistenceError::Corruption(_)));

    // Overlapping / uncoalesced ranges: [0, 100] followed by [100, 200]
    let mut overlapping = vec![FORMAT_VERSION, 2, 0, 0, 0];
    overlapping.extend_from_slice(&0u64.to_le_bytes());
    overlapping.extend_from_slice(&100u64.to_le_bytes());
    overlapping.extend_from_slice(&100u64.to_le_bytes());
    overlapping.extend_from_slice(&200u64.to_le_bytes());
    let err = IntervalSet::from_bytes(&overlapping).unwrap_err();
    assert!(matches!(err, PersistenceError::Corruption(_)));

    // Touching but uncoalesced ranges: [0, 100] followed by [101, 200]
    let mut touching = vec![FORMAT_VERSION, 2, 0, 0, 0];
    touching.extend_from_slice(&0u64.to_le_bytes());
    touching.extend_from_slice(&100u64.to_le_bytes());
    touching.extend_from_slice(&101u64.to_le_bytes());
    touching.extend_from_slice(&200u64.to_le_bytes());
    let err = IntervalSet::from_bytes(&touching).unwrap_err();
    assert!(matches!(err, PersistenceError::Corruption(_)));
}

#[test]
fn test_sqlite_blob_storage_and_retrieval() {
    let db = Database::open_in_memory().unwrap();
    let conn = db.connection();

    let mut set = IntervalSet::new();
    set.add_range(0, 4095).unwrap();
    set.add_range(8192, 16383).unwrap();
    set.add_range(32768, 65535).unwrap();

    conn.execute(
        "CREATE TABLE test_intervals (id INTEGER PRIMARY KEY, intervals BLOB NOT NULL)",
        [],
    )
    .unwrap();

    // Insert using ToSql
    conn.execute(
        "INSERT INTO test_intervals (id, intervals) VALUES (1, ?1)",
        [&set],
    )
    .unwrap();

    // Retrieve using FromSql
    let retrieved: IntervalSet = conn
        .query_row(
            "SELECT intervals FROM test_intervals WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(set, retrieved);
    assert_eq!(retrieved.len(), 3);
    assert_eq!(retrieved.total_verified_bytes(), 4096 + 8192 + 32768);
}
