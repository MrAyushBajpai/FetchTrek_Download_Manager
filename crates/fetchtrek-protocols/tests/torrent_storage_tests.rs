use std::path::PathBuf;
use std::time::Duration;

use fetchtrek_protocols::{
    sha1, BlockRequest, FileSpan, PickerStrategy, PiecePicker, TorrentDiskStorage, TorrentFile,
    TorrentMeta, STANDARD_BLOCK_SIZE,
};

/// Helper to construct a synthetic TorrentMeta for testing.
fn make_test_meta(
    piece_length: u32,
    pieces: Vec<[u8; 20]>,
    files: Vec<TorrentFile>,
    total_length: u64,
) -> TorrentMeta {
    TorrentMeta {
        info_hash: [0x42; 20],
        piece_length,
        pieces,
        name: "test_torrent".into(),
        files,
        total_length,
        announce: None,
        announce_list: Vec::new(),
        is_private: false,
    }
}

/// Test 1: Rarest-First piece picker strategy.
#[test]
fn test_piece_picker_rarest_first() {
    let peer_id = [0x01; 20];
    let piece_len = STANDARD_BLOCK_SIZE; // 1 block per piece for simplicity
    let total_len = (piece_len * 3) as u64;

    let mut picker = PiecePicker::new(3, piece_len, total_len, PickerStrategy::RarestFirst);

    // Piece 0 has 1 peer, Piece 1 has 3 peers, Piece 2 has 2 peers
    picker.set_peer_availability(vec![1, 3, 2]);

    // Peer possesses all 3 pieces
    let req1 = picker.pick_block(&peer_id, |_| true).expect("first block");
    assert_eq!(req1.piece_index, 0, "Rarest piece 0 must be picked first");

    let req2 = picker.pick_block(&peer_id, |_| true).expect("second block");
    assert_eq!(
        req2.piece_index, 2,
        "Second rarest piece 2 must be picked second"
    );

    let req3 = picker.pick_block(&peer_id, |_| true).expect("third block");
    assert_eq!(
        req3.piece_index, 1,
        "Most common piece 1 must be picked last"
    );
}

/// Test 2: Sequential piece picker strategy.
#[test]
fn test_piece_picker_sequential_mode() {
    let peer_id = [0x01; 20];
    let piece_len = STANDARD_BLOCK_SIZE;
    let total_len = (piece_len * 3) as u64;

    let mut picker = PiecePicker::new(3, piece_len, total_len, PickerStrategy::Sequential);

    // Even if availability differs, sequential strategy scans ascending index
    picker.set_peer_availability(vec![5, 1, 3]);

    let req0 = picker.pick_block(&peer_id, |_| true).expect("block 0");
    assert_eq!(req0.piece_index, 0);

    let req1 = picker.pick_block(&peer_id, |_| true).expect("block 1");
    assert_eq!(req1.piece_index, 1);

    let req2 = picker.pick_block(&peer_id, |_| true).expect("block 2");
    assert_eq!(req2.piece_index, 2);
}

/// Test 3: Block chunking and out-of-order reassembly in memory.
#[test]
fn test_block_chunking_and_reassembly() {
    let piece_size = 65_536u32; // Exactly 4 x 16 KiB blocks
    let mut picker = PiecePicker::new(1, piece_size, piece_size as u64, PickerStrategy::Sequential);

    // Prepare distinct 64 KiB test payload
    let mut test_payload = vec![0u8; piece_size as usize];
    for (i, byte) in test_payload.iter_mut().enumerate() {
        *byte = (i % 251) as u8;
    }

    let b0 = &test_payload[0..16_384];
    let b1 = &test_payload[16_384..32_768];
    let b2 = &test_payload[32_768..49_152];
    let b3 = &test_payload[49_152..65_536];

    // Ingest out-of-order: Block 2, Block 0, Block 3, Block 1
    assert_eq!(
        picker.ingest_block(0, 32_768, b2).expect("ingest block 2"),
        None
    );
    assert_eq!(picker.ingest_block(0, 0, b0).expect("ingest block 0"), None);
    assert_eq!(
        picker.ingest_block(0, 49_152, b3).expect("ingest block 3"),
        None
    );

    // Final block 1 completes the piece
    let result = picker.ingest_block(0, 16_384, b1).expect("ingest block 1");
    match result {
        Some(assembled) => {
            assert_eq!(assembled.len(), piece_size as usize);
            assert_eq!(assembled, test_payload);
        }
        None => panic!("Expected piece to be complete after 4th block"),
    }
}

/// Test 4: Final piece partial block sizing.
#[test]
fn test_final_piece_partial_block_sizing() {
    let total_len = 50_000u64;
    let piece_len = 32_768u32;
    let num_pieces = 2;

    let picker = PiecePicker::new(num_pieces, piece_len, total_len, PickerStrategy::Sequential);

    assert_eq!(picker.piece_size(0), 32_768);
    assert_eq!(picker.piece_size(1), 17_232); // 50,000 - 32,768

    let blocks_0 = picker.blocks_for_piece(0);
    assert_eq!(blocks_0.len(), 2);
    assert_eq!(
        blocks_0[0],
        BlockRequest {
            piece_index: 0,
            block_offset: 0,
            block_length: 16_384
        }
    );
    assert_eq!(
        blocks_0[1],
        BlockRequest {
            piece_index: 0,
            block_offset: 16_384,
            block_length: 16_384
        }
    );

    let blocks_1 = picker.blocks_for_piece(1);
    assert_eq!(blocks_1.len(), 2);
    assert_eq!(
        blocks_1[0],
        BlockRequest {
            piece_index: 1,
            block_offset: 0,
            block_length: 16_384
        }
    );
    assert_eq!(
        blocks_1[1],
        BlockRequest {
            piece_index: 1,
            block_offset: 16_384,
            block_length: 848 // 17,232 - 16,384
        }
    );
}

/// Test 5: Single-file span mapping.
#[test]
fn test_file_span_mapping_single_file() {
    let ten_mb = 10 * 1024 * 1024u64;
    let meta = make_test_meta(
        262_144,
        vec![[0u8; 20]; 40],
        vec![TorrentFile {
            length: ten_mb,
            path: PathBuf::from("single.bin"),
        }],
        ten_mb,
    );

    let storage = TorrentDiskStorage::new(PathBuf::from("/tmp/fetchtrek"), meta);
    let spans = storage.map_span(123_456, 65_536).expect("map single span");

    assert_eq!(spans.len(), 1);
    assert_eq!(
        spans[0],
        FileSpan {
            file_index: 0,
            path: PathBuf::from("/tmp/fetchtrek/single.bin"),
            file_offset: 123_456,
            length: 65_536,
        }
    );
}

/// Test 6: Multi-file boundary span mapping across files.
#[test]
fn test_file_span_mapping_multi_file_boundary() {
    let meta = make_test_meta(
        32_768,
        vec![[0u8; 20]],
        vec![
            TorrentFile {
                length: 10_000,
                path: PathBuf::from("file_a.dat"),
            },
            TorrentFile {
                length: 20_000,
                path: PathBuf::from("file_b.dat"),
            },
        ],
        30_000,
    );

    let storage = TorrentDiskStorage::new(PathBuf::from("/data"), meta);

    // Span crosses boundary: 8,000..14,000 (File A has 2,000 left; File B gets first 4,000)
    let spans = storage.map_span(8_000, 6_000).expect("map multi span");

    assert_eq!(spans.len(), 2);
    assert_eq!(
        spans[0],
        FileSpan {
            file_index: 0,
            path: PathBuf::from("/data/file_a.dat"),
            file_offset: 8_000,
            length: 2_000,
        }
    );
    assert_eq!(
        spans[1],
        FileSpan {
            file_index: 1,
            path: PathBuf::from("/data/file_b.dat"),
            file_offset: 0,
            length: 4_000,
        }
    );
}

/// Test 7: Disk storage verified write and read roundtrip with multi-file layout.
#[tokio::test]
async fn test_disk_storage_verified_write_and_read() {
    let temp_dir = std::env::temp_dir().join(format!(
        "fetchtrek_test_storage_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let piece_size = 32_768u32;
    // Generate deterministic 32 KiB test piece
    let mut payload = vec![0u8; piece_size as usize];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i ^ 0x5A) as u8;
    }
    let piece_hash = sha1(&payload);

    // Multi-file: File 1 (10,000 bytes) + File 2 (22,768 bytes) = 32,768 bytes
    let meta = make_test_meta(
        piece_size,
        vec![piece_hash],
        vec![
            TorrentFile {
                length: 10_000,
                path: PathBuf::from("sub/part1.bin"),
            },
            TorrentFile {
                length: 22_768,
                path: PathBuf::from("part2.bin"),
            },
        ],
        piece_size as u64,
    );

    let storage = TorrentDiskStorage::new(temp_dir.clone(), meta);
    storage.initialize_files().await.expect("initialize files");

    // Verified write
    let written = storage
        .verify_and_write_piece(0, &payload)
        .await
        .expect("write piece");
    assert!(written, "Piece must be written when SHA-1 matches");

    // Read back and compare
    let read_back = storage.read_piece(0).await.expect("read piece");
    assert_eq!(read_back, payload);

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

/// Test 8: Corrupt piece cryptographic rejection prevents disk pollution.
#[tokio::test]
async fn test_disk_storage_corrupt_piece_rejection() {
    let temp_dir = std::env::temp_dir().join(format!(
        "fetchtrek_test_corrupt_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let piece_size = 16_384u32;
    let payload = vec![0x77u8; piece_size as usize];
    let valid_hash = sha1(&payload);

    let meta = make_test_meta(
        piece_size,
        vec![valid_hash],
        vec![TorrentFile {
            length: piece_size as u64,
            path: PathBuf::from("clean.bin"),
        }],
        piece_size as u64,
    );

    let storage = TorrentDiskStorage::new(temp_dir.clone(), meta);
    storage.initialize_files().await.expect("initialize files");

    // Corrupt one byte
    let mut corrupt_payload = payload.clone();
    corrupt_payload[42] ^= 0xFF;

    let write_res = storage
        .verify_and_write_piece(0, &corrupt_payload)
        .await
        .expect("verify piece");
    assert!(
        !write_res,
        "Corrupted piece must be rejected by SHA-1 verification"
    );

    // On-disk file must contain 0 bytes of the corrupt data (it was pre-allocated with zeros)
    let on_disk_bytes = tokio::fs::read(temp_dir.join("clean.bin"))
        .await
        .expect("read file");
    assert_eq!(on_disk_bytes.len(), piece_size as usize);
    assert!(
        on_disk_bytes.iter().all(|&b| b == 0),
        "Disk file must remain untouched by corrupted write"
    );

    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
}

/// Test 9: In-flight timeout and block re-dispatch.
#[test]
fn test_block_timeout_and_reallocation() {
    let peer_a = [0xAA; 20];
    let mut picker = PiecePicker::new(
        1,
        STANDARD_BLOCK_SIZE,
        STANDARD_BLOCK_SIZE as u64,
        PickerStrategy::Sequential,
    );

    // First request
    let req1 = picker
        .pick_block(&peer_a, |_| true)
        .expect("pick initial block");
    assert_eq!(picker.in_flight_count(), 1);

    // No un-requested blocks left
    assert_eq!(picker.pick_block(&peer_a, |_| true), None);

    // Timeout duration 0 reclaims all in-flight blocks immediately
    let reclaimed = picker.reclaim_timed_out_requests(Duration::from_millis(0));
    assert_eq!(reclaimed, 1);
    assert_eq!(picker.in_flight_count(), 0);

    // Next request cycle can re-dispatch the timed out block
    let req2 = picker
        .pick_block(&peer_a, |_| true)
        .expect("pick reallocated block");
    assert_eq!(req1, req2);
}
