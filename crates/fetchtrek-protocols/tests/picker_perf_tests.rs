use std::time::Instant;

use fetchtrek_protocols::{PickerStrategy, PiecePicker, STANDARD_BLOCK_SIZE};

#[test]
fn test_picker_perf_sequential() {
    let num_pieces = 5_000usize;
    let piece_len = STANDARD_BLOCK_SIZE * 4; // 64 KiB piece (4 blocks per piece)
    let total_len = (num_pieces as u64) * (piece_len as u64);
    let mut picker = PiecePicker::new(num_pieces, piece_len, total_len, PickerStrategy::Sequential);

    let peer_id = [0x42; 20];
    let start = Instant::now();
    let mut blocks_picked = 0;

    // Pick 10,000 blocks sequentially (covering 2,500 pieces)
    for _ in 0..10_000 {
        if let Some(_req) = picker.pick_block(&peer_id, |_| true) {
            blocks_picked += 1;
        } else {
            break;
        }
    }

    let elapsed = start.elapsed();
    assert_eq!(
        blocks_picked, 10_000,
        "Should pick 10,000 blocks sequentially"
    );
    // In debug mode with unoptimized build, ensure it completes comfortably within budget (< 2000ms)
    assert!(
        elapsed.as_millis() < 2000,
        "Sequential pick_block took too long: {:?}",
        elapsed
    );
}

#[test]
fn test_picker_perf_rarest_first() {
    let num_pieces = 5_000usize;
    let piece_len = STANDARD_BLOCK_SIZE * 2; // 32 KiB piece (2 blocks per piece)
    let total_len = (num_pieces as u64) * (piece_len as u64);
    let mut picker = PiecePicker::new(
        num_pieces,
        piece_len,
        total_len,
        PickerStrategy::RarestFirst,
    );

    // Populate availability with varied counts (1 to 10)
    let availability: Vec<usize> = (0..num_pieces).map(|i| (i % 10) + 1).collect();
    picker.set_peer_availability(availability);

    let peer_id = [0x42; 20];
    let start = Instant::now();
    let mut blocks_picked = 0;

    // Pick 5,000 blocks under rarest-first
    for _ in 0..5_000 {
        if let Some(_req) = picker.pick_block(&peer_id, |_| true) {
            blocks_picked += 1;
        } else {
            break;
        }
    }

    let elapsed = start.elapsed();
    assert_eq!(
        blocks_picked, 5_000,
        "Should pick 5,000 blocks in rarest-first"
    );
    assert!(
        elapsed.as_millis() < 2000,
        "Rarest-first pick_block took too long: {:?}",
        elapsed
    );
}

#[test]
fn test_picker_completion_and_exhaustion() {
    let num_pieces = 100usize;
    let piece_len = STANDARD_BLOCK_SIZE; // 1 block per piece
    let total_len = (num_pieces as u64) * (piece_len as u64);
    let mut picker = PiecePicker::new(num_pieces, piece_len, total_len, PickerStrategy::Sequential);

    let peer_id = [0x01; 20];
    let mut picked = 0;
    while picker.pick_block(&peer_id, |_| true).is_some() {
        picked += 1;
    }
    assert_eq!(picked, 100);
    // Once all blocks are in flight, picker must return None
    assert!(picker.pick_block(&peer_id, |_| true).is_none());
}
