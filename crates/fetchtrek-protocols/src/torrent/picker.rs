use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::torrent::error::TorrentError;

/// Standard BitTorrent request chunk boundary (16 KiB).
pub const STANDARD_BLOCK_SIZE: u32 = 16 * 1024;

/// Strategy determining piece prioritization order during block dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickerStrategy {
    /// Prioritizes pieces with the rarest swarm availability.
    RarestFirst,
    /// Ordered ascending piece selection for sequential stream playback.
    Sequential,
}

/// Identifies a discrete chunk request within a torrent piece.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockRequest {
    pub piece_index: u32,
    pub block_offset: u32,
    pub block_length: u32,
}

/// In-memory staging buffer for accumulating blocks of an active piece prior to SHA-1 hashing.
#[derive(Debug, Clone)]
pub struct PieceBuffer {
    pub data: Vec<u8>,
    pub blocks_received: Vec<bool>,
    pub total_blocks: usize,
    pub remaining_blocks: usize,
}

impl PieceBuffer {
    /// Creates a new buffer pre-sized to the target piece byte size.
    pub fn new(piece_size: u32) -> Self {
        let total_blocks = if piece_size == 0 {
            0
        } else {
            piece_size.div_ceil(STANDARD_BLOCK_SIZE) as usize
        };
        Self {
            data: vec![0u8; piece_size as usize],
            blocks_received: vec![false; total_blocks],
            total_blocks,
            remaining_blocks: total_blocks,
        }
    }

    /// Returns `true` when all constituent blocks have arrived.
    pub fn is_complete(&self) -> bool {
        self.remaining_blocks == 0
    }

    /// Ingests a block payload into the piece buffer at the specified byte offset.
    pub fn insert_block(&mut self, block_offset: u32, block: &[u8]) -> Result<bool, TorrentError> {
        let offset = block_offset as usize;
        let piece_size = self.data.len();

        // 1. Validate block alignment to 16 KiB boundary (FT-QA-005)
        if !block_offset.is_multiple_of(STANDARD_BLOCK_SIZE) {
            return Err(TorrentError::ProtocolError(format!(
                "Block offset {block_offset} is not aligned to 16 KiB boundary"
            )));
        }

        let block_idx = (block_offset / STANDARD_BLOCK_SIZE) as usize;
        if block_idx >= self.total_blocks {
            return Err(TorrentError::ProtocolError(format!(
                "Block index {block_idx} out of range (total blocks: {})",
                self.total_blocks
            )));
        }

        // 2. Validate expected block length (standard 16 KiB, or terminal piece remainder)
        let expected_len = (piece_size - offset).min(STANDARD_BLOCK_SIZE as usize);
        if block.len() != expected_len {
            return Err(TorrentError::ProtocolError(format!(
                "Block length {} at offset {block_offset} does not match expected length {expected_len}",
                block.len()
            )));
        }

        // 3. Reject duplicate or overlapping block writes
        if self.blocks_received[block_idx] {
            return Err(TorrentError::ProtocolError(format!(
                "Duplicate or overlapping block write at index {block_idx} / offset {block_offset}"
            )));
        }

        let end = offset + block.len();
        self.data[offset..end].copy_from_slice(block);
        self.blocks_received[block_idx] = true;
        self.remaining_blocks = self.remaining_blocks.saturating_sub(1);
        Ok(true)
    }
}

/// Coordinates piece selection, block chunk dispatching, peer availability, and in-flight timeouts.
pub struct PiecePicker {
    num_pieces: usize,
    piece_length: u32,
    total_length: u64,
    strategy: PickerStrategy,
    completed: Vec<bool>,
    peer_availability: Vec<usize>,
    in_flight: HashMap<BlockRequest, (Instant, [u8; 20])>,
    active_pieces: HashMap<u32, PieceBuffer>,
}

impl PiecePicker {
    /// Creates a new piece picker instance.
    pub fn new(
        num_pieces: usize,
        piece_length: u32,
        total_length: u64,
        strategy: PickerStrategy,
    ) -> Self {
        Self {
            num_pieces,
            piece_length,
            total_length,
            strategy,
            completed: vec![false; num_pieces],
            peer_availability: vec![0; num_pieces],
            in_flight: HashMap::new(),
            active_pieces: HashMap::new(),
        }
    }

    /// Computes the exact byte length for a given piece index, correctly sizing the final terminal piece.
    pub fn piece_size(&self, piece_index: u32) -> u32 {
        let idx = piece_index as usize;
        if idx >= self.num_pieces {
            return 0;
        }

        if idx == self.num_pieces - 1 {
            let full_pieces = (self.num_pieces - 1) as u64 * self.piece_length as u64;
            self.total_length.saturating_sub(full_pieces) as u32
        } else {
            self.piece_length
        }
    }

    /// Sets or updates the active piece picking strategy.
    pub fn set_strategy(&mut self, strategy: PickerStrategy) {
        self.strategy = strategy;
    }

    /// Returns the active piece picking strategy.
    pub fn strategy(&self) -> PickerStrategy {
        self.strategy
    }

    /// Direct access to swarm piece availability counts.
    pub fn peer_availability(&self) -> &[usize] {
        &self.peer_availability
    }

    /// Explicitly overwrites the piece availability vector (useful in testing).
    pub fn set_peer_availability(&mut self, availability: Vec<usize>) {
        self.peer_availability = availability;
    }

    /// Records peer availability when a peer announces a `Have` message.
    pub fn record_peer_have(&mut self, piece_index: u32) {
        let idx = piece_index as usize;
        if idx < self.num_pieces {
            self.peer_availability[idx] += 1;
        }
    }

    /// Increments availability counts from an initial peer bitfield.
    pub fn record_peer_bitfield(&mut self, bitfield: &[u8]) {
        for i in 0..self.num_pieces {
            let byte_idx = i / 8;
            let bit_idx = 7 - (i % 8);
            if byte_idx < bitfield.len() && (bitfield[byte_idx] & (1 << bit_idx)) != 0 {
                self.peer_availability[i] += 1;
            }
        }
    }

    /// Decrements availability counts when a peer disconnects.
    pub fn record_peer_bitfield_disconnect(&mut self, bitfield: &[u8]) {
        for i in 0..self.num_pieces {
            let byte_idx = i / 8;
            let bit_idx = 7 - (i % 8);
            if byte_idx < bitfield.len() && (bitfield[byte_idx] & (1 << bit_idx)) != 0 {
                self.peer_availability[i] = self.peer_availability[i].saturating_sub(1);
            }
        }
    }

    /// Returns list of standard 16 KiB block requests composing the piece.
    pub fn blocks_for_piece(&self, piece_index: u32) -> Vec<BlockRequest> {
        let size = self.piece_size(piece_index);
        let mut requests = Vec::new();
        let mut offset = 0;

        while offset < size {
            let block_length = (size - offset).min(STANDARD_BLOCK_SIZE);
            requests.push(BlockRequest {
                piece_index,
                block_offset: offset,
                block_length,
            });
            offset += block_length;
        }

        requests
    }

    /// Selects the next block to request from a peer according to the configured picking strategy.
    pub fn pick_block(
        &mut self,
        peer_id: &[u8; 20],
        peer_has_piece: impl Fn(u32) -> bool,
    ) -> Option<BlockRequest> {
        let mut candidate_pieces: Vec<u32> = (0..self.num_pieces as u32)
            .filter(|&i| !self.completed[i as usize] && peer_has_piece(i))
            .collect();

        if candidate_pieces.is_empty() {
            return None;
        }

        match self.strategy {
            PickerStrategy::Sequential => {
                candidate_pieces.sort_unstable();
            }
            PickerStrategy::RarestFirst => {
                candidate_pieces.sort_by_key(|&i| {
                    let avail = self.peer_availability[i as usize];
                    let rank = if avail > 0 { 0 } else { 1 };
                    (rank, avail, i)
                });
            }
        }

        for p in candidate_pieces {
            let blocks = self.blocks_for_piece(p);
            for req in blocks {
                // Skip if already received in active piece buffer
                if let Some(buf) = self.active_pieces.get(&p) {
                    let block_idx = (req.block_offset / STANDARD_BLOCK_SIZE) as usize;
                    if block_idx < buf.blocks_received.len() && buf.blocks_received[block_idx] {
                        continue;
                    }
                }

                // Skip if currently in-flight
                if self.in_flight.contains_key(&req) {
                    continue;
                }

                // Dispatch block
                self.in_flight.insert(req, (Instant::now(), *peer_id));
                let p_size = self.piece_size(p);
                self.active_pieces
                    .entry(p)
                    .or_insert_with(|| PieceBuffer::new(p_size));
                return Some(req);
            }
        }

        None
    }

    /// Ingests a received block into the active piece buffer. If all blocks for the piece
    /// are received, returns the complete piece byte payload for SHA-1 verification.
    pub fn ingest_block(
        &mut self,
        index: u32,
        begin: u32,
        block: &[u8],
    ) -> Result<Option<Vec<u8>>, TorrentError> {
        self.ingest_block_from_peer(index, begin, block, None)
    }

    /// Ingests a received block while verifying in-flight tracking and peer attribution (FT-QA-005).
    pub fn ingest_block_from_peer(
        &mut self,
        index: u32,
        begin: u32,
        block: &[u8],
        peer_id: Option<&[u8; 20]>,
    ) -> Result<Option<Vec<u8>>, TorrentError> {
        let idx = index as usize;
        if idx >= self.num_pieces {
            return Err(TorrentError::ProtocolError(format!(
                "Piece index {index} out of bounds (total pieces: {})",
                self.num_pieces
            )));
        }

        let req = BlockRequest {
            piece_index: index,
            block_offset: begin,
            block_length: block.len() as u32,
        };

        // Validate in-flight request and peer attribution for peer wire network blocks (FT-QA-005)
        if let Some(actual_peer) = peer_id {
            match self.in_flight.get(&req) {
                None => {
                    return Err(TorrentError::ProtocolError(format!(
                        "Unsolicited block received: no in-flight request for piece {index}, offset {begin}, length {}",
                        block.len()
                    )));
                }
                Some((_, requested_peer)) if requested_peer != actual_peer => {
                    return Err(TorrentError::ProtocolError(format!(
                        "Unsolicited block: peer mismatch for piece {index}, offset {begin} (requested from {requested_peer:?}, received from {actual_peer:?})"
                    )));
                }
                _ => {}
            }
        }

        if self.completed[idx] {
            self.in_flight.remove(&req);
            return Ok(None);
        }

        let p_size = self.piece_size(index);
        let buffer = self
            .active_pieces
            .entry(index)
            .or_insert_with(|| PieceBuffer::new(p_size));

        buffer.insert_block(begin, block)?;
        self.in_flight.remove(&req);

        if buffer.is_complete() {
            let completed_buf = self.active_pieces.remove(&index).expect("active piece");
            Ok(Some(completed_buf.data))
        } else {
            Ok(None)
        }
    }

    /// Marks a piece as verified and fully written to disk.
    pub fn mark_completed(&mut self, piece_index: u32) {
        let idx = piece_index as usize;
        if idx < self.num_pieces {
            self.completed[idx] = true;
            self.active_pieces.remove(&piece_index);
            self.in_flight
                .retain(|req, _| req.piece_index != piece_index);
        }
    }

    /// Checks if a piece is marked completed.
    pub fn is_completed(&self, piece_index: u32) -> bool {
        let idx = piece_index as usize;
        if idx < self.num_pieces {
            self.completed[idx]
        } else {
            false
        }
    }

    /// Returns `true` if all pieces in the torrent have been verified and completed.
    pub fn is_all_completed(&self) -> bool {
        self.completed.iter().all(|&c| c)
    }

    /// Returns the number of currently active in-memory piece buffers.
    pub fn active_buffers(&self) -> usize {
        self.active_pieces.len()
    }

    /// Number of verified completed pieces.
    pub fn completed_count(&self) -> usize {
        self.completed.iter().filter(|&&c| c).count()
    }

    /// Reclaims block requests that have been in-flight longer than `timeout`,
    /// returning them to the available pool for re-dispatch.
    pub fn reclaim_timed_out_requests(&mut self, timeout: Duration) -> usize {
        let now = Instant::now();
        let mut timed_out = Vec::new();

        for (req, (started, _)) in &self.in_flight {
            if now.duration_since(*started) >= timeout {
                timed_out.push(*req);
            }
        }

        for req in &timed_out {
            self.in_flight.remove(req);
        }

        timed_out.len()
    }

    /// Cancels all in-flight requests assigned to a specific peer (e.g. upon disconnection).
    pub fn cancel_peer_requests(&mut self, peer_id: &[u8; 20]) -> usize {
        let mut cancelled = Vec::new();

        for (req, (_, assigned_peer)) in &self.in_flight {
            if assigned_peer == peer_id {
                cancelled.push(*req);
            }
        }

        for req in &cancelled {
            self.in_flight.remove(req);
        }

        cancelled.len()
    }

    /// Returns the number of currently in-flight block requests.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }
}
