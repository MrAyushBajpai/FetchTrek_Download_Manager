use std::io::SeekFrom;
use std::path::PathBuf;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::torrent::error::TorrentError;
use crate::torrent::metainfo::TorrentMeta;
use crate::torrent::sha1::sha1;

/// Represents a contiguous slice of bytes within a specific file on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSpan {
    pub file_index: usize,
    pub path: PathBuf,
    pub file_offset: u64,
    pub length: usize,
}

/// Multi-file disk storage mapper and verified piece reader/writer.
#[derive(Debug, Clone)]
pub struct TorrentDiskStorage {
    base_dir: PathBuf,
    meta: TorrentMeta,
}

impl TorrentDiskStorage {
    /// Creates a new disk storage mapper with base output directory and torrent metainfo.
    pub fn new(base_dir: PathBuf, meta: TorrentMeta) -> Self {
        Self { base_dir, meta }
    }

    /// Accesses the underlying torrent metainfo.
    pub fn meta(&self) -> &TorrentMeta {
        &self.meta
    }

    /// Accesses the base download directory.
    pub fn base_dir(&self) -> &std::path::Path {
        &self.base_dir
    }

    /// Pre-allocates files on disk using sparse/direct allocation and creates directory scaffolding.
    pub async fn initialize_files(&self) -> Result<(), TorrentError> {
        for file in &self.meta.files {
            let full_path = self.base_dir.join(&file.path);
            if let Some(parent) = full_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let file_handle = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&full_path)
                .await?;
            file_handle.set_len(file.length).await?;
        }
        Ok(())
    }

    /// Pure-math mapping from continuous torrent offset to file slice spans.
    pub fn map_span(
        &self,
        torrent_offset: u64,
        length: usize,
    ) -> Result<Vec<FileSpan>, TorrentError> {
        let req_start = torrent_offset;
        let req_end = torrent_offset
            .checked_add(length as u64)
            .ok_or_else(|| TorrentError::Storage("Span calculation overflowed u64".into()))?;

        if req_end > self.meta.total_length {
            return Err(TorrentError::Storage(format!(
                "Span [{}, {}) exceeds total torrent length {}",
                req_start, req_end, self.meta.total_length
            )));
        }

        if length == 0 {
            return Ok(Vec::new());
        }

        let mut spans = Vec::new();
        let mut cum_start = 0u64;

        for (idx, file) in self.meta.files.iter().enumerate() {
            let cum_end = cum_start + file.length;

            if req_end <= cum_start {
                break;
            }

            if req_start >= cum_end {
                cum_start = cum_end;
                continue;
            }

            let overlap_start = req_start.max(cum_start);
            let overlap_end = req_end.min(cum_end);
            let span_len = (overlap_end - overlap_start) as usize;
            let file_offset = overlap_start - cum_start;
            let full_path = self.base_dir.join(&file.path);

            spans.push(FileSpan {
                file_index: idx,
                path: full_path,
                file_offset,
                length: span_len,
            });

            cum_start = cum_end;
        }

        Ok(spans)
    }

    /// Verifies piece SHA-1 hash and writes payload across mapped file spans.
    /// Returns Ok(true) if hash matches and data written, Ok(false) if corrupted.
    pub async fn verify_and_write_piece(
        &self,
        piece_index: u32,
        piece_data: &[u8],
    ) -> Result<bool, TorrentError> {
        let idx = piece_index as usize;
        if idx >= self.meta.pieces.len() {
            return Err(TorrentError::Storage(format!(
                "Piece index {piece_index} out of bounds (total pieces: {})",
                self.meta.pieces.len()
            )));
        }

        let expected_len = if idx == self.meta.pieces.len() - 1 {
            let full_len = (self.meta.pieces.len() - 1) as u64 * self.meta.piece_length as u64;
            (self.meta.total_length - full_len) as usize
        } else {
            self.meta.piece_length as usize
        };

        if piece_data.len() != expected_len {
            return Err(TorrentError::Storage(format!(
                "Piece length mismatch for piece {piece_index}: expected {expected_len}, got {}",
                piece_data.len()
            )));
        }

        // Cryptographic integrity check: do NOT write corrupted data to disk
        let computed_hash = sha1(piece_data);
        if computed_hash != self.meta.pieces[idx] {
            return Ok(false);
        }

        let piece_offset = piece_index as u64 * self.meta.piece_length as u64;
        let spans = self.map_span(piece_offset, piece_data.len())?;
        let mut cursor = 0;

        for span in spans {
            let slice = &piece_data[cursor..cursor + span.length];
            if let Some(parent) = span.path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&span.path)
                .await?;
            file.seek(SeekFrom::Start(span.file_offset)).await?;
            file.write_all(slice).await?;
            file.flush().await?;
            cursor += span.length;
        }

        Ok(true)
    }

    /// Reads verified piece bytes from disk.
    pub async fn read_piece(&self, piece_index: u32) -> Result<Vec<u8>, TorrentError> {
        let idx = piece_index as usize;
        if idx >= self.meta.pieces.len() {
            return Err(TorrentError::Storage(format!(
                "Piece index {piece_index} out of bounds (total pieces: {})",
                self.meta.pieces.len()
            )));
        }

        let expected_len = if idx == self.meta.pieces.len() - 1 {
            let full_len = (self.meta.pieces.len() - 1) as u64 * self.meta.piece_length as u64;
            (self.meta.total_length - full_len) as usize
        } else {
            self.meta.piece_length as usize
        };

        let piece_offset = piece_index as u64 * self.meta.piece_length as u64;
        let spans = self.map_span(piece_offset, expected_len)?;
        let mut buffer = vec![0u8; expected_len];
        let mut cursor = 0;

        for span in spans {
            let mut file = OpenOptions::new().read(true).open(&span.path).await?;
            file.seek(SeekFrom::Start(span.file_offset)).await?;
            file.read_exact(&mut buffer[cursor..cursor + span.length])
                .await?;
            cursor += span.length;
        }

        Ok(buffer)
    }
}
