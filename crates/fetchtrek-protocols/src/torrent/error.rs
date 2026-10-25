use thiserror::Error;

/// Error conditions encountered during Bencode decoding, metainfo parsing, and magnet resolution.
#[derive(Debug, Error)]
pub enum TorrentError {
    #[error("Bencode syntax error: {0}")]
    BencodeError(String),
    #[error("Invalid metainfo structure: {0}")]
    InvalidMetainfo(String),
    #[error("Invalid magnet URI: {0}")]
    InvalidMagnet(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
