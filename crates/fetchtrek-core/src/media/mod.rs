pub mod coordinator;
pub mod scheduler;

use std::path::PathBuf;
use std::sync::Arc;

pub use coordinator::{MediaDownloadSummary, MediaIngestionCoordinator};
pub use scheduler::{ChunkState, MediaChunkScheduler, MediaSchedulerSnapshot};

use fetchtrek_types::ByteRange;

use crate::throttle::BandwidthLimiter;
use crate::transport::SessionContext;

/// A discrete media chunk descriptor (e.g. HLS .ts / DASH .m4s or initialization chunk).
#[derive(Debug, Clone, PartialEq)]
pub struct MediaChunk {
    /// Sequential 0-based chunk order in the playlist.
    pub index: usize,
    /// Canonical absolute URL of the chunk.
    pub url: String,
    /// Duration of the chunk in seconds (0.0 for initialization segments).
    pub duration_secs: f64,
    /// Sub-range byte offsets if declared.
    pub byte_range: Option<ByteRange>,
}

impl MediaChunk {
    pub fn new(index: usize, url: impl Into<String>, duration_secs: f64) -> Self {
        Self {
            index,
            url: url.into(),
            duration_secs,
            byte_range: None,
        }
    }

    pub fn with_byte_range(mut self, byte_range: ByteRange) -> Self {
        self.byte_range = Some(byte_range);
        self
    }
}

/// Configuration parameters for parallel media chunk downloads.
#[derive(Debug, Clone)]
pub struct MediaDownloadConfig {
    pub chunks: Vec<MediaChunk>,
    pub destination_dir: PathBuf,
    pub connections: usize,
    pub context: SessionContext,
    pub rate_limiter: Option<Arc<BandwidthLimiter>>,
    pub max_retries_per_chunk: usize,
}

impl MediaDownloadConfig {
    pub fn new(chunks: Vec<MediaChunk>, destination_dir: impl Into<PathBuf>) -> Self {
        Self {
            chunks,
            destination_dir: destination_dir.into(),
            connections: 4,
            context: SessionContext::default(),
            rate_limiter: None,
            max_retries_per_chunk: 3,
        }
    }

    pub fn with_connections(mut self, connections: usize) -> Self {
        self.connections = connections.clamp(1, 16);
        self
    }

    pub fn with_context(mut self, context: SessionContext) -> Self {
        self.context = context;
        self
    }

    pub fn with_rate_limiter(mut self, limiter: Arc<BandwidthLimiter>) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }

    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries_per_chunk = max_retries;
        self
    }
}
