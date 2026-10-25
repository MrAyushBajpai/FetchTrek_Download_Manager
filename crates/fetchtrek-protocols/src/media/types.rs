use fetchtrek_types::{ByteRange, Protocol};

/// Security threshold preventing memory exhaustion from oversized playlists (FT-QA-016).
pub const MAX_MANIFEST_BYTES: usize = 10 * 1024 * 1024; // 10 MiB
/// Security ceiling on total media segments parsed from a single playlist representation.
pub const MAX_MEDIA_SEGMENTS: usize = 50_000;
/// Security ceiling on audio/video stream variants parsed from a master manifest.
pub const MAX_MEDIA_VARIANTS: usize = 1_000;

#[derive(Debug, Clone, PartialEq)]
pub enum MediaManifest {
    Master(MasterPlaylist),
    Media(MediaPlaylist),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct MasterPlaylist {
    pub variants: Vec<MediaVariant>,
    pub audio_tracks: Vec<AudioTrack>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MediaVariant {
    pub bandwidth: u64,
    pub resolution: Option<(u32, u32)>,
    pub codecs: Option<String>,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AudioTrack {
    pub group_id: String,
    pub name: String,
    pub language: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MediaPlaylist {
    pub protocol: Protocol,
    pub target_duration_secs: f64,
    pub is_live: bool,
    pub media_sequence: u64,
    pub init_segment: Option<MediaSegment>,
    pub segments: Vec<MediaSegment>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MediaSegment {
    pub sequence: u64,
    pub url: String,
    pub duration_secs: f64,
    pub byte_range: Option<ByteRange>,
    pub key: Option<KeyMetadata>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyMetadata {
    pub method: String,
    pub url: String,
    pub iv: Option<[u8; 16]>,
}
