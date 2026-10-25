use fetchtrek_types::{ByteRange, Protocol};

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
