#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "media")]
pub mod media;

#[cfg(feature = "http")]
pub use http::HttpTransport;

#[cfg(feature = "media")]
pub use media::{
    parse_dash, parse_dash_media, parse_hls, parse_iso8601_duration, parse_media_manifest,
    resolve_url, AudioTrack, KeyMetadata, MasterPlaylist, MediaManifest, MediaPlaylist,
    MediaSegment, MediaVariant,
};
