#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "media")]
pub mod media;

#[cfg(feature = "http")]
pub use http::HttpTransport;

#[cfg(feature = "media")]
pub use media::{
    assemble_stream, decrypt_aes128_cbc, encrypt_aes128_cbc, is_ffmpeg_available, parse_dash,
    parse_dash_media, parse_hls, parse_iso8601_duration, parse_media_manifest, remux_dual_streams,
    resolve_url, AssemblyConfig, AssemblySummary, AudioTrack, KeyMetadata, MasterPlaylist,
    MediaManifest, MediaPlaylist, MediaSegment, MediaVariant, StreamContainer,
};

#[cfg(feature = "torrent")]
pub mod torrent;

#[cfg(feature = "torrent")]
pub use torrent::{
    decode_base32, extract_raw_dict_value, sha1, to_base32, BencodeValue, MagnetLink, Sha1,
    TorrentError, TorrentFile, TorrentMeta,
};
