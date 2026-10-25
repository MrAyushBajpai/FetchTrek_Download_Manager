#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "media")]
pub mod media;

#[cfg(feature = "http")]
pub use http::{parse_content_range_header, HttpTransport, ParsedContentRange};

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
    announce_http, announce_udp, decode_base32, deduplicate_peers, extract_raw_dict_value, sha1,
    to_base32, validate_torrent_path_component, AnnounceEvent, AnnounceRequest, AnnounceResponse,
    BencodeValue, BlockRequest, FileSpan, MagnetLink, PeerConnection, PeerEndpoint, PeerHandshake,
    PeerMessage, PeerState, PickerStrategy, PieceBuffer, PiecePicker, Sha1, TorrentDiskStorage,
    TorrentError, TorrentFile, TorrentMeta, TrackerCoordinator, HANDSHAKE_LEN, MAX_MESSAGE_LENGTH,
    STANDARD_BLOCK_SIZE,
};
