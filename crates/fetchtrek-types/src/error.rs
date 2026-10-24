use core::fmt;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum ErrorClassification {
    RecoverableTransient,
    TerminalFault,
    IpcTransportFault,
    MediaRemuxFault,
}

impl fmt::Display for ErrorClassification {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecoverableTransient => write!(f, "recoverable_transient"),
            Self::TerminalFault => write!(f, "terminal_fault"),
            Self::IpcTransportFault => write!(f, "ipc_transport_fault"),
            Self::MediaRemuxFault => write!(f, "media_remux_fault"),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum NetworkError {
    ConnectionReset,
    GatewayTimeout,
    RateLimited,
    SocketTimeout,
    NoRangeSupport,
    ETagMismatch,
    AccessDenied,
}

impl NetworkError {
    pub const fn classification(&self) -> ErrorClassification {
        match self {
            Self::ConnectionReset
            | Self::GatewayTimeout
            | Self::RateLimited
            | Self::SocketTimeout => ErrorClassification::RecoverableTransient,
            Self::NoRangeSupport | Self::ETagMismatch | Self::AccessDenied => {
                ErrorClassification::TerminalFault
            }
        }
    }

    pub const fn is_recoverable(&self) -> bool {
        matches!(
            self.classification(),
            ErrorClassification::RecoverableTransient
        )
    }
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionReset => write!(f, "network connection reset"),
            Self::GatewayTimeout => write!(f, "gateway timeout (504)"),
            Self::RateLimited => write!(f, "rate limited (429)"),
            Self::SocketTimeout => write!(f, "socket timeout"),
            Self::NoRangeSupport => write!(f, "server does not support byte ranges"),
            Self::ETagMismatch => write!(f, "etag precondition mismatch"),
            Self::AccessDenied => write!(f, "network access denied (403)"),
        }
    }
}

impl core::error::Error for NetworkError {}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum StorageError {
    DiskFull,
    AccessDenied,
    PathNotFound,
    ChecksumMismatch,
    IoFailure,
}

impl StorageError {
    pub const fn classification(&self) -> ErrorClassification {
        ErrorClassification::TerminalFault
    }

    pub const fn is_recoverable(&self) -> bool {
        false
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DiskFull => write!(f, "disk space exhausted (ENOSPC)"),
            Self::AccessDenied => write!(f, "storage access denied (EACCES)"),
            Self::PathNotFound => write!(f, "destination path not found"),
            Self::ChecksumMismatch => write!(f, "block checksum verification failed"),
            Self::IoFailure => write!(f, "positional storage I/O failure"),
        }
    }
}

impl core::error::Error for StorageError {}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum IpcError {
    InvalidMagic,
    ChannelClosed,
    PayloadTooLarge,
    FramingError,
}

impl IpcError {
    pub const fn classification(&self) -> ErrorClassification {
        ErrorClassification::IpcTransportFault
    }

    pub const fn is_recoverable(&self) -> bool {
        false
    }
}

impl fmt::Display for IpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => write!(f, "invalid IPC framing magic"),
            Self::ChannelClosed => write!(f, "IPC channel closed"),
            Self::PayloadTooLarge => write!(f, "IPC payload exceeds maximum length"),
            Self::FramingError => write!(f, "IPC message framing error"),
        }
    }
}

impl core::error::Error for IpcError {}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum MediaError {
    MalformedManifest,
    FFmpegChildFailed,
    SegmentMissing,
    UnsupportedCodec,
}

impl MediaError {
    pub const fn classification(&self) -> ErrorClassification {
        ErrorClassification::MediaRemuxFault
    }

    pub const fn is_recoverable(&self) -> bool {
        false
    }
}

impl fmt::Display for MediaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedManifest => write!(f, "malformed HLS/DASH manifest"),
            Self::FFmpegChildFailed => write!(f, "ffmpeg remux child process failed"),
            Self::SegmentMissing => write!(f, "media stream segment missing"),
            Self::UnsupportedCodec => write!(f, "unsupported media container or codec"),
        }
    }
}

impl core::error::Error for MediaError {}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum FetchTrekError {
    Network(NetworkError),
    Storage(StorageError),
    Ipc(IpcError),
    Media(MediaError),
}

impl FetchTrekError {
    pub const fn classification(&self) -> ErrorClassification {
        match self {
            Self::Network(err) => err.classification(),
            Self::Storage(err) => err.classification(),
            Self::Ipc(err) => err.classification(),
            Self::Media(err) => err.classification(),
        }
    }

    pub const fn is_recoverable(&self) -> bool {
        matches!(
            self.classification(),
            ErrorClassification::RecoverableTransient
        )
    }
}

impl fmt::Display for FetchTrekError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Network(err) => write!(f, "{}", err),
            Self::Storage(err) => write!(f, "{}", err),
            Self::Ipc(err) => write!(f, "{}", err),
            Self::Media(err) => write!(f, "{}", err),
        }
    }
}

impl core::error::Error for FetchTrekError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Network(err) => Some(err),
            Self::Storage(err) => Some(err),
            Self::Ipc(err) => Some(err),
            Self::Media(err) => Some(err),
        }
    }
}

impl From<NetworkError> for FetchTrekError {
    fn from(err: NetworkError) -> Self {
        Self::Network(err)
    }
}

impl From<StorageError> for FetchTrekError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err)
    }
}

impl From<IpcError> for FetchTrekError {
    fn from(err: IpcError) -> Self {
        Self::Ipc(err)
    }
}

impl From<MediaError> for FetchTrekError {
    fn from(err: MediaError) -> Self {
        Self::Media(err)
    }
}
