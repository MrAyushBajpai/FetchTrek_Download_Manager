use std::fmt;

#[derive(Debug)]
pub enum HostError {
    Eof,
    ZeroLengthMessage,
    OversizedMessage(u32),
    InvalidUtf8(std::string::FromUtf8Error),
    Json(serde_json::Error),
    Ipc(fetchtrek_ipc::IpcError),
    Io(std::io::Error),
    Timeout,
    DaemonOffline(String),
    Registration(String),
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eof => write!(f, "End of stream reached"),
            Self::ZeroLengthMessage => write!(f, "Message length cannot be zero"),
            Self::OversizedMessage(len) => {
                write!(
                    f,
                    "Message length {len} exceeds maximum bound of 1,048,576 bytes"
                )
            }
            Self::InvalidUtf8(e) => write!(f, "Invalid UTF-8 payload: {e}"),
            Self::Json(e) => write!(f, "JSON codec error: {e}"),
            Self::Ipc(e) => write!(f, "IPC transport error: {e}"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Timeout => write!(f, "Daemon timeout"),
            Self::DaemonOffline(msg) => write!(f, "Daemon connection offline: {msg}"),
            Self::Registration(msg) => write!(f, "Registration error: {msg}"),
        }
    }
}

impl std::error::Error for HostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidUtf8(e) => Some(e),
            Self::Json(e) => Some(e),
            Self::Ipc(e) => Some(e),
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for HostError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<std::string::FromUtf8Error> for HostError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Self::InvalidUtf8(e)
    }
}

impl From<serde_json::Error> for HostError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

impl From<fetchtrek_ipc::IpcError> for HostError {
    fn from(e: fetchtrek_ipc::IpcError) -> Self {
        Self::Ipc(e)
    }
}
