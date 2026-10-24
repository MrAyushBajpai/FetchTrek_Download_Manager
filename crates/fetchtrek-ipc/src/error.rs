use thiserror::Error;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid IPC framing magic: expected 0x{expected:08X}, got 0x{actual:08X}")]
    InvalidMagic { expected: u32, actual: u32 },

    #[error("IPC payload too large: length {length} bytes exceeds maximum allowed {max} bytes")]
    PayloadTooLarge { length: u32, max: u32 },

    #[error("IPC connection closed")]
    ConnectionClosed,

    #[error("IPC codec error: {0}")]
    Codec(String),
}

impl From<IpcError> for fetchtrek_types::IpcError {
    fn from(err: IpcError) -> Self {
        match err {
            IpcError::InvalidMagic { .. } => fetchtrek_types::IpcError::InvalidMagic,
            IpcError::PayloadTooLarge { .. } => fetchtrek_types::IpcError::PayloadTooLarge,
            IpcError::ConnectionClosed => fetchtrek_types::IpcError::ChannelClosed,
            _ => fetchtrek_types::IpcError::FramingError,
        }
    }
}
