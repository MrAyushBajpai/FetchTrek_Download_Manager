//! IPC transport and binary framing protocols.

pub mod error;
pub mod framing;
pub mod transport;

pub use error::IpcError;
pub use framing::{
    read_frame, write_frame, IpcFrame, IpcHeader, MessageType, HEADER_LEN, IPC_MAGIC,
    MAX_PAYLOAD_LEN,
};
pub use transport::{default_endpoint, endpoint_with_name, IpcClient, IpcServer, IpcStream};
