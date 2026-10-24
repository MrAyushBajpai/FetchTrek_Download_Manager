use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::IpcError;

pub const IPC_MAGIC: u32 = 0x4654524B; // ASCII 'FTRK'
pub const HEADER_LEN: usize = 16;
pub const MAX_PAYLOAD_LEN: u32 = 16 * 1024 * 1024; // 16 MiB DoS ceiling

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum MessageType {
    Request = 0,
    Response = 1,
    Event = 2,
    Heartbeat = 3,
}

impl MessageType {
    pub const fn as_u8(&self) -> u8 {
        *self as u8
    }
}

impl TryFrom<u8> for MessageType {
    type Error = IpcError;

    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            0 => Ok(Self::Request),
            1 => Ok(Self::Response),
            2 => Ok(Self::Event),
            3 => Ok(Self::Heartbeat),
            other => Err(IpcError::Codec(format!(
                "Invalid IPC message_type: {}",
                other
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpcHeader {
    pub magic: u32,
    pub payload_len: u32,
    pub message_id: u16,
    pub message_type: u8,
    pub flags: u8,
    pub correlation_id: u32,
}

impl IpcHeader {
    pub const fn new(
        payload_len: u32,
        message_id: u16,
        message_type: u8,
        flags: u8,
        correlation_id: u32,
    ) -> Self {
        Self {
            magic: IPC_MAGIC,
            payload_len,
            message_id,
            message_type,
            flags,
            correlation_id,
        }
    }

    pub fn to_bytes(&self) -> [u8; HEADER_LEN] {
        let mut buf = [0u8; HEADER_LEN];
        buf[0..4].copy_from_slice(&self.magic.to_be_bytes());
        buf[4..8].copy_from_slice(&self.payload_len.to_le_bytes());
        buf[8..10].copy_from_slice(&self.message_id.to_le_bytes());
        buf[10] = self.message_type;
        buf[11] = self.flags;
        buf[12..16].copy_from_slice(&self.correlation_id.to_le_bytes());
        buf
    }

    pub fn from_bytes(bytes: &[u8; HEADER_LEN]) -> Result<Self, IpcError> {
        let magic = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if magic != IPC_MAGIC {
            return Err(IpcError::InvalidMagic {
                expected: IPC_MAGIC,
                actual: magic,
            });
        }

        let payload_len = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(IpcError::PayloadTooLarge {
                length: payload_len,
                max: MAX_PAYLOAD_LEN,
            });
        }

        let message_id = u16::from_le_bytes([bytes[8], bytes[9]]);
        let message_type = bytes[10];
        let flags = bytes[11];
        let correlation_id = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);

        Ok(Self {
            magic,
            payload_len,
            message_id,
            message_type,
            flags,
            correlation_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpcFrame {
    pub header: IpcHeader,
    pub payload: Vec<u8>,
}

impl IpcFrame {
    pub fn new(
        message_id: u16,
        message_type: u8,
        flags: u8,
        correlation_id: u32,
        payload: Vec<u8>,
    ) -> Result<Self, IpcError> {
        if payload.len() > MAX_PAYLOAD_LEN as usize {
            return Err(IpcError::PayloadTooLarge {
                length: payload.len() as u32,
                max: MAX_PAYLOAD_LEN,
            });
        }
        let header = IpcHeader::new(
            payload.len() as u32,
            message_id,
            message_type,
            flags,
            correlation_id,
        );
        Ok(Self { header, payload })
    }

    pub fn with_header(header: IpcHeader, payload: Vec<u8>) -> Result<Self, IpcError> {
        if payload.len() != header.payload_len as usize {
            return Err(IpcError::Codec(format!(
                "Payload length mismatch: header specifies {}, but payload buffer length is {}",
                header.payload_len,
                payload.len()
            )));
        }
        Ok(Self { header, payload })
    }
}

pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<IpcFrame, IpcError> {
    let mut header_buf = [0u8; HEADER_LEN];
    let mut read_bytes = 0;

    while read_bytes < HEADER_LEN {
        match reader.read(&mut header_buf[read_bytes..]).await {
            Ok(0) => {
                if read_bytes == 0 {
                    return Err(IpcError::ConnectionClosed);
                } else {
                    return Err(IpcError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Connection closed while reading IPC header",
                    )));
                }
            }
            Ok(n) => {
                read_bytes += n;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(IpcError::Io(e)),
        }
    }

    let header = IpcHeader::from_bytes(&header_buf)?;

    if header.payload_len == 0 {
        return Ok(IpcFrame {
            header,
            payload: Vec::new(),
        });
    }

    let mut payload = vec![0u8; header.payload_len as usize];
    let mut payload_read = 0;

    while payload_read < payload.len() {
        match reader.read(&mut payload[payload_read..]).await {
            Ok(0) => {
                return Err(IpcError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Connection closed while reading IPC payload",
                )));
            }
            Ok(n) => {
                payload_read += n;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(IpcError::Io(e)),
        }
    }

    Ok(IpcFrame { header, payload })
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &IpcFrame,
) -> Result<(), IpcError> {
    if frame.payload.len() > MAX_PAYLOAD_LEN as usize {
        return Err(IpcError::PayloadTooLarge {
            length: frame.payload.len() as u32,
            max: MAX_PAYLOAD_LEN,
        });
    }

    let header_bytes = frame.header.to_bytes();
    writer.write_all(&header_bytes).await?;
    if !frame.payload.is_empty() {
        writer.write_all(&frame.payload).await?;
    }
    writer.flush().await?;
    Ok(())
}
