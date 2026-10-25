use crate::torrent::error::TorrentError;

pub const MAX_MESSAGE_LENGTH: u32 = 256 * 1024; // 256 KiB DoS boundary ceiling
pub const STANDARD_BLOCK_SIZE: u32 = 16 * 1024; // 16 KiB standard request size

pub const ID_CHOKE: u8 = 0;
pub const ID_UNCHOKE: u8 = 1;
pub const ID_INTERESTED: u8 = 2;
pub const ID_NOT_INTERESTED: u8 = 3;
pub const ID_HAVE: u8 = 4;
pub const ID_BITFIELD: u8 = 5;
pub const ID_REQUEST: u8 = 6;
pub const ID_PIECE: u8 = 7;
pub const ID_CANCEL: u8 = 8;
pub const ID_SUGGEST_PIECE: u8 = 13;
pub const ID_HAVE_ALL: u8 = 14;
pub const ID_HAVE_NONE: u8 = 15;
pub const ID_REJECT_REQUEST: u8 = 16;
pub const ID_ALLOWED_FAST: u8 = 17;

/// Peer Wire messages specified by BEP 0003 and BEP 0006 Fast Extension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        piece_index: u32,
    },
    Bitfield(Vec<u8>),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    SuggestPiece {
        piece_index: u32,
    },
    HaveAll,
    HaveNone,
    RejectRequest {
        index: u32,
        begin: u32,
        length: u32,
    },
    AllowedFast {
        piece_index: u32,
    },
}

impl PeerMessage {
    /// Validates a bitfield payload against expected piece count according to BEP 0003:
    /// 1. Byte length must equal ceil(num_pieces / 8).
    /// 2. If num_pieces is not a multiple of 8, all trailing spare bits in the final byte must be zero.
    pub fn validate_bitfield(data: &[u8], num_pieces: usize) -> Result<(), TorrentError> {
        let expected_bytes = num_pieces.div_ceil(8);
        if data.len() != expected_bytes {
            return Err(TorrentError::ProtocolError(format!(
                "Bitfield length {} bytes does not match expected {expected_bytes} bytes for {num_pieces} pieces",
                data.len()
            )));
        }

        let rem = num_pieces % 8;
        if rem != 0 {
            let spare_bits = 8 - rem;
            let spare_mask = (1u8 << spare_bits) - 1;
            if let Some(&last_byte) = data.last() {
                if (last_byte & spare_mask) != 0 {
                    return Err(TorrentError::ProtocolError(format!(
                        "Bitfield contains set bits in trailing spare positions: byte 0x{last_byte:02X}, mask 0x{spare_mask:02X}"
                    )));
                }
            }
        }

        Ok(())
    }

    /// Encodes this message into standard framed wire representation with 4-byte length prefix.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            PeerMessage::KeepAlive => {
                out.extend_from_slice(&0u32.to_be_bytes());
            }
            PeerMessage::Choke => {
                out.extend_from_slice(&1u32.to_be_bytes());
                out.push(ID_CHOKE);
            }
            PeerMessage::Unchoke => {
                out.extend_from_slice(&1u32.to_be_bytes());
                out.push(ID_UNCHOKE);
            }
            PeerMessage::Interested => {
                out.extend_from_slice(&1u32.to_be_bytes());
                out.push(ID_INTERESTED);
            }
            PeerMessage::NotInterested => {
                out.extend_from_slice(&1u32.to_be_bytes());
                out.push(ID_NOT_INTERESTED);
            }
            PeerMessage::Have { piece_index } => {
                out.extend_from_slice(&5u32.to_be_bytes());
                out.push(ID_HAVE);
                out.extend_from_slice(&piece_index.to_be_bytes());
            }
            PeerMessage::Bitfield(data) => {
                let len = 1u32 + data.len() as u32;
                out.extend_from_slice(&len.to_be_bytes());
                out.push(ID_BITFIELD);
                out.extend_from_slice(data);
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                out.extend_from_slice(&13u32.to_be_bytes());
                out.push(ID_REQUEST);
                out.extend_from_slice(&index.to_be_bytes());
                out.extend_from_slice(&begin.to_be_bytes());
                out.extend_from_slice(&length.to_be_bytes());
            }
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => {
                let len = 9u32 + block.len() as u32;
                out.extend_from_slice(&len.to_be_bytes());
                out.push(ID_PIECE);
                out.extend_from_slice(&index.to_be_bytes());
                out.extend_from_slice(&begin.to_be_bytes());
                out.extend_from_slice(block);
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                out.extend_from_slice(&13u32.to_be_bytes());
                out.push(ID_CANCEL);
                out.extend_from_slice(&index.to_be_bytes());
                out.extend_from_slice(&begin.to_be_bytes());
                out.extend_from_slice(&length.to_be_bytes());
            }
            PeerMessage::SuggestPiece { piece_index } => {
                out.extend_from_slice(&5u32.to_be_bytes());
                out.push(ID_SUGGEST_PIECE);
                out.extend_from_slice(&piece_index.to_be_bytes());
            }
            PeerMessage::HaveAll => {
                out.extend_from_slice(&1u32.to_be_bytes());
                out.push(ID_HAVE_ALL);
            }
            PeerMessage::HaveNone => {
                out.extend_from_slice(&1u32.to_be_bytes());
                out.push(ID_HAVE_NONE);
            }
            PeerMessage::RejectRequest {
                index,
                begin,
                length,
            } => {
                out.extend_from_slice(&13u32.to_be_bytes());
                out.push(ID_REJECT_REQUEST);
                out.extend_from_slice(&index.to_be_bytes());
                out.extend_from_slice(&begin.to_be_bytes());
                out.extend_from_slice(&length.to_be_bytes());
            }
            PeerMessage::AllowedFast { piece_index } => {
                out.extend_from_slice(&5u32.to_be_bytes());
                out.push(ID_ALLOWED_FAST);
                out.extend_from_slice(&piece_index.to_be_bytes());
            }
        }
        out
    }

    /// Decodes a framed message from a byte slice (including 4-byte length prefix).
    pub fn decode(bytes: &[u8]) -> Result<Self, TorrentError> {
        if bytes.len() < 4 {
            return Err(TorrentError::ProtocolError(
                "Message too short: missing 4-byte length prefix".into(),
            ));
        }

        let length = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if (bytes.len() - 4) as u32 != length {
            return Err(TorrentError::ProtocolError(format!(
                "Payload length mismatch: declared {length}, available {}",
                bytes.len() - 4
            )));
        }

        Self::decode_payload(length, &bytes[4..])
    }

    /// Decodes a message payload after reading the 4-byte length prefix.
    pub fn decode_payload(length: u32, payload: &[u8]) -> Result<Self, TorrentError> {
        if length > MAX_MESSAGE_LENGTH {
            return Err(TorrentError::ProtocolError(format!(
                "Message length {length} exceeds maximum allowed boundary {MAX_MESSAGE_LENGTH} bytes"
            )));
        }

        if length == 0 {
            return Ok(PeerMessage::KeepAlive);
        }

        if payload.len() != length as usize {
            return Err(TorrentError::ProtocolError(format!(
                "Payload slice length {} does not match declared length {length}",
                payload.len()
            )));
        }

        let msg_id = payload[0];
        let body = &payload[1..];

        match msg_id {
            ID_CHOKE => {
                if !body.is_empty() {
                    return Err(TorrentError::ProtocolError(
                        "Choke payload must be empty".into(),
                    ));
                }
                Ok(PeerMessage::Choke)
            }
            ID_UNCHOKE => {
                if !body.is_empty() {
                    return Err(TorrentError::ProtocolError(
                        "Unchoke payload must be empty".into(),
                    ));
                }
                Ok(PeerMessage::Unchoke)
            }
            ID_INTERESTED => {
                if !body.is_empty() {
                    return Err(TorrentError::ProtocolError(
                        "Interested payload must be empty".into(),
                    ));
                }
                Ok(PeerMessage::Interested)
            }
            ID_NOT_INTERESTED => {
                if !body.is_empty() {
                    return Err(TorrentError::ProtocolError(
                        "NotInterested payload must be empty".into(),
                    ));
                }
                Ok(PeerMessage::NotInterested)
            }
            ID_HAVE => {
                if body.len() != 4 {
                    return Err(TorrentError::ProtocolError(format!(
                        "Have payload must be 4 bytes, got {}",
                        body.len()
                    )));
                }
                let piece_index = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                Ok(PeerMessage::Have { piece_index })
            }
            ID_BITFIELD => Ok(PeerMessage::Bitfield(body.to_vec())),
            ID_REQUEST => {
                if body.len() != 12 {
                    return Err(TorrentError::ProtocolError(format!(
                        "Request payload must be 12 bytes, got {}",
                        body.len()
                    )));
                }
                let index = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                let begin = u32::from_be_bytes([body[4], body[5], body[6], body[7]]);
                let length = u32::from_be_bytes([body[8], body[9], body[10], body[11]]);
                Ok(PeerMessage::Request {
                    index,
                    begin,
                    length,
                })
            }
            ID_PIECE => {
                if body.len() < 8 {
                    return Err(TorrentError::ProtocolError(format!(
                        "Piece payload must be at least 8 bytes, got {}",
                        body.len()
                    )));
                }
                let index = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                let begin = u32::from_be_bytes([body[4], body[5], body[6], body[7]]);
                let block = body[8..].to_vec();
                Ok(PeerMessage::Piece {
                    index,
                    begin,
                    block,
                })
            }
            ID_CANCEL => {
                if body.len() != 12 {
                    return Err(TorrentError::ProtocolError(format!(
                        "Cancel payload must be 12 bytes, got {}",
                        body.len()
                    )));
                }
                let index = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                let begin = u32::from_be_bytes([body[4], body[5], body[6], body[7]]);
                let length = u32::from_be_bytes([body[8], body[9], body[10], body[11]]);
                Ok(PeerMessage::Cancel {
                    index,
                    begin,
                    length,
                })
            }
            ID_SUGGEST_PIECE => {
                if body.len() != 4 {
                    return Err(TorrentError::ProtocolError(
                        "SuggestPiece payload must be 4 bytes".into(),
                    ));
                }
                let piece_index = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                Ok(PeerMessage::SuggestPiece { piece_index })
            }
            ID_HAVE_ALL => {
                if !body.is_empty() {
                    return Err(TorrentError::ProtocolError(
                        "HaveAll payload must be empty".into(),
                    ));
                }
                Ok(PeerMessage::HaveAll)
            }
            ID_HAVE_NONE => {
                if !body.is_empty() {
                    return Err(TorrentError::ProtocolError(
                        "HaveNone payload must be empty".into(),
                    ));
                }
                Ok(PeerMessage::HaveNone)
            }
            ID_REJECT_REQUEST => {
                if body.len() != 12 {
                    return Err(TorrentError::ProtocolError(
                        "RejectRequest payload must be 12 bytes".into(),
                    ));
                }
                let index = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                let begin = u32::from_be_bytes([body[4], body[5], body[6], body[7]]);
                let length = u32::from_be_bytes([body[8], body[9], body[10], body[11]]);
                Ok(PeerMessage::RejectRequest {
                    index,
                    begin,
                    length,
                })
            }
            ID_ALLOWED_FAST => {
                if body.len() != 4 {
                    return Err(TorrentError::ProtocolError(
                        "AllowedFast payload must be 4 bytes".into(),
                    ));
                }
                let piece_index = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                Ok(PeerMessage::AllowedFast { piece_index })
            }
            other => Err(TorrentError::ProtocolError(format!(
                "Unknown message ID: {other}"
            ))),
        }
    }
}
