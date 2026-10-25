use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::torrent::error::TorrentError;

pub const PSTR_V1: &[u8; 19] = b"BitTorrent protocol";
pub const HANDSHAKE_LEN: usize = 68;

/// BEP 0003 binary 68-byte peer wire handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerHandshake {
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl PeerHandshake {
    /// Creates a new handshake with given infohash and peer ID (reserved bytes enabled for Fast Extension).
    pub fn new(info_hash: [u8; 20], peer_id: [u8; 20]) -> Self {
        let mut reserved = [0u8; 8];
        // BEP 0006: Bit 0x04 in byte 7 signals Fast Extension support
        reserved[7] = 0x04;
        Self {
            reserved,
            info_hash,
            peer_id,
        }
    }

    /// Sets explicit 8-byte reserved bitfield.
    pub fn with_reserved(mut self, reserved: [u8; 8]) -> Self {
        self.reserved = reserved;
        self
    }

    /// Returns `true` if peer indicates BEP 0006 Fast Extension support.
    pub fn supports_fast_extension(&self) -> bool {
        (self.reserved[7] & 0x04) != 0
    }

    /// Encodes handshake into exact 68-byte wire representation.
    pub fn encode(&self) -> [u8; HANDSHAKE_LEN] {
        let mut buf = [0u8; HANDSHAKE_LEN];
        buf[0] = 19; // pstrlen
        buf[1..20].copy_from_slice(PSTR_V1);
        buf[20..28].copy_from_slice(&self.reserved);
        buf[28..48].copy_from_slice(&self.info_hash);
        buf[48..68].copy_from_slice(&self.peer_id);
        buf
    }

    /// Decodes a 68-byte wire payload into `PeerHandshake`.
    pub fn decode(bytes: &[u8]) -> Result<Self, TorrentError> {
        if bytes.len() != HANDSHAKE_LEN {
            return Err(TorrentError::HandshakeError(format!(
                "Expected 68 bytes for handshake, received {}",
                bytes.len()
            )));
        }

        if bytes[0] != 19 {
            return Err(TorrentError::HandshakeError(format!(
                "Invalid protocol string length: expected 19, got {}",
                bytes[0]
            )));
        }

        if &bytes[1..20] != PSTR_V1 {
            return Err(TorrentError::HandshakeError(format!(
                "Invalid protocol string: {:?}",
                String::from_utf8_lossy(&bytes[1..20])
            )));
        }

        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&bytes[20..28]);

        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&bytes[28..48]);

        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&bytes[48..68]);

        Ok(Self {
            reserved,
            info_hash,
            peer_id,
        })
    }

    /// Writes the handshake to an async writer.
    pub async fn write_to<W: AsyncWriteExt + Unpin>(
        &self,
        writer: &mut W,
    ) -> Result<(), TorrentError> {
        let bytes = self.encode();
        writer.write_all(&bytes).await?;
        writer.flush().await?;
        Ok(())
    }

    /// Reads exactly 68 bytes from an async reader, decoding and verifying info_hash if requested.
    pub async fn read_from<R: AsyncReadExt + Unpin>(
        reader: &mut R,
        expected_info_hash: Option<&[u8; 20]>,
    ) -> Result<Self, TorrentError> {
        let mut buf = [0u8; HANDSHAKE_LEN];
        reader.read_exact(&mut buf).await?;
        let handshake = Self::decode(&buf)?;

        if let Some(expected) = expected_info_hash {
            if &handshake.info_hash != expected {
                return Err(TorrentError::HandshakeError(format!(
                    "Mismatched info_hash: expected {:?}, got {:?}",
                    expected, handshake.info_hash
                )));
            }
        }

        Ok(handshake)
    }
}
