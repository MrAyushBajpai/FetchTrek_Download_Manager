use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::torrent::error::TorrentError;
use crate::torrent::peer::handshake::PeerHandshake;
use crate::torrent::peer::message::{PeerMessage, MAX_MESSAGE_LENGTH};

/// Connection state tracking choked and interested statuses for local and remote peers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerState {
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
}

impl Default for PeerState {
    fn default() -> Self {
        Self {
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
        }
    }
}

/// BitTorrent peer wire connection managing framing, state transitions, and bitfield tracking.
#[derive(Debug)]
pub struct PeerConnection {
    stream: TcpStream,
    pub state: PeerState,
    pub remote_peer_id: [u8; 20],
    pub remote_reserved: [u8; 8],
    pub bitfield: Option<Vec<u8>>,
    num_pieces: usize,
}

impl PeerConnection {
    /// Connects to a remote peer endpoint, exchanges 68-byte BEP 0003 handshakes, and verifies info_hash.
    pub async fn connect(
        addr: SocketAddr,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        num_pieces: usize,
        timeout_duration: Duration,
    ) -> Result<Self, TorrentError> {
        let handshake_exchange = async {
            let mut stream = TcpStream::connect(addr).await?;

            let local_handshake = PeerHandshake::new(info_hash, peer_id);
            local_handshake.write_to(&mut stream).await?;

            let remote_handshake = PeerHandshake::read_from(&mut stream, Some(&info_hash)).await?;

            Ok(Self {
                stream,
                state: PeerState::default(),
                remote_peer_id: remote_handshake.peer_id,
                remote_reserved: remote_handshake.reserved,
                bitfield: None,
                num_pieces,
            })
        };

        match timeout(timeout_duration, handshake_exchange).await {
            Ok(res) => res,
            Err(_) => Err(TorrentError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Peer connection or handshake to {addr} timed out"),
            ))),
        }
    }

    /// Constructs a `PeerConnection` from an already established `TcpStream` and remote handshake data.
    pub fn from_stream(
        stream: TcpStream,
        remote_peer_id: [u8; 20],
        remote_reserved: [u8; 8],
        num_pieces: usize,
    ) -> Self {
        Self {
            stream,
            state: PeerState::default(),
            remote_peer_id,
            remote_reserved,
            bitfield: None,
            num_pieces,
        }
    }

    /// Accesses the underlying TCP stream reference.
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }

    /// Accesses the underlying TCP stream mutable reference.
    pub fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
    }

    /// Consumes this connection and returns the underlying `TcpStream`.
    pub fn into_inner(self) -> TcpStream {
        self.stream
    }

    /// Total number of pieces expected in this torrent.
    pub fn num_pieces(&self) -> usize {
        self.num_pieces
    }

    /// Returns `true` if the remote peer supports BEP 0006 Fast Extension.
    pub fn supports_fast_extension(&self) -> bool {
        (self.remote_reserved[7] & 0x04) != 0
    }

    /// Sends a peer message with standard 4-byte framing and updates local state.
    pub async fn send(&mut self, msg: &PeerMessage) -> Result<(), TorrentError> {
        let bytes = msg.encode();
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;

        match msg {
            PeerMessage::Choke => self.state.am_choking = true,
            PeerMessage::Unchoke => self.state.am_choking = false,
            PeerMessage::Interested => self.state.am_interested = true,
            PeerMessage::NotInterested => self.state.am_interested = false,
            _ => {}
        }

        Ok(())
    }

    /// Receives and decodes the next peer message, updating peer state machine and bitfield.
    pub async fn recv(&mut self) -> Result<PeerMessage, TorrentError> {
        let mut len_bytes = [0u8; 4];
        match self.stream.read_exact(&mut len_bytes).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(TorrentError::ConnectionClosed);
            }
            Err(e) => return Err(TorrentError::Io(e)),
        }

        let length = u32::from_be_bytes(len_bytes);
        if length > MAX_MESSAGE_LENGTH {
            return Err(TorrentError::ProtocolError(format!(
                "Message length prefix {length} exceeds maximum allowed boundary {MAX_MESSAGE_LENGTH} bytes"
            )));
        }

        if length == 0 {
            return Ok(PeerMessage::KeepAlive);
        }

        let mut payload = vec![0u8; length as usize];
        match self.stream.read_exact(&mut payload).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(TorrentError::ConnectionClosed);
            }
            Err(e) => return Err(TorrentError::Io(e)),
        }

        let msg = PeerMessage::decode_payload(length, &payload)?;

        match &msg {
            PeerMessage::Choke => {
                self.state.peer_choking = true;
            }
            PeerMessage::Unchoke => {
                self.state.peer_choking = false;
            }
            PeerMessage::Interested => {
                self.state.peer_interested = true;
            }
            PeerMessage::NotInterested => {
                self.state.peer_interested = false;
            }
            PeerMessage::Have { piece_index } => {
                self.set_piece_have(*piece_index as usize);
            }
            PeerMessage::Bitfield(data) => {
                if self.num_pieces > 0 {
                    PeerMessage::validate_bitfield(data, self.num_pieces)?;
                }
                self.bitfield = Some(data.clone());
            }
            PeerMessage::HaveAll if self.num_pieces > 0 => {
                let byte_count = self.num_pieces.div_ceil(8);
                let mut bf = vec![0xFFu8; byte_count];
                let spare_bits = (byte_count * 8) - self.num_pieces;
                if spare_bits > 0 {
                    let mask = !((1 << spare_bits) - 1);
                    bf[byte_count - 1] &= mask;
                }
                self.bitfield = Some(bf);
            }
            PeerMessage::HaveNone if self.num_pieces > 0 => {
                let byte_count = self.num_pieces.div_ceil(8);
                self.bitfield = Some(vec![0u8; byte_count]);
            }
            _ => {}
        }

        Ok(msg)
    }

    /// Queries whether the peer has announced availability of the given piece index.
    pub fn has_piece(&self, piece_idx: usize) -> bool {
        if self.num_pieces > 0 && piece_idx >= self.num_pieces {
            return false;
        }

        match &self.bitfield {
            Some(bf) => {
                let byte_idx = piece_idx / 8;
                let bit_idx = 7 - (piece_idx % 8);
                if byte_idx < bf.len() {
                    (bf[byte_idx] & (1 << bit_idx)) != 0
                } else {
                    false
                }
            }
            None => false,
        }
    }

    /// Marks the specified piece as available in the peer's bitfield.
    pub fn set_piece_have(&mut self, piece_idx: usize) {
        if self.num_pieces > 0 && piece_idx >= self.num_pieces {
            return;
        }

        let byte_idx = piece_idx / 8;
        let bit_idx = 7 - (piece_idx % 8);

        let bf = self.bitfield.get_or_insert_with(|| {
            let len = if self.num_pieces > 0 {
                self.num_pieces.div_ceil(8)
            } else {
                byte_idx + 1
            };
            vec![0u8; len]
        });

        if byte_idx >= bf.len() {
            bf.resize(byte_idx + 1, 0);
        }

        bf[byte_idx] |= 1 << bit_idx;
    }
}
