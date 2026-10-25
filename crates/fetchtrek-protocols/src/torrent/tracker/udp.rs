use std::net::{IpAddr, Ipv4Addr};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::net::UdpSocket;

use super::{AnnounceEvent, AnnounceRequest, AnnounceResponse, PeerEndpoint};
use crate::torrent::error::TorrentError;

const PROTOCOL_ID_CONNECT: u64 = 0x0417_2710_1980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_ERROR: u32 = 3;

/// Generates a pseudo-random 32-bit transaction ID using system time and process ID.
fn generate_random_u32() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id() as u128;
    let seed = (now ^ (pid << 32)) as u64;
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    ((z ^ (z >> 31)) & 0xFFFF_FFFF) as u32
}

/// Parses the host and port string from a `udp://host:port/...` tracker URL.
pub fn parse_udp_url(url: &str) -> Result<String, TorrentError> {
    let clean = url.strip_prefix("udp://").ok_or_else(|| {
        TorrentError::UnsupportedTracker(format!("Expected udp:// scheme, got '{url}'"))
    })?;
    let host_port = clean.split('/').next().unwrap_or(clean);
    Ok(host_port.to_string())
}

/// Performs a BEP 0015 binary UDP tracker announce over `tokio::net::UdpSocket`.
pub async fn announce_udp(
    tracker_url: &str,
    req: &AnnounceRequest,
    timeout_duration: Duration,
) -> Result<AnnounceResponse, TorrentError> {
    let host_port = parse_udp_url(tracker_url)?;

    let target_addr = tokio::time::timeout(timeout_duration, tokio::net::lookup_host(&host_port))
        .await
        .map_err(|_| {
            TorrentError::TrackerNetwork(format!("DNS resolution timed out for '{host_port}'"))
        })?
        .map_err(|e| {
            TorrentError::TrackerNetwork(format!("DNS resolution failed for '{host_port}': {e}"))
        })?
        .next()
        .ok_or_else(|| {
            TorrentError::TrackerNetwork(format!("No address resolved for '{host_port}'"))
        })?;

    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| TorrentError::TrackerNetwork(format!("Failed to bind UDP socket: {e}")))?;

    socket
        .connect(target_addr)
        .await
        .map_err(|e| TorrentError::TrackerNetwork(format!("Failed to connect UDP socket: {e}")))?;

    // Step 1: Connect Handshake
    let connect_tx_id = generate_random_u32();
    let mut connect_req = [0u8; 16];
    connect_req[0..8].copy_from_slice(&PROTOCOL_ID_CONNECT.to_be_bytes());
    connect_req[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    connect_req[12..16].copy_from_slice(&connect_tx_id.to_be_bytes());

    socket
        .send(&connect_req)
        .await
        .map_err(|e| TorrentError::TrackerNetwork(format!("Failed to send connect packet: {e}")))?;

    let mut buf = [0u8; 4096];
    let n = tokio::time::timeout(timeout_duration, socket.recv(&mut buf))
        .await
        .map_err(|_| TorrentError::TrackerNetwork("UDP connect handshake timed out".into()))?
        .map_err(|e| {
            TorrentError::TrackerNetwork(format!("Failed to receive connect response: {e}"))
        })?;

    if n < 16 {
        return Err(TorrentError::TrackerError(
            "Connect response packet too short (< 16 bytes)".into(),
        ));
    }

    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let tx_id = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);

    if action == ACTION_ERROR {
        let msg = String::from_utf8_lossy(&buf[8..n]);
        return Err(TorrentError::TrackerError(msg.to_string()));
    }

    if action != ACTION_CONNECT {
        return Err(TorrentError::TrackerError(format!(
            "Invalid action in connect response: {action}"
        )));
    }

    if tx_id != connect_tx_id {
        return Err(TorrentError::TrackerError(
            "Transaction ID mismatch in connect response".into(),
        ));
    }

    let connection_id = u64::from_be_bytes([
        buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
    ]);

    // Step 2: Announce Handshake
    let announce_tx_id = generate_random_u32();
    let event_code: u32 = match req.event {
        AnnounceEvent::None => 0,
        AnnounceEvent::Completed => 1,
        AnnounceEvent::Started => 2,
        AnnounceEvent::Stopped => 3,
    };
    let key = generate_random_u32();
    let num_want = req.num_want.map(|n| n as i32).unwrap_or(50);

    let mut announce_req = [0u8; 98];
    announce_req[0..8].copy_from_slice(&connection_id.to_be_bytes());
    announce_req[8..12].copy_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    announce_req[12..16].copy_from_slice(&announce_tx_id.to_be_bytes());
    announce_req[16..36].copy_from_slice(&req.info_hash);
    announce_req[36..56].copy_from_slice(&req.peer_id);
    announce_req[56..64].copy_from_slice(&req.downloaded.to_be_bytes());
    announce_req[64..72].copy_from_slice(&req.left.to_be_bytes());
    announce_req[72..80].copy_from_slice(&req.uploaded.to_be_bytes());
    announce_req[80..84].copy_from_slice(&event_code.to_be_bytes());
    announce_req[84..88].copy_from_slice(&0u32.to_be_bytes()); // ip_address: default 0
    announce_req[88..92].copy_from_slice(&key.to_be_bytes());
    announce_req[92..96].copy_from_slice(&num_want.to_be_bytes());
    announce_req[96..98].copy_from_slice(&req.port.to_be_bytes());

    socket.send(&announce_req).await.map_err(|e| {
        TorrentError::TrackerNetwork(format!("Failed to send announce packet: {e}"))
    })?;

    let n = tokio::time::timeout(timeout_duration, socket.recv(&mut buf))
        .await
        .map_err(|_| TorrentError::TrackerNetwork("UDP announce handshake timed out".into()))?
        .map_err(|e| {
            TorrentError::TrackerNetwork(format!("Failed to receive announce response: {e}"))
        })?;

    if n < 20 {
        return Err(TorrentError::TrackerError(
            "Announce response packet too short (< 20 bytes)".into(),
        ));
    }

    let action = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let tx_id = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);

    if action == ACTION_ERROR {
        let msg = String::from_utf8_lossy(&buf[8..n]);
        return Err(TorrentError::TrackerError(msg.to_string()));
    }

    if action != ACTION_ANNOUNCE {
        return Err(TorrentError::TrackerError(format!(
            "Invalid action in announce response: {action}"
        )));
    }

    if tx_id != announce_tx_id {
        return Err(TorrentError::TrackerError(
            "Transaction ID mismatch in announce response".into(),
        ));
    }

    let interval = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
    let leechers = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
    let seeders = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);

    let peer_bytes = &buf[20..n];
    if !peer_bytes.len().is_multiple_of(6) {
        return Err(TorrentError::TrackerError(
            "UDP peer payload is not a multiple of 6 bytes".into(),
        ));
    }

    let mut peers = Vec::new();
    for chunk in peer_bytes.as_chunks::<6>().0 {
        let ip = IpAddr::V4(Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]));
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        peers.push(PeerEndpoint {
            ip,
            port,
            peer_id: None,
        });
    }

    Ok(AnnounceResponse {
        interval,
        min_interval: None,
        seeders: Some(seeders),
        leechers: Some(leechers),
        peers,
        warning: None,
    })
}
