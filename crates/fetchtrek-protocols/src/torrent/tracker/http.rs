use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use super::{AnnounceEvent, AnnounceRequest, AnnounceResponse, PeerEndpoint};
use crate::torrent::bencode::BencodeValue;
use crate::torrent::error::TorrentError;

/// Performs an HTTP or HTTPS tracker announce according to BEP 0003 and BEP 0023.
pub async fn announce_http(
    client: &reqwest::Client,
    tracker_url: &str,
    req: &AnnounceRequest,
    timeout_duration: Duration,
) -> Result<AnnounceResponse, TorrentError> {
    let url = build_announce_url(tracker_url, req);

    let res = client
        .get(&url)
        .timeout(timeout_duration)
        .send()
        .await
        .map_err(|e| TorrentError::TrackerNetwork(format!("HTTP tracker request error: {e}")))?;

    if !res.status().is_success() {
        return Err(TorrentError::TrackerNetwork(format!(
            "HTTP tracker returned status code: {}",
            res.status()
        )));
    }

    let body = res
        .bytes()
        .await
        .map_err(|e| TorrentError::TrackerNetwork(format!("Failed to read tracker body: {e}")))?;

    parse_http_tracker_response(&body)
}

/// Constructs the announce GET URL with raw binary percent-encoded info_hash and peer_id.
pub fn build_announce_url(tracker_url: &str, req: &AnnounceRequest) -> String {
    let separator = if tracker_url.contains('?') { '&' } else { '?' };
    let mut url = format!(
        "{}{}info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1&no_peer_id=1&numwant={}",
        tracker_url,
        separator,
        percent_encode_raw(&req.info_hash),
        percent_encode_raw(&req.peer_id),
        req.port,
        req.uploaded,
        req.downloaded,
        req.left,
        req.num_want.unwrap_or(50)
    );

    match req.event {
        AnnounceEvent::Started => url.push_str("&event=started"),
        AnnounceEvent::Stopped => url.push_str("&event=stopped"),
        AnnounceEvent::Completed => url.push_str("&event=completed"),
        AnnounceEvent::None => {}
    }

    url
}

/// Percent-encodes every byte as `%XX` to ensure trackers parse exact binary hash bytes.
pub fn percent_encode_raw(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 3);
    for &b in bytes {
        s.push('%');
        s.push_str(&format!("{b:02X}"));
    }
    s
}

/// Parses the bencoded HTTP tracker response payload.
pub fn parse_http_tracker_response(data: &[u8]) -> Result<AnnounceResponse, TorrentError> {
    let root = BencodeValue::decode(data)?;
    let dict = root.as_dict().ok_or_else(|| {
        TorrentError::TrackerError("HTTP tracker response must be a bencoded dictionary".into())
    })?;

    // Check for failure reason
    if let Some(reason_val) = dict.get(&b"failure reason"[..]) {
        let reason = reason_val
            .as_str()
            .unwrap_or("Unknown tracker failure reason");
        return Err(TorrentError::TrackerError(reason.to_string()));
    }

    let warning = dict
        .get(&b"warning message"[..])
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let interval = dict
        .get(&b"interval"[..])
        .and_then(|v| v.as_int())
        .map(|n| n as u32)
        .ok_or_else(|| {
            TorrentError::TrackerError("Missing 'interval' in tracker response".into())
        })?;

    let min_interval = dict
        .get(&b"min interval"[..])
        .and_then(|v| v.as_int())
        .map(|n| n as u32);

    let seeders = dict
        .get(&b"complete"[..])
        .and_then(|v| v.as_int())
        .map(|n| n as u32);

    let leechers = dict
        .get(&b"incomplete"[..])
        .and_then(|v| v.as_int())
        .map(|n| n as u32);

    let mut peers = Vec::new();

    if let Some(peers_val) = dict.get(&b"peers"[..]) {
        match peers_val {
            BencodeValue::Bytes(b) => {
                // BEP 0023 compact binary format: 6 bytes per IPv4 peer
                if !b.len().is_multiple_of(6) {
                    return Err(TorrentError::TrackerError(format!(
                        "Compact peers length ({}) must be a multiple of 6",
                        b.len()
                    )));
                }

                for chunk in b.as_chunks::<6>().0 {
                    let ip = IpAddr::V4(Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]));
                    let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                    peers.push(PeerEndpoint {
                        ip,
                        port,
                        peer_id: None,
                    });
                }
            }
            BencodeValue::List(list) => {
                // BEP 0003 legacy dictionary format
                for item in list {
                    if let Some(peer_dict) = item.as_dict() {
                        let ip_str = peer_dict
                            .get(&b"ip"[..])
                            .and_then(|v| v.as_str())
                            .ok_or_else(|| {
                                TorrentError::TrackerError("Legacy peer dict missing 'ip'".into())
                            })?;

                        let ip: IpAddr = ip_str.parse().map_err(|_| {
                            TorrentError::TrackerError(format!("Invalid IP address '{ip_str}'"))
                        })?;

                        let port = peer_dict
                            .get(&b"port"[..])
                            .and_then(|v| v.as_int())
                            .map(|n| n as u16)
                            .ok_or_else(|| {
                                TorrentError::TrackerError("Legacy peer dict missing 'port'".into())
                            })?;

                        let peer_id = peer_dict
                            .get(&b"peer id"[..])
                            .and_then(|v| v.as_bytes())
                            .and_then(|b| {
                                if b.len() == 20 {
                                    let mut id = [0u8; 20];
                                    id.copy_from_slice(b);
                                    Some(id)
                                } else {
                                    None
                                }
                            });

                        peers.push(PeerEndpoint { ip, port, peer_id });
                    }
                }
            }
            _ => {
                return Err(TorrentError::TrackerError(
                    "Unexpected type for 'peers' in tracker response".into(),
                ))
            }
        }
    }

    Ok(AnnounceResponse {
        interval,
        min_interval,
        seeders,
        leechers,
        peers,
        warning,
    })
}
