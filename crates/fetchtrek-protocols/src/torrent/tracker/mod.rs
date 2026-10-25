pub mod http;
pub mod udp;

use std::collections::HashSet;
use std::net::IpAddr;
use std::time::Duration;

pub use http::announce_http;
pub use udp::announce_udp;

use crate::torrent::error::TorrentError;

/// Announce event status flags sent to trackers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceEvent {
    None,
    Started,
    Stopped,
    Completed,
}

/// Parameters for initiating a tracker announce.
#[derive(Debug, Clone)]
pub struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: AnnounceEvent,
    pub num_want: Option<u32>,
}

/// A discovered BitTorrent peer network endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerEndpoint {
    pub ip: IpAddr,
    pub port: u16,
    pub peer_id: Option<[u8; 20]>,
}

/// Successful response metrics and peer list returned by a tracker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnounceResponse {
    pub interval: u32,
    pub min_interval: Option<u32>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub peers: Vec<PeerEndpoint>,
    pub warning: Option<String>,
}

/// Multi-tier tracker failover and re-announce coordinator (BEP 0012).
pub struct TrackerCoordinator {
    tiers: Vec<Vec<String>>,
    current_tier_idx: usize,
    current_tracker_idx: usize,
    http_client: reqwest::Client,
    timeout: Duration,
}

impl TrackerCoordinator {
    /// Creates a coordinator from primary announce URL and an announce-list.
    pub fn new(announce: Option<&str>, announce_list: &[String]) -> Self {
        let mut primary_tier = Vec::new();
        if let Some(ann) = announce {
            if !ann.is_empty() {
                primary_tier.push(ann.to_string());
            }
        }
        for item in announce_list {
            if !primary_tier.contains(item) {
                primary_tier.push(item.clone());
            }
        }

        let mut tiers = Vec::new();
        if !primary_tier.is_empty() {
            tiers.push(primary_tier);
        }

        Self {
            tiers,
            current_tier_idx: 0,
            current_tracker_idx: 0,
            http_client: reqwest::Client::builder().build().unwrap_or_default(),
            timeout: Duration::from_secs(5),
        }
    }

    /// Creates a coordinator with explicit tier-based hierarchy (BEP 0012).
    pub fn from_tiers(tiers: Vec<Vec<String>>) -> Self {
        Self {
            tiers,
            current_tier_idx: 0,
            current_tracker_idx: 0,
            http_client: reqwest::Client::builder().build().unwrap_or_default(),
            timeout: Duration::from_secs(5),
        }
    }

    /// Configures the announce network timeout per attempt.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Returns the current tier hierarchy.
    pub fn tiers(&self) -> &[Vec<String>] {
        &self.tiers
    }

    /// Returns the active tier index.
    pub fn current_tier(&self) -> usize {
        self.current_tier_idx
    }

    /// Returns the active tracker URL in the current tier, if available.
    pub fn current_tracker(&self) -> Option<&str> {
        self.tiers
            .get(self.current_tier_idx)
            .and_then(|t| t.get(self.current_tracker_idx))
            .map(|s| s.as_str())
    }

    /// Announces against the tier hierarchy, handling scheme dispatch, tier failover, and peer deduplication.
    pub async fn announce(
        &mut self,
        req: &AnnounceRequest,
    ) -> Result<AnnounceResponse, TorrentError> {
        if self.tiers.is_empty() {
            return Err(TorrentError::TrackerError("No trackers configured".into()));
        }

        let mut last_error = None;
        let num_tiers = self.tiers.len();

        for tier_offset in 0..num_tiers {
            let tier_idx = (self.current_tier_idx + tier_offset) % num_tiers;
            let tier_len = self.tiers[tier_idx].len();

            for tracker_offset in 0..tier_len {
                let tracker_idx = (self.current_tracker_idx + tracker_offset) % tier_len;
                let tracker_url = self.tiers[tier_idx][tracker_idx].clone();

                let announce_res = if tracker_url.starts_with("http://")
                    || tracker_url.starts_with("https://")
                {
                    http::announce_http(&self.http_client, &tracker_url, req, self.timeout).await
                } else if tracker_url.starts_with("udp://") {
                    udp::announce_udp(&tracker_url, req, self.timeout).await
                } else {
                    Err(TorrentError::UnsupportedTracker(format!(
                        "Unsupported tracker scheme: {tracker_url}"
                    )))
                };

                match announce_res {
                    Ok(mut response) => {
                        deduplicate_peers(&mut response.peers);

                        // On success, move responsive tracker to front of this tier (BEP 0012)
                        if tracker_idx != 0 {
                            let responsive = self.tiers[tier_idx].remove(tracker_idx);
                            self.tiers[tier_idx].insert(0, responsive);
                        }
                        self.current_tier_idx = tier_idx;
                        self.current_tracker_idx = 0;

                        return Ok(response);
                    }
                    Err(err) => {
                        last_error = Some(err);
                    }
                }
            }

            self.current_tracker_idx = 0;
        }

        Err(last_error.unwrap_or_else(|| {
            TorrentError::TrackerNetwork("All tracker tiers failed to announce".into())
        }))
    }
}

/// Deduplicates peer endpoints based on (IP, Port), preserving ordering.
pub fn deduplicate_peers(peers: &mut Vec<PeerEndpoint>) {
    let mut seen = HashSet::new();
    peers.retain(|p| seen.insert((p.ip, p.port)));
}
