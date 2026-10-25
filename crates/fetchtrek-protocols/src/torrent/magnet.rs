use super::error::TorrentError;

/// Structured Magnet link representation specified by BEP 0009 and BEP 0053.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MagnetLink {
    pub info_hash: [u8; 20],
    pub display_name: Option<String>,
    pub trackers: Vec<String>,
    pub web_seeds: Vec<String>,
}

impl MagnetLink {
    /// Parses a magnet URI string (e.g. `magnet:?xt=urn:btih:...`).
    /// Supports both 40-character Hexadecimal and 32-character Base32 infohashes.
    pub fn parse(uri: &str) -> Result<Self, TorrentError> {
        let query = uri.strip_prefix("magnet:?").ok_or_else(|| {
            TorrentError::InvalidMagnet("Magnet URI must start with 'magnet:?' scheme".into())
        })?;

        let mut info_hash = None;
        let mut display_name = None;
        let mut trackers = Vec::new();
        let mut web_seeds = Vec::new();

        for part in query.split('&') {
            if part.is_empty() {
                continue;
            }

            let (key, raw_val) = match part.split_once('=') {
                Some((k, v)) => (k, v),
                None => continue,
            };

            match key {
                "xt" => {
                    let decoded_val = percent_decode(raw_val)?;
                    if let Some(hash_str) = decoded_val.strip_prefix("urn:btih:") {
                        let hash = parse_info_hash(hash_str)?;
                        info_hash = Some(hash);
                    }
                }
                "dn" => {
                    display_name = Some(percent_decode(raw_val)?);
                }
                "tr" => {
                    let tracker = percent_decode(raw_val)?;
                    if !tracker.is_empty() && !trackers.contains(&tracker) {
                        trackers.push(tracker);
                    }
                }
                "ws" => {
                    let ws = percent_decode(raw_val)?;
                    if !ws.is_empty() && !web_seeds.contains(&ws) {
                        web_seeds.push(ws);
                    }
                }
                _ => {}
            }
        }

        let info_hash = info_hash.ok_or_else(|| {
            TorrentError::InvalidMagnet("Missing mandatory 'xt=urn:btih:' parameter".into())
        })?;

        Ok(MagnetLink {
            info_hash,
            display_name,
            trackers,
            web_seeds,
        })
    }

    /// Converts this `MagnetLink` into a canonical magnet URI string.
    pub fn to_uri(&self) -> String {
        let mut uri = format!("magnet:?xt=urn:btih:{}", to_hex(&self.info_hash));
        if let Some(ref dn) = self.display_name {
            uri.push_str("&dn=");
            uri.push_str(&percent_encode(dn));
        }
        for tr in &self.trackers {
            uri.push_str("&tr=");
            uri.push_str(&percent_encode(tr));
        }
        for ws in &self.web_seeds {
            uri.push_str("&ws=");
            uri.push_str(&percent_encode(ws));
        }
        uri
    }
}

/// Decodes either 40-character Hex or 32-character Base32 into 20-byte infohash.
fn parse_info_hash(s: &str) -> Result<[u8; 20], TorrentError> {
    if s.len() == 40 {
        decode_hex(s)
    } else if s.len() == 32 {
        decode_base32(s)
    } else {
        Err(TorrentError::InvalidMagnet(format!(
            "Infohash must be 40 hex digits or 32 base32 characters, got length {}",
            s.len()
        )))
    }
}

/// Decodes 40 hexadecimal characters into a 20-byte digest.
fn decode_hex(s: &str) -> Result<[u8; 20], TorrentError> {
    if s.len() != 40 {
        return Err(TorrentError::InvalidMagnet(format!(
            "Hex infohash must be 40 characters, found {}",
            s.len()
        )));
    }
    let mut out = [0u8; 20];
    let bytes = s.as_bytes();
    for i in 0..20 {
        let h = match (bytes[2 * i] as char).to_digit(16) {
            Some(v) => v as u8,
            None => {
                return Err(TorrentError::InvalidMagnet(format!(
                    "Invalid hex digit '{}'",
                    bytes[2 * i] as char
                )))
            }
        };
        let l = match (bytes[2 * i + 1] as char).to_digit(16) {
            Some(v) => v as u8,
            None => {
                return Err(TorrentError::InvalidMagnet(format!(
                    "Invalid hex digit '{}'",
                    bytes[2 * i + 1] as char
                )))
            }
        };
        out[i] = (h << 4) | l;
    }
    Ok(out)
}

/// Decodes 32 RFC 4648 Base32 characters into a 20-byte digest.
pub fn decode_base32(s: &str) -> Result<[u8; 20], TorrentError> {
    if s.len() != 32 {
        return Err(TorrentError::InvalidMagnet(format!(
            "Base32 infohash must be 32 characters, found {}",
            s.len()
        )));
    }
    let mut out = [0u8; 20];
    let bytes = s.as_bytes();
    for (chunk_idx, chunk) in bytes.as_chunks::<8>().0.iter().enumerate() {
        let mut val = 0u64;
        for (i, &b) in chunk.iter().enumerate() {
            let digit = match b {
                b'A'..=b'Z' => (b - b'A') as u64,
                b'a'..=b'z' => (b - b'a') as u64,
                b'2'..=b'7' => (b - b'2' + 26) as u64,
                other => {
                    return Err(TorrentError::InvalidMagnet(format!(
                        "Invalid character in base32 infohash: '{}'",
                        other as char
                    )));
                }
            };
            val |= digit << (35 - 5 * i);
        }
        let out_offset = chunk_idx * 5;
        out[out_offset] = (val >> 32) as u8;
        out[out_offset + 1] = (val >> 24) as u8;
        out[out_offset + 2] = (val >> 16) as u8;
        out[out_offset + 3] = (val >> 8) as u8;
        out[out_offset + 4] = val as u8;
    }
    Ok(out)
}

/// Encodes 20-byte digest into 32 RFC 4648 Base32 characters.
pub fn to_base32(bytes: &[u8; 20]) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut s = String::with_capacity(32);
    for chunk in bytes.as_chunks::<5>().0 {
        let val = ((chunk[0] as u64) << 32)
            | ((chunk[1] as u64) << 24)
            | ((chunk[2] as u64) << 16)
            | ((chunk[3] as u64) << 8)
            | (chunk[4] as u64);
        s.push(ALPHABET[((val >> 35) & 0x1F) as usize] as char);
        s.push(ALPHABET[((val >> 30) & 0x1F) as usize] as char);
        s.push(ALPHABET[((val >> 25) & 0x1F) as usize] as char);
        s.push(ALPHABET[((val >> 20) & 0x1F) as usize] as char);
        s.push(ALPHABET[((val >> 15) & 0x1F) as usize] as char);
        s.push(ALPHABET[((val >> 10) & 0x1F) as usize] as char);
        s.push(ALPHABET[((val >> 5) & 0x1F) as usize] as char);
        s.push(ALPHABET[(val & 0x1F) as usize] as char);
    }
    s
}

/// Decodes percent-encoded string.
fn percent_decode(input: &str) -> Result<String, TorrentError> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(TorrentError::InvalidMagnet(
                    "Incomplete percent-encoding sequence".into(),
                ));
            }
            let h = match (bytes[i + 1] as char).to_digit(16) {
                Some(v) => v as u8,
                None => {
                    return Err(TorrentError::InvalidMagnet(
                        "Invalid percent-encoded hex character".into(),
                    ))
                }
            };
            let l = match (bytes[i + 2] as char).to_digit(16) {
                Some(v) => v as u8,
                None => {
                    return Err(TorrentError::InvalidMagnet(
                        "Invalid percent-encoded hex character".into(),
                    ))
                }
            };
            out.push((h << 4) | l);
            i += 3;
        } else if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| {
        TorrentError::InvalidMagnet("Percent-decoded parameter is not valid UTF-8".into())
    })
}

/// Encodes special characters in URI parameters.
fn percent_encode(s: &str) -> String {
    let mut res = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
            res.push(b as char);
        } else {
            res.push_str(&format!("%{b:02X}"));
        }
    }
    res
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
