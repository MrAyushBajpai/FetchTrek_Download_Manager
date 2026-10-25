use std::path::PathBuf;

use super::bencode::{extract_raw_dict_value, BencodeValue};
use super::error::TorrentError;
use super::sha1::sha1;

/// Represents an individual file within a torrent payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentFile {
    pub length: u64,
    pub path: PathBuf,
}

/// Validates that a path component (root name or sub-path element) is a safe relative name.
pub fn validate_torrent_path_component(comp: &str) -> Result<(), TorrentError> {
    if comp.is_empty() {
        return Err(TorrentError::InvalidMetainfo(
            "Torrent path component cannot be empty".into(),
        ));
    }
    if comp == "." || comp == ".." || comp.contains("..") {
        return Err(TorrentError::InvalidMetainfo(format!(
            "Torrent path component cannot contain traversal sequences: '{comp}'"
        )));
    }
    if comp.contains('/') || comp.contains('\\') {
        return Err(TorrentError::InvalidMetainfo(format!(
            "Torrent path component cannot contain directory separators: '{comp}'"
        )));
    }
    if comp.contains(':') {
        return Err(TorrentError::InvalidMetainfo(format!(
            "Torrent path component cannot contain Windows drive designators: '{comp}'"
        )));
    }
    if comp.chars().any(|c| c.is_control() || c == '\0') {
        return Err(TorrentError::InvalidMetainfo(format!(
            "Torrent path component cannot contain control characters: '{comp}'"
        )));
    }
    Ok(())
}

/// Structured BEP 0003 Metainfo model parsed from a `.torrent` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentMeta {
    pub info_hash: [u8; 20],
    pub piece_length: u32,
    pub pieces: Vec<[u8; 20]>,
    pub name: String,
    pub files: Vec<TorrentFile>,
    pub total_length: u64,
    pub announce: Option<String>,
    pub announce_list: Vec<String>,
    pub is_private: bool,
}

impl TorrentMeta {
    /// Parses a `.torrent` file byte slice into `TorrentMeta`.
    /// Computes `info_hash` directly from the raw byte slice of the `info` dictionary.
    pub fn from_bytes(data: &[u8]) -> Result<Self, TorrentError> {
        let root = BencodeValue::decode(data)?;
        let root_dict = root.as_dict().ok_or_else(|| {
            TorrentError::InvalidMetainfo("Torrent metainfo root must be a dictionary".into())
        })?;

        // 1. Slice raw info dictionary bytes and compute SHA-1 infohash
        let raw_info_bytes = extract_raw_dict_value(data, b"info")?;
        let info_hash = sha1(raw_info_bytes);

        // 2. Parse parsed info dict AST
        let info_val = root_dict.get(&b"info"[..]).ok_or_else(|| {
            TorrentError::InvalidMetainfo("Missing mandatory 'info' dictionary".into())
        })?;
        let info = info_val.as_dict().ok_or_else(|| {
            TorrentError::InvalidMetainfo("'info' value must be a dictionary".into())
        })?;

        // 3. Name (filename for single-file, root directory for multi-file)
        let name_bytes = info
            .get(&b"name"[..])
            .and_then(|v| v.as_bytes())
            .ok_or_else(|| {
                TorrentError::InvalidMetainfo("Missing mandatory 'name' in info dict".into())
            })?;
        let name = String::from_utf8(name_bytes.to_vec()).map_err(|_| {
            TorrentError::InvalidMetainfo("'name' field contains invalid UTF-8".into())
        })?;
        validate_torrent_path_component(&name)?;

        // 4. Piece length
        let piece_length_int = info
            .get(&b"piece length"[..])
            .and_then(|v| v.as_int())
            .ok_or_else(|| {
                TorrentError::InvalidMetainfo("Missing mandatory 'piece length'".into())
            })?;
        if piece_length_int <= 0 {
            return Err(TorrentError::InvalidMetainfo(
                "'piece length' must be a positive integer".into(),
            ));
        }
        let piece_length = piece_length_int as u32;

        // 5. Pieces byte string (exact multiple of 20)
        let pieces_raw = info
            .get(&b"pieces"[..])
            .and_then(|v| v.as_bytes())
            .ok_or_else(|| TorrentError::InvalidMetainfo("Missing mandatory 'pieces'".into()))?;

        if !pieces_raw.len().is_multiple_of(20) {
            return Err(TorrentError::InvalidMetainfo(format!(
                "Pieces length ({}) must be an exact multiple of 20",
                pieces_raw.len()
            )));
        }

        let pieces: Vec<[u8; 20]> = pieces_raw.as_chunks::<20>().0.to_vec();

        // 6. Private flag
        let is_private = info
            .get(&b"private"[..])
            .and_then(|v| v.as_int())
            .map(|val| val == 1)
            .unwrap_or(false);

        // 7. Files and total length
        let (files, total_length) = if let Some(len_val) = info.get(&b"length"[..]) {
            // Single-file torrent
            let len = len_val.as_int().ok_or_else(|| {
                TorrentError::InvalidMetainfo("Single-file 'length' must be an integer".into())
            })?;
            if len < 0 {
                return Err(TorrentError::InvalidMetainfo(
                    "File length cannot be negative".into(),
                ));
            }
            let ulen = len as u64;
            let file = TorrentFile {
                length: ulen,
                path: PathBuf::from(&name),
            };
            (vec![file], ulen)
        } else if let Some(files_val) = info.get(&b"files"[..]) {
            // Multi-file torrent
            let files_list = files_val.as_list().ok_or_else(|| {
                TorrentError::InvalidMetainfo("Multi-file 'files' must be a list".into())
            })?;
            if files_list.is_empty() {
                return Err(TorrentError::InvalidMetainfo(
                    "Multi-file 'files' list cannot be empty".into(),
                ));
            }

            let mut files = Vec::with_capacity(files_list.len());
            let mut sum_length = 0u64;

            for (idx, item) in files_list.iter().enumerate() {
                let file_dict = item.as_dict().ok_or_else(|| {
                    TorrentError::InvalidMetainfo(format!("File #{idx} must be a dictionary"))
                })?;

                let length_int = file_dict
                    .get(&b"length"[..])
                    .and_then(|v| v.as_int())
                    .ok_or_else(|| {
                        TorrentError::InvalidMetainfo(format!("File #{idx} missing length"))
                    })?;
                if length_int < 0 {
                    return Err(TorrentError::InvalidMetainfo(format!(
                        "File #{idx} length cannot be negative"
                    )));
                }
                let file_len = length_int as u64;
                sum_length = sum_length.checked_add(file_len).ok_or_else(|| {
                    TorrentError::InvalidMetainfo("Total file length overflow".into())
                })?;

                let path_list = file_dict
                    .get(&b"path"[..])
                    .and_then(|v| v.as_list())
                    .ok_or_else(|| {
                        TorrentError::InvalidMetainfo(format!("File #{idx} missing path list"))
                    })?;
                if path_list.is_empty() {
                    return Err(TorrentError::InvalidMetainfo(format!(
                        "File #{idx} path list cannot be empty"
                    )));
                }

                let mut full_path = PathBuf::from(&name);
                for path_part in path_list {
                    let part_bytes = path_part.as_bytes().ok_or_else(|| {
                        TorrentError::InvalidMetainfo(format!(
                            "File #{idx} path component must be byte string"
                        ))
                    })?;
                    let part_str = std::str::from_utf8(part_bytes).map_err(|_| {
                        TorrentError::InvalidMetainfo(format!(
                            "File #{idx} path component invalid UTF-8"
                        ))
                    })?;
                    validate_torrent_path_component(part_str)?;
                    full_path.push(part_str);
                }

                files.push(TorrentFile {
                    length: file_len,
                    path: full_path,
                });
            }

            (files, sum_length)
        } else {
            return Err(TorrentError::InvalidMetainfo(
                "Info dictionary must contain either 'length' or 'files'".into(),
            ));
        };

        // 8. Trackers: announce & announce-list (BEP 0012)
        let announce = root_dict
            .get(&b"announce"[..])
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let mut announce_list = Vec::new();
        if let Some(list_val) = root_dict
            .get(&b"announce-list"[..])
            .and_then(|v| v.as_list())
        {
            for tier in list_val {
                if let Some(tier_list) = tier.as_list() {
                    for tracker in tier_list {
                        if let Some(s) = tracker.as_str() {
                            let tracker_str = s.to_string();
                            if !announce_list.contains(&tracker_str) {
                                announce_list.push(tracker_str);
                            }
                        }
                    }
                }
            }
        }

        // If announce is set but not in announce_list, prepend or append it
        if let Some(ref primary) = announce {
            if !announce_list.contains(primary) {
                announce_list.insert(0, primary.clone());
            }
        }

        Ok(TorrentMeta {
            info_hash,
            piece_length,
            pieces,
            name,
            files,
            total_length,
            announce,
            announce_list,
            is_private,
        })
    }
}
