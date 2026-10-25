use std::collections::HashMap;

use fetchtrek_types::{ByteRange, MediaError, Protocol};

use super::resolve_url;
use super::types::{
    AudioTrack, KeyMetadata, MasterPlaylist, MediaManifest, MediaPlaylist, MediaSegment,
    MediaVariant, MAX_MANIFEST_BYTES, MAX_MEDIA_SEGMENTS, MAX_MEDIA_VARIANTS,
};

pub fn parse_hls(content: &str, base_url: &str) -> Result<MediaManifest, MediaError> {
    if content.len() > MAX_MANIFEST_BYTES {
        return Err(MediaError::MalformedManifest);
    }

    let trimmed = content.trim_start().trim_start_matches('\u{FEFF}');
    if !trimmed.starts_with("#EXTM3U") {
        return Err(MediaError::MalformedManifest);
    }

    let is_master = trimmed.lines().any(|l| l.starts_with("#EXT-X-STREAM-INF:"));

    if is_master {
        parse_hls_master(trimmed, base_url)
    } else {
        parse_hls_media(trimmed, base_url)
    }
}

fn parse_hls_master(content: &str, base_url: &str) -> Result<MediaManifest, MediaError> {
    let mut variants = Vec::new();
    let mut audio_tracks = Vec::new();

    let mut lines = content.lines();

    while let Some(line) = lines.next() {
        let line = line.trim();
        if let Some(attr_str) = line.strip_prefix("#EXT-X-STREAM-INF:") {
            let attrs = parse_hls_attributes(attr_str);

            let bandwidth = attrs
                .get("BANDWIDTH")
                .and_then(|b| b.parse::<u64>().ok())
                .unwrap_or(0);

            let resolution = attrs.get("RESOLUTION").and_then(|r| {
                let parts: Vec<&str> = r.split(['x', 'X']).collect();
                if parts.len() == 2 {
                    let w = parts[0].trim().parse::<u32>().ok()?;
                    let h = parts[1].trim().parse::<u32>().ok()?;
                    Some((w, h))
                } else {
                    None
                }
            });

            let codecs = attrs.get("CODECS").cloned();

            // Next non-empty, non-comment line is the variant URL
            let mut variant_url = None;
            for next_line in lines.by_ref() {
                let next_trimmed = next_line.trim();
                if !next_trimmed.is_empty() && !next_trimmed.starts_with('#') {
                    variant_url = Some(resolve_url(base_url, next_trimmed));
                    break;
                }
            }

            if let Some(url) = variant_url {
                variants.push(MediaVariant {
                    bandwidth,
                    resolution,
                    codecs,
                    url,
                });
                if variants.len() > MAX_MEDIA_VARIANTS {
                    return Err(MediaError::MalformedManifest);
                }
            }
        } else if let Some(attr_str) = line.strip_prefix("#EXT-X-MEDIA:") {
            let attrs = parse_hls_attributes(attr_str);

            if attrs.get("TYPE").map(|s| s.as_str()) == Some("AUDIO") {
                let group_id = attrs.get("GROUP-ID").cloned().unwrap_or_default();
                let name = attrs.get("NAME").cloned().unwrap_or_default();
                let language = attrs.get("LANGUAGE").cloned();
                let url = attrs.get("URI").map(|u| resolve_url(base_url, u));

                audio_tracks.push(AudioTrack {
                    group_id,
                    name,
                    language,
                    url,
                });
                if audio_tracks.len() > MAX_MEDIA_VARIANTS {
                    return Err(MediaError::MalformedManifest);
                }
            }
        }
    }

    if variants.is_empty()
        || variants.len() > MAX_MEDIA_VARIANTS
        || audio_tracks.len() > MAX_MEDIA_VARIANTS
    {
        return Err(MediaError::MalformedManifest);
    }

    Ok(MediaManifest::Master(MasterPlaylist {
        variants,
        audio_tracks,
    }))
}

fn parse_hls_media(content: &str, base_url: &str) -> Result<MediaManifest, MediaError> {
    let mut target_duration_secs = 0.0f64;
    let mut is_live = true;
    let mut media_sequence = 0u64;
    let mut seq_counter = 0u64;
    let mut init_segment = None;
    let mut segments = Vec::new();
    let mut current_key = None;
    let mut pending_duration = None;
    let mut pending_byterange = None;
    let mut current_offset = 0u64;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(val) = trimmed.strip_prefix("#EXT-X-TARGETDURATION:") {
            if let Ok(d) = val.trim().parse::<f64>() {
                target_duration_secs = d;
            }
        } else if let Some(val) = trimmed.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
            if let Ok(s) = val.trim().parse::<u64>() {
                media_sequence = s;
                seq_counter = s;
            }
        } else if trimmed == "#EXT-X-ENDLIST" {
            is_live = false;
        } else if let Some(stripped) = trimmed.strip_prefix("#EXT-X-MAP:") {
            let attrs = parse_hls_attributes(stripped);
            if let Some(uri) = attrs.get("URI") {
                let init_url = resolve_url(base_url, uri);
                let byte_range = attrs.get("BYTERANGE").and_then(|br| {
                    let mut dummy = 0;
                    parse_byte_range(br, &mut dummy)
                });
                init_segment = Some(MediaSegment {
                    sequence: 0,
                    url: init_url,
                    duration_secs: 0.0,
                    byte_range,
                    key: current_key.clone(),
                });
            }
        } else if let Some(stripped) = trimmed.strip_prefix("#EXT-X-KEY:") {
            let attrs = parse_hls_attributes(stripped);
            let method = attrs.get("METHOD").cloned().unwrap_or_default();
            if method == "NONE" {
                current_key = None;
            } else {
                let key_url = attrs
                    .get("URI")
                    .map(|u| resolve_url(base_url, u))
                    .unwrap_or_default();
                let iv = attrs.get("IV").and_then(|i| parse_hex_iv(i));
                current_key = Some(KeyMetadata {
                    method,
                    url: key_url,
                    iv,
                });
            }
        } else if let Some(br_str) = trimmed.strip_prefix("#EXT-X-BYTERANGE:") {
            pending_byterange = parse_byte_range(br_str, &mut current_offset);
        } else if let Some(stripped) = trimmed.strip_prefix("#EXTINF:") {
            let dur_part = stripped.split(',').next().unwrap_or("").trim();
            if let Ok(d) = dur_part.parse::<f64>() {
                pending_duration = Some(d);
            }
        } else if !trimmed.starts_with('#') {
            if segments.len() >= MAX_MEDIA_SEGMENTS {
                return Err(MediaError::MalformedManifest);
            }
            let seg_url = resolve_url(base_url, trimmed);
            let duration_secs = pending_duration.take().unwrap_or(target_duration_secs);
            let byte_range = pending_byterange.take();

            segments.push(MediaSegment {
                sequence: seq_counter,
                url: seg_url,
                duration_secs,
                byte_range,
                key: current_key.clone(),
            });
            seq_counter += 1;
        }
    }

    if segments.is_empty() && init_segment.is_none() && target_duration_secs == 0.0 {
        return Err(MediaError::MalformedManifest);
    }

    Ok(MediaManifest::Media(MediaPlaylist {
        protocol: Protocol::Hls,
        target_duration_secs,
        is_live,
        media_sequence,
        init_segment,
        segments,
    }))
}

pub fn parse_hls_attributes(attr_str: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut chars = attr_str.chars().peekable();

    while chars.peek().is_some() {
        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            chars.next();
            if c == '=' {
                break;
            }
            if !c.is_whitespace() {
                key.push(c);
            }
        }
        if key.is_empty() {
            break;
        }

        let mut val = String::new();
        let mut in_quotes = false;
        while let Some(&c) = chars.peek() {
            if c == '"' {
                in_quotes = !in_quotes;
                chars.next();
                continue;
            }
            if c == ',' && !in_quotes {
                chars.next();
                break;
            }
            chars.next();
            val.push(c);
        }
        map.insert(key, val.trim().to_string());
    }
    map
}

pub fn parse_byte_range(val: &str, current_offset: &mut u64) -> Option<ByteRange> {
    let parts: Vec<&str> = val.split('@').collect();
    let length: u64 = parts[0].trim().parse().ok()?;
    if length == 0 {
        return None;
    }

    let offset = if parts.len() > 1 {
        let o: u64 = parts[1].trim().parse().ok()?;
        *current_offset = o;
        o
    } else {
        *current_offset
    };

    *current_offset += length;
    Some(ByteRange::new(offset, offset + length - 1))
}

pub fn parse_hex_iv(iv_str: &str) -> Option<[u8; 16]> {
    let s = iv_str
        .strip_prefix("0x")
        .or_else(|| iv_str.strip_prefix("0X"))
        .unwrap_or(iv_str)
        .trim();

    if s.len() != 32 {
        return None;
    }

    let mut iv = [0u8; 16];
    #[allow(clippy::chunks_exact_to_as_chunks)]
    for (i, chunk) in s.as_bytes().chunks_exact(2).enumerate() {
        let hex_str = std::str::from_utf8(chunk).ok()?;
        iv[i] = u8::from_str_radix(hex_str, 16).ok()?;
    }
    Some(iv)
}
