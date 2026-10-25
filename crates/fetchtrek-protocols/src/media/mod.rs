pub mod dash;
pub mod hls;
pub mod types;

use fetchtrek_types::MediaError;

pub use dash::{parse_dash, parse_dash_media, parse_iso8601_duration};
pub use hls::parse_hls;
pub use types::*;

pub fn parse_media_manifest(content: &str, base_url: &str) -> Result<MediaManifest, MediaError> {
    let trimmed = content.trim_start().trim_start_matches('\u{FEFF}');
    if trimmed.starts_with("#EXTM3U") {
        parse_hls(content, base_url)
    } else if trimmed.contains("<MPD") || trimmed.contains("<mpd") {
        parse_dash(content, base_url)
    } else {
        Err(MediaError::MalformedManifest)
    }
}

pub fn resolve_url(base_url: &str, relative: &str) -> String {
    let rel_trimmed = relative.trim();
    if rel_trimmed.starts_with("http://") || rel_trimmed.starts_with("https://") {
        return rel_trimmed.to_string();
    }

    if rel_trimmed.starts_with('#') {
        let base_clean = base_url.split('#').next().unwrap_or(base_url);
        return format!("{base_clean}{rel_trimmed}");
    }

    if rel_trimmed.starts_with('/') {
        if let Some(pos) = base_url.find("://") {
            let after_scheme = &base_url[pos + 3..];
            let host_end = after_scheme
                .find('/')
                .map(|p| pos + 3 + p)
                .unwrap_or(base_url.len());
            let host = &base_url[..host_end];
            return format!("{host}{rel_trimmed}");
        }
        return rel_trimmed.to_string();
    }

    let base_clean = base_url.split('?').next().unwrap_or(base_url);
    let base_clean = base_clean.split('#').next().unwrap_or(base_clean);

    let base_dir = if let Some(pos) = base_clean.find("://") {
        let after_scheme = &base_clean[pos + 3..];
        match after_scheme.rfind('/') {
            Some(slash_pos) => &base_clean[..pos + 3 + slash_pos + 1],
            None => return format!("{base_clean}/{rel_trimmed}"),
        }
    } else {
        match base_clean.rfind('/') {
            Some(slash_pos) => &base_clean[..slash_pos + 1],
            None => return format!("{base_clean}/{rel_trimmed}"),
        }
    };

    normalize_path_join(base_dir, rel_trimmed)
}

fn normalize_path_join(base_dir: &str, relative: &str) -> String {
    let (prefix, path) = if let Some(pos) = base_dir.find("://") {
        let after_scheme = &base_dir[pos + 3..];
        if let Some(slash_idx) = after_scheme.find('/') {
            let split_idx = pos + 3 + slash_idx;
            (&base_dir[..split_idx], &base_dir[split_idx..])
        } else {
            (base_dir, "/")
        }
    } else {
        ("", base_dir)
    };

    let mut segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    for part in relative.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            s => {
                segments.push(s);
            }
        }
    }

    if prefix.is_empty() {
        format!("/{}", segments.join("/"))
    } else {
        format!("{}/{}", prefix, segments.join("/"))
    }
}
