use quick_xml::events::Event;
use quick_xml::reader::Reader;

use fetchtrek_types::{MediaError, Protocol};

use super::resolve_url;
use super::types::{
    AudioTrack, MasterPlaylist, MediaManifest, MediaPlaylist, MediaSegment, MediaVariant,
};

#[derive(Debug, Clone)]
struct DashSegmentTemplate {
    initialization: Option<String>,
    media: Option<String>,
    start_number: u64,
    timescale: u64,
    duration: u64,
}

impl Default for DashSegmentTemplate {
    fn default() -> Self {
        Self {
            initialization: None,
            media: None,
            start_number: 1,
            timescale: 1,
            duration: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct DashRepresentation {
    id: String,
    bandwidth: u64,
    width: Option<u32>,
    height: Option<u32>,
    codecs: Option<String>,
    is_video: bool,
    lang: Option<String>,
    template: DashSegmentTemplate,
}

#[derive(Debug, Clone)]
struct DashModel {
    presentation_duration: Option<f64>,
    is_live: bool,
    representations: Vec<DashRepresentation>,
}

pub fn parse_dash(xml_content: &str, base_url: &str) -> Result<MediaManifest, MediaError> {
    let model = parse_dash_model(xml_content)?;

    // If base_url carries a fragment identifier (e.g. "https://example.com/manifest.mpd#1080p"),
    // expand that specific representation directly into a MediaPlaylist.
    if let Some((_, rep_id)) = base_url.split_once('#') {
        let rep = model
            .representations
            .iter()
            .find(|r| r.id == rep_id)
            .or_else(|| model.representations.first())
            .ok_or(MediaError::MalformedManifest)?;

        let playlist = expand_representation(
            rep,
            model.presentation_duration.unwrap_or(0.0),
            model.is_live,
            base_url,
        );
        return Ok(MediaManifest::Media(playlist));
    }

    let video_reps: Vec<&DashRepresentation> = model
        .representations
        .iter()
        .filter(|r| r.is_video)
        .collect();
    let audio_reps: Vec<&DashRepresentation> = model
        .representations
        .iter()
        .filter(|r| !r.is_video)
        .collect();

    // If multiple video representations or explicit audio tracks exist, present as MasterPlaylist.
    if video_reps.len() > 1 || !audio_reps.is_empty() {
        let variants = video_reps
            .iter()
            .map(|r| {
                let resolution = match (r.width, r.height) {
                    (Some(w), Some(h)) => Some((w, h)),
                    _ => None,
                };
                let base_clean = base_url.split('#').next().unwrap_or(base_url);
                MediaVariant {
                    bandwidth: r.bandwidth,
                    resolution,
                    codecs: r.codecs.clone(),
                    url: format!("{base_clean}#{}", r.id),
                }
            })
            .collect();

        let audio_tracks = audio_reps
            .iter()
            .map(|r| {
                let base_clean = base_url.split('#').next().unwrap_or(base_url);
                AudioTrack {
                    group_id: "audio".to_string(),
                    name: r.id.clone(),
                    language: r.lang.clone(),
                    url: Some(format!("{base_clean}#{}", r.id)),
                }
            })
            .collect();

        Ok(MediaManifest::Master(MasterPlaylist {
            variants,
            audio_tracks,
        }))
    } else if let Some(rep) = video_reps
        .first()
        .copied()
        .or_else(|| model.representations.first())
    {
        let playlist = expand_representation(
            rep,
            model.presentation_duration.unwrap_or(0.0),
            model.is_live,
            base_url,
        );
        Ok(MediaManifest::Media(playlist))
    } else {
        Err(MediaError::MalformedManifest)
    }
}

pub fn parse_dash_media(
    xml_content: &str,
    base_url: &str,
    representation_id: Option<&str>,
) -> Result<MediaPlaylist, MediaError> {
    let model = parse_dash_model(xml_content)?;

    let target_rep = match representation_id {
        Some(id) => model
            .representations
            .iter()
            .find(|r| r.id == id)
            .ok_or(MediaError::MalformedManifest)?,
        None => model
            .representations
            .iter()
            .filter(|r| r.is_video)
            .max_by_key(|r| r.bandwidth)
            .or_else(|| model.representations.first())
            .ok_or(MediaError::MalformedManifest)?,
    };

    Ok(expand_representation(
        target_rep,
        model.presentation_duration.unwrap_or(0.0),
        model.is_live,
        base_url,
    ))
}

fn parse_dash_model(xml_content: &str) -> Result<DashModel, MediaError> {
    let mut reader = Reader::from_str(xml_content);
    reader.config_mut().trim_text(true);

    let mut presentation_duration = None;
    let mut is_live = false;
    let mut representations = Vec::new();

    let mut current_is_video = true;
    let mut current_lang = None;
    let mut set_template = DashSegmentTemplate::default();
    let mut current_rep: Option<DashRepresentation> = None;

    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name().as_ref() {
                b"MPD" | b"mpd" => {
                    for attr in e.attributes().flatten() {
                        match attr.key.as_ref() {
                            b"mediaPresentationDuration" => {
                                if let Ok(val) = std::str::from_utf8(&attr.value) {
                                    presentation_duration = parse_iso8601_duration(val);
                                }
                            }
                            b"type" => {
                                if let Ok(val) = std::str::from_utf8(&attr.value) {
                                    is_live = val.eq_ignore_ascii_case("dynamic");
                                }
                            }
                            _ => {}
                        }
                    }
                }
                b"AdaptationSet" => {
                    current_is_video = true;
                    current_lang = None;
                    set_template = DashSegmentTemplate::default();

                    for attr in e.attributes().flatten() {
                        match attr.key.as_ref() {
                            b"contentType" => {
                                if let Ok(val) = std::str::from_utf8(&attr.value) {
                                    current_is_video = val.eq_ignore_ascii_case("video");
                                }
                            }
                            b"mimeType" => {
                                if let Ok(val) = std::str::from_utf8(&attr.value) {
                                    if val.contains("audio") {
                                        current_is_video = false;
                                    } else if val.contains("video") {
                                        current_is_video = true;
                                    }
                                }
                            }
                            b"lang" => {
                                if let Ok(val) = std::str::from_utf8(&attr.value) {
                                    current_lang = Some(val.to_string());
                                }
                            }
                            _ => {}
                        }
                    }
                }
                b"SegmentTemplate" => {
                    parse_segment_template_attrs(
                        e,
                        current_rep
                            .as_mut()
                            .map(|r| &mut r.template)
                            .unwrap_or(&mut set_template),
                    );
                }
                b"Representation" => {
                    let rep = parse_representation_element(
                        e,
                        current_is_video,
                        current_lang.clone(),
                        set_template.clone(),
                    );
                    current_rep = Some(rep);
                }
                _ => {}
            },
            Ok(Event::Empty(ref e)) => match e.name().as_ref() {
                b"SegmentTemplate" => {
                    parse_segment_template_attrs(
                        e,
                        current_rep
                            .as_mut()
                            .map(|r| &mut r.template)
                            .unwrap_or(&mut set_template),
                    );
                }
                b"Representation" => {
                    let rep = parse_representation_element(
                        e,
                        current_is_video,
                        current_lang.clone(),
                        set_template.clone(),
                    );
                    representations.push(rep);
                }
                _ => {}
            },
            Ok(Event::End(ref e)) => match e.name().as_ref() {
                b"Representation" => {
                    if let Some(rep) = current_rep.take() {
                        representations.push(rep);
                    }
                }
                b"AdaptationSet" => {
                    set_template = DashSegmentTemplate::default();
                    current_lang = None;
                }
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(_) => return Err(MediaError::MalformedManifest),
            _ => {}
        }
        buf.clear();
    }

    if representations.is_empty() {
        return Err(MediaError::MalformedManifest);
    }

    Ok(DashModel {
        presentation_duration,
        is_live,
        representations,
    })
}

fn parse_segment_template_attrs(
    e: &quick_xml::events::BytesStart<'_>,
    template: &mut DashSegmentTemplate,
) {
    for attr in e.attributes().flatten() {
        match attr.key.as_ref() {
            b"initialization" => {
                if let Ok(val) = std::str::from_utf8(&attr.value) {
                    template.initialization = Some(val.to_string());
                }
            }
            b"media" => {
                if let Ok(val) = std::str::from_utf8(&attr.value) {
                    template.media = Some(val.to_string());
                }
            }
            b"startNumber" => {
                if let Ok(val) = std::str::from_utf8(&attr.value) {
                    if let Ok(num) = val.parse::<u64>() {
                        template.start_number = num;
                    }
                }
            }
            b"timescale" => {
                if let Ok(val) = std::str::from_utf8(&attr.value) {
                    if let Ok(scale) = val.parse::<u64>() {
                        template.timescale = scale.max(1);
                    }
                }
            }
            b"duration" => {
                if let Ok(val) = std::str::from_utf8(&attr.value) {
                    if let Ok(dur) = val.parse::<u64>() {
                        template.duration = dur;
                    }
                }
            }
            _ => {}
        }
    }
}

fn parse_representation_element(
    e: &quick_xml::events::BytesStart<'_>,
    is_video: bool,
    lang: Option<String>,
    template: DashSegmentTemplate,
) -> DashRepresentation {
    let mut id = String::new();
    let mut bandwidth = 0u64;
    let mut width = None;
    let mut height = None;
    let mut codecs = None;

    for attr in e.attributes().flatten() {
        match attr.key.as_ref() {
            b"id" => {
                if let Ok(val) = std::str::from_utf8(&attr.value) {
                    id = val.to_string();
                }
            }
            b"bandwidth" => {
                if let Ok(val) = std::str::from_utf8(&attr.value) {
                    bandwidth = val.parse().unwrap_or(0);
                }
            }
            b"width" => {
                if let Ok(val) = std::str::from_utf8(&attr.value) {
                    width = val.parse().ok();
                }
            }
            b"height" => {
                if let Ok(val) = std::str::from_utf8(&attr.value) {
                    height = val.parse().ok();
                }
            }
            b"codecs" => {
                if let Ok(val) = std::str::from_utf8(&attr.value) {
                    codecs = Some(val.to_string());
                }
            }
            _ => {}
        }
    }

    DashRepresentation {
        id,
        bandwidth,
        width,
        height,
        codecs,
        is_video,
        lang,
        template,
    }
}

fn expand_representation(
    rep: &DashRepresentation,
    total_duration: f64,
    is_live: bool,
    base_url: &str,
) -> MediaPlaylist {
    let timescale = if rep.template.timescale > 0 {
        rep.template.timescale
    } else {
        1
    };
    let duration_units = rep.template.duration;
    let segment_duration_secs = if duration_units > 0 {
        (duration_units as f64) / (timescale as f64)
    } else {
        2.0
    };

    let target_duration_secs = segment_duration_secs;
    let start_number = if rep.template.start_number > 0 {
        rep.template.start_number
    } else {
        1
    };

    let num_segments = if segment_duration_secs > 0.0 && total_duration > 0.0 {
        (total_duration / segment_duration_secs).ceil() as u64
    } else {
        1
    };

    let init_segment = rep.template.initialization.as_ref().map(|init_tmpl| {
        let expanded = format_dash_template(init_tmpl, &rep.id, 0);
        let init_url = resolve_url(base_url, &expanded);
        MediaSegment {
            sequence: 0,
            url: init_url,
            duration_secs: 0.0,
            byte_range: None,
            key: None,
        }
    });

    let media_template = rep
        .template
        .media
        .as_deref()
        .unwrap_or("chunk_$RepresentationID$_$Number$.m4s");

    let mut segments = Vec::with_capacity(num_segments as usize);
    for i in 0..num_segments {
        let number = start_number + i;
        let expanded = format_dash_template(media_template, &rep.id, number);
        let seg_url = resolve_url(base_url, &expanded);

        let remaining = total_duration - (i as f64 * segment_duration_secs);
        let duration_secs = if remaining > 0.0 && remaining < segment_duration_secs {
            remaining
        } else {
            segment_duration_secs
        };

        segments.push(MediaSegment {
            sequence: number,
            url: seg_url,
            duration_secs,
            byte_range: None,
            key: None,
        });
    }

    MediaPlaylist {
        protocol: Protocol::Dash,
        target_duration_secs,
        is_live,
        media_sequence: start_number,
        init_segment,
        segments,
    }
}

pub fn format_dash_template(template: &str, rep_id: &str, number: u64) -> String {
    let mut result = template.replace("$RepresentationID$", rep_id);

    while let Some(start) = result.find("$Number") {
        let after_prefix = start + "$Number".len();
        if let Some(end_offset) = result[after_prefix..].find('$') {
            let end = after_prefix + end_offset;
            let placeholder = result[start..=end].to_string();
            let formatted_num = if let Some(rest) = placeholder.strip_prefix("$Number%0") {
                if let Some(width_str) = rest.strip_suffix("d$") {
                    if let Ok(width) = width_str.parse::<usize>() {
                        format!("{:0width$}", number, width = width)
                    } else {
                        number.to_string()
                    }
                } else {
                    number.to_string()
                }
            } else {
                number.to_string()
            };
            result = result.replace(&placeholder, &formatted_num);
        } else {
            break;
        }
    }

    result
}

pub fn parse_iso8601_duration(s: &str) -> Option<f64> {
    let s = s.trim();
    if !s.starts_with('P') {
        return None;
    }
    let s = &s[1..];
    let (date_part, time_part) = match s.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };

    let mut total_secs = 0.0f64;

    if !date_part.is_empty() {
        let mut num_buf = String::new();
        for c in date_part.chars() {
            if c.is_ascii_digit() || c == '.' {
                num_buf.push(c);
            } else {
                let val: f64 = num_buf.parse().ok()?;
                num_buf.clear();
                match c {
                    'D' => total_secs += val * 86400.0,
                    'W' => total_secs += val * 604800.0,
                    'Y' => total_secs += val * 31536000.0,
                    _ => {}
                }
            }
        }
    }

    if let Some(t_part) = time_part {
        let mut num_buf = String::new();
        for c in t_part.chars() {
            if c.is_ascii_digit() || c == '.' {
                num_buf.push(c);
            } else {
                let val: f64 = num_buf.parse().ok()?;
                num_buf.clear();
                match c {
                    'H' => total_secs += val * 3600.0,
                    'M' => total_secs += val * 60.0,
                    'S' => total_secs += val,
                    _ => {}
                }
            }
        }
    }

    Some(total_secs)
}
