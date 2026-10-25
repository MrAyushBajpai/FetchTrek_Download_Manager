use std::time::Instant;

use fetchtrek_protocols::media::{
    parse_dash, parse_dash_media, parse_hls, parse_iso8601_duration, parse_media_manifest,
    resolve_url, MediaManifest,
};
use fetchtrek_types::{ByteRange, MediaError, Protocol};

#[test]
fn test_parse_hls_master_playlist() {
    let manifest_content = r#"#EXTM3U
#EXT-X-VERSION:4
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",NAME="English",DEFAULT=YES,AUTOSELECT=YES,LANGUAGE="en",URI="audio/en/prog_index.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080,CODECS="avc1.640028,mp4a.40.2",AUDIO="audio"
1080p/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720,CODECS="avc1.4d401f,mp4a.40.2",AUDIO="audio"
720p/index.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=854x480,CODECS="avc1.4d401e,mp4a.40.2",AUDIO="audio"
480p/index.m3u8
"#;

    let base_url = "https://cdn.example.com/hls/master.m3u8";
    let manifest = parse_media_manifest(manifest_content, base_url).expect("parse master playlist");

    match manifest {
        MediaManifest::Master(master) => {
            assert_eq!(master.variants.len(), 3);
            assert_eq!(master.audio_tracks.len(), 1);

            // 1080p variant
            let v1080 = &master.variants[0];
            assert_eq!(v1080.bandwidth, 5_000_000);
            assert_eq!(v1080.resolution, Some((1920, 1080)));
            assert_eq!(v1080.codecs.as_deref(), Some("avc1.640028,mp4a.40.2"));
            assert_eq!(v1080.url, "https://cdn.example.com/hls/1080p/index.m3u8");

            // 720p variant
            let v720 = &master.variants[1];
            assert_eq!(v720.bandwidth, 2_500_000);
            assert_eq!(v720.resolution, Some((1280, 720)));
            assert_eq!(v720.url, "https://cdn.example.com/hls/720p/index.m3u8");

            // 480p variant
            let v480 = &master.variants[2];
            assert_eq!(v480.bandwidth, 1_000_000);
            assert_eq!(v480.resolution, Some((854, 480)));
            assert_eq!(v480.url, "https://cdn.example.com/hls/480p/index.m3u8");

            // Audio track
            let audio = &master.audio_tracks[0];
            assert_eq!(audio.group_id, "audio");
            assert_eq!(audio.name, "English");
            assert_eq!(audio.language.as_deref(), Some("en"));
            assert_eq!(
                audio.url.as_deref(),
                Some("https://cdn.example.com/hls/audio/en/prog_index.m3u8")
            );
        }
        MediaManifest::Media(_) => panic!("Expected MasterPlaylist variant"),
    }
}

#[test]
fn test_parse_hls_vod_media_playlist() {
    let manifest_content = r#"#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-MEDIA-SEQUENCE:0
#EXTINF:9.009,
segment_0.ts
#EXTINF:10.000,
segment_1.ts
#EXTINF:8.500,
segment_2.ts
#EXT-X-ENDLIST
"#;

    let base_url = "https://cdn.example.com/vod/playlist.m3u8";
    let manifest = parse_hls(manifest_content, base_url).expect("parse vod media playlist");

    match manifest {
        MediaManifest::Media(playlist) => {
            assert_eq!(playlist.protocol, Protocol::Hls);
            assert_eq!(playlist.target_duration_secs, 10.0);
            assert!(!playlist.is_live, "VOD stream must have is_live = false");
            assert_eq!(playlist.media_sequence, 0);
            assert_eq!(playlist.segments.len(), 3);

            assert_eq!(playlist.segments[0].sequence, 0);
            assert_eq!(
                playlist.segments[0].url,
                "https://cdn.example.com/vod/segment_0.ts"
            );
            assert_eq!(playlist.segments[0].duration_secs, 9.009);

            assert_eq!(playlist.segments[1].sequence, 1);
            assert_eq!(
                playlist.segments[1].url,
                "https://cdn.example.com/vod/segment_1.ts"
            );
            assert_eq!(playlist.segments[1].duration_secs, 10.0);

            assert_eq!(playlist.segments[2].sequence, 2);
            assert_eq!(
                playlist.segments[2].url,
                "https://cdn.example.com/vod/segment_2.ts"
            );
            assert_eq!(playlist.segments[2].duration_secs, 8.5);
        }
        MediaManifest::Master(_) => panic!("Expected MediaPlaylist"),
    }
}

#[test]
fn test_parse_hls_fmp4_with_init_map() {
    let manifest_content = r#"#EXTM3U
#EXT-X-VERSION:6
#EXT-X-TARGETDURATION:6
#EXT-X-MAP:URI="init.mp4",BYTERANGE="820@0"
#EXT-X-BYTERANGE:150000@820
#EXTINF:6.0,
main.mp4
#EXT-X-BYTERANGE:160000
#EXTINF:6.0,
main.mp4
#EXT-X-ENDLIST
"#;

    let base_url = "https://cdn.example.com/fmp4/playlist.m3u8";
    let manifest = parse_hls(manifest_content, base_url).expect("parse fmp4 playlist");

    match manifest {
        MediaManifest::Media(playlist) => {
            let init = playlist.init_segment.expect("must have init_segment");
            assert_eq!(init.url, "https://cdn.example.com/fmp4/init.mp4");
            assert_eq!(init.byte_range, Some(ByteRange::new(0, 819)));

            assert_eq!(playlist.segments.len(), 2);

            let seg0 = &playlist.segments[0];
            assert_eq!(seg0.url, "https://cdn.example.com/fmp4/main.mp4");
            assert_eq!(seg0.byte_range, Some(ByteRange::new(820, 150819)));

            // Next segment continues from 150820
            let seg1 = &playlist.segments[1];
            assert_eq!(seg1.url, "https://cdn.example.com/fmp4/main.mp4");
            assert_eq!(seg1.byte_range, Some(ByteRange::new(150820, 310819)));
        }
        MediaManifest::Master(_) => panic!("Expected MediaPlaylist"),
    }
}

#[test]
fn test_parse_hls_aes128_encrypted() {
    let manifest_content = r#"#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:10
#EXT-X-KEY:METHOD=AES-128,URI="https://priv.example.com/key.bin",IV=0x0123456789abcdef0123456789abcdef
#EXTINF:10.0,
enc_01.ts
#EXTINF:10.0,
enc_02.ts
#EXT-X-ENDLIST
"#;

    let base_url = "https://cdn.example.com/hls/playlist.m3u8";
    let manifest = parse_hls(manifest_content, base_url).expect("parse encrypted playlist");

    match manifest {
        MediaManifest::Media(playlist) => {
            assert_eq!(playlist.segments.len(), 2);
            for seg in &playlist.segments {
                let key = seg.key.as_ref().expect("segment must have key");
                assert_eq!(key.method, "AES-128");
                assert_eq!(key.url, "https://priv.example.com/key.bin");
                assert_eq!(
                    key.iv,
                    Some([
                        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67,
                        0x89, 0xab, 0xcd, 0xef
                    ])
                );
            }
        }
        MediaManifest::Master(_) => panic!("Expected MediaPlaylist"),
    }
}

#[test]
fn test_parse_hls_live_stream_detection() {
    let manifest_content = r#"#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:105
#EXTINF:4.0,
live_105.ts
#EXTINF:4.0,
live_106.ts
#EXTINF:4.0,
live_107.ts
"#;

    let base_url = "https://cdn.example.com/live/stream.m3u8";
    let manifest = parse_hls(manifest_content, base_url).expect("parse live playlist");

    match manifest {
        MediaManifest::Media(playlist) => {
            assert!(
                playlist.is_live,
                "Missing #EXT-X-ENDLIST must flag is_live = true"
            );
            assert_eq!(playlist.media_sequence, 105);
            assert_eq!(playlist.segments.len(), 3);
            assert_eq!(playlist.segments[0].sequence, 105);
            assert_eq!(playlist.segments[1].sequence, 106);
            assert_eq!(playlist.segments[2].sequence, 107);
        }
        MediaManifest::Master(_) => panic!("Expected MediaPlaylist"),
    }
}

#[test]
fn test_parse_dash_mpd_template() {
    let mpd_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"
     mediaPresentationDuration="PT0H3M20.00S"
     type="static">
  <Period id="1">
    <AdaptationSet contentType="video" mimeType="video/mp4">
      <SegmentTemplate initialization="video/init_$RepresentationID$.mp4"
                       media="video/chunk_$RepresentationID$_$Number$.m4s"
                       startNumber="1"
                       timescale="1000"
                       duration="2000"/>
      <Representation id="1080p" bandwidth="5000000" width="1920" height="1080" codecs="avc1.640028"/>
      <Representation id="720p" bandwidth="2500000" width="1280" height="720" codecs="avc1.4d401f"/>
    </AdaptationSet>
    <AdaptationSet contentType="audio" mimeType="audio/mp4" lang="en">
      <SegmentTemplate initialization="audio/init.mp4"
                       media="audio/chunk_$Number$.m4s"
                       startNumber="1"
                       timescale="48000"
                       duration="96000"/>
      <Representation id="audio_en" bandwidth="128000" codecs="mp4a.40.2"/>
    </AdaptationSet>
  </Period>
</MPD>
"#;

    let base_url = "https://cdn.example.com/dash/manifest.mpd";

    // 1. Master manifest view (extracting adaptation sets)
    let manifest = parse_media_manifest(mpd_content, base_url).expect("parse dash mpd");
    match manifest {
        MediaManifest::Master(master) => {
            assert_eq!(master.variants.len(), 2);
            assert_eq!(master.audio_tracks.len(), 1);

            let v1080 = &master.variants[0];
            assert_eq!(v1080.bandwidth, 5_000_000);
            assert_eq!(v1080.resolution, Some((1920, 1080)));
            assert_eq!(v1080.codecs.as_deref(), Some("avc1.640028"));
            assert_eq!(v1080.url, "https://cdn.example.com/dash/manifest.mpd#1080p");

            let v720 = &master.variants[1];
            assert_eq!(v720.bandwidth, 2_500_000);
            assert_eq!(v720.resolution, Some((1280, 720)));
            assert_eq!(v720.codecs.as_deref(), Some("avc1.4d401f"));
            assert_eq!(v720.url, "https://cdn.example.com/dash/manifest.mpd#720p");

            let audio = &master.audio_tracks[0];
            assert_eq!(audio.name, "audio_en");
            assert_eq!(audio.language.as_deref(), Some("en"));
            assert_eq!(
                audio.url.as_deref(),
                Some("https://cdn.example.com/dash/manifest.mpd#audio_en")
            );
        }
        MediaManifest::Media(_) => panic!("Expected MasterPlaylist for multi-representation DASH"),
    }

    // 2. Media playlist view (expanding SegmentTemplate for 1080p)
    let media_playlist = parse_dash_media(mpd_content, base_url, Some("1080p"))
        .expect("expand 1080p dash representation");

    assert_eq!(media_playlist.protocol, Protocol::Dash);
    assert_eq!(media_playlist.target_duration_secs, 2.0);
    assert!(!media_playlist.is_live);
    assert_eq!(media_playlist.media_sequence, 1);

    // Init segment check
    let init = media_playlist.init_segment.expect("must have init segment");
    assert_eq!(
        init.url,
        "https://cdn.example.com/dash/video/init_1080p.mp4"
    );

    // Segment count check: 200s duration / 2s chunk = 100 segments
    assert_eq!(media_playlist.segments.len(), 100);

    let first = &media_playlist.segments[0];
    assert_eq!(first.sequence, 1);
    assert_eq!(
        first.url,
        "https://cdn.example.com/dash/video/chunk_1080p_1.m4s"
    );
    assert_eq!(first.duration_secs, 2.0);

    let last = &media_playlist.segments[99];
    assert_eq!(last.sequence, 100);
    assert_eq!(
        last.url,
        "https://cdn.example.com/dash/video/chunk_1080p_100.m4s"
    );
    assert_eq!(last.duration_secs, 2.0);

    // Also verify fragment URL resolution via parse_dash
    let frag_manifest = parse_dash(
        mpd_content,
        "https://cdn.example.com/dash/manifest.mpd#1080p",
    )
    .expect("parse dash with fragment url");
    assert!(matches!(frag_manifest, MediaManifest::Media(_)));
}

#[test]
fn test_malformed_manifest_rejection() {
    let base_url = "https://cdn.example.com/stream/playlist.m3u8";

    // Empty string
    assert_eq!(
        parse_media_manifest("", base_url),
        Err(MediaError::MalformedManifest)
    );

    // Random non-manifest text
    assert_eq!(
        parse_media_manifest("Not a valid manifest", base_url),
        Err(MediaError::MalformedManifest)
    );

    // Truncated HLS header
    assert_eq!(
        parse_media_manifest("#EXT", base_url),
        Err(MediaError::MalformedManifest)
    );

    // Empty M3U8 without segments or duration
    assert_eq!(
        parse_media_manifest("#EXTM3U\n", base_url),
        Err(MediaError::MalformedManifest)
    );

    // Invalid XML for DASH
    assert_eq!(
        parse_dash("<MPD mediaPresentationDuration=", base_url),
        Err(MediaError::MalformedManifest)
    );
}

#[test]
fn test_large_playlist_parsing_performance() {
    let mut large_m3u8 =
        String::from("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:1\n");
    for i in 1..=1000 {
        large_m3u8.push_str(&format!("#EXTINF:6.0,\nseg_{i}.ts\n"));
    }
    large_m3u8.push_str("#EXT-X-ENDLIST\n");

    let base_url = "https://cdn.example.com/hls/large.m3u8";

    let start = Instant::now();
    let manifest = parse_hls(&large_m3u8, base_url).expect("parse 1000-segment playlist");
    let elapsed = start.elapsed();

    match manifest {
        MediaManifest::Media(playlist) => {
            assert_eq!(playlist.segments.len(), 1000);
            assert_eq!(
                playlist.segments[0].url,
                "https://cdn.example.com/hls/seg_1.ts"
            );
            assert_eq!(
                playlist.segments[999].url,
                "https://cdn.example.com/hls/seg_1000.ts"
            );
        }
        _ => panic!("Expected MediaPlaylist"),
    }

    println!("1,000-segment playlist parsed in {:?}", elapsed);
    assert!(
        elapsed.as_millis() < 5,
        "Parsing 1,000 segments took {:?}, exceeding 5ms threshold",
        elapsed
    );
}

#[test]
fn test_iso8601_duration_parsing() {
    assert_eq!(parse_iso8601_duration("PT0H3M20.00S"), Some(200.0));
    assert_eq!(parse_iso8601_duration("PT1H30M"), Some(5400.0));
    assert_eq!(parse_iso8601_duration("PT45S"), Some(45.0));
    assert_eq!(parse_iso8601_duration("P1DT2H"), Some(93600.0));
    assert_eq!(parse_iso8601_duration("invalid"), None);
}

#[test]
fn test_url_resolution_corner_cases() {
    let base = "https://cdn.example.com/live/1080p/index.m3u8?token=abc";

    // Sibling relative path
    assert_eq!(
        resolve_url(base, "chunk_01.ts"),
        "https://cdn.example.com/live/1080p/chunk_01.ts"
    );

    // Parent directory traversal
    assert_eq!(
        resolve_url(base, "../audio/track.m4s"),
        "https://cdn.example.com/live/audio/track.m4s"
    );

    // Root-relative
    assert_eq!(
        resolve_url(base, "/media/seg.ts"),
        "https://cdn.example.com/media/seg.ts"
    );

    // Absolute
    assert_eq!(
        resolve_url(base, "https://other.com/seg.ts"),
        "https://other.com/seg.ts"
    );

    // Fragment
    assert_eq!(
        resolve_url(base, "#720p"),
        "https://cdn.example.com/live/1080p/index.m3u8?token=abc#720p"
    );
}
