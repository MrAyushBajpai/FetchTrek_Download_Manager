#![allow(clippy::chunks_exact_to_as_chunks)]

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_core::{
    MediaChunk, MediaChunkScheduler, MediaDownloadConfig, MediaIngestionCoordinator,
    NetworkTransport, SessionContext,
};
use fetchtrek_protocols::{
    assemble_stream, encrypt_aes128_cbc, parse_dash_media, parse_hls, parse_media_manifest,
    AssemblyConfig, HttpTransport, MediaManifest, StreamContainer,
};
use fetchtrek_storage::SlabPool;
use fetchtrek_types::{FetchTrekError, MediaError, NetworkError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch, Mutex};

struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(prefix: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "fetchtrek_media_capture_{}_{}_{}",
            prefix,
            std::process::id(),
            timestamp
        ));
        let _ = std::fs::create_dir_all(&path);
        Self { path }
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// Zero-dependency stdlib SHA-256 implementation
fn sha256(data: &[u8]) -> [u8; 32] {
    #[allow(clippy::unreadable_literal)]
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    #[allow(clippy::unreadable_literal)]
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg = data.to_vec();
    msg.push(0x80);
    while (msg.len() % 64) != 56 {
        msg.push(0x00);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[4 * i],
                chunk[4 * i + 1],
                chunk[4 * i + 2],
                chunk[4 * i + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut h_val = h[7];

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h_val
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            h_val = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(h_val);
    }

    let mut digest = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        digest[4 * i..4 * i + 4].copy_from_slice(&word.to_be_bytes());
    }
    digest
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// Generate valid 188-byte MPEG-TS packet with 0x47 sync byte
fn create_valid_ts_packet(packet_id: u8) -> [u8; 188] {
    let mut packet = [0u8; 188];
    packet[0] = 0x47; // ISO/IEC 13818-1 sync byte
    packet[1] = 0x00;
    packet[2] = 0x11;
    packet[3] = 0x10 | (packet_id & 0x0F);
    for (i, byte) in packet.iter_mut().enumerate().skip(4) {
        *byte = packet_id.wrapping_add(i as u8);
    }
    packet
}

// Generate synthetic fMP4 initialization box (ftyp + moov)
fn create_synthetic_fmp4_init() -> Vec<u8> {
    let mut init = Vec::new();
    // ftyp box (24 bytes)
    init.extend_from_slice(&24u32.to_be_bytes());
    init.extend_from_slice(b"ftyp");
    init.extend_from_slice(b"iso6");
    init.extend_from_slice(&1u32.to_be_bytes()); // minor version
    init.extend_from_slice(b"mp41dash");

    // moov box (32 bytes)
    init.extend_from_slice(&32u32.to_be_bytes());
    init.extend_from_slice(b"moov");
    init.extend_from_slice(&24u32.to_be_bytes());
    init.extend_from_slice(b"mvhd");
    init.extend_from_slice(&[0u8; 16]);

    init
}

// Generate synthetic fMP4 media fragment (moof + mdat)
fn create_synthetic_fmp4_fragment(seq: u32, payload_size: usize) -> Vec<u8> {
    let mut fragment = Vec::new();

    // moof box (32 bytes)
    fragment.extend_from_slice(&32u32.to_be_bytes());
    fragment.extend_from_slice(b"moof");
    fragment.extend_from_slice(&24u32.to_be_bytes());
    fragment.extend_from_slice(b"mfhd");
    fragment.extend_from_slice(&[0u8; 4]); // version & flags
    fragment.extend_from_slice(&seq.to_be_bytes());
    fragment.extend_from_slice(&[0u8; 8]);

    // mdat box (8 + payload_size bytes)
    let mdat_size = (payload_size + 8) as u32;
    fragment.extend_from_slice(&mdat_size.to_be_bytes());
    fragment.extend_from_slice(b"mdat");
    for i in 0..payload_size {
        fragment.push((seq as u8).wrapping_add(i as u8));
    }

    fragment
}

#[derive(Clone)]
struct MockRoute {
    content_type: String,
    body: Vec<u8>,
    required_headers: Vec<(String, String)>,
    delay: Duration,
    fail_attempts: usize,
    current_attempts: Arc<AtomicUsize>,
}

struct MockStreamingServer {
    url: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    #[allow(dead_code)]
    pub request_counts: Arc<Mutex<HashMap<String, usize>>>,
}

impl MockStreamingServer {
    async fn start(routes: HashMap<String, MockRoute>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock streaming server");
        let port = listener.local_addr().expect("local addr").port();
        let server_url = format!("http://127.0.0.1:{port}");
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let request_counts = Arc::new(Mutex::new(HashMap::new()));
        let req_counts_clone = Arc::clone(&request_counts);
        let routes_arc = Arc::new(routes);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    Ok((mut socket, _)) = listener.accept() => {
                        let routes = Arc::clone(&routes_arc);
                        let req_counts = Arc::clone(&req_counts_clone);

                        tokio::spawn(async move {
                            let mut buf = vec![0u8; 4096];
                            let n = match socket.read(&mut buf).await {
                                Ok(n) if n > 0 => n,
                                _ => return,
                            };
                            let req = String::from_utf8_lossy(&buf[..n]);
                            let mut lines = req.lines();
                            let first_line = lines.next().unwrap_or("");
                            let parts: Vec<&str> = first_line.split_whitespace().collect();
                            if parts.len() < 2 {
                                return;
                            }
                            let method = parts[0];
                            let raw_path = parts[1].split('?').next().unwrap_or("");
                            let clean_path = raw_path.trim_start_matches('/').to_string();

                            let mut headers = HashMap::new();
                            for line in lines {
                                if line.is_empty() || line == "\r" {
                                    break;
                                }
                                if let Some((k, v)) = line.split_once(':') {
                                    headers.insert(k.trim().to_lowercase(), v.trim().to_string());
                                }
                            }

                            {
                                let mut counts = req_counts.lock().await;
                                *counts.entry(clean_path.clone()).or_insert(0) += 1;
                            }

                            if let Some(route) = routes.get(&clean_path) {
                                // 1. Validate required authentication / session headers
                                let mut authorized = true;
                                for (req_k, req_v) in &route.required_headers {
                                    if let Some(val) = headers.get(&req_k.to_lowercase()) {
                                        if !val.contains(req_v) {
                                            authorized = false;
                                            break;
                                        }
                                    } else {
                                        authorized = false;
                                        break;
                                    }
                                }

                                if !authorized {
                                    let resp = b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                                    let _ = socket.write_all(resp).await;
                                    let _ = socket.shutdown().await;
                                    return;
                                }

                                // 2. Check transient fault injection (503 Service Unavailable)
                                let prev_attempts = route.current_attempts.fetch_add(1, Ordering::SeqCst);
                                if prev_attempts < route.fail_attempts {
                                    let resp = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                                    let _ = socket.write_all(resp).await;
                                    let _ = socket.shutdown().await;
                                    return;
                                }

                                // 3. Delay simulation if configured
                                if route.delay > Duration::ZERO {
                                    tokio::time::sleep(route.delay).await;
                                }

                                // 4. Return successful response
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    route.content_type,
                                    route.body.len()
                                );
                                let _ = socket.write_all(resp.as_bytes()).await;
                                if method != "HEAD" {
                                    let _ = socket.write_all(&route.body).await;
                                }
                                let _ = socket.shutdown().await;
                            } else {
                                let not_found = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                                let _ = socket.write_all(not_found).await;
                                let _ = socket.shutdown().await;
                            }
                        });
                    }
                }
            }
        });

        Self {
            url: server_url,
            shutdown_tx: Some(shutdown_tx),
            request_counts,
        }
    }
}

impl Drop for MockStreamingServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Scenario 1: High-Bitrate HLS (MPEG-TS) Multi-Variant Capture Benchmark
/// - Parses `master.m3u8`, auto-selects highest-bandwidth 1080p stream.
/// - Downloads 20 chunks concurrently across 8 worker connections.
/// - Assembles stream and verifies ISO/IEC 13818-1 sync byte `0x47` across all packet boundaries.
/// - Asserts 100% SHA-256 byte parity against source MPEG-TS stream.
/// - Asserts throughput > 50 MB/s on loopback and assembly latency < 150 ms.
#[tokio::test]
async fn test_scenario_1_hls_ts_multivariant_capture_and_line_rate_benchmark() {
    let temp = TempDirGuard::new("s1_hls_ts");
    const CHUNK_COUNT: usize = 20;
    const PACKETS_PER_CHUNK: usize = 1500; // 1500 * 188 = 282,000 bytes/chunk (~5.64 MB total)

    let mut expected_ts_bytes = Vec::with_capacity(CHUNK_COUNT * PACKETS_PER_CHUNK * 188);
    let mut routes = HashMap::new();

    // Master playlist with 3 variants
    let master_playlist = r#"#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=1000000,RESOLUTION=854x480
media_480p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080,CODECS="avc1.640028,mp4a.40.2"
media_1080p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720,CODECS="avc1.4d401f,mp4a.40.2"
media_720p.m3u8
"#;

    routes.insert(
        "master.m3u8".to_string(),
        MockRoute {
            content_type: "application/vnd.apple.mpegurl".to_string(),
            body: master_playlist.as_bytes().to_vec(),
            required_headers: Vec::new(),
            delay: Duration::ZERO,
            fail_attempts: 0,
            current_attempts: Arc::new(AtomicUsize::new(0)),
        },
    );

    // 1080p Media playlist
    let mut media_1080p = String::from(
        "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n",
    );
    for i in 0..CHUNK_COUNT {
        let seg_name = format!("chunk_1080p_{i}.ts");
        media_1080p.push_str(&format!("#EXTINF:6.0,\n{seg_name}\n"));

        let mut seg_bytes = Vec::with_capacity(PACKETS_PER_CHUNK * 188);
        for p in 0..PACKETS_PER_CHUNK {
            let pkt = create_valid_ts_packet(((i * 17 + p) % 256) as u8);
            seg_bytes.extend_from_slice(&pkt);
            expected_ts_bytes.extend_from_slice(&pkt);
        }

        routes.insert(
            seg_name,
            MockRoute {
                content_type: "video/mp2t".to_string(),
                body: seg_bytes,
                required_headers: Vec::new(),
                delay: Duration::ZERO,
                fail_attempts: 0,
                current_attempts: Arc::new(AtomicUsize::new(0)),
            },
        );
    }
    media_1080p.push_str("#EXT-X-ENDLIST\n");

    routes.insert(
        "media_1080p.m3u8".to_string(),
        MockRoute {
            content_type: "application/vnd.apple.mpegurl".to_string(),
            body: media_1080p.into_bytes(),
            required_headers: Vec::new(),
            delay: Duration::ZERO,
            fail_attempts: 0,
            current_attempts: Arc::new(AtomicUsize::new(0)),
        },
    );

    let server = MockStreamingServer::start(routes).await;
    let master_url = format!("{}/master.m3u8", server.url);
    let transport: Arc<dyn NetworkTransport> = Arc::new(HttpTransport::new());

    // 1. Fetch & parse Master playlist
    let session_ctx = SessionContext::new();
    let mut stream = transport
        .open_stream(&master_url, &session_ctx)
        .await
        .expect("open master");
    let mut master_content = String::new();
    stream
        .read_to_string(&mut master_content)
        .await
        .expect("read master");

    let manifest = parse_media_manifest(&master_content, &master_url).expect("parse master");
    let variant_url = match manifest {
        MediaManifest::Master(master) => {
            assert_eq!(master.variants.len(), 3);
            let best = master
                .variants
                .iter()
                .max_by_key(|v| v.bandwidth)
                .expect("best variant");
            assert_eq!(best.bandwidth, 5_000_000);
            assert_eq!(best.resolution, Some((1920, 1080)));
            best.url.clone()
        }
        _ => panic!("Expected MasterPlaylist"),
    };

    // 2. Fetch & parse highest-bandwidth media playlist
    let mut stream = transport
        .open_stream(&variant_url, &session_ctx)
        .await
        .expect("open media");
    let mut media_content = String::new();
    stream
        .read_to_string(&mut media_content)
        .await
        .expect("read media");

    let media_manifest = parse_hls(&media_content, &variant_url).expect("parse hls media");
    let media_playlist = match media_manifest {
        MediaManifest::Media(p) => p,
        _ => panic!("Expected MediaPlaylist"),
    };

    assert_eq!(media_playlist.segments.len(), CHUNK_COUNT);

    let chunks: Vec<MediaChunk> = media_playlist
        .segments
        .into_iter()
        .map(|s| MediaChunk::new(s.sequence as usize, s.url, s.duration_secs))
        .collect();

    // 3. Parallel chunk ingestion across 8 connections
    let staging_dir = temp.path.join("staging_s1");
    let config = MediaDownloadConfig::new(chunks, &staging_dir).with_connections(8);
    let memory_pool = SlabPool::new();
    let coordinator = Arc::new(MediaIngestionCoordinator::new(
        config,
        transport.clone(),
        memory_pool,
    ));

    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let download_start = Instant::now();
    let summary = coordinator
        .run(cancel_rx)
        .await
        .expect("parallel download must succeed");
    let download_duration = download_start.elapsed();

    assert_eq!(summary.total_chunks, CHUNK_COUNT);
    assert_eq!(summary.total_bytes, expected_ts_bytes.len() as u64);

    let throughput_mb_s =
        (summary.total_bytes as f64 / download_duration.as_secs_f64()) / (1024.0 * 1024.0);
    println!(
        "[Benchmark] S1 Parallel Ingestion: {} bytes across {} chunks in {:?} ({:.2} MB/s)",
        summary.total_bytes, CHUNK_COUNT, download_duration, throughput_mb_s
    );
    assert!(
        throughput_mb_s > 50.0,
        "Loopback throughput must exceed 50 MB/s, achieved {:.2} MB/s",
        throughput_mb_s
    );

    // 4. Zero-reencode stream assembly
    let output_file = temp.path.join("high_quality_video.ts");
    let assembly_config = AssemblyConfig::new(
        StreamContainer::MpegTs,
        &staging_dir,
        CHUNK_COUNT,
        &output_file,
    );

    let assembly_start = Instant::now();
    let assembly_summary = assemble_stream(&assembly_config).expect("assembly must succeed");
    let assembly_duration = assembly_start.elapsed();

    println!("[Benchmark] S1 Assembly Latency: {:?}", assembly_duration);
    assert!(
        assembly_duration < Duration::from_millis(150),
        "Final assembly latency must be < 150ms, took {:?}",
        assembly_duration
    );
    assert_eq!(assembly_summary.chunks_merged, CHUNK_COUNT);
    assert_eq!(assembly_summary.total_bytes, expected_ts_bytes.len() as u64);

    // 5. Bit-for-bit Parity & Sync Byte Validation
    let assembled_bytes = std::fs::read(&output_file).expect("read assembled file");
    assert_eq!(
        assembled_bytes.len() % 188,
        0,
        "Length must be multiple of 188"
    );

    for (idx, packet) in assembled_bytes.chunks_exact(188).enumerate() {
        assert_eq!(
            packet[0], 0x47,
            "Packet {idx} must begin with ISO/IEC 13818-1 sync byte 0x47"
        );
    }

    let expected_hash = sha256(&expected_ts_bytes);
    let actual_hash = sha256(&assembled_bytes);
    assert_eq!(
        actual_hash,
        expected_hash,
        "100% SHA-256 byte parity required: actual={} expected={}",
        to_hex(&actual_hash),
        to_hex(&expected_hash)
    );
    assert_eq!(assembled_bytes, expected_ts_bytes);

    // 6. Staging cleanup
    assert!(
        !staging_dir.exists(),
        "Staging dir must be purged on completion"
    );
}

/// Scenario 2: Fragmented MP4 (fMP4 / MPEG-DASH) Stream Capture
/// - Parses MPEG-DASH XML manifest, expands segment template URLs.
/// - Downloads `init.mp4` and all 15 `.m4s` media fragments in parallel.
/// - Assembles into `dash_movie.mp4` with ISOBMFF container structure validation.
/// - Asserts 100% SHA-256 byte parity against source fragmented MP4 stream.
#[tokio::test]
async fn test_scenario_2_fmp4_dash_stream_capture_parity() {
    let temp = TempDirGuard::new("s2_dash_fmp4");
    const FRAGMENT_COUNT: usize = 15;
    const FRAGMENT_PAYLOAD_SIZE: usize = 4096;

    let init_bytes = create_synthetic_fmp4_init();
    let mut expected_fmp4_bytes = init_bytes.clone();
    let mut routes = HashMap::new();

    // Serve init segment
    routes.insert(
        "video/init_1080p.mp4".to_string(),
        MockRoute {
            content_type: "video/mp4".to_string(),
            body: init_bytes.clone(),
            required_headers: Vec::new(),
            delay: Duration::ZERO,
            fail_attempts: 0,
            current_attempts: Arc::new(AtomicUsize::new(0)),
        },
    );

    // Serve 15 .m4s fragments
    for i in 1..=FRAGMENT_COUNT {
        let frag_name = format!("video/chunk_1080p_{i}.m4s");
        let frag_bytes = create_synthetic_fmp4_fragment(i as u32, FRAGMENT_PAYLOAD_SIZE);
        expected_fmp4_bytes.extend_from_slice(&frag_bytes);

        routes.insert(
            frag_name,
            MockRoute {
                content_type: "video/mp4".to_string(),
                body: frag_bytes,
                required_headers: Vec::new(),
                delay: Duration::ZERO,
                fail_attempts: 0,
                current_attempts: Arc::new(AtomicUsize::new(0)),
            },
        );
    }

    // DASH MPD manifest
    let mpd_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011"
     mediaPresentationDuration="PT0H0M30.00S"
     type="static">
  <Period id="1">
    <AdaptationSet contentType="video" mimeType="video/mp4">
      <SegmentTemplate initialization="video/init_$RepresentationID$.mp4"
                       media="video/chunk_$RepresentationID$_$Number$.m4s"
                       startNumber="1"
                       timescale="1000"
                       duration="2000"/>
      <Representation id="1080p" bandwidth="5000000" width="1920" height="1080" codecs="avc1.640028"/>
    </AdaptationSet>
  </Period>
</MPD>
"#;

    routes.insert(
        "manifest.mpd".to_string(),
        MockRoute {
            content_type: "application/dash+xml".to_string(),
            body: mpd_content.as_bytes().to_vec(),
            required_headers: Vec::new(),
            delay: Duration::ZERO,
            fail_attempts: 0,
            current_attempts: Arc::new(AtomicUsize::new(0)),
        },
    );

    let server = MockStreamingServer::start(routes).await;
    let manifest_url = format!("{}/manifest.mpd", server.url);
    let transport: Arc<dyn NetworkTransport> = Arc::new(HttpTransport::new());
    let session_ctx = SessionContext::new();

    // 1. Fetch & parse DASH manifest with SegmentTemplate expansion
    let mut stream = transport
        .open_stream(&manifest_url, &session_ctx)
        .await
        .expect("open mpd");
    let mut fetched_mpd = String::new();
    stream
        .read_to_string(&mut fetched_mpd)
        .await
        .expect("read mpd");

    let media_playlist =
        parse_dash_media(&fetched_mpd, &manifest_url, Some("1080p")).expect("parse dash 1080p");
    assert_eq!(media_playlist.segments.len(), FRAGMENT_COUNT);

    let init_seg = media_playlist
        .init_segment
        .expect("DASH must declare init segment");

    // 2. Download init segment into staging
    let staging_dir = temp.path.join("staging_s2");
    tokio::fs::create_dir_all(&staging_dir)
        .await
        .expect("create staging dir");
    let init_file_path = staging_dir.join("init.mp4");

    let mut init_stream = transport
        .open_stream(&init_seg.url, &session_ctx)
        .await
        .expect("open init stream");
    let mut init_file = tokio::fs::File::create(&init_file_path)
        .await
        .expect("create init.mp4");
    let mut downloaded_init = Vec::new();
    init_stream
        .read_to_end(&mut downloaded_init)
        .await
        .expect("read init stream");
    init_file
        .write_all(&downloaded_init)
        .await
        .expect("write init file");
    init_file.flush().await.expect("flush init file");
    drop(init_file);

    assert_eq!(downloaded_init, init_bytes);

    // 3. Parallel download of 15 media fragments
    let chunks: Vec<MediaChunk> = media_playlist
        .segments
        .into_iter()
        .map(|s| {
            MediaChunk::new(
                s.sequence.saturating_sub(1) as usize,
                s.url,
                s.duration_secs,
            )
        })
        .collect();

    let config = MediaDownloadConfig::new(chunks, &staging_dir).with_connections(4);
    let coordinator = Arc::new(MediaIngestionCoordinator::new(
        config,
        transport.clone(),
        SlabPool::new(),
    ));

    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let summary = coordinator
        .run(cancel_rx)
        .await
        .expect("download fragments");
    assert_eq!(summary.total_chunks, FRAGMENT_COUNT);

    // 4. Stream assembly into dash_movie.mp4
    let output_file = temp.path.join("dash_movie.mp4");
    let assembly_config = AssemblyConfig::new(
        StreamContainer::FragmentedMp4,
        &staging_dir,
        FRAGMENT_COUNT,
        &output_file,
    )
    .with_init_segment(&init_file_path);

    let assembly_summary = assemble_stream(&assembly_config).expect("assemble dash movie");
    assert_eq!(assembly_summary.chunks_merged, FRAGMENT_COUNT);
    assert_eq!(
        assembly_summary.total_bytes,
        expected_fmp4_bytes.len() as u64
    );

    // 5. ISOBMFF box verification & SHA-256 byte parity
    let assembled = std::fs::read(&output_file).expect("read dash_movie.mp4");
    assert!(assembled.len() > 32);
    assert_eq!(&assembled[4..8], b"ftyp", "Must begin with ftyp box");
    assert_eq!(&assembled[8..12], b"iso6", "Major brand must be iso6");

    let actual_hash = sha256(&assembled);
    let expected_hash = sha256(&expected_fmp4_bytes);
    assert_eq!(
        actual_hash, expected_hash,
        "Fragmented MP4 SHA-256 match required"
    );
    assert_eq!(assembled, expected_fmp4_bytes);

    // 6. Staging directory cleanup
    assert!(
        !staging_dir.exists(),
        "Staging directory must be cleaned up on success"
    );
}

/// Scenario 3: AES-128 Encrypted HLS Stream Capture & Decryption Parity
/// - Ingests encrypted playlist declaring `#EXT-X-KEY:METHOD=AES-128,URI="...",IV=...`.
/// - Fetches key over HTTP authenticating with session cookies and User-Agent (`SessionContext`).
/// - Decrypts all 20 chunks on-the-fly and assembles plaintext output.
/// - Asserts 100% SHA-256 byte parity against unencrypted ground-truth media stream.
#[tokio::test]
async fn test_scenario_3_aes128_encrypted_hls_stream_capture_and_decryption_parity() {
    let temp = TempDirGuard::new("s3_aes128");
    const CHUNK_COUNT: usize = 20;
    const PACKETS_PER_CHUNK: usize = 50; // 50 * 188 = 9,400 bytes/chunk (multiple of 16)

    let key: [u8; 16] = [
        0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf, 0x4f,
        0x3c,
    ];
    let iv: [u8; 16] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];

    let mut expected_plaintext = Vec::new();
    let mut routes = HashMap::new();

    // Authenticated key endpoint requiring session cookies & User-Agent
    routes.insert(
        "key.bin".to_string(),
        MockRoute {
            content_type: "application/octet-stream".to_string(),
            body: key.to_vec(),
            required_headers: vec![
                (
                    "Cookie".to_string(),
                    "session_token=secret_media_token_777".to_string(),
                ),
                (
                    "User-Agent".to_string(),
                    "FetchTrek-MediaCapture/1.0".to_string(),
                ),
            ],
            delay: Duration::ZERO,
            fail_attempts: 0,
            current_attempts: Arc::new(AtomicUsize::new(0)),
        },
    );

    // Encrypted Media Playlist
    let mut m3u8 = format!(
        "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-KEY:METHOD=AES-128,URI=\"key.bin\",IV=0x{}\n",
        to_hex(&iv)
    );

    for i in 0..CHUNK_COUNT {
        let seg_name = format!("chunk_enc_{i}.ts");
        m3u8.push_str(&format!("#EXTINF:6.0,\n{seg_name}\n"));

        let mut pt = Vec::with_capacity(PACKETS_PER_CHUNK * 188);
        for p in 0..PACKETS_PER_CHUNK {
            let pkt = create_valid_ts_packet(((i * 7 + p) % 256) as u8);
            pt.extend_from_slice(&pkt);
            expected_plaintext.extend_from_slice(&pkt);
        }

        // Encrypt segment with AES-128-CBC & PKCS#7 padding
        let ct = encrypt_aes128_cbc(&pt, &key, &iv);

        routes.insert(
            seg_name,
            MockRoute {
                content_type: "video/mp2t".to_string(),
                body: ct,
                required_headers: Vec::new(),
                delay: Duration::ZERO,
                fail_attempts: 0,
                current_attempts: Arc::new(AtomicUsize::new(0)),
            },
        );
    }
    m3u8.push_str("#EXT-X-ENDLIST\n");

    routes.insert(
        "encrypted.m3u8".to_string(),
        MockRoute {
            content_type: "application/vnd.apple.mpegurl".to_string(),
            body: m3u8.into_bytes(),
            required_headers: Vec::new(),
            delay: Duration::ZERO,
            fail_attempts: 0,
            current_attempts: Arc::new(AtomicUsize::new(0)),
        },
    );

    let server = MockStreamingServer::start(routes).await;
    let manifest_url = format!("{}/encrypted.m3u8", server.url);
    let transport: Arc<dyn NetworkTransport> = Arc::new(HttpTransport::new());

    // 1. Attempt unauthenticated key retrieval -> must fail with AccessDenied (401)
    let unauth_ctx = SessionContext::new();
    let unauth_key_res = transport
        .open_stream(&format!("{}/key.bin", server.url), &unauth_ctx)
        .await;
    assert!(
        matches!(unauth_key_res, Err(NetworkError::AccessDenied)),
        "Unauthenticated key request must return AccessDenied"
    );

    // 2. Authenticated retrieval using SessionContext
    let auth_ctx = SessionContext::new()
        .with_cookies("session_token=secret_media_token_777")
        .with_user_agent("FetchTrek-MediaCapture/1.0");

    let mut stream = transport
        .open_stream(&manifest_url, &auth_ctx)
        .await
        .expect("open manifest");
    let mut manifest_body = String::new();
    stream
        .read_to_string(&mut manifest_body)
        .await
        .expect("read manifest");

    let playlist_manifest = parse_hls(&manifest_body, &manifest_url).expect("parse encrypted hls");
    let playlist = match playlist_manifest {
        MediaManifest::Media(p) => p,
        _ => panic!("Expected MediaPlaylist"),
    };

    let key_meta = playlist.segments[0]
        .key
        .as_ref()
        .expect("Must declare AES-128 key metadata");
    assert_eq!(key_meta.method, "AES-128");
    assert_eq!(key_meta.iv, Some(iv));

    // Fetch key with active session context
    let mut key_stream = transport
        .open_stream(&key_meta.url, &auth_ctx)
        .await
        .expect("fetch key with session");
    let mut fetched_key_bytes = Vec::new();
    key_stream
        .read_to_end(&mut fetched_key_bytes)
        .await
        .expect("read key");
    assert_eq!(fetched_key_bytes.len(), 16);
    assert_eq!(fetched_key_bytes.as_slice(), &key);

    let fetched_key: [u8; 16] = fetched_key_bytes.try_into().unwrap();

    // 3. Parallel chunk download of encrypted parts into staging
    let staging_dir = temp.path.join("staging_s3");
    let chunks: Vec<MediaChunk> = playlist
        .segments
        .into_iter()
        .map(|s| MediaChunk::new(s.sequence as usize, s.url, s.duration_secs))
        .collect();

    let config = MediaDownloadConfig::new(chunks, &staging_dir)
        .with_connections(4)
        .with_context(auth_ctx);
    let coordinator = Arc::new(MediaIngestionCoordinator::new(
        config,
        transport.clone(),
        SlabPool::new(),
    ));

    let (_cancel_tx, cancel_rx) = watch::channel(false);
    let summary = coordinator
        .run(cancel_rx)
        .await
        .expect("download encrypted chunks");
    assert_eq!(summary.total_chunks, CHUNK_COUNT);

    // 4. Stream assembly with on-the-fly AES-128-CBC decryption
    let output_file = temp.path.join("decrypted_output.ts");
    let assembly_config = AssemblyConfig::new(
        StreamContainer::MpegTs,
        &staging_dir,
        CHUNK_COUNT,
        &output_file,
    )
    .with_decryption(fetched_key, Some(iv));

    let assembly_summary = assemble_stream(&assembly_config).expect("assemble and decrypt");
    assert_eq!(assembly_summary.chunks_merged, CHUNK_COUNT);
    assert_eq!(
        assembly_summary.total_bytes,
        expected_plaintext.len() as u64
    );

    // 5. Verify byte parity against unencrypted ground-truth
    let assembled = std::fs::read(&output_file).expect("read decrypted output");
    assert_eq!(assembled.len() % 188, 0);

    for (idx, packet) in assembled.chunks_exact(188).enumerate() {
        assert_eq!(
            packet[0], 0x47,
            "Decrypted packet {idx} must begin with sync byte 0x47"
        );
    }

    let actual_hash = sha256(&assembled);
    let expected_hash = sha256(&expected_plaintext);
    assert_eq!(
        actual_hash, expected_hash,
        "Decrypted output must achieve 100% SHA-256 match with unencrypted source"
    );
    assert_eq!(assembled, expected_plaintext);

    assert!(
        !staging_dir.exists(),
        "Staging dir must be purged on success"
    );
}

/// Scenario 4: Transient Network Fault Recovery & Idempotent Resumption
/// - Simulates 20% transient 503 failures across chunk requests.
/// - Validates that work-stealing scheduler recovers failed chunks via retries.
/// - Pauses mid-stream (at >= 50% completion) and asserts staging parts are preserved.
/// - Resumes download, skips completed parts, and finishes stream with 100% SHA-256 parity.
#[tokio::test]
async fn test_scenario_4_transient_network_fault_recovery_and_idempotent_resumption() {
    let temp = TempDirGuard::new("s4_fault_resume");
    const CHUNK_COUNT: usize = 20;
    const PACKETS_PER_CHUNK: usize = 100; // 100 * 188 = 18,800 bytes/chunk

    let mut expected_bytes = Vec::new();
    let mut routes = HashMap::new();
    let mut m3u8 = String::from(
        "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n",
    );

    for i in 0..CHUNK_COUNT {
        let seg_name = format!("chunk_fault_{i}.ts");
        m3u8.push_str(&format!("#EXTINF:6.0,\n{seg_name}\n"));

        let mut seg_data = Vec::new();
        for p in 0..PACKETS_PER_CHUNK {
            let pkt = create_valid_ts_packet(((i * 3 + p) % 256) as u8);
            seg_data.extend_from_slice(&pkt);
            expected_bytes.extend_from_slice(&pkt);
        }

        // 20% of chunks fail with 503 on first attempt: chunks 1, 6, 11, 16 (i % 5 == 1)
        let fail_attempts = if i % 5 == 1 { 1 } else { 0 };

        // Chunks >= 10 have delay to ensure a clean 50% pause window
        let delay = if i >= 10 {
            Duration::from_millis(60)
        } else {
            Duration::ZERO
        };

        routes.insert(
            seg_name,
            MockRoute {
                content_type: "video/mp2t".to_string(),
                body: seg_data,
                required_headers: Vec::new(),
                delay,
                fail_attempts,
                current_attempts: Arc::new(AtomicUsize::new(0)),
            },
        );
    }
    m3u8.push_str("#EXT-X-ENDLIST\n");

    routes.insert(
        "playlist.m3u8".to_string(),
        MockRoute {
            content_type: "application/vnd.apple.mpegurl".to_string(),
            body: m3u8.into_bytes(),
            required_headers: Vec::new(),
            delay: Duration::ZERO,
            fail_attempts: 0,
            current_attempts: Arc::new(AtomicUsize::new(0)),
        },
    );

    let server = MockStreamingServer::start(routes).await;
    let manifest_url = format!("{}/playlist.m3u8", server.url);
    let transport: Arc<dyn NetworkTransport> = Arc::new(HttpTransport::new());

    let mut stream = transport
        .open_stream(&manifest_url, &SessionContext::new())
        .await
        .expect("open m3u8");
    let mut manifest_content = String::new();
    stream
        .read_to_string(&mut manifest_content)
        .await
        .expect("read m3u8");

    let playlist = match parse_hls(&manifest_content, &manifest_url).expect("parse playlist") {
        MediaManifest::Media(p) => p,
        _ => panic!("Expected MediaPlaylist"),
    };

    let chunks: Vec<MediaChunk> = playlist
        .segments
        .into_iter()
        .map(|s| MediaChunk::new(s.sequence as usize, s.url, s.duration_secs))
        .collect();

    let staging_dir = temp.path.join("staging_s4");
    let config = MediaDownloadConfig::new(chunks.clone(), &staging_dir).with_connections(4);
    let scheduler = Arc::new(MediaChunkScheduler::new(chunks.clone(), 3));
    let memory_pool = SlabPool::new();
    let coordinator = Arc::new(MediaIngestionCoordinator::with_scheduler(
        config.clone(),
        transport.clone(),
        memory_pool.clone(),
        scheduler.clone(),
    ));

    let (cancel_tx, cancel_rx) = watch::channel(false);

    // 1. Start download and cancel after at least 10 chunks (50%) complete
    let coordinator_clone = Arc::clone(&coordinator);
    let handle = tokio::spawn(async move { coordinator_clone.run(cancel_rx).await });

    let mut paused_completed_count = 0;
    while paused_completed_count < 10 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        paused_completed_count = scheduler.snapshot().completed_chunks;
    }

    let _ = cancel_tx.send(true);
    let join_res = handle.await.expect("join handle");
    assert!(
        matches!(
            join_res,
            Err(FetchTrekError::Network(NetworkError::ConnectionReset))
        ),
        "Must exit cleanly on cancel token"
    );

    // 2. Pause Invariant Assertions: Staging directory & completed .part files preserved
    assert!(
        staging_dir.is_dir(),
        "Staging directory must exist while paused"
    );
    let mut preserved_parts = 0;
    for i in 0..CHUNK_COUNT {
        let part_file = staging_dir.join(format!("part_{i:06}.part"));
        if part_file.is_file() {
            preserved_parts += 1;
        }
    }
    assert!(
        preserved_parts >= 10,
        "At least 10 chunks must be preserved on disk while paused, found {preserved_parts}"
    );

    // 3. Resumption: Pre-mark existing complete parts in scheduler to avoid duplicate network transfers
    let resume_scheduler = Arc::new(MediaChunkScheduler::new(chunks.clone(), 3));
    for i in 0..CHUNK_COUNT {
        let part_file = staging_dir.join(format!("part_{i:06}.part"));
        if let Ok(meta) = std::fs::metadata(&part_file) {
            if meta.len() == (PACKETS_PER_CHUNK * 188) as u64 {
                resume_scheduler.mark_completed(i, meta.len());
            }
        }
    }

    let pre_completed = resume_scheduler.snapshot().completed_chunks;
    assert_eq!(
        pre_completed, preserved_parts,
        "Scheduler must register existing disk parts"
    );

    let resume_coordinator = Arc::new(MediaIngestionCoordinator::with_scheduler(
        config,
        transport.clone(),
        memory_pool,
        resume_scheduler.clone(),
    ));

    let (_new_cancel_tx, new_cancel_rx) = watch::channel(false);
    let resume_summary = resume_coordinator
        .run(new_cancel_rx)
        .await
        .expect("resumption must succeed");
    assert_eq!(resume_summary.total_chunks, CHUNK_COUNT);
    assert_eq!(resume_summary.total_bytes, expected_bytes.len() as u64);
    assert!(resume_scheduler.is_finished());

    // 4. Assemble resumed stream and verify zero corrupt frames
    let output_file = temp.path.join("resumed_output.ts");
    let assembly_config = AssemblyConfig::new(
        StreamContainer::MpegTs,
        &staging_dir,
        CHUNK_COUNT,
        &output_file,
    );
    let summary = assemble_stream(&assembly_config).expect("assemble resumed stream");
    assert_eq!(summary.chunks_merged, CHUNK_COUNT);

    let assembled = std::fs::read(&output_file).expect("read resumed output");
    assert_eq!(assembled.len() % 188, 0);

    for (idx, packet) in assembled.chunks_exact(188).enumerate() {
        assert_eq!(
            packet[0], 0x47,
            "Resumed packet {idx} must start with sync byte 0x47 (zero corrupt frames)"
        );
    }

    let actual_hash = sha256(&assembled);
    let expected_hash = sha256(&expected_bytes);
    assert_eq!(
        actual_hash, expected_hash,
        "Resumed download must achieve 100% SHA-256 byte parity"
    );
    assert_eq!(assembled, expected_bytes);
    assert!(!staging_dir.exists(), "Staging dir purged on completion");
}

/// Scenario 5: Staging Directory Cleanup Lifecycle
/// - Validates complete staging directory purge upon verified successful assembly.
/// - Asserts staging directory preservation on assembly failure (missing intermediate parts).
#[test]
fn test_scenario_5_staging_directory_cleanup_lifecycle() {
    let temp = TempDirGuard::new("s5_staging_cleanup");
    let staging_dir = temp.path.join("output.ts.fetchtrek_parts_105");
    std::fs::create_dir_all(&staging_dir).expect("create staging dir");

    const TOTAL_CHUNKS: usize = 5;
    for i in 0..TOTAL_CHUNKS {
        let part_file = staging_dir.join(format!("part_{i:06}.part"));
        let mut f = File::create(&part_file).expect("create part");
        let pkt = create_valid_ts_packet(i as u8);
        f.write_all(&pkt).expect("write packet");
    }

    let output_file = temp.path.join("output.ts");
    let config = AssemblyConfig::new(
        StreamContainer::MpegTs,
        &staging_dir,
        TOTAL_CHUNKS,
        &output_file,
    );

    // 1. Success path: staging directory MUST be completely purged
    let summary = assemble_stream(&config).expect("assemble stream");
    assert_eq!(summary.chunks_merged, TOTAL_CHUNKS);
    assert!(output_file.is_file(), "Output file must exist");
    assert!(
        !staging_dir.exists(),
        "Staging directory MUST be completely purged after verified completion"
    );

    // 2. Failure path: missing chunk -> staging directory MUST be preserved
    let fail_staging = temp.path.join("fail.ts.fetchtrek_parts_106");
    std::fs::create_dir_all(&fail_staging).expect("create fail staging");

    // Write parts 0, 1, 3, 4 (part 2 missing)
    for i in [0, 1, 3, 4] {
        let part_file = fail_staging.join(format!("part_{i:06}.part"));
        let mut f = File::create(&part_file).expect("create part");
        let pkt = create_valid_ts_packet(i as u8);
        f.write_all(&pkt).expect("write packet");
    }

    let fail_output = temp.path.join("fail.ts");
    let fail_config = AssemblyConfig::new(
        StreamContainer::MpegTs,
        &fail_staging,
        TOTAL_CHUNKS,
        &fail_output,
    );
    let fail_res = assemble_stream(&fail_config);

    assert!(
        matches!(fail_res, Err(MediaError::SegmentMissing)),
        "Missing part must return SegmentMissing"
    );
    assert!(
        fail_staging.is_dir(),
        "Staging directory MUST be strictly preserved on failure so user does not lose progress"
    );
    assert!(fail_staging.join("part_000000.part").is_file());
    assert!(fail_staging.join("part_000001.part").is_file());
    assert!(fail_staging.join("part_000003.part").is_file());
    assert!(fail_staging.join("part_000004.part").is_file());
}
