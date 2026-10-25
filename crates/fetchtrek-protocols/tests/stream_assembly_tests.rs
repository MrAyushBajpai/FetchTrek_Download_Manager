use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use fetchtrek_protocols::{
    assemble_stream, decrypt_aes128_cbc, encrypt_aes128_cbc, remux_dual_streams, AssemblyConfig,
    StreamContainer,
};
use fetchtrek_types::MediaError;

struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(prefix: &str) -> Self {
        let unique = format!(
            "fetchtrek_muxer_test_{}_{}_{}",
            prefix,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(unique);
        let _ = std::fs::create_dir_all(&path);
        Self { path }
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

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

#[test]
fn test_mpeg_ts_assembly_and_sync_byte_validation() {
    let temp = TempDirGuard::new("ts_assembly");
    let staging_dir = temp.path.join("staging");
    std::fs::create_dir_all(&staging_dir).expect("create staging");

    const TOTAL_CHUNKS: usize = 10;
    const PACKETS_PER_CHUNK: usize = 5;
    let mut expected_bytes = Vec::new();

    for chunk_idx in 0..TOTAL_CHUNKS {
        let part_file = staging_dir.join(format!("part_{chunk_idx:06}.part"));
        let mut f = File::create(&part_file).expect("create part file");
        for p in 0..PACKETS_PER_CHUNK {
            let pkt = create_valid_ts_packet((chunk_idx * 10 + p) as u8);
            f.write_all(&pkt).expect("write packet");
            expected_bytes.extend_from_slice(&pkt);
        }
    }

    let output_path = temp.path.join("output.ts");
    let config = AssemblyConfig::new(
        StreamContainer::MpegTs,
        &staging_dir,
        TOTAL_CHUNKS,
        &output_path,
    );

    let summary = assemble_stream(&config).expect("assemble stream must succeed");
    assert_eq!(summary.chunks_merged, TOTAL_CHUNKS);
    assert_eq!(
        summary.total_bytes,
        (TOTAL_CHUNKS * PACKETS_PER_CHUNK * 188) as u64
    );
    assert_eq!(summary.output_file, output_path);

    // Verify output file continuity and byte parity
    let assembled = std::fs::read(&output_path).expect("read assembled output");
    assert_eq!(assembled, expected_bytes);

    // Validate sync byte on all 188-byte boundaries
    assert_eq!(assembled.len() % 188, 0);
    for offset in (0..assembled.len()).step_by(188) {
        assert_eq!(assembled[offset], 0x47, "sync byte at offset {offset}");
    }

    // Verify staging directory was purged
    assert!(
        !staging_dir.exists(),
        "staging dir must be purged on success"
    );
}

#[test]
fn test_mpeg_ts_corrupted_sync_byte_rejection() {
    let temp = TempDirGuard::new("ts_corrupted");
    let staging_dir = temp.path.join("staging");
    std::fs::create_dir_all(&staging_dir).expect("create staging");

    const TOTAL_CHUNKS: usize = 3;

    for chunk_idx in 0..TOTAL_CHUNKS {
        let part_file = staging_dir.join(format!("part_{chunk_idx:06}.part"));
        let mut f = File::create(&part_file).expect("create part file");
        let mut pkt = create_valid_ts_packet(chunk_idx as u8);
        if chunk_idx == 1 {
            // Corrupt sync byte
            pkt[0] = 0x00;
        }
        f.write_all(&pkt).expect("write packet");
    }

    let output_path = temp.path.join("output.ts");
    let config = AssemblyConfig::new(
        StreamContainer::MpegTs,
        &staging_dir,
        TOTAL_CHUNKS,
        &output_path,
    );

    let res = assemble_stream(&config);
    assert!(
        matches!(res, Err(MediaError::MalformedManifest)),
        "corrupted sync byte must return MalformedManifest"
    );

    // Intermediate parts must be preserved on error
    assert!(
        staging_dir.exists(),
        "staging dir must be preserved on failure"
    );
    assert!(staging_dir.join("part_000000.part").exists());
    assert!(staging_dir.join("part_000001.part").exists());
    assert!(staging_dir.join("part_000002.part").exists());
}

#[test]
fn test_fmp4_init_and_fragment_concatenation() {
    let temp = TempDirGuard::new("fmp4_assembly");
    let staging_dir = temp.path.join("staging");
    std::fs::create_dir_all(&staging_dir).expect("create staging");

    // Synthetic ftyp + moov init box
    let init_path = staging_dir.join("init.mp4");
    let init_payload = b"....ftypisom....moov....mvhd....".to_vec();
    std::fs::write(&init_path, &init_payload).expect("write init.mp4");

    const TOTAL_CHUNKS: usize = 5;
    let mut expected_bytes = init_payload.clone();

    for chunk_idx in 0..TOTAL_CHUNKS {
        let part_file = staging_dir.join(format!("part_{chunk_idx:06}.part"));
        let fragment_data =
            format!("moof_fragment_{chunk_idx}__mdat_data_{chunk_idx}").into_bytes();
        std::fs::write(&part_file, &fragment_data).expect("write fragment");
        expected_bytes.extend_from_slice(&fragment_data);
    }

    let output_path = temp.path.join("movie.mp4");
    let config = AssemblyConfig::new(
        StreamContainer::FragmentedMp4,
        &staging_dir,
        TOTAL_CHUNKS,
        &output_path,
    )
    .with_init_segment(&init_path);

    let summary = assemble_stream(&config).expect("assemble fmp4 must succeed");
    assert_eq!(summary.chunks_merged, TOTAL_CHUNKS);
    assert_eq!(summary.total_bytes, expected_bytes.len() as u64);

    let assembled = std::fs::read(&output_path).expect("read movie.mp4");
    assert_eq!(assembled, expected_bytes);
    assert!(
        !staging_dir.exists(),
        "staging dir must be purged on success"
    );
}

#[test]
fn test_aes128_cbc_decryption_and_assembly() {
    let temp = TempDirGuard::new("aes_assembly");
    let staging_dir = temp.path.join("staging");
    std::fs::create_dir_all(&staging_dir).expect("create staging");

    let key: [u8; 16] = [
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f,
    ];
    let iv: [u8; 16] = [
        0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae,
        0xaf,
    ];

    const TOTAL_CHUNKS: usize = 4;
    let mut expected_plaintext = Vec::new();

    for chunk_idx in 0..TOTAL_CHUNKS {
        let part_file = staging_dir.join(format!("part_{chunk_idx:06}.part"));

        // Generate 2 valid TS packets as plaintext
        let mut pt = Vec::new();
        pt.extend_from_slice(&create_valid_ts_packet(chunk_idx as u8));
        pt.extend_from_slice(&create_valid_ts_packet((chunk_idx + 10) as u8));
        expected_plaintext.extend_from_slice(&pt);

        // Encrypt with AES-128-CBC and PKCS#7 padding
        let ciphertext = encrypt_aes128_cbc(&pt, &key, &iv);
        assert_eq!(ciphertext.len() % 16, 0);

        // Verify decryption round trip standalone
        let decrypted = decrypt_aes128_cbc(&ciphertext, &key, &iv).expect("decrypt round-trip");
        assert_eq!(decrypted, pt);

        std::fs::write(&part_file, &ciphertext).expect("write encrypted part");
    }

    let output_path = temp.path.join("decrypted_output.ts");
    let config = AssemblyConfig::new(
        StreamContainer::MpegTs,
        &staging_dir,
        TOTAL_CHUNKS,
        &output_path,
    )
    .with_decryption(key, Some(iv));

    let summary = assemble_stream(&config).expect("assemble encrypted stream must succeed");
    assert_eq!(summary.chunks_merged, TOTAL_CHUNKS);
    assert_eq!(summary.total_bytes, expected_plaintext.len() as u64);

    let assembled = std::fs::read(&output_path).expect("read assembled output");
    assert_eq!(assembled, expected_plaintext);
    assert!(
        !staging_dir.exists(),
        "staging dir must be purged on success"
    );
}

#[test]
fn test_staging_directory_cleanup_on_success() {
    let temp = TempDirGuard::new("cleanup_success");
    let staging_dir = temp.path.join("scratch_staging");
    std::fs::create_dir_all(&staging_dir).expect("create staging");

    for i in 0..3 {
        let part = staging_dir.join(format!("part_{i:06}.part"));
        let pkt = create_valid_ts_packet(i as u8);
        std::fs::write(part, pkt).expect("write part");
    }

    let output_path = temp.path.join("result.ts");
    let config = AssemblyConfig::new(StreamContainer::MpegTs, &staging_dir, 3, &output_path);

    let res = assemble_stream(&config);
    assert!(res.is_ok());
    assert!(!staging_dir.exists(), "staging directory must not exist");
    assert!(output_path.exists(), "output file must exist");
}

#[test]
fn test_staging_directory_preservation_on_error() {
    let temp = TempDirGuard::new("cleanup_error");
    let staging_dir = temp.path.join("scratch_staging");
    std::fs::create_dir_all(&staging_dir).expect("create staging");

    // Only create parts 0 and 1, but specify 4 chunks -> SegmentMissing
    for i in 0..2 {
        let part = staging_dir.join(format!("part_{i:06}.part"));
        let pkt = create_valid_ts_packet(i as u8);
        std::fs::write(&part, pkt).expect("write part");
    }

    let output_path = temp.path.join("result.ts");
    let config = AssemblyConfig::new(StreamContainer::MpegTs, &staging_dir, 4, &output_path);

    let res = assemble_stream(&config);
    assert!(matches!(res, Err(MediaError::SegmentMissing)));
    assert!(staging_dir.exists(), "staging dir must be preserved");
    assert!(staging_dir.join("part_000000.part").exists());
    assert!(staging_dir.join("part_000001.part").exists());
}

#[test]
fn test_dual_stream_remuxing_fallback() {
    let temp = TempDirGuard::new("dual_remux");
    let video_path = temp.path.join("video.mp4");
    let audio_path = temp.path.join("audio.m4a");
    let output_path = temp.path.join("muxed.mp4");

    std::fs::write(&video_path, b"video_track_data").expect("write video");
    std::fs::write(&audio_path, b"audio_track_data").expect("write audio");

    let res = remux_dual_streams(&video_path, &audio_path, &output_path);

    if fetchtrek_protocols::is_ffmpeg_available() {
        // If ffmpeg is installed, check that remux succeeded or returned ffmpeg status
        if res.is_ok() {
            assert!(output_path.exists());
        }
    } else {
        // If ffmpeg is absent, files must be strictly preserved
        assert!(matches!(res, Err(MediaError::FFmpegChildFailed)));
        assert!(
            video_path.exists(),
            "video must be preserved when ffmpeg is absent"
        );
        assert!(
            audio_path.exists(),
            "audio must be preserved when ffmpeg is absent"
        );
    }
}
