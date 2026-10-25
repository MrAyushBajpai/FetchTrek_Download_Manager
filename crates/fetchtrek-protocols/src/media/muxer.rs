use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use fetchtrek_types::MediaError;

use super::decrypt::decrypt_aes128_cbc;

/// Container formats supported for native stream assembly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamContainer {
    /// MPEG-2 Transport Stream (.ts) composed of sequential 188-byte packets.
    MpegTs,
    /// Fragmented MP4 (.mp4) composed of init box (ftyp + moov) followed by moof/mdat fragments.
    FragmentedMp4,
}

/// Configuration parameters for assembling discrete media chunks into a single media file.
#[derive(Debug, Clone)]
pub struct AssemblyConfig {
    pub container: StreamContainer,
    pub staging_dir: PathBuf,
    pub total_chunks: usize,
    pub init_segment_path: Option<PathBuf>,
    pub output_path: PathBuf,
    pub decryption_key: Option<[u8; 16]>,
    pub default_iv: Option<[u8; 16]>,
}

impl AssemblyConfig {
    pub fn new(
        container: StreamContainer,
        staging_dir: impl Into<PathBuf>,
        total_chunks: usize,
        output_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            container,
            staging_dir: staging_dir.into(),
            total_chunks,
            init_segment_path: None,
            output_path: output_path.into(),
            decryption_key: None,
            default_iv: None,
        }
    }

    pub fn with_init_segment(mut self, path: impl Into<PathBuf>) -> Self {
        self.init_segment_path = Some(path.into());
        self
    }

    pub fn with_decryption(mut self, key: [u8; 16], iv: Option<[u8; 16]>) -> Self {
        self.decryption_key = Some(key);
        self.default_iv = iv;
        self
    }
}

/// Summary metrics of a completed stream assembly operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssemblySummary {
    pub output_file: PathBuf,
    pub total_bytes: u64,
    pub chunks_merged: usize,
}

/// Assembles discrete staged chunks into a single playable container (.ts or .mp4).
///
/// Executes native byte concatenation with optional AES-128-CBC decryption and MPEG-TS
/// 188-byte sync byte verification. Staging directory and intermediate parts are cleanly purged
/// on success, and preserved on failure.
pub fn assemble_stream(config: &AssemblyConfig) -> Result<AssemblySummary, MediaError> {
    // 1. Verify all chunk files exist on disk before creating output file
    for idx in 0..config.total_chunks {
        let part_path = config.staging_dir.join(format!("part_{idx:06}.part"));
        if !part_path.is_file() {
            return Err(MediaError::SegmentMissing);
        }
    }

    // 2. Fragmented MP4 requires the ftyp + moov initialization segment
    if config.container == StreamContainer::FragmentedMp4 {
        let init_path = config
            .init_segment_path
            .as_ref()
            .ok_or(MediaError::SegmentMissing)?;
        if !init_path.is_file() {
            return Err(MediaError::SegmentMissing);
        }
    }

    // 3. Prepare output directory and buffered writer
    if let Some(parent) = config.output_path.parent() {
        std::fs::create_dir_all(parent).map_err(|_| MediaError::MalformedManifest)?;
    }

    let out_file = File::create(&config.output_path).map_err(|_| MediaError::MalformedManifest)?;
    let mut writer = BufWriter::with_capacity(64 * 1024, out_file);
    let mut total_bytes_written = 0u64;

    // 4. Write init segment if fMP4
    if config.container == StreamContainer::FragmentedMp4 {
        if let Some(ref init_path) = config.init_segment_path {
            let init_bytes = std::fs::read(init_path).map_err(|_| MediaError::SegmentMissing)?;
            writer
                .write_all(&init_bytes)
                .map_err(|_| MediaError::MalformedManifest)?;
            total_bytes_written += init_bytes.len() as u64;
        }
    }

    // 5. Sequentially assemble all chunks
    for idx in 0..config.total_chunks {
        let part_path = config.staging_dir.join(format!("part_{idx:06}.part"));
        let raw_bytes = std::fs::read(&part_path).map_err(|_| MediaError::SegmentMissing)?;

        // Decrypt if encryption key is configured
        let chunk_bytes = if let Some(key) = &config.decryption_key {
            let iv = config.default_iv.unwrap_or_else(|| {
                let mut derived = [0u8; 16];
                let seq = idx as u32;
                derived[12..16].copy_from_slice(&seq.to_be_bytes());
                derived
            });
            decrypt_aes128_cbc(&raw_bytes, key, &iv)?
        } else {
            raw_bytes
        };

        // MPEG-TS packet validation: 188-byte boundaries with 0x47 sync byte
        if config.container == StreamContainer::MpegTs {
            if chunk_bytes.is_empty() || !chunk_bytes.len().is_multiple_of(188) {
                return Err(MediaError::MalformedManifest);
            }
            for offset in (0..chunk_bytes.len()).step_by(188) {
                if chunk_bytes[offset] != 0x47 {
                    return Err(MediaError::MalformedManifest);
                }
            }
        }

        writer
            .write_all(&chunk_bytes)
            .map_err(|_| MediaError::MalformedManifest)?;
        total_bytes_written += chunk_bytes.len() as u64;
    }

    // 6. Flush and sync
    writer.flush().map_err(|_| MediaError::MalformedManifest)?;
    let file = writer
        .into_inner()
        .map_err(|_| MediaError::MalformedManifest)?;
    file.sync_all().map_err(|_| MediaError::MalformedManifest)?;

    // 7. Verify output file size matches bytes written
    let metadata = file.metadata().map_err(|_| MediaError::MalformedManifest)?;
    if metadata.len() != total_bytes_written {
        return Err(MediaError::MalformedManifest);
    }

    // 8. Clean up staging directory on success
    for idx in 0..config.total_chunks {
        let part_path = config.staging_dir.join(format!("part_{idx:06}.part"));
        let _ = std::fs::remove_file(part_path);
    }

    if let Some(ref init_path) = config.init_segment_path {
        if init_path.starts_with(&config.staging_dir) {
            let _ = std::fs::remove_file(init_path);
        }
    }
    let _ = std::fs::remove_dir_all(&config.staging_dir);

    Ok(AssemblySummary {
        output_file: config.output_path.clone(),
        total_bytes: total_bytes_written,
        chunks_merged: config.total_chunks,
    })
}

/// Checks if ffmpeg is available in the system PATH.
pub fn is_ffmpeg_available() -> bool {
    std::process::Command::new("ffmpeg")
        .arg("-version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Remuxes separate video and audio tracks into a single container via ffmpeg stream copy (-c copy).
///
/// If ffmpeg is unavailable, returns `MediaError::FFmpegChildFailed` while strictly preserving
/// both video and audio intermediate files.
pub fn remux_dual_streams(
    video_path: &Path,
    audio_path: &Path,
    output_path: &Path,
) -> Result<(), MediaError> {
    if !video_path.is_file() || !audio_path.is_file() {
        return Err(MediaError::SegmentMissing);
    }

    if !is_ffmpeg_available() {
        return Err(MediaError::FFmpegChildFailed);
    }

    if let Some(parent) = output_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    let status = std::process::Command::new("ffmpeg")
        .arg("-y")
        .arg("-i")
        .arg(video_path)
        .arg("-i")
        .arg(audio_path)
        .arg("-c")
        .arg("copy")
        .arg(output_path)
        .status()
        .map_err(|_| MediaError::FFmpegChildFailed)?;

    if status.success() {
        let _ = std::fs::remove_file(video_path);
        let _ = std::fs::remove_file(audio_path);
        Ok(())
    } else {
        Err(MediaError::FFmpegChildFailed)
    }
}
