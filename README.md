# FetchTrek Download Manager

[![CI](https://github.com/MrAyushBajpai/FetchTrek_Download_Manager/actions/workflows/ci.yml/badge.svg)](https://github.com/MrAyushBajpai/FetchTrek_Download_Manager/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust: 1.80+](https://img.shields.io/badge/Rust-1.80+-orange.svg)](https://www.rust-lang.org)

FetchTrek is a high-performance, modular download acceleration manager, media stream capture engine, and BitTorrent client built in Rust. It pairs line-rate parallel transfers and dynamic bisection with a clean, zero-telemetry architecture.

---

## Key Features

- **Multi-Connection HTTP/HTTPS Acceleration**: Dynamic range bisection, HTTP 206 `Content-Range` alignment, ETag pinning with `If-Range` validation, and zero-second positional disk merging.
- **Media Stream Capture**: Native parsing for HLS (M3U8) and MPEG-DASH (MPD), hardware-accelerated AES-128-CBC decryption, TS sync-byte alignment, and lossless stream remuxing.
- **BitTorrent P2P Distribution**: Bencode codec, BEP 0003 peer wire protocol, BEP 0015 binary UDP & HTTP trackers, rarest-first piece picking, and SHA-1 piece verification.
- **Local-First & Zero Telemetry**: Absolutely zero telemetry, phone-home beacons, or external tracking. All states and credentials remain local in SQLite WAL and OS hardware vaults.
- **Out-of-Process Native Architecture**: Headless daemon service (`fetchtrek-daemon`), lightweight CLI client (`fetchtrek-cli`), GPU-accelerated Slint desktop GUI (`fetchtrek-ui`), and Chromium Native Messaging bridge (`fetchtrek-host`).
- **Resilient Crash Recovery**: Atomic WAL transactions, transactional staging files (`.fetchtrek_temp_*`), on-disk size inspection, and automated cursor rewinding.

---

## Architecture Overview

```
+─────────────────────────────────────────────────────────────+
|                     FETCHTREK WORKSPACE                     |
+─────────────────────────────────────────────────────────────+
  apps/
    fetchtrek-daemon  : Headless background daemon orchestrator
    fetchtrek-ui      : Slint desktop workstation GUI
    fetchtrek-cli     : Command-line interface client
    fetchtrek-host    : Native Messaging host for browser extension
  crates/
    fetchtrek-core    : Download coordinator & dynamic scheduler
    fetchtrek-protocols: HTTP, HLS/DASH media, and BitTorrent protocols
    fetchtrek-storage : Positional sparse disk I/O & slab memory pool
    fetchtrek-persistence: SQLite WAL engine and transaction repository
    fetchtrek-ipc     : Fast binary/JSON IPC message frames & transports
    fetchtrek-types   : Canonical domain types, states, and errors
  extensions/
    chromium          : Manifest V3 download grabber & media sniffer
```

---

## System Requirements

- **Supported Operating Systems**:
  - Windows 10 / 11 / Server 2019+ (x86_64) - *Tier 1 Certified*
  - Linux: Ubuntu 22.04+, Debian, Fedora, Arch (x86_64) - *Tier 2*
  - macOS: Monterey 12+ (aarch64 / x86_64) - *Tier 2*
- **Memory**: 128 MB RAM minimum.
- **Storage**: 50 MB free disk space.
- **Toolchain (for building from source)**: Rust 1.80+ (stable), Cargo.

For detailed platform compatibility, consult [docs/PLATFORMS.md](docs/PLATFORMS.md).

---

## Building from Source

Ensure the latest stable Rust toolchain is installed:

```bash
# Clone the repository
git clone https://github.com/MrAyushBajpai/FetchTrek_Download_Manager.git
cd FetchTrek_Download_Manager

# Build all workspace crates in release mode
cargo build --release

# Run the complete test suite
cargo test --workspace
```

The compiled binaries will be available in `target/release/`:
- `fetchtrek-daemon`
- `fetchtrek-ui`
- `fetchtrek-cli`
- `fetchtrek-host`

---

## Quick Start & Usage

### 1. Launching the Background Daemon
The daemon runs as a local service listening on a secure, restricted IPC endpoint:

```bash
# Windows (starts named pipe: \\.\pipe\fetchtrek-daemon)
target/release/fetchtrek-daemon

# Custom database or download directory
target/release/fetchtrek-daemon --db-path ./data.db --download-dir ./downloads
```

### 2. Desktop User Interface
Launch the graphical workstation:

```bash
target/release/fetchtrek-ui

# Launch with sample visualization data (demo mode)
target/release/fetchtrek-ui --demo
```

### 3. Command Line Interface (CLI)

```bash
# Add an HTTP download with 8 parallel connections
fetchtrek-cli add https://releases.ubuntu.com/24.04/ubuntu-24.04-desktop-amd64.iso --connections 8

# Add a BitTorrent download from a magnet link or .torrent file
fetchtrek-cli add "magnet:?xt=urn:btih:..."

# List active downloads and real-time speeds
fetchtrek-cli list

# Pause, resume, or cancel a task
fetchtrek-cli pause <TASK_ID>
fetchtrek-cli resume <TASK_ID>
fetchtrek-cli cancel <TASK_ID> --delete-file
```

---

## Browser Extension Setup (Chromium)

The FetchTrek Chromium extension captures downloads directly from Google Chrome, Brave, Edge, and other Chromium-based browsers:

1. Open `chrome://extensions/` and enable **Developer mode**.
2. Click **Load unpacked** and select the `extensions/chromium` directory.
3. Note the generated Extension ID.
4. Register the Native Messaging Host manifest for your OS:
   - On Windows: Run `fetchtrek-host --register <EXTENSION_ID>` or write the manifest to `HKCU\Software\Google\Chrome\NativeMessagingHosts\com.fetchtrek.host`.
   - On Linux/macOS: Link the JSON manifest into `~/.config/google-chrome/NativeMessagingHosts/com.fetchtrek.host.json`.

---

## Security & Privacy Documentation

- **Zero-Telemetry Policy**: [docs/PRIVACY.md](docs/PRIVACY.md)
- **Threat Model & Boundaries**: [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md)
- **Supported Platforms & Tiers**: [docs/PLATFORMS.md](docs/PLATFORMS.md)
- **Third-Party Open Source Notices**: [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)

---

## License

FetchTrek is released under the [MIT License](LICENSE).
