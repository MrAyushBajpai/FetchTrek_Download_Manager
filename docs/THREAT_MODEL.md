# FetchTrek Threat Model & Security Architecture

**Version:** 1.0 (General Availability)  
**Standard:** STRIDE & Defense-in-Depth  
**Audience:** Security Engineers, System Integrators, Contributors  

---

## 1. System Architecture & Trust Zones

FetchTrek is divided into isolated processes and privilege boundaries:

```
+-------------------------------------------------------------+
|                     UNTRUSTED NETWORK                       |
|   HTTP/HTTPS Servers | HLS/DASH CDNs | BitTorrent Swarm     |
+-------------------------------------------------------------+
                              │ (TLS, TCP, UDP)
                              ▼
+─────────────────────────────────────────────────────────────+
|               FETCHTREK DAEMON SERVICE PROCESS              |
|  - Protocol Parsers (Bencode, M3U8, MPD XML, HTTP Headers)  |
|  - Memory Arena & Positional Disk Storage Mapper           |
|  - Cryptographic Verification Engine (SHA-1, AES-128-CBC)   |
|  - SQLite WAL Persistence Engine                            |
+─────────────────────────────────────────────────────────────+
                              ▲ (Local IPC Only)
            ┌─────────────────┴─────────────────┐
            │ Named Pipe DACL / POSIX 0700      │ Stdio Length-Prefixed
            ▼                                   ▼
+───────────────────────+           +─────────────────────────+
|     DESKTOP UI        |           |  NATIVE MESSAGING HOST  |
|  Slint Native GUI     |           |  apps/fetchtrek-host    |
+───────────────────────+           +─────────────────────────+
                                                ▲
                                                │ Chrome Stdio Protocol
                                                ▼
                                    +─────────────────────────+
                                    |    BROWSER EXTENSION    |
                                    |   extensions/chromium   |
                                    +─────────────────────────+
```

---

## 2. Threat Analysis & Mitigations

### A. Malicious BitTorrent Swarm Inputs (BEP 0003, BEP 0015)
- **Threat (Path Traversal - FT-QA-001)**: Malicious `.torrent` files or magnet metadata containing `../`, absolute Windows drive roots (`C:\`), or UNC paths (`\\server\share`) designed to overwrite system files.
  - *Mitigation*: The `TorrentMeta` parser strictly rejects absolute paths, null bytes, reserved Windows device names (`CON`, `PRN`, `AUX`, `NUL`, `COM1-9`, `LPT1-9`), and traversal sequences before any filesystem descriptors are allocated.
- **Threat (Poisoned Blocks & Swarm Sybil Attacks - FT-QA-005)**: Malicious peers injecting corrupt or misaligned payload blocks.
  - *Mitigation*: Unsolicited or misaligned blocks (not aligned to 16 KiB) are discarded immediately. Incoming blocks are assembled exclusively in memory. The entire piece payload is cryptographically validated against the torrent's SHA-1 hash before being committed to persistent storage.

### B. Malicious Media Streams (HLS & MPEG-DASH - FT-QA-016)
- **Threat (Memory Exhaustion / Parser Denial of Service)**: Infinite or circular playlists, manifests with extreme durations (`mediaPresentationDuration="PT100000000S"`), or infinitesimal segment durations (`timescale="0" duration="0"`) designed to cause gigabyte heap allocations.
  - *Mitigation*: Strict architectural ceilings are enforced:
    - Maximum manifest size: `10 MiB` (`MAX_MANIFEST_BYTES`).
    - Maximum media variants: `1,000` (`MAX_MEDIA_VARIANTS`).
    - Maximum media segments: `50,000` (`MAX_MEDIA_SEGMENTS`).
    - Manifests violating these bounds or specifying non-positive segment durations fail parsing with `MediaError::MalformedManifest`.

### C. Local IPC Endpoint Hijacking (FT-QA-002)
- **Threat (Elevation of Privilege / Unauthorized Control)**: Other non-privileged user accounts or local processes injecting download commands or exfiltrating download metadata.
  - *Mitigation*:
    - **Windows**: The daemon creates Named Pipes with custom Security Descriptors (DACLs) granting access *exclusively* to the current user's security token and Administrators.
    - **Unix / macOS**: Domain sockets are created inside directory paths with strict `0700` permissions and verified to reject following pre-existing symlinks or foreign sockets (`O_NOFOLLOW`).

### D. Filesystem Pollution & Pre-existing File Safety (FT-QA-007)
- **Threat (Accidental Data Destruction)**: A download task pointing to an existing user file (e.g., `Documents/Report.pdf`) might wipe or truncate pre-existing user data upon failure or cancellation.
  - *Mitigation*: Transfers write exclusively to session-scoped temporary staging files (`.fetchtrek_temp_<uuid>`). Existing target files are preserved untouched until a download completes with verified integrity, after which an atomic filesystem rename replaces the target.

### E. Native Messaging Isolation
- **Threat (Arbitrary JSON / Buffer Overflow via Browser Extension)**: Compromised browser tabs or malicious extensions passing malformed payloads to `fetchtrek-host`.
  - *Mitigation*: The host enforces a strict 4-byte native messaging length prefix bounded at 1 MiB (`MAX_MESSAGE_BYTES`), validates incoming JSON against strict Rust serde data structures, and executes with dropped privileges.
