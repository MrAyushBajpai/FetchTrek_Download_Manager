# FetchTrek Supported Platform Matrix

**Version:** 1.0 (General Availability)  
**Standard:** Tiered Platform Support Model  

---

## Tier Definition

- **Tier 1 (Primary / Certified)**: Automated CI validation on every commit, full hardware acceleration, OS credential vault integration, and native GUI packaging. Guaranteed zero degradation.
- **Tier 2 (Supported)**: Regular CI validation, functional parity for daemon and CLI, native socket IPC and credential management.
- **Tier 3 (Community / Experimental)**: Source builds supported, community-maintained packaging.

---

## Platform Support Matrix

| OS Platform | Architecture | Tier | IPC Transport | Storage Backend | Credential Vault | UI Backend |
| :--- | :--- | :---: | :--- | :--- | :--- | :--- |
| **Windows 10 / 11** | `x86_64` | **Tier 1** | Named Pipe with DACL | Win32 Direct IOCP, Sparse (`FSCTL_SET_SPARSE`) | Windows Credential Manager / DPAPI | Slint Win32 / Direct3D / Software |
| **Windows Server 2019+** | `x86_64` | **Tier 1** | Named Pipe with DACL | Win32 Direct IOCP, Sparse | DPAPI | Headless Daemon & CLI |
| **Ubuntu Linux 22.04+** | `x86_64` | **Tier 2** | Unix Domain Socket (`0700`) | POSIX AIO / `fallocate` sparse preallocation | SecretService (`libsecret`) | Slint Wayland / X11 |
| **Debian / Fedora / Arch** | `x86_64` | **Tier 2** | Unix Domain Socket (`0700`) | POSIX AIO / `fallocate` | SecretService | Slint Wayland / X11 |
| **macOS 12+ (Monterey+)** | `aarch64` (Apple Silicon) | **Tier 2** | POSIX Domain Socket (`0700`) | POSIX AIO / `fstore_t` preallocation | macOS Keychain Services | Slint Cocoa / Metal |
| **macOS 12+ (Monterey+)** | `x86_64` (Intel) | **Tier 2** | POSIX Domain Socket (`0700`) | POSIX AIO / `fstore_t` preallocation | macOS Keychain Services | Slint Cocoa / Metal |

---

## Minimum System Requirements

- **RAM**: 128 MB minimum (Daemon idle: < 25 MB; Active download with 256 slabs: < 60 MB).
- **Disk**: 50 MB available storage for binaries and SQLite WAL persistence.
- **CPU**: Any x86_64 or aarch64 processor supporting SSE4.2 / ARMv8 NEON (for hardware SHA-1/SHA-256 and AES acceleration).
