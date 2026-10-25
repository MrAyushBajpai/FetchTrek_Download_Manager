# FetchTrek Privacy Policy & Zero-Telemetry Guarantee

**Version:** 1.0 (General Availability)  
**Effective Date:** October 2026  
**Audience:** Users, System Administrators, Security Auditors  

---

## 1. Zero-Telemetry Architecture

FetchTrek is engineered from the ground up under a strict **Zero-Telemetry Principle**. The application does not collect, track, transmit, or monetize any user data, download URLs, telemetry metrics, or operational logs.

### Formal Guarantees
1. **No External Network Calls**: FetchTrek initiates network connections *exclusively* to endpoints explicitly requested by the user (HTTP/HTTPS file downloads, HLS/DASH media stream sources, BitTorrent trackers, and BitTorrent swarm peers).
2. **Zero Third-Party SDKs**: No analytics platforms (e.g., Google Analytics, Mixpanel, Segment), error-reporting beacons (e.g., Sentry, Bugsnag, Crashlytics), or remote configuration services are bundled or linked into any binary crate.
3. **No Automatic Update Pings**: FetchTrek does not ping external update servers in the background. Software releases and package updates are delivered transparently via native OS package managers and GitHub releases.
4. **No Identity Profiling**: The application does not assign, store, or transmit machine GUIDs, hardware IDs, MAC addresses, or persistent telemetry identifiers.

---

## 2. Local-First Data Storage & Containment

All operational data remains strictly local to the user's workstation:

| Data Type | Storage Location | Security & Privacy Controls |
| :--- | :--- | :--- |
| **Download History & State** | SQLite Database (`fetchtrek.db`) | Local database operating in Write-Ahead Logging (WAL) mode. Bound to the user's application data directory. Never synced remotely. |
| **Credentials & Auth Tokens** | OS Secure Storage (DPAPI / Keychain / SecretService) | Passwords, proxy credentials, and private session cookies are encrypted using hardware/OS-level cryptographic vaults. |
| **Staging & Active Downloads** | User-configured Download Directory | Temporary data is written to hidden, session-unique staging files (`.fetchtrek_temp_*`). Existing user files are never overwritten or deleted on failure. |
| **IPC Sockets & Pipes** | OS IPC Namespace | Local-only Inter-Process Communication. On Windows, Named Pipes are restricted via Discretionary Access Control Lists (DACLs) to the calling user token. On Unix/macOS, domain sockets enforce strict `0700` POSIX permissions. |

---

## 3. Browser Extension Privacy Contract

The Chromium Native Messaging extension (`extensions/chromium`) adheres to the following privacy boundaries:

- **Local-Only Communication**: The extension interacts exclusively with the local `fetchtrek-host` binary via standard input/output (stdio) native messaging.
- **Selective Interception**: Downloads are intercepted only when user-defined URL criteria, minimum file sizes, and whitelist/blacklist filters match.
- **Sensitive Headers Filtering**: Session cookies and authorization tokens captured for download continuity are transferred directly to the local daemon over the secure IPC channel and are never dispatched elsewhere.

---

## 4. Verification & Auditability

FetchTrek is 100% open source. Users and independent security auditors can verify compliance by:
- Inspecting source code in public repositories.
- Monitoring network traffic using packet analyzers (e.g., Wireshark) during idle, download, and error states.
- Auditing local SQLite database files and disk writes.
