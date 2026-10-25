use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fetchtrek_host::manifest::{
    create_chromium_manifest, create_firefox_manifest, normalize_path, ChromiumHostManifest,
    FirefoxHostManifest, HOST_DESCRIPTION, HOST_NAME, HOST_TYPE,
};
use fetchtrek_host::registration::{
    check_manifest_in_dir, parse_reg_query_default_value, register_manifest_in_dir,
    unregister_manifest_in_dir, BrowserTarget,
};
use fetchtrek_host::{parse_cli_command, CliCommand};

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
            "fetchtrek_reg_test_{}_{}_{}",
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

#[test]
fn test_chromium_manifest_schema() {
    let host_path = Path::new(r"C:\Program Files\FetchTrek\fetchtrek-host.exe");
    let origins = vec!["chrome-extension://abcdefghijklmnopqrstuvwxyz/".to_string()];
    let manifest = create_chromium_manifest(host_path, &origins);

    assert_eq!(manifest.name, HOST_NAME);
    assert_eq!(manifest.description, HOST_DESCRIPTION);
    assert_eq!(
        manifest.path,
        r"C:\Program Files\FetchTrek\fetchtrek-host.exe"
    );
    assert_eq!(manifest.type_, HOST_TYPE);
    assert_eq!(manifest.allowed_origins, origins);

    let json = serde_json::to_string_pretty(&manifest).expect("serialize chromium manifest");
    assert!(json.contains(r#""name": "com.fetchtrek.downloadmanager""#));
    assert!(json.contains(r#""description": "FetchTrek Download Manager Native Messaging Host""#));
    assert!(json.contains(r#""type": "stdio""#));
    assert!(json.contains(r#""allowed_origins": ["#));
    assert!(!json.contains("type_"));

    let deserialized: ChromiumHostManifest =
        serde_json::from_str(&json).expect("deserialize chromium manifest");
    assert_eq!(manifest, deserialized);
}

#[test]
fn test_firefox_manifest_schema() {
    let host_path = Path::new("/usr/local/bin/fetchtrek-host");
    let extensions = vec!["fetchtrek@fetchtrek.org".to_string()];
    let manifest = create_firefox_manifest(host_path, &extensions);

    assert_eq!(manifest.name, HOST_NAME);
    assert_eq!(manifest.description, HOST_DESCRIPTION);
    assert_eq!(manifest.path, "/usr/local/bin/fetchtrek-host");
    assert_eq!(manifest.type_, HOST_TYPE);
    assert_eq!(manifest.allowed_extensions, extensions);

    let json = serde_json::to_string_pretty(&manifest).expect("serialize firefox manifest");
    assert!(json.contains(r#""name": "com.fetchtrek.downloadmanager""#));
    assert!(json.contains(r#""type": "stdio""#));
    assert!(json.contains(r#""allowed_extensions": ["#));
    assert!(!json.contains("allowed_origins"));

    let deserialized: FirefoxHostManifest =
        serde_json::from_str(&json).expect("deserialize firefox manifest");
    assert_eq!(manifest, deserialized);
}

#[test]
fn test_path_normalization_and_windows_backslash_escaping() {
    let raw_unc = Path::new(r"\\?\C:\Users\tester\AppData\Local\FetchTrek\fetchtrek-host.exe");
    let normalized = normalize_path(raw_unc);
    assert_eq!(
        normalized,
        r"C:\Users\tester\AppData\Local\FetchTrek\fetchtrek-host.exe"
    );

    let manifest = create_chromium_manifest(Path::new(&normalized), &[]);
    let json = serde_json::to_string(&manifest).expect("serialize to string");

    // In JSON, backslashes must be escaped with double backslashes
    assert!(json.contains(r"C:\\Users\\tester\\AppData\\Local\\FetchTrek\\fetchtrek-host.exe"));

    let deserialized: ChromiumHostManifest =
        serde_json::from_str(&json).expect("deserialize backslash path");
    assert_eq!(
        deserialized.path,
        r"C:\Users\tester\AppData\Local\FetchTrek\fetchtrek-host.exe"
    );
}

#[test]
fn test_mock_filesystem_registration_and_cleanup() {
    let temp = TempDirGuard::new("fs_reg");
    let bin_path = temp.path.join("fetchtrek-host.exe");
    std::fs::write(&bin_path, b"mock-binary").expect("create mock binary");

    let manifest = create_chromium_manifest(&bin_path, &[]);
    let manifest_json = serde_json::to_string_pretty(&manifest).expect("serialize manifest");

    let manifest_dir = temp.path.join("NativeMessagingHosts");
    let registered_path = register_manifest_in_dir(
        &manifest_dir,
        "com.fetchtrek.downloadmanager.json",
        &manifest_json,
    )
    .expect("register in dir");

    assert!(registered_path.exists());

    // Check status
    let (registered, path_opt, valid) =
        check_manifest_in_dir(&manifest_dir, "com.fetchtrek.downloadmanager.json");
    assert!(registered);
    assert_eq!(path_opt, Some(registered_path.clone()));
    assert!(valid, "Manifest should be valid since binary exists");

    // Unregister
    let removed = unregister_manifest_in_dir(&manifest_dir, "com.fetchtrek.downloadmanager.json")
        .expect("unregister from dir");
    assert!(removed);
    assert!(!registered_path.exists());

    let (registered_after, _, valid_after) =
        check_manifest_in_dir(&manifest_dir, "com.fetchtrek.downloadmanager.json");
    assert!(!registered_after);
    assert!(!valid_after);
}

#[test]
fn test_cli_argument_parsing() {
    // 1. Stdio loop default
    assert_eq!(parse_cli_command(&[]).unwrap(), CliCommand::StdioLoop);

    // 2. Status
    assert_eq!(parse_cli_command(&["status"]).unwrap(), CliCommand::Status);

    // 3. Version
    assert_eq!(
        parse_cli_command(&["--version"]).unwrap(),
        CliCommand::Version
    );
    assert_eq!(parse_cli_command(&["-v"]).unwrap(), CliCommand::Version);

    // 4. Ping daemon
    assert_eq!(
        parse_cli_command(&["--ping-daemon"]).unwrap(),
        CliCommand::PingDaemon
    );

    // 5. Help
    assert_eq!(parse_cli_command(&["help"]).unwrap(), CliCommand::Help);
    assert_eq!(parse_cli_command(&["--help"]).unwrap(), CliCommand::Help);
    assert_eq!(parse_cli_command(&["-h"]).unwrap(), CliCommand::Help);

    // 6. Register defaults
    assert_eq!(
        parse_cli_command(&["register"]).unwrap(),
        CliCommand::Register {
            browser: BrowserTarget::All,
            extension_id: None,
            host_path: None,
        }
    );

    // 7. Register specific browser and extension ID
    assert_eq!(
        parse_cli_command(&[
            "register",
            "--browser",
            "chrome",
            "--extension-id",
            "abcdef123456"
        ])
        .unwrap(),
        CliCommand::Register {
            browser: BrowserTarget::Chrome,
            extension_id: Some("abcdef123456".to_string()),
            host_path: None,
        }
    );

    // 8. Register with host-path override
    assert_eq!(
        parse_cli_command(&[
            "register",
            "--browser",
            "firefox",
            "--host-path",
            r"C:\Custom\host.exe"
        ])
        .unwrap(),
        CliCommand::Register {
            browser: BrowserTarget::Firefox,
            extension_id: None,
            host_path: Some(PathBuf::from(r"C:\Custom\host.exe")),
        }
    );

    // 9. Unregister default
    assert_eq!(
        parse_cli_command(&["unregister"]).unwrap(),
        CliCommand::Unregister {
            browser: BrowserTarget::All
        }
    );

    // 10. Unregister specific
    assert_eq!(
        parse_cli_command(&["unregister", "--browser", "edge"]).unwrap(),
        CliCommand::Unregister {
            browser: BrowserTarget::Edge
        }
    );

    // 11. Error cases
    assert!(parse_cli_command(&["foobar"]).is_err());
    assert!(parse_cli_command(&["register", "--browser"]).is_err());
    assert!(parse_cli_command(&["register", "--extension-id"]).is_err());
    assert!(parse_cli_command(&["register", "--host-path"]).is_err());
    assert!(parse_cli_command(&["unregister", "--unknown"]).is_err());
}

#[test]
fn test_reg_query_output_parsing() {
    let mock_reg_output = r#"
HKEY_CURRENT_USER\Software\Google\Chrome\NativeMessagingHosts\com.fetchtrek.downloadmanager
    (Default)    REG_SZ    C:\Users\test user\AppData\Local\FetchTrek\manifests\com.fetchtrek.downloadmanager.json
"#;

    let parsed = parse_reg_query_default_value(mock_reg_output);
    assert_eq!(
        parsed,
        Some(PathBuf::from(
            r"C:\Users\test user\AppData\Local\FetchTrek\manifests\com.fetchtrek.downloadmanager.json"
        ))
    );

    let empty_output = "ERROR: The system was unable to find the specified registry key or value.";
    assert_eq!(parse_reg_query_default_value(empty_output), None);
}
