use std::path::{Path, PathBuf};
use std::str::FromStr;

use crate::error::HostError;
use crate::manifest::{
    create_chromium_manifest, create_firefox_manifest, write_manifest_to_path,
    ChromiumHostManifest, FirefoxHostManifest, HOST_NAME,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowserTarget {
    Chrome,
    Edge,
    Brave,
    Firefox,
    All,
}

impl BrowserTarget {
    pub fn concrete_targets(&self) -> Vec<BrowserTarget> {
        match self {
            BrowserTarget::All => vec![
                BrowserTarget::Chrome,
                BrowserTarget::Edge,
                BrowserTarget::Brave,
                BrowserTarget::Firefox,
            ],
            target => vec![*target],
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            BrowserTarget::Chrome => "chrome",
            BrowserTarget::Edge => "edge",
            BrowserTarget::Brave => "brave",
            BrowserTarget::Firefox => "firefox",
            BrowserTarget::All => "all",
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            BrowserTarget::Chrome => "Google Chrome",
            BrowserTarget::Edge => "Microsoft Edge",
            BrowserTarget::Brave => "Brave Browser",
            BrowserTarget::Firefox => "Mozilla Firefox",
            BrowserTarget::All => "All Supported Browsers",
        }
    }

    pub fn is_firefox(&self) -> bool {
        matches!(self, BrowserTarget::Firefox)
    }
}

impl FromStr for BrowserTarget {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "chrome" => Ok(BrowserTarget::Chrome),
            "edge" => Ok(BrowserTarget::Edge),
            "brave" => Ok(BrowserTarget::Brave),
            "firefox" => Ok(BrowserTarget::Firefox),
            "all" => Ok(BrowserTarget::All),
            other => Err(format!(
                "Unknown browser target: '{other}'. Expected chrome, edge, brave, firefox, or all."
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetStatus {
    pub browser: BrowserTarget,
    pub registered: bool,
    pub manifest_path: Option<PathBuf>,
    pub valid: bool,
}

pub fn default_manifest_dir() -> PathBuf {
    #[cfg(target_os = "windows")]
    {
        if let Ok(local_app_data) = std::env::var("LOCALAPPDATA") {
            return PathBuf::from(local_app_data)
                .join("FetchTrek")
                .join("manifests");
        }
        if let Ok(user_profile) = std::env::var("USERPROFILE") {
            return PathBuf::from(user_profile)
                .join("AppData")
                .join("Local")
                .join("FetchTrek")
                .join("manifests");
        }
        PathBuf::from(r"C:\ProgramData\FetchTrek\manifests")
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home)
                .join("Library")
                .join("Application Support")
                .join("FetchTrek")
                .join("manifests");
        }
        PathBuf::from("/Library/Application Support/FetchTrek/manifests")
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        if let Ok(data_home) = std::env::var("XDG_DATA_HOME") {
            return PathBuf::from(data_home).join("fetchtrek").join("manifests");
        }
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home)
                .join(".local")
                .join("share")
                .join("fetchtrek")
                .join("manifests");
        }
        PathBuf::from("/tmp/fetchtrek/manifests")
    }
}

pub fn windows_registry_key(target: BrowserTarget) -> Option<&'static str> {
    match target {
        BrowserTarget::Chrome => {
            Some(r"Software\Google\Chrome\NativeMessagingHosts\com.fetchtrek.downloadmanager")
        }
        BrowserTarget::Edge => {
            Some(r"Software\Microsoft\Edge\NativeMessagingHosts\com.fetchtrek.downloadmanager")
        }
        BrowserTarget::Brave => Some(
            r"Software\BraveSoftware\Brave-Browser\NativeMessagingHosts\com.fetchtrek.downloadmanager",
        ),
        BrowserTarget::Firefox => {
            Some(r"Software\Mozilla\NativeMessagingHosts\com.fetchtrek.downloadmanager")
        }
        BrowserTarget::All => None,
    }
}

pub fn unix_browser_manifest_dir(target: BrowserTarget) -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let home = std::env::var("HOME").ok()?;
        let app_support = PathBuf::from(home)
            .join("Library")
            .join("Application Support");
        match target {
            BrowserTarget::Chrome => Some(
                app_support
                    .join("Google")
                    .join("Chrome")
                    .join("NativeMessagingHosts"),
            ),
            BrowserTarget::Edge => Some(
                app_support
                    .join("Microsoft Edge")
                    .join("NativeMessagingHosts"),
            ),
            BrowserTarget::Brave => Some(
                app_support
                    .join("BraveSoftware")
                    .join("Brave-Browser")
                    .join("NativeMessagingHosts"),
            ),
            BrowserTarget::Firefox => {
                Some(app_support.join("Mozilla").join("NativeMessagingHosts"))
            }
            BrowserTarget::All => None,
        }
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    {
        let home = std::env::var("HOME").ok()?;
        let config = match std::env::var("XDG_CONFIG_HOME") {
            Ok(c) => PathBuf::from(c),
            Err(_) => PathBuf::from(&home).join(".config"),
        };
        match target {
            BrowserTarget::Chrome => {
                Some(config.join("google-chrome").join("NativeMessagingHosts"))
            }
            BrowserTarget::Edge => Some(config.join("microsoft-edge").join("NativeMessagingHosts")),
            BrowserTarget::Brave => Some(
                config
                    .join("BraveSoftware")
                    .join("Brave-Browser")
                    .join("NativeMessagingHosts"),
            ),
            BrowserTarget::Firefox => Some(
                PathBuf::from(home)
                    .join(".mozilla")
                    .join("native-messaging-hosts"),
            ),
            BrowserTarget::All => None,
        }
    }
    #[cfg(target_os = "windows")]
    {
        let _ = target;
        None
    }
}

pub fn resolve_host_exe_path() -> Result<PathBuf, HostError> {
    let exe = std::env::current_exe().map_err(HostError::Io)?;
    #[cfg(target_os = "windows")]
    {
        let s = exe.to_string_lossy();
        if let Some(stripped) = s.strip_prefix(r"\\?\") {
            return Ok(PathBuf::from(stripped));
        }
    }
    Ok(exe)
}

pub fn parse_reg_query_default_value(output: &str) -> Option<PathBuf> {
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.contains("REG_SZ") {
            if let Some(pos) = trimmed.find("REG_SZ") {
                let val = trimmed[pos + 6..].trim();
                if !val.is_empty() {
                    return Some(PathBuf::from(val));
                }
            }
        }
    }
    None
}

#[cfg(target_os = "windows")]
fn run_reg_command(args: &[&str]) -> Result<String, HostError> {
    let output = std::process::Command::new("reg")
        .args(args)
        .output()
        .map_err(|e| HostError::Registration(format!("Failed to execute reg.exe: {e}")))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        let err = String::from_utf8_lossy(&output.stderr);
        Err(HostError::Registration(format!(
            "reg.exe exited with {}: {err}",
            output.status
        )))
    }
}

#[cfg(target_os = "windows")]
fn query_windows_registry_key(key: &str) -> Option<PathBuf> {
    let full_key = format!(r"HKCU\{}", key);
    let output = std::process::Command::new("reg")
        .args(["query", &full_key, "/ve"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_reg_query_default_value(&stdout)
}

pub fn register(
    target: BrowserTarget,
    host_exe: &Path,
    extension_id: Option<&str>,
) -> Result<Vec<(BrowserTarget, PathBuf)>, HostError> {
    let concrete = target.concrete_targets();
    let mut registered = Vec::with_capacity(concrete.len());

    // Prepare Chromium manifest
    let chromium_origins = match extension_id {
        Some(id) if !id.trim().is_empty() => vec![format!("chrome-extension://{id}/")],
        _ => Vec::new(),
    };
    let chromium_manifest = create_chromium_manifest(host_exe, &chromium_origins);

    // Prepare Firefox manifest
    let firefox_extensions = match extension_id {
        Some(id) if !id.trim().is_empty() => vec![id.to_string()],
        _ => Vec::new(),
    };
    let firefox_manifest = create_firefox_manifest(host_exe, &firefox_extensions);

    #[cfg(target_os = "windows")]
    {
        let manifests_dir = default_manifest_dir();
        let chromium_manifest_path = manifests_dir.join(format!("{HOST_NAME}.json"));
        let firefox_manifest_path = manifests_dir.join(format!("{HOST_NAME}.firefox.json"));

        // Write manifests to centralized FetchTrek app data directory
        let has_chromium = concrete.iter().any(|t| !t.is_firefox());
        let has_firefox = concrete.iter().any(|t| t.is_firefox());

        if has_chromium {
            write_manifest_to_path(&chromium_manifest, &chromium_manifest_path)?;
        }
        if has_firefox {
            write_manifest_to_path(&firefox_manifest, &firefox_manifest_path)?;
        }

        for t in concrete {
            let key = windows_registry_key(t).ok_or_else(|| {
                HostError::Registration(format!("No registry key mapping for target {:?}", t))
            })?;
            let full_key = format!(r"HKCU\{key}");
            let target_manifest = if t.is_firefox() {
                &firefox_manifest_path
            } else {
                &chromium_manifest_path
            };

            let path_str = target_manifest.to_string_lossy().to_string();
            run_reg_command(&[
                "add", &full_key, "/ve", "/t", "REG_SZ", "/d", &path_str, "/f",
            ])?;
            registered.push((t, target_manifest.clone()));
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        for t in concrete {
            let target_dir = unix_browser_manifest_dir(t).ok_or_else(|| {
                HostError::Registration(format!("No manifest directory mapping for target {:?}", t))
            })?;
            let manifest_path = target_dir.join(format!("{HOST_NAME}.json"));
            if t.is_firefox() {
                write_manifest_to_path(&firefox_manifest, &manifest_path)?;
            } else {
                write_manifest_to_path(&chromium_manifest, &manifest_path)?;
            }
            registered.push((t, manifest_path));
        }
    }

    Ok(registered)
}

pub fn unregister(target: BrowserTarget) -> Result<Vec<BrowserTarget>, HostError> {
    let concrete = target.concrete_targets();
    let mut unregistered = Vec::with_capacity(concrete.len());

    #[cfg(target_os = "windows")]
    {
        for t in &concrete {
            if let Some(key) = windows_registry_key(*t) {
                let full_key = format!(r"HKCU\{key}");
                let _ = run_reg_command(&["delete", &full_key, "/f"]);
                unregistered.push(*t);
            }
        }

        if target == BrowserTarget::All {
            let manifests_dir = default_manifest_dir();
            let c_path = manifests_dir.join(format!("{HOST_NAME}.json"));
            let f_path = manifests_dir.join(format!("{HOST_NAME}.firefox.json"));
            let _ = std::fs::remove_file(c_path);
            let _ = std::fs::remove_file(f_path);
            let _ = std::fs::remove_dir(manifests_dir);
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        for t in &concrete {
            if let Some(dir) = unix_browser_manifest_dir(*t) {
                let path = dir.join(format!("{HOST_NAME}.json"));
                if path.exists() {
                    let _ = std::fs::remove_file(path);
                }
                unregistered.push(*t);
            }
        }
    }

    Ok(unregistered)
}

pub fn check_status() -> Vec<TargetStatus> {
    let targets = [
        BrowserTarget::Chrome,
        BrowserTarget::Edge,
        BrowserTarget::Brave,
        BrowserTarget::Firefox,
    ];

    targets
        .into_iter()
        .map(|t| {
            #[cfg(target_os = "windows")]
            {
                let key = windows_registry_key(t);
                let manifest_path = key.and_then(query_windows_registry_key);
                let registered = manifest_path.is_some();
                let valid = if let Some(ref path) = manifest_path {
                    is_manifest_valid(path, t.is_firefox())
                } else {
                    false
                };

                TargetStatus {
                    browser: t,
                    registered,
                    manifest_path,
                    valid,
                }
            }

            #[cfg(not(target_os = "windows"))]
            {
                let dir = unix_browser_manifest_dir(t);
                let manifest_path = dir.map(|d| d.join(format!("{HOST_NAME}.json")));
                let registered = manifest_path.as_ref().map(|p| p.exists()).unwrap_or(false);
                let valid = if registered {
                    if let Some(ref path) = manifest_path {
                        is_manifest_valid(path, t.is_firefox())
                    } else {
                        false
                    }
                } else {
                    false
                };

                TargetStatus {
                    browser: t,
                    registered,
                    manifest_path,
                    valid,
                }
            }
        })
        .collect()
}

fn is_manifest_valid(manifest_path: &Path, is_firefox: bool) -> bool {
    if !manifest_path.exists() {
        return false;
    }

    let content = match std::fs::read_to_string(manifest_path) {
        Ok(c) => c,
        Err(_) => return false,
    };

    let host_bin_path = if is_firefox {
        match serde_json::from_str::<FirefoxHostManifest>(&content) {
            Ok(m) => m.path,
            Err(_) => return false,
        }
    } else {
        match serde_json::from_str::<ChromiumHostManifest>(&content) {
            Ok(m) => m.path,
            Err(_) => return false,
        }
    };

    Path::new(&host_bin_path).exists()
}

pub fn register_manifest_in_dir(
    dir: &Path,
    manifest_name: &str,
    content: &str,
) -> Result<PathBuf, HostError> {
    std::fs::create_dir_all(dir).map_err(HostError::Io)?;
    let target_file = dir.join(manifest_name);
    std::fs::write(&target_file, content).map_err(HostError::Io)?;
    Ok(target_file)
}

pub fn unregister_manifest_in_dir(dir: &Path, manifest_name: &str) -> Result<bool, HostError> {
    let target_file = dir.join(manifest_name);
    if target_file.exists() {
        std::fs::remove_file(target_file).map_err(HostError::Io)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

pub fn check_manifest_in_dir(dir: &Path, manifest_name: &str) -> (bool, Option<PathBuf>, bool) {
    let target_file = dir.join(manifest_name);
    if !target_file.exists() {
        return (false, None, false);
    }

    let is_valid = match std::fs::read_to_string(&target_file) {
        Ok(content) => {
            if let Ok(m) = serde_json::from_str::<ChromiumHostManifest>(&content) {
                Path::new(&m.path).exists()
            } else if let Ok(m) = serde_json::from_str::<FirefoxHostManifest>(&content) {
                Path::new(&m.path).exists()
            } else {
                false
            }
        }
        Err(_) => false,
    };

    (true, Some(target_file), is_valid)
}
