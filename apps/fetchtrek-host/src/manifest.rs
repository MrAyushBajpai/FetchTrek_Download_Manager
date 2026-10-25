use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::HostError;

pub const HOST_NAME: &str = "com.fetchtrek.downloadmanager";
pub const HOST_DESCRIPTION: &str = "FetchTrek Download Manager Native Messaging Host";
pub const HOST_TYPE: &str = "stdio";
pub const DEFAULT_CHROMIUM_ORIGIN: &str = "chrome-extension://*/*";
pub const DEFAULT_FIREFOX_EXTENSION_ID: &str = "fetchtrek@fetchtrek.org";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChromiumHostManifest {
    pub name: String,
    pub description: String,
    pub path: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub allowed_origins: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FirefoxHostManifest {
    pub name: String,
    pub description: String,
    pub path: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub allowed_extensions: Vec<String>,
}

pub fn normalize_path(path: &Path) -> String {
    let s = path.to_string_lossy();
    if let Some(stripped) = s.strip_prefix(r"\\?\") {
        stripped.to_string()
    } else {
        s.to_string()
    }
}

pub fn create_chromium_manifest(
    host_exe: &Path,
    allowed_origins: &[String],
) -> ChromiumHostManifest {
    let origins = if allowed_origins.is_empty() {
        vec![DEFAULT_CHROMIUM_ORIGIN.to_string()]
    } else {
        allowed_origins.to_vec()
    };

    ChromiumHostManifest {
        name: HOST_NAME.to_string(),
        description: HOST_DESCRIPTION.to_string(),
        path: normalize_path(host_exe),
        type_: HOST_TYPE.to_string(),
        allowed_origins: origins,
    }
}

pub fn create_firefox_manifest(
    host_exe: &Path,
    allowed_extensions: &[String],
) -> FirefoxHostManifest {
    let extensions = if allowed_extensions.is_empty() {
        vec![DEFAULT_FIREFOX_EXTENSION_ID.to_string()]
    } else {
        allowed_extensions.to_vec()
    };

    FirefoxHostManifest {
        name: HOST_NAME.to_string(),
        description: HOST_DESCRIPTION.to_string(),
        path: normalize_path(host_exe),
        type_: HOST_TYPE.to_string(),
        allowed_extensions: extensions,
    }
}

pub fn write_manifest_to_path<T: Serialize>(
    manifest: &T,
    target_path: &Path,
) -> Result<(), HostError> {
    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent).map_err(HostError::Io)?;
    }
    let content = serde_json::to_string_pretty(manifest).map_err(HostError::Json)?;
    std::fs::write(target_path, content).map_err(HostError::Io)?;
    Ok(())
}
