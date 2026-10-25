use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserSettingsData {
    pub theme_mode: i32, // 0: Auto, 1: Dark, 2: Light, 3: OLED
    pub start_on_boot: bool,
    pub minimize_to_tray: bool,
    pub show_notifications: bool,
    pub default_download_path: String,
    pub sparse_preallocation: bool,
    pub cursor_rewind_kb: i32,
    pub global_speed_limit_mb: i32,
    pub max_active_tasks: i32,
    pub default_connections: i32,
}

impl Default for UserSettingsData {
    fn default() -> Self {
        let default_download_path = get_default_download_dir();
        Self {
            theme_mode: 1, // Dark
            start_on_boot: false,
            minimize_to_tray: true,
            show_notifications: true,
            default_download_path,
            sparse_preallocation: true,
            cursor_rewind_kb: 64,
            global_speed_limit_mb: 0,
            max_active_tasks: 5,
            default_connections: 4,
        }
    }
}

pub fn get_default_download_dir() -> String {
    #[cfg(target_os = "windows")]
    {
        if let Ok(userprofile) = std::env::var("USERPROFILE") {
            let p = PathBuf::from(userprofile).join("Downloads");
            return p.to_string_lossy().to_string();
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        if let Ok(home) = std::env::var("HOME") {
            let p = PathBuf::from(home).join("Downloads");
            return p.to_string_lossy().to_string();
        }
    }

    "C:\\Downloads".to_string()
}

pub fn config_file_path() -> PathBuf {
    #[cfg(target_os = "windows")]
    {
        if let Ok(appdata) = std::env::var("APPDATA") {
            let dir = PathBuf::from(appdata).join("FetchTrek");
            let _ = fs::create_dir_all(&dir);
            return dir.join("settings.json");
        }
    }

    #[cfg(not(target_os = "windows"))]
    {
        if let Ok(home) = std::env::var("HOME") {
            let dir = PathBuf::from(home).join(".config").join("fetchtrek");
            let _ = fs::create_dir_all(&dir);
            return dir.join("settings.json");
        }
    }

    PathBuf::from("fetchtrek_settings.json")
}

pub fn load_settings() -> UserSettingsData {
    let path = config_file_path();
    if let Ok(content) = fs::read_to_string(&path) {
        if let Ok(settings) = serde_json::from_str::<UserSettingsData>(&content) {
            return settings;
        }
    }
    UserSettingsData::default()
}

pub fn save_settings(settings: &UserSettingsData) -> std::io::Result<()> {
    let path = config_file_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let json = serde_json::to_string_pretty(settings).map_err(std::io::Error::other)?;
    fs::write(path, json)
}
