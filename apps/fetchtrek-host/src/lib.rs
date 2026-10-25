pub mod cli;
pub mod error;
pub mod framing;
pub mod manifest;
pub mod protocol;
pub mod registration;
pub mod relay;

pub use cli::{parse_cli_command, print_help, CliCommand};
pub use error::HostError;
pub use framing::{read_native_message, write_native_message, MAX_MESSAGE_SIZE};
pub use manifest::{
    create_chromium_manifest, create_firefox_manifest, write_manifest_to_path,
    ChromiumHostManifest, FirefoxHostManifest, DEFAULT_CHROMIUM_ORIGIN,
    DEFAULT_FIREFOX_EXTENSION_ID, HOST_DESCRIPTION, HOST_NAME, HOST_TYPE,
};
pub use protocol::{BrowserRequest, HostResponse};
pub use registration::{
    check_status, default_manifest_dir, register, resolve_host_exe_path, unregister, BrowserTarget,
    TargetStatus,
};
pub use relay::{HostRelay, HOST_VERSION};
