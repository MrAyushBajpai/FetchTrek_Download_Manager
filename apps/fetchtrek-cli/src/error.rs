use thiserror::Error;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("IPC transport error: {0}")]
    Ipc(#[from] fetchtrek_ipc::IpcError),

    #[error("Server returned error (code {code}): {message}")]
    Server { code: u32, message: String },

    #[error("Unexpected response from server: {0}")]
    UnexpectedResponse(String),

    #[error("Request timed out")]
    Timeout,
}
