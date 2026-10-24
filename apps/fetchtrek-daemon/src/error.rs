use thiserror::Error;

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Persistence error: {0}")]
    Persistence(#[from] fetchtrek_persistence::PersistenceError),

    #[error("Storage error: {0}")]
    Storage(#[from] fetchtrek_types::StorageError),

    #[error("IPC error: {0}")]
    Ipc(#[from] fetchtrek_ipc::IpcError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
