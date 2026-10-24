use thiserror::Error;

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("SQLite database error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("Database corruption detected: {0}")]
    Corruption(String),

    #[error("Pragma configuration failed for '{pragma}': expected '{expected}', found '{actual}'")]
    PragmaFailed {
        pragma: &'static str,
        expected: String,
        actual: String,
    },

    #[error("Schema migration error: {0}")]
    Migration(String),

    #[error("Credential vault error: {0}")]
    Vault(String),
}
