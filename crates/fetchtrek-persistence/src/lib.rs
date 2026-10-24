pub mod db;
pub mod error;
pub mod intervals;
pub mod recovery;
pub mod repository;
pub mod vault;

pub use db::{Database, SCHEMA_DDL};
pub use error::PersistenceError;
pub use intervals::{IntervalSet, FORMAT_VERSION};
pub use recovery::{rewind_cursor_to_cluster, RecoveredTask, CLUSTER_ALIGNMENT_MASK, CLUSTER_SIZE};
pub use repository::{NewTask, SegmentCheckpoint, TaskRecord, TaskRepository, TaskSegmentRecord};
pub use vault::{default_vault, CredentialVault, InMemoryVault};

#[cfg(windows)]
pub use vault::{dpapi_decrypt, dpapi_encrypt, DpapiVault};
