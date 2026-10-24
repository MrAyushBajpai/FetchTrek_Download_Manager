#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

pub mod error;
pub mod primitives;
pub mod state;

pub use error::{
    ErrorClassification, FetchTrekError, IpcError, MediaError, NetworkError, StorageError,
};
pub use primitives::{ByteRange, Protocol, SegmentStatus, TaskId};
pub use state::{transition, InvalidTransitionError, TaskEvent, TaskState};
