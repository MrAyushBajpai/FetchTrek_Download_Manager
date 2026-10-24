use core::fmt;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(u8)]
pub enum TaskState {
    Queued = 0,
    Resolving = 1,
    Allocating = 2,
    Transferring = 3,
    Paused = 4,
    Retrying = 5,
    PostProcessing = 6,
    Completed = 7,
    FatalError = 8,
}

impl TaskState {
    pub const ACTIVE: TaskState = TaskState::Transferring;
    #[allow(non_upper_case_globals)]
    pub const Active: TaskState = TaskState::Transferring;
    #[allow(non_upper_case_globals)]
    pub const Failed: TaskState = TaskState::FatalError;
    #[allow(non_upper_case_globals)]
    pub const Cancelled: TaskState = TaskState::FatalError;

    pub const fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub const fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Resolving
                | Self::Allocating
                | Self::Transferring
                | Self::Retrying
                | Self::PostProcessing
        )
    }

    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::FatalError)
    }

    pub const fn can_pause(&self) -> bool {
        matches!(self, Self::Transferring)
    }

    pub const fn can_resume(&self) -> bool {
        matches!(self, Self::Paused)
    }
}

impl TryFrom<u8> for TaskState {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Queued),
            1 => Ok(Self::Resolving),
            2 => Ok(Self::Allocating),
            3 => Ok(Self::Transferring),
            4 => Ok(Self::Paused),
            5 => Ok(Self::Retrying),
            6 => Ok(Self::PostProcessing),
            7 => Ok(Self::Completed),
            8 => Ok(Self::FatalError),
            _ => Err(()),
        }
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queued => write!(f, "queued"),
            Self::Resolving => write!(f, "resolving"),
            Self::Allocating => write!(f, "allocating"),
            Self::Transferring => write!(f, "transferring"),
            Self::Paused => write!(f, "paused"),
            Self::Retrying => write!(f, "retrying"),
            Self::PostProcessing => write!(f, "post_processing"),
            Self::Completed => write!(f, "completed"),
            Self::FatalError => write!(f, "fatal_error"),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum TaskEvent {
    StartGranted,
    ProbeSucceeded,
    SocketTimeout,
    SparseCreated,
    BytesIngested,
    UserPause,
    LastByteFlushed,
    DirectFinalize,
    RemuxFinished,
    DiskError,
}

impl fmt::Display for TaskEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StartGranted => write!(f, "start_granted"),
            Self::ProbeSucceeded => write!(f, "probe_succeeded"),
            Self::SocketTimeout => write!(f, "socket_timeout"),
            Self::SparseCreated => write!(f, "sparse_created"),
            Self::BytesIngested => write!(f, "bytes_ingested"),
            Self::UserPause => write!(f, "user_pause"),
            Self::LastByteFlushed => write!(f, "last_byte_flushed"),
            Self::DirectFinalize => write!(f, "direct_finalize"),
            Self::RemuxFinished => write!(f, "remux_finished"),
            Self::DiskError => write!(f, "disk_error"),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct InvalidTransitionError {
    pub current: TaskState,
    pub event: TaskEvent,
}

impl fmt::Display for InvalidTransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid task state transition from {:?} on event {:?}",
            self.current, self.event
        )
    }
}

impl core::error::Error for InvalidTransitionError {}

pub const fn transition(
    current: TaskState,
    event: TaskEvent,
) -> Result<TaskState, InvalidTransitionError> {
    match (current, event) {
        (TaskState::Queued, TaskEvent::StartGranted) => Ok(TaskState::Resolving),
        (TaskState::Resolving, TaskEvent::ProbeSucceeded) => Ok(TaskState::Allocating),
        (TaskState::Resolving, TaskEvent::SocketTimeout) => Ok(TaskState::Retrying),
        (TaskState::Allocating, TaskEvent::SparseCreated) => Ok(TaskState::Transferring),
        (TaskState::Allocating, TaskEvent::DiskError) => Ok(TaskState::FatalError),
        (TaskState::Transferring, TaskEvent::BytesIngested) => Ok(TaskState::Transferring),
        (TaskState::Transferring, TaskEvent::UserPause) => Ok(TaskState::Paused),
        (TaskState::Transferring, TaskEvent::LastByteFlushed) => Ok(TaskState::PostProcessing),
        (TaskState::PostProcessing, TaskEvent::DirectFinalize) => Ok(TaskState::Completed),
        (TaskState::PostProcessing, TaskEvent::RemuxFinished) => Ok(TaskState::Completed),
        _ => Err(InvalidTransitionError { current, event }),
    }
}
