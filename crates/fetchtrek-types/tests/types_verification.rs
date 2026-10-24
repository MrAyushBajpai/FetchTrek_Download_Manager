use fetchtrek_types::{
    transition, ByteRange, ErrorClassification, FetchTrekError, InvalidTransitionError, IpcError,
    MediaError, NetworkError, Protocol, SegmentStatus, StorageError, TaskEvent, TaskId, TaskState,
};

#[test]
fn test_task_id_properties() {
    let id1 = TaskId::new(42);
    let id2 = TaskId(42);
    let id3 = TaskId::new(100);

    assert_eq!(id1, id2);
    assert!(id1 < id3);
    assert_eq!(id1.as_u64(), 42);
    assert_eq!(format!("{id1}"), "42");
}

#[test]
fn test_byte_range_length_and_inclusion() {
    let single = ByteRange::new(0, 0);
    assert_eq!(single.len(), 1);
    assert!(!single.is_empty());
    assert!(single.contains(0));
    assert!(!single.contains(1));

    let range = ByteRange::new(100, 199);
    assert_eq!(range.len(), 100);
    assert!(!range.is_empty());
    assert!(range.contains(100));
    assert!(range.contains(150));
    assert!(range.contains(199));
    assert!(!range.contains(99));
    assert!(!range.contains(200));
    assert_eq!(format!("{range}"), "100-199");
}

#[test]
fn test_byte_range_overlaps() {
    let base = ByteRange::new(100, 200);

    // Overlapping cases
    assert!(base.overlaps(&ByteRange::new(100, 200)));
    assert!(base.overlaps(&ByteRange::new(50, 150)));
    assert!(base.overlaps(&ByteRange::new(150, 250)));
    assert!(base.overlaps(&ByteRange::new(50, 250)));
    assert!(base.overlaps(&ByteRange::new(120, 180)));
    assert!(base.overlaps(&ByteRange::new(50, 100)));
    assert!(base.overlaps(&ByteRange::new(200, 300)));

    // Disjoint cases
    assert!(!base.overlaps(&ByteRange::new(0, 99)));
    assert!(!base.overlaps(&ByteRange::new(201, 300)));
}

#[test]
#[should_panic(expected = "ByteRange start must be <= end")]
fn test_byte_range_invariant_violation_panics() {
    let _ = ByteRange::new(200, 100);
}

#[test]
fn test_protocol_and_segment_status_representations() {
    assert_eq!(Protocol::Http as u8, 0);
    assert_eq!(Protocol::Https as u8, 1);
    assert_eq!(Protocol::BitTorrent as u8, 2);
    assert_eq!(Protocol::Hls as u8, 3);
    assert_eq!(Protocol::Dash as u8, 4);

    assert_eq!(Protocol::try_from(0), Ok(Protocol::Http));
    assert_eq!(Protocol::try_from(1), Ok(Protocol::Https));
    assert_eq!(Protocol::try_from(2), Ok(Protocol::BitTorrent));
    assert_eq!(Protocol::try_from(3), Ok(Protocol::Hls));
    assert_eq!(Protocol::try_from(4), Ok(Protocol::Dash));
    assert_eq!(Protocol::try_from(5), Err(()));

    assert_eq!(SegmentStatus::Pending as u8, 0);
    assert_eq!(SegmentStatus::Downloading as u8, 1);
    assert_eq!(SegmentStatus::Verifying as u8, 2);
    assert_eq!(SegmentStatus::Completed as u8, 3);
    assert_eq!(SegmentStatus::Failed as u8, 4);

    assert_eq!(SegmentStatus::try_from(0), Ok(SegmentStatus::Pending));
    assert_eq!(SegmentStatus::try_from(4), Ok(SegmentStatus::Failed));
    assert_eq!(SegmentStatus::try_from(5), Err(()));
}

#[test]
fn test_task_state_queries() {
    let states = [
        TaskState::Queued,
        TaskState::Resolving,
        TaskState::Allocating,
        TaskState::Transferring,
        TaskState::Paused,
        TaskState::Retrying,
        TaskState::PostProcessing,
        TaskState::Completed,
        TaskState::FatalError,
    ];

    for state in states {
        let is_active = matches!(
            state,
            TaskState::Resolving
                | TaskState::Allocating
                | TaskState::Transferring
                | TaskState::Retrying
                | TaskState::PostProcessing
        );
        let is_terminal = matches!(state, TaskState::Completed | TaskState::FatalError);
        let can_pause = matches!(state, TaskState::Transferring);
        let can_resume = matches!(state, TaskState::Paused);

        assert_eq!(
            state.is_active(),
            is_active,
            "is_active check for {state:?}"
        );
        assert_eq!(
            state.is_terminal(),
            is_terminal,
            "is_terminal check for {state:?}"
        );
        assert_eq!(
            state.can_pause(),
            can_pause,
            "can_pause check for {state:?}"
        );
        assert_eq!(
            state.can_resume(),
            can_resume,
            "can_resume check for {state:?}"
        );
    }
}

#[test]
fn test_state_machine_valid_transitions() {
    assert_eq!(
        transition(TaskState::Queued, TaskEvent::StartGranted),
        Ok(TaskState::Resolving)
    );
    assert_eq!(
        transition(TaskState::Resolving, TaskEvent::ProbeSucceeded),
        Ok(TaskState::Allocating)
    );
    assert_eq!(
        transition(TaskState::Resolving, TaskEvent::SocketTimeout),
        Ok(TaskState::Retrying)
    );
    assert_eq!(
        transition(TaskState::Allocating, TaskEvent::SparseCreated),
        Ok(TaskState::Transferring)
    );
    assert_eq!(
        transition(TaskState::Allocating, TaskEvent::DiskError),
        Ok(TaskState::FatalError)
    );
    assert_eq!(
        transition(TaskState::Transferring, TaskEvent::BytesIngested),
        Ok(TaskState::Transferring)
    );
    assert_eq!(
        transition(TaskState::Transferring, TaskEvent::UserPause),
        Ok(TaskState::Paused)
    );
    assert_eq!(
        transition(TaskState::Transferring, TaskEvent::LastByteFlushed),
        Ok(TaskState::PostProcessing)
    );
    assert_eq!(
        transition(TaskState::PostProcessing, TaskEvent::DirectFinalize),
        Ok(TaskState::Completed)
    );
    assert_eq!(
        transition(TaskState::PostProcessing, TaskEvent::RemuxFinished),
        Ok(TaskState::Completed)
    );
}

#[test]
fn test_state_machine_rejected_invalid_transitions() {
    let all_states = [
        TaskState::Queued,
        TaskState::Resolving,
        TaskState::Allocating,
        TaskState::Transferring,
        TaskState::Paused,
        TaskState::Retrying,
        TaskState::PostProcessing,
        TaskState::Completed,
        TaskState::FatalError,
    ];

    let all_events = [
        TaskEvent::StartGranted,
        TaskEvent::ProbeSucceeded,
        TaskEvent::SocketTimeout,
        TaskEvent::SparseCreated,
        TaskEvent::BytesIngested,
        TaskEvent::UserPause,
        TaskEvent::LastByteFlushed,
        TaskEvent::DirectFinalize,
        TaskEvent::RemuxFinished,
        TaskEvent::DiskError,
    ];

    let valid_pairs = [
        (TaskState::Queued, TaskEvent::StartGranted),
        (TaskState::Resolving, TaskEvent::ProbeSucceeded),
        (TaskState::Resolving, TaskEvent::SocketTimeout),
        (TaskState::Allocating, TaskEvent::SparseCreated),
        (TaskState::Allocating, TaskEvent::DiskError),
        (TaskState::Transferring, TaskEvent::BytesIngested),
        (TaskState::Transferring, TaskEvent::UserPause),
        (TaskState::Transferring, TaskEvent::LastByteFlushed),
        (TaskState::PostProcessing, TaskEvent::DirectFinalize),
        (TaskState::PostProcessing, TaskEvent::RemuxFinished),
    ];

    for state in all_states {
        for event in all_events {
            let result = transition(state, event);
            if valid_pairs.contains(&(state, event)) {
                assert!(
                    result.is_ok(),
                    "Expected valid transition for {state:?} + {event:?}"
                );
            } else {
                assert_eq!(
                    result,
                    Err(InvalidTransitionError {
                        current: state,
                        event
                    }),
                    "Expected invalid transition error for {state:?} + {event:?}"
                );
            }
        }
    }
}

#[test]
fn test_error_classification_and_recovery() {
    // Recoverable transient errors
    let recoverable_network = [
        NetworkError::ConnectionReset,
        NetworkError::GatewayTimeout,
        NetworkError::RateLimited,
        NetworkError::SocketTimeout,
    ];
    for err in recoverable_network {
        assert_eq!(
            err.classification(),
            ErrorClassification::RecoverableTransient
        );
        assert!(err.is_recoverable());
        let unified = FetchTrekError::from(err);
        assert_eq!(
            unified.classification(),
            ErrorClassification::RecoverableTransient
        );
        assert!(unified.is_recoverable());
    }

    // Terminal network errors
    let terminal_network = [
        NetworkError::NoRangeSupport,
        NetworkError::ETagMismatch,
        NetworkError::AccessDenied,
    ];
    for err in terminal_network {
        assert_eq!(err.classification(), ErrorClassification::TerminalFault);
        assert!(!err.is_recoverable());
        let unified = FetchTrekError::from(err);
        assert_eq!(unified.classification(), ErrorClassification::TerminalFault);
        assert!(!unified.is_recoverable());
    }

    // Terminal storage errors
    let storage_errors = [
        StorageError::DiskFull,
        StorageError::AccessDenied,
        StorageError::PathNotFound,
        StorageError::ChecksumMismatch,
        StorageError::IoFailure,
        StorageError::AllocationFailed,
    ];
    for err in storage_errors {
        assert_eq!(err.classification(), ErrorClassification::TerminalFault);
        assert!(!err.is_recoverable());
        let unified = FetchTrekError::from(err);
        assert_eq!(unified.classification(), ErrorClassification::TerminalFault);
        assert!(!unified.is_recoverable());
    }

    // IPC transport errors
    let ipc_errors = [
        IpcError::InvalidMagic,
        IpcError::ChannelClosed,
        IpcError::PayloadTooLarge,
        IpcError::FramingError,
    ];
    for err in ipc_errors {
        assert_eq!(err.classification(), ErrorClassification::IpcTransportFault);
        assert!(!err.is_recoverable());
        let unified = FetchTrekError::from(err);
        assert_eq!(
            unified.classification(),
            ErrorClassification::IpcTransportFault
        );
        assert!(!unified.is_recoverable());
    }

    // Media remux errors
    let media_errors = [
        MediaError::MalformedManifest,
        MediaError::FFmpegChildFailed,
        MediaError::SegmentMissing,
        MediaError::UnsupportedCodec,
    ];
    for err in media_errors {
        assert_eq!(err.classification(), ErrorClassification::MediaRemuxFault);
        assert!(!err.is_recoverable());
        let unified = FetchTrekError::from(err);
        assert_eq!(
            unified.classification(),
            ErrorClassification::MediaRemuxFault
        );
        assert!(!unified.is_recoverable());
    }
}
