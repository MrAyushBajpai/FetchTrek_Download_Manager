use fetchtrek_ipc::{
    decode_request, decode_response, decode_telemetry, encode_request, encode_response,
    encode_telemetry, HeartbeatTracker, IpcRequest, IpcResponse, MessageType, TaskProgress,
    TaskSummary, TelemetryBroadcaster, TelemetrySnapshot, OP_TELEMETRY_EVENT,
};
use fetchtrek_types::TaskState;

#[test]
fn test_rpc_codec_round_trip() {
    let requests = [
        IpcRequest::Ping,
        IpcRequest::AddTask {
            url: "https://example.com/file.zip".to_string(),
            destination_path: Some("/downloads/file.zip".to_string()),
            filename: Some("file.zip".to_string()),
            connections: Some(8),
        },
        IpcRequest::PauseTask { task_id: 101 },
        IpcRequest::ResumeTask { task_id: 102 },
        IpcRequest::CancelTask {
            task_id: 103,
            delete_file: true,
        },
        IpcRequest::ListTasks {
            status_filter: Some(TaskState::Transferring),
        },
        IpcRequest::ListTasks {
            status_filter: None,
        },
        IpcRequest::QueryTask { task_id: 104 },
    ];

    for (i, req) in requests.iter().enumerate() {
        let correlation_id = (i as u32 + 1) * 100;
        let frame = encode_request(req, correlation_id).expect("encode_request failed");

        assert_eq!(frame.header.message_type, MessageType::Request.as_u8());
        assert_eq!(frame.header.correlation_id, correlation_id);
        assert_eq!(frame.header.message_id, req.opcode());

        let (decoded_req, decoded_corr) = decode_request(&frame).expect("decode_request failed");
        assert_eq!(&decoded_req, req);
        assert_eq!(decoded_corr, correlation_id);
    }

    let responses = [
        IpcResponse::Pong,
        IpcResponse::TaskCreated {
            task_id: 1,
            uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        },
        IpcResponse::TaskUpdated {
            task_id: 1,
            status: TaskState::Paused,
        },
        IpcResponse::TaskCancelled { task_id: 1 },
        IpcResponse::TaskList {
            tasks: vec![TaskSummary {
                id: 1,
                uuid: "uuid-1".to_string(),
                url: "https://example.com/1".to_string(),
                destination_path: "/tmp/1".to_string(),
                filename: "1".to_string(),
                total_bytes: 1024,
                downloaded_bytes: 512,
                status: TaskState::Transferring,
                speed_bps: 100_000,
                progress_percent: 50.0,
            }],
        },
        IpcResponse::TaskDetail {
            task: TaskSummary {
                id: 2,
                uuid: "uuid-2".to_string(),
                url: "https://example.com/2".to_string(),
                destination_path: "/tmp/2".to_string(),
                filename: "2".to_string(),
                total_bytes: 2048,
                downloaded_bytes: 2048,
                status: TaskState::Completed,
                speed_bps: 0,
                progress_percent: 100.0,
            },
        },
        IpcResponse::Error {
            code: 404,
            message: "Task not found".to_string(),
        },
    ];

    for (i, resp) in responses.iter().enumerate() {
        let correlation_id = (i as u32 + 1) * 200;
        let frame = encode_response(resp, correlation_id).expect("encode_response failed");

        assert_eq!(frame.header.message_type, MessageType::Response.as_u8());
        assert_eq!(frame.header.correlation_id, correlation_id);
        assert_eq!(frame.header.message_id, resp.opcode());

        let (decoded_resp, decoded_corr) = decode_response(&frame).expect("decode_response failed");
        assert_eq!(&decoded_resp, resp);
        assert_eq!(decoded_corr, correlation_id);
    }
}

#[test]
fn test_telemetry_event_codec() {
    let mut tasks = Vec::with_capacity(16);
    for i in 0..16 {
        tasks.push(TaskProgress {
            task_id: (i + 1) as i64,
            downloaded_bytes: (i as u64 + 1) * 1_000_000,
            total_bytes: 100_000_000,
            speed_bps: 25_000_000,
            active_connections: 4,
            status: TaskState::Transferring,
        });
    }

    let snapshot = TelemetrySnapshot {
        timestamp_ms: 1_700_000_000_000,
        global_download_speed_bps: 400_000_000,
        global_upload_speed_bps: 10_000_000,
        active_task_count: 16,
        tasks: tasks.clone(),
    };

    let frame = encode_telemetry(&snapshot).expect("encode_telemetry failed");
    assert_eq!(frame.header.message_type, MessageType::Event.as_u8());
    assert_eq!(frame.header.message_id, OP_TELEMETRY_EVENT);
    assert_eq!(frame.header.correlation_id, 0);

    let decoded = decode_telemetry(&frame).expect("decode_telemetry failed");
    assert_eq!(decoded.timestamp_ms, snapshot.timestamp_ms);
    assert_eq!(
        decoded.global_download_speed_bps,
        snapshot.global_download_speed_bps
    );
    assert_eq!(
        decoded.global_upload_speed_bps,
        snapshot.global_upload_speed_bps
    );
    assert_eq!(decoded.active_task_count, 16);
    assert_eq!(decoded.tasks.len(), 16);
    assert_eq!(decoded.tasks, tasks);
}

#[test]
fn test_10hz_telemetry_rate_limiting() {
    let mut broadcaster = TelemetryBroadcaster::with_start(0);

    let mut emitted_count = 0;
    // Simulate rapid progress ticks every 10ms across 500ms duration
    for t in (0..=500).step_by(10) {
        if broadcaster.should_emit(t) {
            emitted_count += 1;
        }
    }

    // Over 500ms window, 10 Hz rate limit (every 100ms) allows:
    // t=100, 200, 300, 400, 500 (5 emissions)
    // or t=0, 100, 200, 300, 400, 500 (6 emissions)
    assert!(
        (5..=6).contains(&emitted_count),
        "Expected 5-6 telemetry emissions over 500ms at 10 Hz, got {}",
        emitted_count
    );
}

#[test]
fn test_heartbeat_watchdog_timeout() {
    let tracker = HeartbeatTracker::new(0);

    // At t = 2,000ms: ping interval reached, but not timed out
    assert!(tracker.should_ping(2_000));
    assert!(!tracker.is_timed_out(2_000));

    // At t = 5,999ms: 3 missed pings pending, not quite timed out yet
    assert!(!tracker.is_timed_out(5_999));

    // At t = 6,000ms: watchdog timeout triggers
    assert!(tracker.is_timed_out(6_000));
    assert!(tracker.is_timed_out(6_001));
}
