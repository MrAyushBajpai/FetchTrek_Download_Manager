use fetchtrek_host::{
    read_native_message, write_native_message, BrowserRequest, HostError, HostRelay, HostResponse,
    HOST_VERSION, MAX_MESSAGE_SIZE,
};
use fetchtrek_ipc::{
    decode_request, encode_response, endpoint_with_name, IpcRequest, IpcResponse, IpcServer,
    MessageType, TaskSummary,
};
use fetchtrek_types::TaskState;
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn test_native_framing_round_trip() {
    let (mut client, mut server) = tokio::io::duplex(4096);

    let sample_json = r#"{"action":"ping","extra":"测试-über-file"}"#;
    write_native_message(&mut client, sample_json)
        .await
        .expect("write message should succeed");

    let received = read_native_message(&mut server)
        .await
        .expect("read message should succeed");

    assert_eq!(received, sample_json);
}

#[tokio::test]
async fn test_rejection_of_zero_length_payload() {
    let (mut client, mut server) = tokio::io::duplex(64);

    // Test write rejection
    let write_err = write_native_message(&mut client, "").await.unwrap_err();
    assert!(matches!(write_err, HostError::ZeroLengthMessage));

    // Test read rejection (explicitly write 4 zero bytes)
    client.write_all(&0u32.to_le_bytes()).await.unwrap();
    client.flush().await.unwrap();

    let read_err = read_native_message(&mut server).await.unwrap_err();
    assert!(matches!(read_err, HostError::ZeroLengthMessage));
}

#[tokio::test]
async fn test_rejection_of_oversized_payload() {
    let (mut client, mut server) = tokio::io::duplex(64);

    // Test write rejection (> 1 MiB)
    let oversized_len = MAX_MESSAGE_SIZE + 1;
    let large_str = "a".repeat(oversized_len as usize);
    let write_err = write_native_message(&mut client, &large_str)
        .await
        .unwrap_err();
    assert!(matches!(write_err, HostError::OversizedMessage(len) if len == oversized_len));

    // Test read rejection (send 4 bytes with length > 1 MiB)
    client
        .write_all(&oversized_len.to_le_bytes())
        .await
        .unwrap();
    client.flush().await.unwrap();

    let read_err = read_native_message(&mut server).await.unwrap_err();
    assert!(matches!(read_err, HostError::OversizedMessage(len) if len == oversized_len));
}

#[tokio::test]
async fn test_json_deserialization_of_browser_requests() {
    // 1. Ping
    let ping_json = r#"{"action":"ping"}"#;
    let req: BrowserRequest = serde_json::from_str(ping_json).unwrap();
    assert_eq!(req, BrowserRequest::Ping);

    // 2. InterceptDownload
    let intercept_json = r#"{
        "action": "intercept_download",
        "url": "https://example.com/archive.tar.gz",
        "filename": "archive.tar.gz",
        "destination_path": null,
        "headers": { "Referer": "https://example.com/", "User-Agent": "FetchTrek/1.0" },
        "cookies": "session=abc; cf_clearance=xyz",
        "connections": 4
    }"#;
    let req: BrowserRequest = serde_json::from_str(intercept_json).unwrap();
    match req {
        BrowserRequest::InterceptDownload {
            url,
            filename,
            destination_path,
            headers,
            cookies,
            connections,
            ..
        } => {
            assert_eq!(url, "https://example.com/archive.tar.gz");
            assert_eq!(filename.as_deref(), Some("archive.tar.gz"));
            assert_eq!(destination_path, None);
            let h = headers.unwrap();
            assert_eq!(
                h.get("Referer").map(|s| s.as_str()),
                Some("https://example.com/")
            );
            assert_eq!(cookies.as_deref(), Some("session=abc; cf_clearance=xyz"));
            assert_eq!(connections, Some(4));
        }
        other => panic!("Unexpected parsed request: {other:?}"),
    }

    // 3. QueryStatus
    let query_json = r#"{"action":"query_status","task_id":1}"#;
    let req: BrowserRequest = serde_json::from_str(query_json).unwrap();
    assert_eq!(req, BrowserRequest::QueryStatus { task_id: 1 });

    // 4. PauseTask
    let pause_json = r#"{"action":"pause_task","task_id":1}"#;
    let req: BrowserRequest = serde_json::from_str(pause_json).unwrap();
    assert_eq!(req, BrowserRequest::PauseTask { task_id: 1 });

    // 5. ResumeTask
    let resume_json = r#"{"action":"resume_task","task_id":1}"#;
    let req: BrowserRequest = serde_json::from_str(resume_json).unwrap();
    assert_eq!(req, BrowserRequest::ResumeTask { task_id: 1 });

    // 6. CancelTask (with delete_file: true)
    let cancel_json = r#"{"action":"cancel_task","task_id":1,"delete_file":true}"#;
    let req: BrowserRequest = serde_json::from_str(cancel_json).unwrap();
    assert_eq!(
        req,
        BrowserRequest::CancelTask {
            task_id: 1,
            delete_file: true
        }
    );

    // 7. CancelTask (default delete_file: false)
    let cancel_json2 = r#"{"action":"cancel_task","task_id":2}"#;
    let req: BrowserRequest = serde_json::from_str(cancel_json2).unwrap();
    assert_eq!(
        req,
        BrowserRequest::CancelTask {
            task_id: 2,
            delete_file: false
        }
    );
}

#[tokio::test]
async fn test_host_response_serialization() {
    // 1. Pong
    let pong = HostResponse::pong(HOST_VERSION);
    let json = serde_json::to_string(&pong).unwrap();
    assert_eq!(
        json,
        format!(r#"{{"status":"ok","action":"pong","version":"{HOST_VERSION}"}}"#)
    );

    // 2. TaskCreated
    let created = HostResponse::task_created(1, "uuid-1234");
    let json = serde_json::to_string(&created).unwrap();
    assert_eq!(
        json,
        r#"{"status":"ok","action":"task_created","success":true,"task_id":1,"uuid":"uuid-1234"}"#
    );

    // 3. TaskUpdated
    let updated = HostResponse::task_updated();
    let json = serde_json::to_string(&updated).unwrap();
    assert_eq!(json, r#"{"status":"ok","action":"task_updated"}"#);

    // 4. Error
    let err = HostResponse::error("Daemon connection offline");
    let json = serde_json::to_string(&err).unwrap();
    assert_eq!(
        json,
        r#"{"status":"error","message":"Daemon connection offline"}"#
    );

    // 5. TaskStatus
    let summary = TaskSummary {
        id: 1,
        uuid: "uuid-abc".into(),
        url: "https://example.com/test.bin".into(),
        destination_path: "C:\\Downloads".into(),
        filename: "test.bin".into(),
        total_bytes: 1000,
        downloaded_bytes: 500,
        status: TaskState::Active,
        speed_bps: 100_000,
        progress_percent: 50.0,
    };
    let status_resp = HostResponse::task_status(summary);
    let json = serde_json::to_string(&status_resp).unwrap();
    assert!(json.contains(r#""action":"task_status""#));
    assert!(json.contains(r#""task_id":1"#) || json.contains(r#""id":1"#));
}

#[tokio::test]
async fn test_clean_eof_handling() {
    let (client, mut server) = tokio::io::duplex(64);
    // Explicitly drop writer
    drop(client);

    let err = read_native_message(&mut server).await.unwrap_err();
    assert!(matches!(err, HostError::Eof));
}

#[tokio::test]
async fn test_mock_loopback_relay() {
    let endpoint = endpoint_with_name("fetchtrek_test_host_relay");
    let mut server = IpcServer::bind(&endpoint).expect("bind mock IPC server");

    // Spawn mock IPC server background task
    let server_handle = tokio::spawn(async move {
        let mut stream = server.accept().await.expect("accept connection");

        // Loop to handle incoming IPC frames
        while let Ok(frame) = stream.read_frame().await {
            if frame.header.message_type == MessageType::Request.as_u8() {
                let (req, corr_id) = decode_request(&frame).expect("decode request");
                match req {
                    IpcRequest::AddTask { url, .. } => {
                        assert_eq!(url, "https://example.com/test.iso");
                        let resp = IpcResponse::TaskCreated {
                            task_id: 101,
                            uuid: "test-uuid-456".into(),
                        };
                        let resp_frame = encode_response(&resp, corr_id).expect("encode response");
                        stream
                            .write_frame(&resp_frame)
                            .await
                            .expect("write response");
                    }
                    IpcRequest::QueryTask { task_id } => {
                        assert_eq!(task_id, 101);
                        let summary = TaskSummary {
                            id: 101,
                            uuid: "test-uuid-456".into(),
                            url: "https://example.com/test.iso".into(),
                            destination_path: "C:\\Downloads".into(),
                            filename: "test.iso".into(),
                            total_bytes: 5000,
                            downloaded_bytes: 2500,
                            status: TaskState::Active,
                            speed_bps: 200_000,
                            progress_percent: 50.0,
                        };
                        let resp = IpcResponse::TaskDetail { task: summary };
                        let resp_frame = encode_response(&resp, corr_id).expect("encode response");
                        stream
                            .write_frame(&resp_frame)
                            .await
                            .expect("write response");
                    }
                    IpcRequest::PauseTask { task_id } => {
                        assert_eq!(task_id, 101);
                        let resp = IpcResponse::TaskUpdated {
                            task_id: 101,
                            status: TaskState::Paused,
                        };
                        let resp_frame = encode_response(&resp, corr_id).expect("encode response");
                        stream
                            .write_frame(&resp_frame)
                            .await
                            .expect("write response");
                    }
                    _ => panic!("Unexpected mock request: {req:?}"),
                }
            }
        }
    });

    let mut relay = HostRelay::new(endpoint);

    // 1. Dispatch Ping (handled directly by host without IPC call)
    let ping_resp = relay.dispatch(BrowserRequest::Ping).await;
    assert_eq!(ping_resp, HostResponse::pong(HOST_VERSION));

    // 2. Dispatch InterceptDownload -> TaskCreated
    let add_req = BrowserRequest::InterceptDownload {
        url: "https://example.com/test.iso".into(),
        filename: Some("test.iso".into()),
        destination_path: None,
        headers: None,
        cookies: None,
        user_agent: None,
        referer: None,
        connections: Some(8),
    };
    let add_resp = relay.dispatch(add_req).await;
    assert_eq!(add_resp, HostResponse::task_created(101, "test-uuid-456"));

    // 3. Dispatch QueryStatus -> TaskStatus
    let query_req = BrowserRequest::QueryStatus { task_id: 101 };
    let query_resp = relay.dispatch(query_req).await;
    match query_resp {
        HostResponse::TaskStatus { task, .. } => {
            assert_eq!(task.id, 101);
            assert_eq!(task.uuid, "test-uuid-456");
            assert_eq!(task.downloaded_bytes, 2500);
        }
        other => panic!("Unexpected query response: {other:?}"),
    }

    // 4. Dispatch PauseTask -> TaskUpdated
    let pause_req = BrowserRequest::PauseTask { task_id: 101 };
    let pause_resp = relay.dispatch(pause_req).await;
    assert_eq!(pause_resp, HostResponse::task_updated());

    server_handle.abort();
}

#[tokio::test]
async fn test_daemon_offline_graceful_handling() {
    let nonexistent = endpoint_with_name("fetchtrek_nonexistent_pipe_9999");
    let mut relay = HostRelay::new(nonexistent);

    let req = BrowserRequest::InterceptDownload {
        url: "https://example.com/test.iso".into(),
        filename: None,
        destination_path: None,
        headers: None,
        cookies: None,
        user_agent: None,
        referer: None,
        connections: None,
    };

    let resp = relay.dispatch(req).await;
    assert_eq!(resp, HostResponse::error("Daemon connection offline"));
}

#[tokio::test]
async fn test_relay_session_context_forwarding() {
    let endpoint = endpoint_with_name("fetchtrek_test_session_forwarding");
    let mut server = IpcServer::bind(&endpoint).expect("bind mock IPC server");

    let server_handle = tokio::spawn(async move {
        let mut stream = server.accept().await.expect("accept connection");
        if let Ok(frame) = stream.read_frame().await {
            let (req, corr_id) = decode_request(&frame).expect("decode request");
            match req {
                IpcRequest::AddTask {
                    url,
                    headers,
                    cookies,
                    user_agent,
                    referer,
                    ..
                } => {
                    assert_eq!(url, "https://protected.example.com/data.bin");
                    assert_eq!(
                        cookies.as_deref(),
                        Some("session_id=secret123; cf_clearance=tok999")
                    );
                    assert_eq!(user_agent.as_deref(), Some("Mozilla/5.0 Chrome/130"));
                    assert_eq!(referer.as_deref(), Some("https://portal.example.com"));
                    let h = headers.expect("headers must be forwarded");
                    assert!(h
                        .iter()
                        .any(|(k, v)| k == "User-Agent" && v == "Mozilla/5.0 Chrome/130"));

                    let resp = IpcResponse::TaskCreated {
                        task_id: 202,
                        uuid: "uuid-session-789".into(),
                    };
                    let resp_frame = encode_response(&resp, corr_id).expect("encode response");
                    stream.write_frame(&resp_frame).await.expect("write frame");
                }
                other => panic!("Unexpected request: {other:?}"),
            }
        }
    });

    let mut relay = HostRelay::new(endpoint);
    let mut headers = std::collections::HashMap::new();
    headers.insert(
        "User-Agent".to_string(),
        "Mozilla/5.0 Chrome/130".to_string(),
    );
    headers.insert(
        "Referer".to_string(),
        "https://portal.example.com".to_string(),
    );

    let req = BrowserRequest::InterceptDownload {
        url: "https://protected.example.com/data.bin".into(),
        filename: Some("data.bin".into()),
        destination_path: None,
        headers: Some(headers),
        cookies: Some("session_id=secret123; cf_clearance=tok999".into()),
        user_agent: None,
        referer: None,
        connections: Some(4),
    };

    let resp = relay.dispatch(req).await;
    assert_eq!(resp, HostResponse::task_created(202, "uuid-session-789"));

    server_handle.abort();
}
