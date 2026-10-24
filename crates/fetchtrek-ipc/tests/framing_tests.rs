use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fetchtrek_ipc::{
    default_endpoint, endpoint_with_name, read_frame, write_frame, IpcClient, IpcError, IpcFrame,
    IpcHeader, IpcServer, MessageType, HEADER_LEN, IPC_MAGIC, MAX_PAYLOAD_LEN,
};

#[test]
fn test_header_wire_format_exact_bit_layout() {
    let header = IpcHeader::new(
        1024,                         // payload_len
        42,                           // message_id
        MessageType::Request.as_u8(), // message_type
        0x80,                         // flags
        9999,                         // correlation_id
    );

    let bytes = header.to_bytes();

    // Assert exact 16-byte fixed length
    assert_eq!(bytes.len(), HEADER_LEN);
    assert_eq!(bytes.len(), 16);

    // Assert byte offsets 0..4 match [0x46, 0x54, 0x52, 0x4B] ('FTRK')
    assert_eq!(&bytes[0..4], &[0x46, 0x54, 0x52, 0x4B]);

    // Assert other byte offsets
    assert_eq!(&bytes[4..8], &1024u32.to_le_bytes());
    assert_eq!(&bytes[8..10], &42u16.to_le_bytes());
    assert_eq!(bytes[10], MessageType::Request.as_u8());
    assert_eq!(bytes[11], 0x80);
    assert_eq!(&bytes[12..16], &9999u32.to_le_bytes());

    // Round-trip deserialize and assert complete field equality
    let deserialized = IpcHeader::from_bytes(&bytes).expect("from_bytes failed");
    assert_eq!(header, deserialized);
    assert_eq!(deserialized.magic, IPC_MAGIC);
    assert_eq!(deserialized.payload_len, 1024);
    assert_eq!(deserialized.message_id, 42);
    assert_eq!(deserialized.message_type, MessageType::Request.as_u8());
    assert_eq!(deserialized.flags, 0x80);
    assert_eq!(deserialized.correlation_id, 9999);
}

#[tokio::test]
async fn test_async_frame_streaming_over_duplex() {
    let (mut client_io, mut server_io) = tokio::io::duplex(65536);

    let test_payloads = [
        vec![],                // 0 bytes
        vec![0xAA; 512],       // 512 bytes
        vec![0x55; 64 * 1024], // 64 KiB
        b"Hello FetchTrek!".to_vec(),
    ];

    let writer_task = tokio::spawn(async move {
        for (idx, payload) in test_payloads.iter().enumerate() {
            let frame = IpcFrame::new(
                idx as u16,
                MessageType::Event.as_u8(),
                0,
                (idx * 10) as u32,
                payload.clone(),
            )
            .unwrap();

            write_frame(&mut client_io, &frame)
                .await
                .expect("write_frame failed");
        }
    });

    let reader_task = tokio::spawn(async move {
        for idx in 0..4 {
            let frame = read_frame(&mut server_io).await.expect("read_frame failed");

            assert_eq!(frame.header.message_id, idx as u16);
            assert_eq!(frame.header.message_type, MessageType::Event.as_u8());
            assert_eq!(frame.header.correlation_id, (idx * 10) as u32);

            match idx {
                0 => assert_eq!(frame.payload.len(), 0),
                1 => {
                    assert_eq!(frame.payload.len(), 512);
                    assert!(frame.payload.iter().all(|&b| b == 0xAA));
                }
                2 => {
                    assert_eq!(frame.payload.len(), 64 * 1024);
                    assert!(frame.payload.iter().all(|&b| b == 0x55));
                }
                3 => assert_eq!(frame.payload, b"Hello FetchTrek!"),
                _ => unreachable!(),
            }
        }
    });

    let (res_w, res_r) = tokio::join!(writer_task, reader_task);
    res_w.unwrap();
    res_r.unwrap();
}

#[tokio::test]
async fn test_malformed_frame_rejection_and_bounds() {
    // 1. Corrupt magic bytes -> verify immediate InvalidMagic error
    let mut bad_magic_bytes = [0u8; HEADER_LEN];
    bad_magic_bytes[0..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    let err = IpcHeader::from_bytes(&bad_magic_bytes).unwrap_err();
    assert!(matches!(
        err,
        IpcError::InvalidMagic {
            expected: IPC_MAGIC,
            actual: 0xDEADBEEF
        }
    ));

    // 2. Payload length exceeds MAX_PAYLOAD_LEN -> verify PayloadTooLarge
    let mut oversized_bytes = [0u8; HEADER_LEN];
    oversized_bytes[0..4].copy_from_slice(&IPC_MAGIC.to_be_bytes());
    let too_large_len = MAX_PAYLOAD_LEN + 1;
    oversized_bytes[4..8].copy_from_slice(&too_large_len.to_le_bytes());

    let err = IpcHeader::from_bytes(&oversized_bytes).unwrap_err();
    assert!(matches!(
        err,
        IpcError::PayloadTooLarge {
            length,
            max: MAX_PAYLOAD_LEN
        } if length == too_large_len
    ));

    // Test rejection over streaming duplex
    let (mut client_io, mut server_io) = tokio::io::duplex(1024);

    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        client_io.write_all(&oversized_bytes).await.unwrap();
    });

    let stream_err = read_frame(&mut server_io).await.unwrap_err();
    assert!(matches!(stream_err, IpcError::PayloadTooLarge { .. }));
}

#[tokio::test]
async fn test_ipc_loopback_connection_transport() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pipe_name = format!("ft_test_pipe_{}_{}", std::process::id(), nanos);
    let endpoint = endpoint_with_name(&pipe_name);

    let mut server = IpcServer::bind(&endpoint).expect("server bind failed");

    let server_task = tokio::spawn(async move {
        let mut server_stream = server.accept().await.expect("server accept failed");

        // Handle 5 sequential ping/pong exchanges
        for _ in 0..5 {
            let req = server_stream.read_frame().await.expect("server read ping");
            assert_eq!(req.header.message_type, MessageType::Request.as_u8());
            assert_eq!(req.payload, b"ping");

            let resp = IpcFrame::new(
                req.header.message_id,
                MessageType::Response.as_u8(),
                0,
                req.header.correlation_id,
                b"pong".to_vec(),
            )
            .unwrap();

            server_stream
                .write_frame(&resp)
                .await
                .expect("server write pong");
        }
    });

    let mut client_stream = IpcClient::connect(&endpoint)
        .await
        .expect("client connect failed");

    // Warm-up ping
    let warmup_frame =
        IpcFrame::new(1, MessageType::Request.as_u8(), 0, 0, b"ping".to_vec()).unwrap();
    client_stream.write_frame(&warmup_frame).await.unwrap();
    let warmup_resp = client_stream.read_frame().await.unwrap();
    assert_eq!(warmup_resp.payload, b"pong");

    // Benchmark 4 sequential round trips
    let mut total_latency = Duration::ZERO;
    for i in 1..=4 {
        let ping_frame = IpcFrame::new(
            100 + i,
            MessageType::Request.as_u8(),
            0,
            i as u32,
            b"ping".to_vec(),
        )
        .unwrap();

        let start = Instant::now();
        client_stream.write_frame(&ping_frame).await.unwrap();
        let pong_frame = client_stream.read_frame().await.unwrap();
        let latency = start.elapsed();
        total_latency += latency;

        assert_eq!(pong_frame.header.message_id, 100 + i);
        assert_eq!(
            pong_frame.header.message_type,
            MessageType::Response.as_u8()
        );
        assert_eq!(pong_frame.header.correlation_id, i as u32);
        assert_eq!(pong_frame.payload, b"pong");
    }

    let avg_latency = total_latency / 4;
    println!(
        "IPC loopback delivery average round-trip latency: {:?}",
        avg_latency
    );

    // Sub-millisecond requirement: avg_latency < 1.0ms (< 1000 µs, target < 100 µs)
    assert!(
        avg_latency < Duration::from_millis(1),
        "IPC round-trip latency must be sub-millisecond, got {:?}",
        avg_latency
    );

    server_task.await.unwrap();

    // Verify default_endpoint string resolution
    let def_ep = default_endpoint();
    assert!(!def_ep.is_empty());
}
