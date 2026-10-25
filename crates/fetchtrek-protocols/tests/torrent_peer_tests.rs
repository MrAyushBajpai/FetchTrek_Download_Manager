use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use fetchtrek_protocols::{
    PeerConnection, PeerHandshake, PeerMessage, TorrentError, STANDARD_BLOCK_SIZE,
};

const TEST_INFO_HASH: [u8; 20] = [
    0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
    0x12, 0x34, 0x56, 0x78,
];
const CLIENT_PEER_ID: [u8; 20] = *b"-FT0001-CLIENTPEERID";
const SERVER_PEER_ID: [u8; 20] = *b"-FT0001-SERVERPEERID";

/// Test 1: Mock TCP server and client exchange valid 68-byte handshakes.
#[tokio::test]
async fn test_handshake_roundtrip() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let client_hs = PeerHandshake::read_from(&mut socket, Some(&TEST_INFO_HASH))
            .await
            .expect("server read handshake");
        assert_eq!(client_hs.peer_id, CLIENT_PEER_ID);
        assert_eq!(client_hs.info_hash, TEST_INFO_HASH);

        // Server responds with Fast Extension enabled
        let mut server_reserved = [0u8; 8];
        server_reserved[7] = 0x04;
        let server_hs =
            PeerHandshake::new(TEST_INFO_HASH, SERVER_PEER_ID).with_reserved(server_reserved);
        server_hs
            .write_to(&mut socket)
            .await
            .expect("server write handshake");
    });

    let conn = PeerConnection::connect(
        addr,
        TEST_INFO_HASH,
        CLIENT_PEER_ID,
        100,
        Duration::from_secs(3),
    )
    .await
    .expect("client connect");

    assert_eq!(conn.remote_peer_id, SERVER_PEER_ID);
    assert_eq!(conn.remote_reserved[7], 0x04);
    assert!(conn.supports_fast_extension());
    assert_eq!(conn.num_pieces(), 100);

    server_task.await.expect("join server task");
}

/// Test 2: Client connects to mock server that responds with a mismatched info_hash.
#[tokio::test]
async fn test_handshake_mismatched_info_hash() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let _ = PeerHandshake::read_from(&mut socket, None).await;

        let wrong_hash = [0xFFu8; 20];
        let server_hs = PeerHandshake::new(wrong_hash, SERVER_PEER_ID);
        let _ = server_hs.write_to(&mut socket).await;
    });

    let result = PeerConnection::connect(
        addr,
        TEST_INFO_HASH,
        CLIENT_PEER_ID,
        100,
        Duration::from_secs(3),
    )
    .await;

    match result {
        Err(TorrentError::HandshakeError(msg)) => {
            assert!(
                msg.contains("Mismatched info_hash") || msg.contains("mismatch"),
                "Expected mismatched info_hash error, got: {msg}"
            );
        }
        other => panic!("Expected HandshakeError, got: {other:?}"),
    }

    server_task.await.expect("join server task");
}

/// Test 3: Server sends invalid protocol prefix ("BitTorrent wrong").
#[tokio::test]
async fn test_handshake_invalid_protocol_string() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let _ = PeerHandshake::read_from(&mut socket, None).await;

        // Malformed handshake with "BitTorrent wrong!19"
        let mut malformed = [0u8; 68];
        malformed[0] = 19;
        malformed[1..20].copy_from_slice(b"BitTorrent wrong!19");
        malformed[28..48].copy_from_slice(&TEST_INFO_HASH);
        malformed[48..68].copy_from_slice(&SERVER_PEER_ID);

        socket
            .write_all(&malformed)
            .await
            .expect("server write malformed");
    });

    let result = PeerConnection::connect(
        addr,
        TEST_INFO_HASH,
        CLIENT_PEER_ID,
        100,
        Duration::from_secs(3),
    )
    .await;

    match result {
        Err(TorrentError::HandshakeError(msg)) => {
            assert!(
                msg.contains("Invalid protocol string"),
                "Expected invalid protocol string error, got: {msg}"
            );
        }
        other => panic!("Expected HandshakeError, got: {other:?}"),
    }

    server_task.await.expect("join server task");
}

/// Test 4: Encode and decode every PeerMessage variant (wire codec roundtrip).
#[test]
fn test_wire_message_codec_roundtrip() {
    let test_messages = vec![
        PeerMessage::KeepAlive,
        PeerMessage::Choke,
        PeerMessage::Unchoke,
        PeerMessage::Interested,
        PeerMessage::NotInterested,
        PeerMessage::Have { piece_index: 42 },
        PeerMessage::Bitfield(vec![0xAA, 0x55, 0xF0, 0x0F]),
        PeerMessage::Request {
            index: 3,
            begin: 16384,
            length: STANDARD_BLOCK_SIZE,
        },
        PeerMessage::Piece {
            index: 3,
            begin: 16384,
            block: vec![0xAB; 16384],
        },
        PeerMessage::Cancel {
            index: 3,
            begin: 16384,
            length: STANDARD_BLOCK_SIZE,
        },
        PeerMessage::SuggestPiece { piece_index: 12 },
        PeerMessage::HaveAll,
        PeerMessage::HaveNone,
        PeerMessage::RejectRequest {
            index: 5,
            begin: 0,
            length: 16384,
        },
        PeerMessage::AllowedFast { piece_index: 7 },
    ];

    for original in test_messages {
        let encoded = original.encode();
        let decoded = PeerMessage::decode(&encoded)
            .unwrap_or_else(|e| panic!("Failed to decode {original:?}: {e}"));
        assert_eq!(decoded, original);

        let re_encoded = decoded.encode();
        assert_eq!(re_encoded, encoded);
    }
}

/// Test 5: Bitfield and Have tracking.
#[tokio::test]
async fn test_bitfield_and_have_tracking() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let _ = PeerHandshake::read_from(&mut socket, Some(&TEST_INFO_HASH))
            .await
            .expect("server read handshake");

        let server_hs = PeerHandshake::new(TEST_INFO_HASH, SERVER_PEER_ID);
        server_hs
            .write_to(&mut socket)
            .await
            .expect("server write handshake");

        // Pieces [0, 2, 7] set for a 10-piece torrent (2 bytes total: 10 pieces)
        // Byte 0: piece 0 (0x80) | piece 2 (0x20) | piece 7 (0x01) = 0xA1
        // Byte 1: piece 8, 9 (0x00) + 6 spare 0-bits
        let bitfield_msg = PeerMessage::Bitfield(vec![0xA1, 0x00]);
        socket
            .write_all(&bitfield_msg.encode())
            .await
            .expect("server send bitfield");

        // Followed by Have for piece 3
        let have_msg = PeerMessage::Have { piece_index: 3 };
        socket
            .write_all(&have_msg.encode())
            .await
            .expect("server send have");
    });

    let mut conn = PeerConnection::connect(
        addr,
        TEST_INFO_HASH,
        CLIENT_PEER_ID,
        10,
        Duration::from_secs(3),
    )
    .await
    .expect("client connect");

    // Receive Bitfield
    let msg1 = conn.recv().await.expect("recv bitfield");
    assert!(matches!(msg1, PeerMessage::Bitfield(_)));

    assert!(conn.has_piece(0));
    assert!(!conn.has_piece(1));
    assert!(conn.has_piece(2));
    assert!(!conn.has_piece(3));
    assert!(!conn.has_piece(4));
    assert!(!conn.has_piece(5));
    assert!(!conn.has_piece(6));
    assert!(conn.has_piece(7));
    assert!(!conn.has_piece(8));
    assert!(!conn.has_piece(9));

    // Receive Have { piece_index: 3 }
    let msg2 = conn.recv().await.expect("recv have");
    assert_eq!(msg2, PeerMessage::Have { piece_index: 3 });

    // Verify 0, 2, 3, 7 return true, and all others return false
    assert!(conn.has_piece(0));
    assert!(!conn.has_piece(1));
    assert!(conn.has_piece(2));
    assert!(conn.has_piece(3));
    assert!(!conn.has_piece(4));
    assert!(!conn.has_piece(5));
    assert!(!conn.has_piece(6));
    assert!(conn.has_piece(7));
    assert!(!conn.has_piece(8));
    assert!(!conn.has_piece(9));
    assert!(!conn.has_piece(10)); // Out of bounds

    server_task.await.expect("join server task");
}

/// Test 6: Block request and piece exchange.
#[tokio::test]
async fn test_block_request_and_piece_exchange() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let _ = PeerHandshake::read_from(&mut socket, Some(&TEST_INFO_HASH))
            .await
            .expect("server read handshake");

        let server_hs = PeerHandshake::new(TEST_INFO_HASH, SERVER_PEER_ID);
        server_hs
            .write_to(&mut socket)
            .await
            .expect("server write handshake");

        // Read Interested from client
        let mut server_conn = PeerConnection::from_stream(socket, CLIENT_PEER_ID, [0u8; 8], 10);
        let msg = server_conn.recv().await.expect("server recv interested");
        assert_eq!(msg, PeerMessage::Interested);
        assert!(server_conn.state.peer_interested);

        // Send Unchoke to client
        server_conn
            .send(&PeerMessage::Unchoke)
            .await
            .expect("server send unchoke");

        // Read Request { index: 0, begin: 0, length: 16384 }
        let req_msg = server_conn.recv().await.expect("server recv request");
        assert_eq!(
            req_msg,
            PeerMessage::Request {
                index: 0,
                begin: 0,
                length: STANDARD_BLOCK_SIZE,
            }
        );

        // Send Piece with 16 KiB test payload
        let test_block = vec![0x5A; STANDARD_BLOCK_SIZE as usize];
        server_conn
            .send(&PeerMessage::Piece {
                index: 0,
                begin: 0,
                block: test_block,
            })
            .await
            .expect("server send piece");
    });

    let mut conn = PeerConnection::connect(
        addr,
        TEST_INFO_HASH,
        CLIENT_PEER_ID,
        10,
        Duration::from_secs(3),
    )
    .await
    .expect("client connect");

    assert!(conn.state.peer_choking);

    // Client sends Interested
    conn.send(&PeerMessage::Interested)
        .await
        .expect("send interested");
    assert!(conn.state.am_interested);

    // Client receives Unchoke
    let unchoke_msg = conn.recv().await.expect("recv unchoke");
    assert_eq!(unchoke_msg, PeerMessage::Unchoke);
    assert!(!conn.state.peer_choking);

    // Client requests block
    conn.send(&PeerMessage::Request {
        index: 0,
        begin: 0,
        length: STANDARD_BLOCK_SIZE,
    })
    .await
    .expect("send request");

    // Client receives Piece
    let piece_msg = conn.recv().await.expect("recv piece");
    match piece_msg {
        PeerMessage::Piece {
            index,
            begin,
            block,
        } => {
            assert_eq!(index, 0);
            assert_eq!(begin, 0);
            assert_eq!(block.len(), STANDARD_BLOCK_SIZE as usize);
            assert_eq!(block[0], 0x5A);
            assert_eq!(block[STANDARD_BLOCK_SIZE as usize - 1], 0x5A);
        }
        other => panic!("Expected Piece message, got: {other:?}"),
    }

    server_task.await.expect("join server task");
}

/// Test 7: Oversized message DoS rejection.
#[tokio::test]
async fn test_oversized_message_dos_rejection() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let _ = PeerHandshake::read_from(&mut socket, Some(&TEST_INFO_HASH))
            .await
            .expect("server read handshake");

        let server_hs = PeerHandshake::new(TEST_INFO_HASH, SERVER_PEER_ID);
        server_hs
            .write_to(&mut socket)
            .await
            .expect("server write handshake");

        // Send 4-byte length prefix of 500 KiB (> 256 KiB ceiling)
        let oversized_len: u32 = 500 * 1024;
        socket
            .write_all(&oversized_len.to_be_bytes())
            .await
            .expect("server write oversized length");
    });

    let mut conn = PeerConnection::connect(
        addr,
        TEST_INFO_HASH,
        CLIENT_PEER_ID,
        10,
        Duration::from_secs(3),
    )
    .await
    .expect("client connect");

    let recv_result = conn.recv().await;
    match recv_result {
        Err(TorrentError::ProtocolError(msg)) => {
            assert!(
                msg.contains("exceeds maximum allowed boundary"),
                "Expected boundary error, got: {msg}"
            );
        }
        other => panic!("Expected ProtocolError, got: {other:?}"),
    }

    server_task.await.expect("join server task");
}

/// Test 8: Bitfield trailing spare bits rejection.
#[tokio::test]
async fn test_bitfield_trailing_spare_bits_rejection() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let _ = PeerHandshake::read_from(&mut socket, Some(&TEST_INFO_HASH))
            .await
            .expect("server read handshake");

        let server_hs = PeerHandshake::new(TEST_INFO_HASH, SERVER_PEER_ID);
        server_hs
            .write_to(&mut socket)
            .await
            .expect("server write handshake");

        // For a 10-piece torrent, last byte has 6 spare bits (bits 0..=5).
        // If bit 0 of last byte is set (0x01), it violates BEP 0003.
        let invalid_bitfield = PeerMessage::Bitfield(vec![0x00, 0x01]);
        socket
            .write_all(&invalid_bitfield.encode())
            .await
            .expect("server write invalid bitfield");
    });

    let mut conn = PeerConnection::connect(
        addr,
        TEST_INFO_HASH,
        CLIENT_PEER_ID,
        10,
        Duration::from_secs(3),
    )
    .await
    .expect("client connect");

    let recv_result = conn.recv().await;
    match recv_result {
        Err(TorrentError::ProtocolError(msg)) => {
            assert!(
                msg.contains("trailing spare positions"),
                "Expected trailing spare positions error, got: {msg}"
            );
        }
        other => panic!("Expected ProtocolError, got: {other:?}"),
    }

    server_task.await.expect("join server task");
}

/// Test 9: Graceful connection close on EOF.
#[tokio::test]
async fn test_connection_closed_on_remote_eof() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let _ = PeerHandshake::read_from(&mut socket, Some(&TEST_INFO_HASH))
            .await
            .expect("server read handshake");

        let server_hs = PeerHandshake::new(TEST_INFO_HASH, SERVER_PEER_ID);
        server_hs
            .write_to(&mut socket)
            .await
            .expect("server write handshake");

        // Explicitly shutdown/drop server socket
        socket.shutdown().await.expect("shutdown socket");
    });

    let mut conn = PeerConnection::connect(
        addr,
        TEST_INFO_HASH,
        CLIENT_PEER_ID,
        10,
        Duration::from_secs(3),
    )
    .await
    .expect("client connect");

    let recv_result = conn.recv().await;
    match recv_result {
        Err(TorrentError::ConnectionClosed) => {}
        other => panic!("Expected ConnectionClosed, got: {other:?}"),
    }

    server_task.await.expect("join server task");
}
