use std::collections::BTreeMap;

use fetchtrek_core::{NetworkTransport, SessionContext};
use fetchtrek_protocols::{
    BencodeValue, HttpTransport, PeerMessage, PickerStrategy, PieceBuffer, PiecePicker,
    TorrentError, TorrentMeta, MAX_MESSAGE_LENGTH,
};
use fetchtrek_types::{ByteRange, NetworkError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

fn make_test_torrent(piece_length: i64, pieces_bytes: Vec<u8>, file_length: i64) -> Vec<u8> {
    let mut info = BTreeMap::new();
    info.insert(
        b"piece length".to_vec(),
        BencodeValue::Integer(piece_length),
    );
    info.insert(b"pieces".to_vec(), BencodeValue::Bytes(pieces_bytes));
    info.insert(
        b"name".to_vec(),
        BencodeValue::Bytes(b"test_payload.bin".to_vec()),
    );
    info.insert(b"length".to_vec(), BencodeValue::Integer(file_length));

    let mut root = BTreeMap::new();
    root.insert(b"info".to_vec(), BencodeValue::Dict(info));
    root.insert(
        b"announce".to_vec(),
        BencodeValue::Bytes(b"http://tracker.example.com/announce".to_vec()),
    );

    BencodeValue::Dict(root).encode()
}

#[test]
fn test_torrent_piece_length_overflow() {
    // 1. Piece length exceeds u32::MAX (> 4 GiB)
    let raw_overflow = make_test_torrent(5_000_000_000, vec![0u8; 20], 5_000_000_000);
    let err =
        TorrentMeta::from_bytes(&raw_overflow).expect_err("Should reject piece length > u32::MAX");
    match err {
        TorrentError::InvalidMetainfo(msg) => {
            assert!(msg.contains("piece length") || msg.contains("overflow"));
        }
        other => panic!("Expected InvalidMetainfo, got: {:?}", other),
    }

    // 2. Piece length zero or negative
    let raw_zero = make_test_torrent(0, vec![0u8; 20], 1024);
    assert!(TorrentMeta::from_bytes(&raw_zero).is_err());

    let raw_neg = make_test_torrent(-16384, vec![0u8; 20], 1024);
    assert!(TorrentMeta::from_bytes(&raw_neg).is_err());
}

#[test]
fn test_torrent_piece_count_inconsistency() {
    // 100,000 bytes with 16,384 piece length requires ceil(100000 / 16384) = 7 pieces (140 bytes)
    // Providing only 3 pieces (60 bytes) must fail piece count consistency check
    let raw_inconsistent = make_test_torrent(16384, vec![0u8; 60], 100_000);
    let err = TorrentMeta::from_bytes(&raw_inconsistent)
        .expect_err("Should reject piece count inconsistency");
    match err {
        TorrentError::InvalidMetainfo(msg) => {
            assert!(msg.contains("Piece count inconsistency") || msg.contains("pieces"));
        }
        other => panic!("Expected InvalidMetainfo, got: {:?}", other),
    }
}

#[test]
fn test_torrent_unsolicited_block_rejection() {
    let num_pieces = 4;
    let piece_length = 32768; // 2 blocks per piece
    let total_size = (num_pieces as u64) * (piece_length as u64);
    let mut picker = PiecePicker::new(
        num_pieces,
        piece_length,
        total_size,
        PickerStrategy::Sequential,
    );

    let peer_a = [1u8; 20];
    let peer_b = [2u8; 20];
    let block_data = vec![0xABu8; 16384];

    // 1. Ingestion without any prior request must be rejected
    let err = picker
        .ingest_block_from_peer(0, 0, &block_data, Some(&peer_a))
        .expect_err("Unsolicited block must be rejected");
    match err {
        TorrentError::ProtocolError(msg) => {
            assert!(msg.contains("Unsolicited block"));
        }
        other => panic!("Expected ProtocolError, got: {:?}", other),
    }

    // Verify no buffer was allocated for piece 0
    assert_eq!(picker.active_buffers(), 0);

    // 2. Announce peer_a has pieces and pick a block
    picker.record_peer_bitfield(&[0xF0]); // peer_a has all 4 pieces
    let picked = picker.pick_block(&peer_a, |_| true).expect("pick a block");
    assert_eq!(picked.piece_index, 0);
    assert_eq!(picked.block_offset, 0);

    // 3. Ingestion attributed to peer_b while peer_a requested it must be rejected
    let err_peer = picker
        .ingest_block_from_peer(0, 0, &block_data, Some(&peer_b))
        .expect_err("Block from non-requesting peer must be rejected");
    match err_peer {
        TorrentError::ProtocolError(msg) => {
            assert!(msg.contains("peer mismatch"));
        }
        other => panic!("Expected ProtocolError, got: {:?}", other),
    }

    // 4. Ingestion from correct peer_a succeeds
    let res = picker.ingest_block_from_peer(0, 0, &block_data, Some(&peer_a));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), None); // piece 0 has 2 blocks, 1 remaining
    assert_eq!(picker.active_buffers(), 1);
}

#[test]
fn test_torrent_misaligned_block_rejection() {
    let mut buffer = PieceBuffer::new(32768);

    // 1. Misaligned offset (not a multiple of 16 KiB)
    let misaligned_err = buffer
        .insert_block(100, &vec![0u8; 16384])
        .expect_err("Misaligned block offset must be rejected");
    match misaligned_err {
        TorrentError::ProtocolError(msg) => {
            assert!(msg.contains("aligned"));
        }
        other => panic!("Expected ProtocolError, got: {:?}", other),
    }

    // 2. Invalid block length (5000 bytes instead of 16384 bytes)
    let invalid_len_err = buffer
        .insert_block(0, &vec![0u8; 5000])
        .expect_err("Invalid block length must be rejected");
    match invalid_len_err {
        TorrentError::ProtocolError(msg) => {
            assert!(msg.contains("length"));
        }
        other => panic!("Expected ProtocolError, got: {:?}", other),
    }

    // 3. First valid block insert succeeds
    let res = buffer
        .insert_block(0, &vec![0u8; 16384])
        .expect("Valid insert");
    assert!(res);
    assert!(!buffer.is_complete());

    // 4. Duplicate block insertion at offset 0 must be rejected
    let duplicate_err = buffer
        .insert_block(0, &vec![0u8; 16384])
        .expect_err("Duplicate block insert must be rejected");
    match duplicate_err {
        TorrentError::ProtocolError(msg) => {
            assert!(msg.contains("Duplicate or overlapping"));
        }
        other => panic!("Expected ProtocolError, got: {:?}", other),
    }
}

async fn start_mock_http_integrity_server() -> (String, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock server");
    let addr = listener.local_addr().expect("local addr");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                Ok((mut socket, _)) = listener.accept() => {
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 4096];
                        let n = match socket.read(&mut buf).await {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        };
                        let req = String::from_utf8_lossy(&buf[..n]);
                        let first_line = req.lines().next().unwrap_or("");
                        let parts: Vec<&str> = first_line.split_whitespace().collect();
                        if parts.len() < 2 {
                            return;
                        }
                        let path = parts[1];

                        let response: Vec<u8> = match path {
                            "/mismatched-range" => {
                                // Client requests 100-199, server sends 50-149
                                b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 50-149/1000\r\nContent-Length: 100\r\nConnection: close\r\n\r\n".to_vec()
                            }
                            "/missing-content-range" => {
                                // 206 without Content-Range header
                                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 100\r\nConnection: close\r\n\r\n".to_vec()
                            }
                            "/mismatched-total" => {
                                // Expected total 1000, Content-Range claims 500
                                b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 100-199/500\r\nContent-Length: 100\r\nConnection: close\r\n\r\n".to_vec()
                            }
                            "/precondition-failed" => {
                                b"HTTP/1.1 412 Precondition Failed\r\nConnection: close\r\n\r\n".to_vec()
                            }
                            "/ignored-range-200" => {
                                // If-Range mismatch: server ignores Range and serves full 200 OK
                                b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nETag: \"new-etag\"\r\nConnection: close\r\n\r\n".to_vec()
                            }
                            "/etag-header-mismatch" => {
                                // 206 with mismatched ETag header
                                b"HTTP/1.1 206 Partial Content\r\nETag: \"etag-version-2\"\r\nContent-Range: bytes 100-199/1000\r\nContent-Length: 100\r\nConnection: close\r\n\r\n".to_vec()
                            }
                            _ => b"HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n".to_vec(),
                        };

                        let _ = socket.write_all(&response).await;
                        let _ = socket.shutdown().await;
                    });
                }
            }
        }
    });

    (format!("http://{}", addr), shutdown_tx)
}

#[tokio::test]
async fn test_http_range_mismatched_content_range() {
    let (base_url, shutdown_tx) = start_mock_http_integrity_server().await;
    let transport = HttpTransport::new();
    let range = ByteRange::new(100, 199);

    // 1. Mismatched start/end offsets in 206 response
    let ctx = SessionContext::default();
    let err = transport
        .open_range_stream(&format!("{}/mismatched-range", base_url), range, &ctx)
        .await
        .expect_err("Should reject mismatched start/end in Content-Range");
    assert_eq!(err, NetworkError::ProtocolError);

    // 2. Missing Content-Range in 206 response
    let err_missing = transport
        .open_range_stream(&format!("{}/missing-content-range", base_url), range, &ctx)
        .await
        .expect_err("Should reject missing Content-Range in 206");
    assert_eq!(err_missing, NetworkError::ProtocolError);

    // 3. Mismatched total bytes against SessionContext expectation
    let ctx_pinned_total = SessionContext::default().with_expected_total_bytes(1000);
    let err_total = transport
        .open_range_stream(
            &format!("{}/mismatched-total", base_url),
            range,
            &ctx_pinned_total,
        )
        .await
        .expect_err("Should reject mismatched total bytes in Content-Range");
    assert_eq!(err_total, NetworkError::ProtocolError);

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn test_http_etag_mismatch_aborts_segmented_download() {
    let (base_url, shutdown_tx) = start_mock_http_integrity_server().await;
    let transport = HttpTransport::new();
    let range = ByteRange::new(100, 199);
    let ctx_pinned = SessionContext::default().with_etag("etag-version-1");

    // 1. 412 Precondition Failed
    let err_412 = transport
        .open_range_stream(
            &format!("{}/precondition-failed", base_url),
            range,
            &ctx_pinned,
        )
        .await
        .expect_err("Should convert 412 to ETagMismatch");
    assert_eq!(err_412, NetworkError::ETagMismatch);

    // 2. 200 OK returned on range request with pinned ETag
    let err_200 = transport
        .open_range_stream(
            &format!("{}/ignored-range-200", base_url),
            range,
            &ctx_pinned,
        )
        .await
        .expect_err("Should convert 200 on range with pinned ETag to ETagMismatch");
    assert_eq!(err_200, NetworkError::ETagMismatch);

    // 3. 206 Partial Content returning an ETag header that mismatches pinned context
    let err_etag_resp = transport
        .open_range_stream(
            &format!("{}/etag-header-mismatch", base_url),
            range,
            &ctx_pinned,
        )
        .await
        .expect_err("Should reject mismatched ETag in 206 response");
    assert_eq!(err_etag_resp, NetworkError::ETagMismatch);

    let _ = shutdown_tx.send(());
}

#[test]
fn test_peer_wire_oversized_length_rejected_before_alloc() {
    // 1. Oversized message length boundary check
    let oversized_len = MAX_MESSAGE_LENGTH + 1;
    let err = PeerMessage::decode_payload(oversized_len, &[])
        .expect_err("Should reject message length > MAX_MESSAGE_LENGTH");
    match err {
        TorrentError::ProtocolError(msg) => {
            assert!(msg.contains("exceeds maximum allowed boundary"));
        }
        other => panic!("Expected ProtocolError, got: {:?}", other),
    }

    // 2. BEP 0003 Bitfield validation
    // 10 pieces -> ceil(10/8) = 2 bytes.
    // Byte 0: pieces 0..7
    // Byte 1: pieces 8..9, spare bits 0..5 (mask 0x3F)

    // Valid: pieces 8 and 9 set (0xC0), trailing bits zero
    let valid_bitfield = vec![0xFF, 0xC0];
    assert!(PeerMessage::validate_bitfield(&valid_bitfield, 10).is_ok());

    // Invalid length: only 1 byte provided for 10 pieces
    let short_bitfield = vec![0xFF];
    let err_len = PeerMessage::validate_bitfield(&short_bitfield, 10)
        .expect_err("Should reject bitfield of incorrect length");
    match err_len {
        TorrentError::ProtocolError(msg) => {
            assert!(msg.contains("does not match expected"));
        }
        other => panic!("Expected ProtocolError, got: {:?}", other),
    }

    // Invalid trailing spare bits: lowest bit set in byte 1 (0xC1)
    let dirty_spare_bitfield = vec![0xFF, 0xC1];
    let err_spare = PeerMessage::validate_bitfield(&dirty_spare_bitfield, 10)
        .expect_err("Should reject non-zero spare bits");
    match err_spare {
        TorrentError::ProtocolError(msg) => {
            assert!(msg.contains("trailing spare positions"));
        }
        other => panic!("Expected ProtocolError, got: {:?}", other),
    }
}
