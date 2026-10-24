use fetchtrek_core::{NetworkTransport, SessionContext};
use fetchtrek_protocols::HttpTransport;
use fetchtrek_types::{ByteRange, NetworkError};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

async fn start_mock_server() -> (String, oneshot::Sender<()>) {
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
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
                        let method = parts[0];
                        let path = parts[1];

                        let response = if path == "/range-supported.bin" {
                            if method == "HEAD" {
                                b"HTTP/1.1 200 OK\r\nContent-Length: 1048576\r\nAccept-Ranges: bytes\r\nETag: \"etag-123\"\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"range-supported.bin\"\r\nConnection: close\r\n\r\n".to_vec()
                            } else if method == "GET" {
                                let mut body = vec![77u8; 100];
                                let mut header = format!(
                                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 100-199/1048576\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                    body.len()
                                ).into_bytes();
                                header.append(&mut body);
                                header
                            } else {
                                b"HTTP/1.1 405 Method Not Allowed\r\nConnection: close\r\n\r\n".to_vec()
                            }
                        } else if path == "/rate-limited" {
                            b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
                        } else if path == "/no-range" {
                            b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
                        } else if path == "/fallback-405" {
                            if method == "HEAD" {
                                b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
                            } else if method == "GET" {
                                b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-0/2048\r\nContent-Length: 1\r\nConnection: close\r\n\r\nX".to_vec()
                            } else {
                                b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n".to_vec()
                            }
                        } else {
                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
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
async fn test_probe_range_supported_endpoint() {
    let (base_url, _shutdown) = start_mock_server().await;
    let transport = HttpTransport::new();
    let ctx = SessionContext::new();

    let url = format!("{}/range-supported.bin", base_url);
    let probe = transport
        .probe(&url, &ctx)
        .await
        .expect("probe should succeed");

    assert_eq!(probe.total_bytes, Some(1_048_576));
    assert!(probe.supports_ranges);
    assert_eq!(probe.etag.as_deref(), Some("etag-123"));
    assert_eq!(probe.mime_type.as_deref(), Some("application/octet-stream"));
    assert_eq!(
        probe.suggested_filename.as_deref(),
        Some("range-supported.bin")
    );
}

#[tokio::test]
async fn test_range_stream_ingestion() {
    let (base_url, _shutdown) = start_mock_server().await;
    let transport = HttpTransport::new();
    let ctx = SessionContext::new();

    let url = format!("{}/range-supported.bin", base_url);
    let mut stream = transport
        .open_range_stream(&url, ByteRange::new(100, 199), &ctx)
        .await
        .expect("open range stream");

    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .expect("read stream to end");

    assert_eq!(buf.len(), 100);
    assert_eq!(buf, vec![77u8; 100]);
}

#[tokio::test]
async fn test_recovery_mapping_for_429() {
    let (base_url, _shutdown) = start_mock_server().await;
    let transport = HttpTransport::new();
    let ctx = SessionContext::new();

    let url = format!("{}/rate-limited", base_url);
    let err = transport
        .probe(&url, &ctx)
        .await
        .expect_err("should return rate limited error");

    assert_eq!(err, NetworkError::RateLimited);
    assert!(
        err.is_recoverable(),
        "429 must be classified as recoverable transient"
    );
}

#[tokio::test]
async fn test_terminal_mapping_for_416() {
    let (base_url, _shutdown) = start_mock_server().await;
    let transport = HttpTransport::new();
    let ctx = SessionContext::new();

    let url = format!("{}/no-range", base_url);
    let res = transport
        .open_range_stream(&url, ByteRange::new(100, 199), &ctx)
        .await;

    match res {
        Err(err) => {
            assert_eq!(err, NetworkError::NoRangeSupport);
            assert!(
                !err.is_recoverable(),
                "416 must be classified as terminal fault"
            );
        }
        Ok(_) => panic!("expected Err(NoRangeSupport), got Ok"),
    }
}

#[tokio::test]
async fn test_probe_fallback_on_405() {
    let (base_url, _shutdown) = start_mock_server().await;
    let transport = HttpTransport::new();
    let ctx = SessionContext::new();

    let url = format!("{}/fallback-405", base_url);
    let probe = transport
        .probe(&url, &ctx)
        .await
        .expect("probe should succeed via GET fallback");

    assert_eq!(probe.total_bytes, Some(2048));
    assert!(probe.supports_ranges);
}
