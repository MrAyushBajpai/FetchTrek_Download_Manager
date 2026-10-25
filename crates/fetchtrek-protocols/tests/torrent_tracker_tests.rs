use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use fetchtrek_protocols::{
    announce_http, announce_udp, AnnounceEvent, AnnounceRequest, BencodeValue, PeerEndpoint,
    TorrentError, TrackerCoordinator,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::oneshot;

fn sample_announce_request() -> AnnounceRequest {
    AnnounceRequest {
        info_hash: [
            0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC,
            0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78,
        ],
        peer_id: *b"-FT0001-123456789012",
        port: 6881,
        uploaded: 1024,
        downloaded: 2048,
        left: 100_000,
        event: AnnounceEvent::Started,
        num_want: Some(50),
    }
}

/// Test 1: HTTP Tracker with BEP 0023 compact binary peer representations.
#[tokio::test]
async fn test_http_tracker_compact_peers() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind http tracker listener");
    let port = listener.local_addr().expect("local addr").port();
    let tracker_url = format!("http://127.0.0.1:{port}/announce");

    // Spawn mock HTTP tracker server
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let mut buf = vec![0u8; 4096];
        let n = socket.read(&mut buf).await.expect("read request");
        let req_str = String::from_utf8_lossy(&buf[..n]);

        // Invariant: info_hash must be raw binary percent-encoded, not hex
        assert!(req_str.contains("info_hash=%12%34%56%78%9A%BC%DE%F0"));
        assert!(req_str.contains("compact=1"));
        assert!(req_str.contains("event=started"));

        let mut map = BTreeMap::new();
        map.insert(b"interval".to_vec(), BencodeValue::Integer(1800));
        map.insert(b"min interval".to_vec(), BencodeValue::Integer(300));
        map.insert(b"complete".to_vec(), BencodeValue::Integer(42));
        map.insert(b"incomplete".to_vec(), BencodeValue::Integer(5));

        // 2 compact peers (6 bytes each): 127.0.0.1:6881 and 192.168.1.50:51413
        let mut peer_bytes = Vec::new();
        peer_bytes.extend_from_slice(&[127, 0, 0, 1, 0x1A, 0xE1]);
        peer_bytes.extend_from_slice(&[192, 168, 1, 50, 0xC8, 0xD5]);
        map.insert(b"peers".to_vec(), BencodeValue::Bytes(peer_bytes));

        let body = BencodeValue::Dict(map).encode();
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(resp.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
        socket.shutdown().await.unwrap();
    });

    let client = reqwest::Client::new();
    let req = sample_announce_request();
    let res = announce_http(&client, &tracker_url, &req, Duration::from_secs(3))
        .await
        .expect("announce http");

    assert_eq!(res.interval, 1800);
    assert_eq!(res.min_interval, Some(300));
    assert_eq!(res.seeders, Some(42));
    assert_eq!(res.leechers, Some(5));
    assert_eq!(res.peers.len(), 2);

    assert_eq!(
        res.peers[0],
        PeerEndpoint {
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 6881,
            peer_id: None,
        }
    );
    assert_eq!(
        res.peers[1],
        PeerEndpoint {
            ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 50)),
            port: 51413,
            peer_id: None,
        }
    );
}

/// Test 2: HTTP Tracker with legacy BEP 0003 dictionary peer representations.
#[tokio::test]
async fn test_http_tracker_dict_peers() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind http tracker listener");
    let port = listener.local_addr().expect("local addr").port();
    let tracker_url = format!("http://127.0.0.1:{port}/announce");

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let mut buf = vec![0u8; 4096];
        let _ = socket.read(&mut buf).await.unwrap();

        let mut map = BTreeMap::new();
        map.insert(b"interval".to_vec(), BencodeValue::Integer(900));

        let mut peer1 = BTreeMap::new();
        peer1.insert(b"ip".to_vec(), BencodeValue::Bytes(b"10.0.0.1".to_vec()));
        peer1.insert(b"port".to_vec(), BencodeValue::Integer(8999));
        peer1.insert(b"peer id".to_vec(), BencodeValue::Bytes(vec![0x77; 20]));

        let mut peer2 = BTreeMap::new();
        peer2.insert(b"ip".to_vec(), BencodeValue::Bytes(b"10.0.0.2".to_vec()));
        peer2.insert(b"port".to_vec(), BencodeValue::Integer(9000));

        map.insert(
            b"peers".to_vec(),
            BencodeValue::List(vec![BencodeValue::Dict(peer1), BencodeValue::Dict(peer2)]),
        );

        let body = BencodeValue::Dict(map).encode();
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(resp.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
        socket.shutdown().await.unwrap();
    });

    let client = reqwest::Client::new();
    let req = sample_announce_request();
    let res = announce_http(&client, &tracker_url, &req, Duration::from_secs(3))
        .await
        .expect("announce http legacy dict");

    assert_eq!(res.interval, 900);
    assert_eq!(res.peers.len(), 2);
    assert_eq!(
        res.peers[0],
        PeerEndpoint {
            ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            port: 8999,
            peer_id: Some([0x77; 20]),
        }
    );
    assert_eq!(
        res.peers[1],
        PeerEndpoint {
            ip: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            port: 9000,
            peer_id: None,
        }
    );
}

/// Test 3: HTTP Tracker failure reason response.
#[tokio::test]
async fn test_http_tracker_failure_response() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind http tracker listener");
    let port = listener.local_addr().expect("local addr").port();
    let tracker_url = format!("http://127.0.0.1:{port}/announce");

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let mut buf = vec![0u8; 4096];
        let _ = socket.read(&mut buf).await.unwrap();

        let mut map = BTreeMap::new();
        map.insert(
            b"failure reason".to_vec(),
            BencodeValue::Bytes(b"torrent not found".to_vec()),
        );

        let body = BencodeValue::Dict(map).encode();
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(resp.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
        socket.shutdown().await.unwrap();
    });

    let client = reqwest::Client::new();
    let req = sample_announce_request();
    let err = announce_http(&client, &tracker_url, &req, Duration::from_secs(3))
        .await
        .expect_err("must return failure reason error");

    match err {
        TorrentError::TrackerError(msg) => {
            assert_eq!(msg, "torrent not found");
        }
        other => panic!("Expected TrackerError, got: {other:?}"),
    }
}

/// Test 4: BEP 0015 Binary UDP Tracker complete lifecycle (Connect + Announce).
#[tokio::test]
async fn test_udp_tracker_lifecycle() {
    let socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind udp tracker server");
    let port = socket.local_addr().expect("local addr").port();
    let tracker_url = format!("udp://127.0.0.1:{port}/announce");

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let mut buf = [0u8; 1024];

        // 1. Await Connect packet (16 bytes)
        tokio::select! {
            _ = &mut shutdown_rx => return,
            res = socket.recv_from(&mut buf) => {
                let (n, peer_addr) = res.expect("recv connect");
                assert_eq!(n, 16);
                let protocol_id = u64::from_be_bytes([buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]]);
                assert_eq!(protocol_id, 0x0417_2710_1980);
                let action = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
                assert_eq!(action, 0); // Connect action
                let tx_id = &buf[12..16];

                // Send connect response: action (0), tx_id, connection_id
                let mut resp = [0u8; 16];
                resp[0..4].copy_from_slice(&0u32.to_be_bytes());
                resp[4..8].copy_from_slice(tx_id);
                resp[8..16].copy_from_slice(&0x1234_5678_9ABC_DEF0u64.to_be_bytes());
                socket.send_to(&resp, peer_addr).await.expect("send connect response");
            }
        }

        // 2. Await Announce packet (98 bytes)
        let (n, peer_addr) = socket.recv_from(&mut buf).await.expect("recv announce");
        assert_eq!(n, 98);
        let connection_id = u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        assert_eq!(connection_id, 0x1234_5678_9ABC_DEF0u64);
        let action = u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]);
        assert_eq!(action, 1); // Announce action
        let tx_id = &buf[12..16];

        // Send announce response: action (1), tx_id, interval (1800), leechers (10), seeders (25), 2 peers
        let mut resp = Vec::new();
        resp.extend_from_slice(&1u32.to_be_bytes());
        resp.extend_from_slice(tx_id);
        resp.extend_from_slice(&1800u32.to_be_bytes()); // interval
        resp.extend_from_slice(&10u32.to_be_bytes()); // leechers
        resp.extend_from_slice(&25u32.to_be_bytes()); // seeders

        // Compact peers (127.0.0.1:6881, 10.10.10.10:8080)
        resp.extend_from_slice(&[127, 0, 0, 1, 0x1A, 0xE1]);
        resp.extend_from_slice(&[10, 10, 10, 10, 0x1F, 0x90]);

        socket
            .send_to(&resp, peer_addr)
            .await
            .expect("send announce response");
    });

    let req = sample_announce_request();
    let res = announce_udp(&tracker_url, &req, Duration::from_secs(3))
        .await
        .expect("announce udp");

    assert_eq!(res.interval, 1800);
    assert_eq!(res.leechers, Some(10));
    assert_eq!(res.seeders, Some(25));
    assert_eq!(res.peers.len(), 2);

    assert_eq!(
        res.peers[0],
        PeerEndpoint {
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port: 6881,
            peer_id: None,
        }
    );
    assert_eq!(
        res.peers[1],
        PeerEndpoint {
            ip: IpAddr::V4(Ipv4Addr::new(10, 10, 10, 10)),
            port: 8080,
            peer_id: None,
        }
    );

    let _ = shutdown_tx.send(());
}

/// Test 5: UDP Tracker error packet (action == 3).
#[tokio::test]
async fn test_udp_tracker_error_packet() {
    let socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind udp tracker server");
    let port = socket.local_addr().expect("local addr").port();
    let tracker_url = format!("udp://127.0.0.1:{port}");

    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        let (n, peer_addr) = socket.recv_from(&mut buf).await.expect("recv packet");
        assert!(n >= 16);
        let tx_id = &buf[12..16];

        // Respond with action = 3 (error)
        let mut resp = Vec::new();
        resp.extend_from_slice(&3u32.to_be_bytes());
        resp.extend_from_slice(tx_id);
        resp.extend_from_slice(b"unauthorized torrent access");
        socket.send_to(&resp, peer_addr).await.unwrap();
    });

    let req = sample_announce_request();
    let err = announce_udp(&tracker_url, &req, Duration::from_secs(3))
        .await
        .expect_err("udp error packet must fail");

    match err {
        TorrentError::TrackerError(msg) => {
            assert_eq!(msg, "unauthorized torrent access");
        }
        other => panic!("Expected TrackerError, got: {other:?}"),
    }
}

/// Test 6: UDP Tracker timeout handling.
#[tokio::test]
async fn test_udp_tracker_timeout() {
    let socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind silent udp server");
    let port = socket.local_addr().expect("local addr").port();
    let tracker_url = format!("udp://127.0.0.1:{port}");

    // Silent server never responds
    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        let _ = socket.recv_from(&mut buf).await;
        // Do not respond
    });

    let req = sample_announce_request();
    let err = announce_udp(&tracker_url, &req, Duration::from_millis(50))
        .await
        .expect_err("must time out");

    match err {
        TorrentError::TrackerNetwork(msg) => {
            assert!(msg.contains("timed out"));
        }
        other => panic!("Expected TrackerNetwork timeout error, got: {other:?}"),
    }
}

/// Test 7: Multi-tier failover and tracker priority promotion (BEP 0012).
#[tokio::test]
async fn test_tracker_tier_failover() {
    // Tier 0: Failing tracker on unopened port
    let dead_port = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    };
    let dead_tracker_url = format!("http://127.0.0.1:{dead_port}/announce");

    // Tier 1: Working tracker
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind working tracker");
    let live_port = listener.local_addr().expect("local addr").port();
    let live_tracker_url = format!("http://127.0.0.1:{live_port}/announce");

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept connection");
        let mut buf = vec![0u8; 4096];
        let _ = socket.read(&mut buf).await.unwrap();

        let mut map = BTreeMap::new();
        map.insert(b"interval".to_vec(), BencodeValue::Integer(1800));
        let mut peers = Vec::new();
        peers.extend_from_slice(&[10, 0, 0, 99, 0x1A, 0xE1]);
        map.insert(b"peers".to_vec(), BencodeValue::Bytes(peers));

        let body = BencodeValue::Dict(map).encode();
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(resp.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
        socket.shutdown().await.unwrap();
    });

    // Configure 2 tiers: Tier 0 = dead tracker, Tier 1 = live tracker
    let tiers = vec![
        vec![dead_tracker_url.clone()],
        vec![live_tracker_url.clone()],
    ];

    let mut coordinator =
        TrackerCoordinator::from_tiers(tiers).with_timeout(Duration::from_millis(200));

    assert_eq!(coordinator.current_tier(), 0);
    assert_eq!(
        coordinator.current_tracker(),
        Some(dead_tracker_url.as_str())
    );

    let req = sample_announce_request();
    let res = coordinator
        .announce(&req)
        .await
        .expect("coordinator must failover to tier 1");

    assert_eq!(res.interval, 1800);
    assert_eq!(res.peers.len(), 1);
    assert_eq!(res.peers[0].ip, IpAddr::V4(Ipv4Addr::new(10, 0, 0, 99)));

    // Active tier must be promoted to Tier 1
    assert_eq!(coordinator.current_tier(), 1);
    assert_eq!(
        coordinator.current_tracker(),
        Some(live_tracker_url.as_str())
    );
}
