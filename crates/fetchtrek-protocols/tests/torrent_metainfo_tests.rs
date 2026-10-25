use std::collections::BTreeMap;
use std::path::PathBuf;

use fetchtrek_protocols::{
    decode_base32, sha1, to_base32, BencodeValue, MagnetLink, Sha1, TorrentError, TorrentFile,
    TorrentMeta,
};

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn test_bencode_primitives() {
    // 1. Integers
    for raw in [b"i0e".as_slice(), b"i42e", b"i-999e", b"i12345678901234e"] {
        let val = BencodeValue::decode(raw).expect("decode int");
        assert_eq!(val.encode(), raw);
    }

    assert_eq!(BencodeValue::decode(b"i0e").unwrap().as_int(), Some(0));
    assert_eq!(BencodeValue::decode(b"i42e").unwrap().as_int(), Some(42));
    assert_eq!(
        BencodeValue::decode(b"i-999e").unwrap().as_int(),
        Some(-999)
    );

    // 2. Byte strings
    let raw_str = b"4:spam";
    let val_str = BencodeValue::decode(raw_str).expect("decode str");
    assert_eq!(val_str.as_bytes(), Some(b"spam".as_slice()));
    assert_eq!(val_str.as_str(), Some("spam"));
    assert_eq!(val_str.encode(), raw_str);

    let raw_empty_str = b"0:";
    let val_empty = BencodeValue::decode(raw_empty_str).expect("decode empty str");
    assert_eq!(val_empty.as_bytes(), Some(b"".as_slice()));
    assert_eq!(val_empty.encode(), raw_empty_str);

    // 3. Lists
    let raw_list = b"l4:spami42ee";
    let val_list = BencodeValue::decode(raw_list).expect("decode list");
    let list_items = val_list.as_list().expect("is list");
    assert_eq!(list_items.len(), 2);
    assert_eq!(list_items[0].as_str(), Some("spam"));
    assert_eq!(list_items[1].as_int(), Some(42));
    assert_eq!(val_list.encode(), raw_list);

    // 4. Dictionaries
    let raw_dict = b"d3:bar4:spam3:fooi42ee";
    let val_dict = BencodeValue::decode(raw_dict).expect("decode dict");
    assert_eq!(val_dict.get(b"bar").unwrap().as_str(), Some("spam"));
    assert_eq!(val_dict.get(b"foo").unwrap().as_int(), Some(42));
    assert_eq!(val_dict.encode(), raw_dict);
}

#[test]
fn test_bencode_invalid_syntax() {
    // Negative zero is forbidden
    assert!(matches!(
        BencodeValue::decode(b"i-0e"),
        Err(TorrentError::BencodeError(_))
    ));

    // Leading zeros in integer are forbidden
    assert!(matches!(
        BencodeValue::decode(b"i03e"),
        Err(TorrentError::BencodeError(_))
    ));
    assert!(matches!(
        BencodeValue::decode(b"i-05e"),
        Err(TorrentError::BencodeError(_))
    ));

    // Unterminated integer
    assert!(matches!(
        BencodeValue::decode(b"i12"),
        Err(TorrentError::BencodeError(_))
    ));
    assert!(matches!(
        BencodeValue::decode(b"ie"),
        Err(TorrentError::BencodeError(_))
    ));

    // Leading zeros in byte string length forbidden
    assert!(matches!(
        BencodeValue::decode(b"04:spam"),
        Err(TorrentError::BencodeError(_))
    ));

    // Unterminated list and dict
    assert!(matches!(
        BencodeValue::decode(b"l4:spam"),
        Err(TorrentError::BencodeError(_))
    ));
    assert!(matches!(
        BencodeValue::decode(b"d3:bar4:spam"),
        Err(TorrentError::BencodeError(_))
    ));

    // Out-of-order dictionary keys (must be sorted lexicographically)
    // "foo" (0x66) before "bar" (0x62) is illegal
    let out_of_order = b"d3:foo4:spam3:bar4:spame";
    assert!(matches!(
        BencodeValue::decode(out_of_order),
        Err(TorrentError::BencodeError(_))
    ));

    // Duplicate dictionary keys are illegal
    let duplicate_keys = b"d3:foo4:spam3:foo4:spame";
    assert!(matches!(
        BencodeValue::decode(duplicate_keys),
        Err(TorrentError::BencodeError(_))
    ));

    // Trailing garbage after valid bencode
    assert!(matches!(
        BencodeValue::decode(b"i42egarbage"),
        Err(TorrentError::BencodeError(_))
    ));
}

#[test]
fn test_sha1_nist_vectors() {
    // Vector 1: Empty string
    let d1 = sha1(b"");
    assert_eq!(to_hex(&d1), "da39a3ee5e6b4b0d3255bfef95601890afd80709");

    // Vector 2: "abc"
    let d2 = sha1(b"abc");
    assert_eq!(to_hex(&d2), "a9993e364706816aba3e25717850c26c9cd0d89d");

    // Vector 3: 56-byte string
    let d3 = sha1(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
    assert_eq!(to_hex(&d3), "84983e441c3bd26ebaae4aa1f95129e5e54670f1");

    // Vector 4: 1,000,000 repetitions of 'a'
    let mut hasher = Sha1::new();
    let chunk = [b'a'; 1000];
    for _ in 0..1000 {
        hasher.update(&chunk);
    }
    let d4 = hasher.finalize();
    assert_eq!(to_hex(&d4), "34aa973cd4c4daa4f61eeb2bdbad27316534016f");
}

#[test]
fn test_single_file_torrent_parsing() {
    // Build canonical single-file torrent payload
    let mut info_map = BTreeMap::new();
    info_map.insert(b"length".to_vec(), BencodeValue::Integer(6_000_000_000));
    info_map.insert(
        b"name".to_vec(),
        BencodeValue::Bytes(b"ubuntu-24.04-desktop-amd64.iso".to_vec()),
    );
    info_map.insert(b"piece length".to_vec(), BencodeValue::Integer(262144));

    // 3 pieces = 60 bytes
    let mut pieces = Vec::with_capacity(60);
    pieces.extend_from_slice(&[0x11; 20]);
    pieces.extend_from_slice(&[0x22; 20]);
    pieces.extend_from_slice(&[0x33; 20]);
    info_map.insert(b"pieces".to_vec(), BencodeValue::Bytes(pieces));

    let info_val = BencodeValue::Dict(info_map);
    let expected_info_bytes = info_val.encode();
    let expected_info_hash = sha1(&expected_info_bytes);

    let mut root_map = BTreeMap::new();
    root_map.insert(
        b"announce".to_vec(),
        BencodeValue::Bytes(b"http://tracker.ubuntu.com:6969/announce".to_vec()),
    );

    let announce_tier1 = BencodeValue::List(vec![BencodeValue::Bytes(
        b"http://tracker.ubuntu.com:6969/announce".to_vec(),
    )]);
    let announce_tier2 = BencodeValue::List(vec![BencodeValue::Bytes(
        b"http://backup.tracker.com/announce".to_vec(),
    )]);
    root_map.insert(
        b"announce-list".to_vec(),
        BencodeValue::List(vec![announce_tier1, announce_tier2]),
    );

    root_map.insert(b"info".to_vec(), info_val);

    let torrent_bytes = BencodeValue::Dict(root_map).encode();

    // Parse metainfo
    let meta = TorrentMeta::from_bytes(&torrent_bytes).expect("parse valid single-file torrent");

    assert_eq!(meta.info_hash, expected_info_hash);
    assert_eq!(meta.name, "ubuntu-24.04-desktop-amd64.iso");
    assert_eq!(meta.piece_length, 262144);
    assert_eq!(meta.pieces.len(), 3);
    assert_eq!(meta.pieces[0], [0x11; 20]);
    assert_eq!(meta.pieces[1], [0x22; 20]);
    assert_eq!(meta.pieces[2], [0x33; 20]);
    assert_eq!(meta.total_length, 6_000_000_000);
    assert!(!meta.is_private);

    assert_eq!(
        meta.files,
        vec![TorrentFile {
            length: 6_000_000_000,
            path: PathBuf::from("ubuntu-24.04-desktop-amd64.iso"),
        }]
    );

    assert_eq!(
        meta.announce.as_deref(),
        Some("http://tracker.ubuntu.com:6969/announce")
    );
    assert_eq!(
        meta.announce_list,
        vec![
            "http://tracker.ubuntu.com:6969/announce".to_string(),
            "http://backup.tracker.com/announce".to_string(),
        ]
    );
}

#[test]
fn test_multi_file_torrent_parsing() {
    let mut info_map = BTreeMap::new();
    info_map.insert(
        b"name".to_vec(),
        BencodeValue::Bytes(b"music_album".to_vec()),
    );
    info_map.insert(b"piece length".to_vec(), BencodeValue::Integer(131072));

    let mut pieces = Vec::with_capacity(40);
    pieces.extend_from_slice(&[0xAA; 20]);
    pieces.extend_from_slice(&[0xBB; 20]);
    info_map.insert(b"pieces".to_vec(), BencodeValue::Bytes(pieces));
    info_map.insert(b"private".to_vec(), BencodeValue::Integer(1));

    // 3 files in multi-file torrent
    let mut file1 = BTreeMap::new();
    file1.insert(b"length".to_vec(), BencodeValue::Integer(5_000_000));
    file1.insert(
        b"path".to_vec(),
        BencodeValue::List(vec![
            BencodeValue::Bytes(b"disc1".to_vec()),
            BencodeValue::Bytes(b"track01.flac".to_vec()),
        ]),
    );

    let mut file2 = BTreeMap::new();
    file2.insert(b"length".to_vec(), BencodeValue::Integer(7_500_000));
    file2.insert(
        b"path".to_vec(),
        BencodeValue::List(vec![
            BencodeValue::Bytes(b"disc1".to_vec()),
            BencodeValue::Bytes(b"track02.flac".to_vec()),
        ]),
    );

    let mut file3 = BTreeMap::new();
    file3.insert(b"length".to_vec(), BencodeValue::Integer(1_200_000));
    file3.insert(
        b"path".to_vec(),
        BencodeValue::List(vec![BencodeValue::Bytes(b"cover.jpg".to_vec())]),
    );

    info_map.insert(
        b"files".to_vec(),
        BencodeValue::List(vec![
            BencodeValue::Dict(file1),
            BencodeValue::Dict(file2),
            BencodeValue::Dict(file3),
        ]),
    );

    let info_val = BencodeValue::Dict(info_map);
    let expected_info_bytes = info_val.encode();
    let expected_info_hash = sha1(&expected_info_bytes);

    let mut root_map = BTreeMap::new();
    root_map.insert(b"info".to_vec(), info_val);
    root_map.insert(
        b"announce".to_vec(),
        BencodeValue::Bytes(b"http://tracker.private.org/announce".to_vec()),
    );

    let torrent_bytes = BencodeValue::Dict(root_map).encode();
    let meta = TorrentMeta::from_bytes(&torrent_bytes).expect("parse multi-file torrent");

    assert_eq!(meta.info_hash, expected_info_hash);
    assert_eq!(meta.name, "music_album");
    assert_eq!(meta.piece_length, 131072);
    assert_eq!(meta.pieces.len(), 2);
    assert!(meta.is_private);
    assert_eq!(meta.total_length, 13_700_000);

    assert_eq!(meta.files.len(), 3);
    assert_eq!(
        meta.files[0],
        TorrentFile {
            length: 5_000_000,
            path: PathBuf::from("music_album/disc1/track01.flac"),
        }
    );
    assert_eq!(
        meta.files[1],
        TorrentFile {
            length: 7_500_000,
            path: PathBuf::from("music_album/disc1/track02.flac"),
        }
    );
    assert_eq!(
        meta.files[2],
        TorrentFile {
            length: 1_200_000,
            path: PathBuf::from("music_album/cover.jpg"),
        }
    );
}

#[test]
fn test_magnet_hex_parsing() {
    let raw_uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=Ubuntu&tr=http%3A%2F%2Ftracker.com%2Fannounce&ws=http%3A%2F%2Fwebseed.org%2Ffile";
    let magnet = MagnetLink::parse(raw_uri).expect("parse hex magnet");

    assert_eq!(
        to_hex(&magnet.info_hash),
        "da39a3ee5e6b4b0d3255bfef95601890afd80709"
    );
    assert_eq!(magnet.display_name.as_deref(), Some("Ubuntu"));
    assert_eq!(
        magnet.trackers,
        vec!["http://tracker.com/announce".to_string()]
    );
    assert_eq!(
        magnet.web_seeds,
        vec!["http://webseed.org/file".to_string()]
    );

    // Round-trip verification
    let re_encoded = magnet.to_uri();
    let re_parsed = MagnetLink::parse(&re_encoded).expect("reparse magnet");
    assert_eq!(magnet, re_parsed);
}

#[test]
fn test_magnet_base32_parsing() {
    let expected_hash: [u8; 20] = [
        0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55, 0xbf, 0xef, 0x95, 0x60, 0x18,
        0x90, 0xaf, 0xd8, 0x07, 0x09,
    ];

    let b32_upper = to_base32(&expected_hash);
    assert_eq!(b32_upper.len(), 32);

    // Verify standalone base32 decode
    let decoded = decode_base32(&b32_upper).expect("decode base32");
    assert_eq!(decoded, expected_hash);

    // Test uppercase Base32 URI
    let uri_upper = format!("magnet:?xt=urn:btih:{b32_upper}&dn=Fedora_Linux");
    let magnet_upper = MagnetLink::parse(&uri_upper).expect("parse upper base32 magnet");
    assert_eq!(magnet_upper.info_hash, expected_hash);
    assert_eq!(magnet_upper.display_name.as_deref(), Some("Fedora_Linux"));

    // Test lowercase Base32 URI (case-insensitivity invariant)
    let b32_lower = b32_upper.to_lowercase();
    let uri_lower = format!("magnet:?xt=urn:btih:{b32_lower}&dn=Fedora_Linux");
    let magnet_lower = MagnetLink::parse(&uri_lower).expect("parse lower base32 magnet");
    assert_eq!(magnet_lower.info_hash, expected_hash);
}

#[test]
fn test_malformed_torrent_rejection() {
    let mut info_map = BTreeMap::new();
    info_map.insert(b"name".to_vec(), BencodeValue::Bytes(b"test".to_vec()));
    info_map.insert(b"length".to_vec(), BencodeValue::Integer(100));
    info_map.insert(b"piece length".to_vec(), BencodeValue::Integer(16384));
    // 25 bytes is NOT a multiple of 20
    info_map.insert(b"pieces".to_vec(), BencodeValue::Bytes(vec![0; 25]));

    let mut root_map = BTreeMap::new();
    root_map.insert(b"info".to_vec(), BencodeValue::Dict(info_map));
    let torrent_bytes = BencodeValue::Dict(root_map).encode();

    // 1. Truncated pieces string (len % 20 != 0)
    assert!(matches!(
        TorrentMeta::from_bytes(&torrent_bytes),
        Err(TorrentError::InvalidMetainfo(_))
    ));

    // 2. Missing piece length
    let mut no_piece_len = BTreeMap::new();
    no_piece_len.insert(b"name".to_vec(), BencodeValue::Bytes(b"test".to_vec()));
    no_piece_len.insert(b"length".to_vec(), BencodeValue::Integer(100));
    no_piece_len.insert(b"pieces".to_vec(), BencodeValue::Bytes(vec![0; 20]));
    let mut root2 = BTreeMap::new();
    root2.insert(b"info".to_vec(), BencodeValue::Dict(no_piece_len));
    assert!(matches!(
        TorrentMeta::from_bytes(&BencodeValue::Dict(root2).encode()),
        Err(TorrentError::InvalidMetainfo(_))
    ));

    // 3. Negative file length
    let mut neg_len = BTreeMap::new();
    neg_len.insert(b"name".to_vec(), BencodeValue::Bytes(b"test".to_vec()));
    neg_len.insert(b"length".to_vec(), BencodeValue::Integer(-100));
    neg_len.insert(b"piece length".to_vec(), BencodeValue::Integer(16384));
    neg_len.insert(b"pieces".to_vec(), BencodeValue::Bytes(vec![0; 20]));
    let mut root3 = BTreeMap::new();
    root3.insert(b"info".to_vec(), BencodeValue::Dict(neg_len));
    assert!(matches!(
        TorrentMeta::from_bytes(&BencodeValue::Dict(root3).encode()),
        Err(TorrentError::InvalidMetainfo(_))
    ));

    // 4. Missing info dictionary
    let mut root4 = BTreeMap::new();
    root4.insert(
        b"announce".to_vec(),
        BencodeValue::Bytes(b"http://tracker.com".to_vec()),
    );
    assert!(matches!(
        TorrentMeta::from_bytes(&BencodeValue::Dict(root4).encode()),
        Err(TorrentError::InvalidMetainfo(_))
    ));

    // 5. Non-dictionary root
    assert!(matches!(
        TorrentMeta::from_bytes(b"i42e"),
        Err(TorrentError::InvalidMetainfo(_))
    ));

    // 6. Malformed magnet URI (missing scheme)
    assert!(matches!(
        MagnetLink::parse("http://example.com/not_magnet"),
        Err(TorrentError::InvalidMagnet(_))
    ));

    // 7. Malformed magnet URI (missing xt)
    assert!(matches!(
        MagnetLink::parse("magnet:?dn=TestFile"),
        Err(TorrentError::InvalidMagnet(_))
    ));

    // 8. Malformed magnet URI (invalid xt length)
    assert!(matches!(
        MagnetLink::parse("magnet:?xt=urn:btih:tooshort"),
        Err(TorrentError::InvalidMagnet(_))
    ));
}
