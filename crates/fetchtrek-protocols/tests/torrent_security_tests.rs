use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fetchtrek_protocols::{
    validate_torrent_path_component, BencodeValue, TorrentDiskStorage, TorrentError, TorrentFile,
    TorrentMeta,
};

fn encode_torrent(
    name: &str,
    files: Option<Vec<(i64, Vec<&str>)>>,
    single_length: Option<i64>,
) -> Vec<u8> {
    let mut info = BTreeMap::new();
    info.insert(b"piece length".to_vec(), BencodeValue::Integer(16384));
    info.insert(b"pieces".to_vec(), BencodeValue::Bytes(vec![0u8; 20]));
    info.insert(
        b"name".to_vec(),
        BencodeValue::Bytes(name.as_bytes().to_vec()),
    );

    if let Some(file_list) = files {
        let mut b_files = Vec::new();
        for (len, path_parts) in file_list {
            let mut f_dict = BTreeMap::new();
            f_dict.insert(b"length".to_vec(), BencodeValue::Integer(len));
            let path_bencode = path_parts
                .into_iter()
                .map(|p| BencodeValue::Bytes(p.as_bytes().to_vec()))
                .collect();
            f_dict.insert(b"path".to_vec(), BencodeValue::List(path_bencode));
            b_files.push(BencodeValue::Dict(f_dict));
        }
        info.insert(b"files".to_vec(), BencodeValue::List(b_files));
    } else if let Some(len) = single_length {
        info.insert(b"length".to_vec(), BencodeValue::Integer(len));
    }

    let mut root = BTreeMap::new();
    root.insert(b"info".to_vec(), BencodeValue::Dict(info));
    root.insert(
        b"announce".to_vec(),
        BencodeValue::Bytes(b"http://tracker.example.com/announce".to_vec()),
    );

    BencodeValue::Dict(root).encode()
}

#[test]
fn test_torrent_name_absolute_path_rejection() {
    // 1. Direct validation tests
    assert!(validate_torrent_path_component("/etc/passwd").is_err());
    assert!(validate_torrent_path_component("C:\\Windows\\System32").is_err());
    assert!(validate_torrent_path_component("D:file.txt").is_err());
    assert!(validate_torrent_path_component("\\\\server\\share").is_err());
    assert!(validate_torrent_path_component("").is_err());
    assert!(validate_torrent_path_component(".").is_err());
    assert!(validate_torrent_path_component("..").is_err());
    assert!(validate_torrent_path_component("sub/dir").is_err());
    assert!(validate_torrent_path_component("null\0byte").is_err());
    assert!(validate_torrent_path_component("ctrl\x1fchar").is_err());
    assert!(validate_torrent_path_component("valid_filename.bin").is_ok());

    // 2. Full TorrentMeta decoding with absolute root name
    let raw_unix_abs = encode_torrent("/etc/passwd", None, Some(1024));
    let err = TorrentMeta::from_bytes(&raw_unix_abs).expect_err("Must reject unix absolute name");
    match err {
        TorrentError::InvalidMetainfo(msg) => {
            assert!(msg.contains("directory separators") || msg.contains("traversal"));
        }
        other => panic!("Expected InvalidMetainfo, got: {other:?}"),
    }

    let raw_win_abs = encode_torrent("C:\\Windows\\System32", None, Some(1024));
    let err = TorrentMeta::from_bytes(&raw_win_abs).expect_err("Must reject windows absolute name");
    match err {
        TorrentError::InvalidMetainfo(msg) => {
            assert!(
                msg.contains("directory separators")
                    || msg.contains("drive")
                    || msg.contains("traversal")
            );
        }
        other => panic!("Expected InvalidMetainfo, got: {other:?}"),
    }
}

#[test]
fn test_torrent_name_traversal_rejection() {
    // 1. Single-file with traversal name
    let raw_traversal = encode_torrent("../../etc/shadow", None, Some(1024));
    let err =
        TorrentMeta::from_bytes(&raw_traversal).expect_err("Must reject traversal in root name");
    match err {
        TorrentError::InvalidMetainfo(msg) => {
            assert!(msg.contains("traversal") || msg.contains("directory separators"));
        }
        other => panic!("Expected InvalidMetainfo, got: {other:?}"),
    }

    // 2. Multi-file mode with traversal in path components
    let raw_multifile_traversal = encode_torrent(
        "safe_root",
        Some(vec![
            (512, vec!["sub", "file1.txt"]),
            (512, vec!["sub", "..", "escaped.txt"]),
        ]),
        None,
    );
    let err = TorrentMeta::from_bytes(&raw_multifile_traversal)
        .expect_err("Must reject traversal in multi-file subpath");
    match err {
        TorrentError::InvalidMetainfo(msg) => {
            assert!(msg.contains("traversal"));
        }
        other => panic!("Expected InvalidMetainfo, got: {other:?}"),
    }

    // 3. Multi-file mode with empty or root separator subpath
    let raw_multifile_slash = encode_torrent(
        "safe_root",
        Some(vec![(512, vec!["sub/escaped", "file1.txt"])]),
        None,
    );
    assert!(TorrentMeta::from_bytes(&raw_multifile_slash).is_err());
}

#[tokio::test]
async fn test_torrent_multifile_containment() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let temp_dir = std::env::temp_dir().join(format!("fetchtrek_contain_{now}"));
    let _ = std::fs::create_dir_all(&temp_dir);

    // Directly construct a synthetic TorrentMeta with an escaping path
    let malicious_meta = TorrentMeta {
        info_hash: [0x55; 20],
        piece_length: 16384,
        pieces: vec![[0x00; 20]],
        name: "malicious_torrent".into(),
        files: vec![
            TorrentFile {
                length: 1024,
                path: PathBuf::from("normal.txt"),
            },
            TorrentFile {
                length: 2048,
                path: PathBuf::from("../escaped.txt"),
            },
        ],
        total_length: 3072,
        announce: None,
        announce_list: Vec::new(),
        is_private: false,
    };

    let storage = TorrentDiskStorage::new(temp_dir.clone(), malicious_meta);

    // initialize_files must return error
    let res = storage.initialize_files().await;
    assert!(
        res.is_err(),
        "initialize_files must fail closed when file path escapes base directory"
    );

    // Verify 0 files or directories created inside temp_dir
    let entries: Vec<_> = std::fs::read_dir(&temp_dir)
        .expect("read temp_dir")
        .collect();
    assert_eq!(
        entries.len(),
        0,
        "Zero files or folders must be created on disk when containment check fails"
    );

    // map_span must also fail closed
    let map_res = storage.map_span(0, 1024);
    assert!(
        map_res.is_err(),
        "map_span must fail closed when files escape base directory"
    );

    // Direct ensure_contained verification
    let valid_child = temp_dir.join("downloads/file.bin");
    assert!(TorrentDiskStorage::ensure_contained(&temp_dir, &valid_child).is_ok());

    let outside_path = Path::new("/etc/passwd");
    assert!(TorrentDiskStorage::ensure_contained(&temp_dir, outside_path).is_err());

    let relative_escape = Path::new("../../escape.bin");
    assert!(TorrentDiskStorage::ensure_contained(&temp_dir, relative_escape).is_err());

    let _ = std::fs::remove_dir_all(&temp_dir);
}
