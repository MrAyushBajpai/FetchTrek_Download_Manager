use fetchtrek_ipc::{default_endpoint, endpoint_with_name, IpcServer};

#[test]
fn test_endpoint_generation() {
    let def = default_endpoint();
    assert!(!def.is_empty());

    let named = endpoint_with_name("custom");
    assert!(!named.is_empty());
    assert!(named.contains("custom"));

    #[cfg(windows)]
    {
        assert!(def.starts_with(r"\\.\pipe\fetchtrek_ipc_"));
        assert_eq!(named, r"\\.\pipe\custom");
    }

    #[cfg(unix)]
    {
        assert!(def.ends_with(".sock") || def.ends_with("/ipc.sock"));
        assert!(named.ends_with("custom.sock"));
    }
}

#[cfg(unix)]
#[tokio::test]
async fn test_unix_socket_symlink_attack_rejection() {
    use std::os::unix::fs::symlink;
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let target_file = std::env::temp_dir().join(format!("fetchtrek_sec_target_{now}.txt"));
    let symlink_sock = std::env::temp_dir().join(format!("fetchtrek_sec_symlink_{now}.sock"));

    std::fs::write(&target_file, b"CRITICAL_USER_DATA").expect("write target file");
    symlink(&target_file, &symlink_sock).expect("create symlink");

    // IpcServer::bind MUST fail closed and must NOT unlink the symlink target
    let bind_res = IpcServer::bind(symlink_sock.to_str().unwrap());
    assert!(
        bind_res.is_err(),
        "IpcServer::bind must reject symlink endpoints"
    );

    // Verify the target file was NOT deleted or overwritten
    assert!(
        target_file.exists(),
        "Symlink target file must not be deleted or touched"
    );
    let content = std::fs::read(&target_file).expect("read target");
    assert_eq!(content, b"CRITICAL_USER_DATA");

    let _ = std::fs::remove_file(&symlink_sock);
    let _ = std::fs::remove_file(&target_file);
}

#[cfg(unix)]
#[tokio::test]
async fn test_unix_socket_permissions() {
    use std::os::unix::fs::MetadataExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let sock_path = std::env::temp_dir().join(format!("fetchtrek_sec_perm_{now}.sock"));
    let endpoint_str = sock_path.to_str().unwrap();

    let server = IpcServer::bind(endpoint_str).expect("bind socket");
    let meta = std::fs::metadata(&sock_path).expect("get socket metadata");
    let mode = meta.mode() & 0o777;

    assert_eq!(
        mode, 0o600,
        "IPC socket permissions must be 0600 (owner read/write only)"
    );

    drop(server);
    let _ = std::fs::remove_file(&sock_path);
}

#[cfg(windows)]
#[tokio::test]
async fn test_windows_named_pipe_binding() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pipe_name = format!(r"\\.\pipe\fetchtrek-sec-test-{now}");

    let server = IpcServer::bind(&pipe_name);
    assert!(server.is_ok(), "Named pipe server should bind successfully");
}
