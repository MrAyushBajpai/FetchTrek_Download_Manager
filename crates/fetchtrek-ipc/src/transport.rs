use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::error::IpcError;
use crate::framing::IpcFrame;

#[cfg(windows)]
pub fn default_endpoint() -> String {
    let username = std::env::var("USERNAME").unwrap_or_else(|_| "default".to_string());
    format!(r"\\.\pipe\fetchtrek_ipc_{}", username)
}

#[cfg(unix)]
pub fn default_endpoint() -> String {
    if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
        format!("{}/fetchtrek/ipc.sock", runtime_dir)
    } else {
        let uid = unsafe { libc::getuid() };
        format!("/tmp/fetchtrek-{}/ipc.sock", uid)
    }
}

pub fn endpoint_with_name(name: &str) -> String {
    #[cfg(windows)]
    {
        format!(r"\\.\pipe\{}", name)
    }
    #[cfg(unix)]
    {
        if let Ok(runtime_dir) = std::env::var("XDG_RUNTIME_DIR") {
            format!("{}/{}.sock", runtime_dir, name)
        } else {
            let uid = unsafe { libc::getuid() };
            format!("/tmp/fetchtrek-{}/{}.sock", uid, name)
        }
    }
}

pub enum IpcStream {
    #[cfg(windows)]
    NamedPipeServer(tokio::net::windows::named_pipe::NamedPipeServer),
    #[cfg(windows)]
    NamedPipeClient(tokio::net::windows::named_pipe::NamedPipeClient),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

impl IpcStream {
    pub async fn read_frame(&mut self) -> Result<IpcFrame, IpcError> {
        crate::framing::read_frame(self).await
    }

    pub async fn write_frame(&mut self, frame: &IpcFrame) -> Result<(), IpcError> {
        crate::framing::write_frame(self, frame).await
    }
}

impl AsyncRead for IpcStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(windows)]
            Self::NamedPipeServer(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(windows)]
            Self::NamedPipeClient(c) => Pin::new(c).poll_read(cx, buf),
            #[cfg(unix)]
            Self::Unix(u) => Pin::new(u).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for IpcStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            #[cfg(windows)]
            Self::NamedPipeServer(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(windows)]
            Self::NamedPipeClient(c) => Pin::new(c).poll_write(cx, buf),
            #[cfg(unix)]
            Self::Unix(u) => Pin::new(u).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(windows)]
            Self::NamedPipeServer(s) => Pin::new(s).poll_flush(cx),
            #[cfg(windows)]
            Self::NamedPipeClient(c) => Pin::new(c).poll_flush(cx),
            #[cfg(unix)]
            Self::Unix(u) => Pin::new(u).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            #[cfg(windows)]
            Self::NamedPipeServer(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(windows)]
            Self::NamedPipeClient(c) => Pin::new(c).poll_shutdown(cx),
            #[cfg(unix)]
            Self::Unix(u) => Pin::new(u).poll_shutdown(cx),
        }
    }
}

#[cfg(windows)]
pub struct IpcServer {
    endpoint: String,
    server: Option<tokio::net::windows::named_pipe::NamedPipeServer>,
}

#[cfg(windows)]
impl IpcServer {
    pub fn bind(endpoint: &str) -> Result<Self, IpcError> {
        let server = tokio::net::windows::named_pipe::ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .create(endpoint)?;

        Ok(Self {
            endpoint: endpoint.to_string(),
            server: Some(server),
        })
    }

    pub async fn accept(&mut self) -> Result<IpcStream, IpcError> {
        let server = self.server.take().ok_or(IpcError::ConnectionClosed)?;
        server.connect().await?;

        // Instantiate next server pipe instance for subsequent connections
        let next_server = tokio::net::windows::named_pipe::ServerOptions::new()
            .first_pipe_instance(false)
            .reject_remote_clients(true)
            .create(&self.endpoint)?;
        self.server = Some(next_server);

        Ok(IpcStream::NamedPipeServer(server))
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[cfg(windows)]
pub struct IpcClient;

#[cfg(windows)]
impl IpcClient {
    pub async fn connect(endpoint: &str) -> Result<IpcStream, IpcError> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(2000);
        loop {
            match tokio::net::windows::named_pipe::ClientOptions::new().open(endpoint) {
                Ok(client) => return Ok(IpcStream::NamedPipeClient(client)),
                Err(e) if e.raw_os_error() == Some(231) || e.raw_os_error() == Some(2) => {
                    // 231 = ERROR_PIPE_BUSY, 2 = ERROR_FILE_NOT_FOUND
                    if tokio::time::Instant::now() >= deadline {
                        return Err(IpcError::Io(e));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(e) => return Err(IpcError::Io(e)),
            }
        }
    }
}

#[cfg(unix)]
pub struct IpcServer {
    endpoint: String,
    listener: tokio::net::UnixListener,
}

#[cfg(unix)]
impl IpcServer {
    pub fn bind(endpoint: &str) -> Result<Self, IpcError> {
        use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt};

        let path = std::path::Path::new(endpoint);

        // Stale Socket Recovery with symlink and ownership verification
        if let Ok(meta) = std::fs::symlink_metadata(path) {
            if meta.file_type().is_symlink() {
                return Err(IpcError::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "IPC endpoint cannot be a symlink",
                )));
            }
            if !meta.file_type().is_socket() {
                return Err(IpcError::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "IPC endpoint exists and is not a socket",
                )));
            }
            let current_uid = unsafe { libc::getuid() };
            if meta.uid() != current_uid {
                return Err(IpcError::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "IPC endpoint owned by different user",
                )));
            }
            std::fs::remove_file(path)?;
        }

        // Create parent directory with 0700 permissions if needed
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::DirBuilder::new()
                    .mode(0o700)
                    .recursive(true)
                    .create(parent)?;
            }
        }

        let listener = tokio::net::UnixListener::bind(endpoint)?;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));

        Ok(Self {
            endpoint: endpoint.to_string(),
            listener,
        })
    }

    pub async fn accept(&mut self) -> Result<IpcStream, IpcError> {
        let (stream, _) = self.listener.accept().await?;
        Ok(IpcStream::Unix(stream))
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[cfg(unix)]
impl Drop for IpcServer {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.endpoint);
    }
}

#[cfg(unix)]
pub struct IpcClient;

#[cfg(unix)]
impl IpcClient {
    pub async fn connect(endpoint: &str) -> Result<IpcStream, IpcError> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(2000);
        loop {
            match tokio::net::UnixStream::connect(endpoint).await {
                Ok(stream) => return Ok(IpcStream::Unix(stream)),
                Err(e)
                    if e.kind() == std::io::ErrorKind::NotFound
                        || e.kind() == std::io::ErrorKind::ConnectionRefused =>
                {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(IpcError::Io(e));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(e) => return Err(IpcError::Io(e)),
            }
        }
    }
}
