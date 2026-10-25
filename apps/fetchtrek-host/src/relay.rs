use std::time::{Duration, Instant};

use fetchtrek_ipc::{
    decode_response, encode_request, IpcClient, IpcRequest, IpcResponse, IpcStream, MessageType,
};

use crate::error::HostError;
use crate::protocol::{BrowserRequest, HostResponse};

pub const HOST_VERSION: &str = "0.1.0";
const IPC_TIMEOUT: Duration = Duration::from_millis(5000);

pub struct HostRelay {
    endpoint: String,
    stream: Option<IpcStream>,
    correlation_seq: u32,
}

impl HostRelay {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            stream: None,
            correlation_seq: 1,
        }
    }

    pub fn default_endpoint() -> Self {
        Self::new(fetchtrek_ipc::default_endpoint())
    }

    async fn get_or_connect_stream(&mut self) -> Result<&mut IpcStream, HostError> {
        if self.stream.is_none() {
            match IpcClient::connect(&self.endpoint).await {
                Ok(stream) => {
                    self.stream = Some(stream);
                }
                Err(e) => {
                    return Err(HostError::DaemonOffline(format!(
                        "Failed to connect to daemon at {}: {}",
                        self.endpoint, e
                    )));
                }
            }
        }
        Ok(self.stream.as_mut().expect("stream must be Some"))
    }

    pub async fn ping_daemon(&mut self) -> Result<Duration, HostError> {
        let start = Instant::now();
        let resp = self.send_request(&IpcRequest::Ping).await?;
        match resp {
            IpcResponse::Pong => Ok(start.elapsed()),
            IpcResponse::Error { message, .. } => Err(HostError::DaemonOffline(message)),
            other => Err(HostError::DaemonOffline(format!(
                "Unexpected response: {other:?}"
            ))),
        }
    }

    async fn send_request(&mut self, request: &IpcRequest) -> Result<IpcResponse, HostError> {
        let correlation_id = self.correlation_seq;
        self.correlation_seq = self.correlation_seq.wrapping_add(1);

        let frame = encode_request(request, correlation_id)?;

        for attempt in 0..2 {
            let stream = match self.get_or_connect_stream().await {
                Ok(s) => s,
                Err(e) => {
                    if attempt == 1 {
                        return Err(e);
                    }
                    continue;
                }
            };

            if let Err(e) = stream.write_frame(&frame).await {
                eprintln!("[fetchtrek-host] Failed to write IPC frame (attempt {attempt}): {e}");
                self.stream = None;
                if attempt == 1 {
                    return Err(HostError::Ipc(e));
                }
                continue;
            }

            let response_result = tokio::time::timeout(IPC_TIMEOUT, async {
                loop {
                    let f = stream.read_frame().await?;
                    if f.header.message_type == MessageType::Response.as_u8() {
                        return Ok::<_, fetchtrek_ipc::IpcError>(f);
                    }
                }
            })
            .await;

            match response_result {
                Ok(Ok(f)) => {
                    let (resp, _corr) = decode_response(&f)?;
                    return Ok(resp);
                }
                Ok(Err(e)) => {
                    eprintln!("[fetchtrek-host] Failed to read IPC frame (attempt {attempt}): {e}");
                    self.stream = None;
                    if attempt == 1 {
                        return Err(HostError::Ipc(e));
                    }
                }
                Err(_) => {
                    self.stream = None;
                    return Err(HostError::Timeout);
                }
            }
        }

        Err(HostError::DaemonOffline("Daemon connection severed".into()))
    }

    pub async fn dispatch(&mut self, request: BrowserRequest) -> HostResponse {
        match request {
            BrowserRequest::Ping => HostResponse::pong(HOST_VERSION),
            BrowserRequest::InterceptDownload {
                url,
                filename,
                destination_path,
                headers: _,
                cookies: _,
                connections,
            } => {
                let ipc_req = IpcRequest::AddTask {
                    url,
                    destination_path,
                    filename,
                    connections,
                };
                match self.send_request(&ipc_req).await {
                    Ok(IpcResponse::TaskCreated { task_id, uuid }) => {
                        HostResponse::task_created(task_id, uuid)
                    }
                    Ok(IpcResponse::Error { message, .. }) => HostResponse::error(message),
                    Ok(other) => HostResponse::error(format!("Unexpected IPC response: {other:?}")),
                    Err(HostError::DaemonOffline(_)) => {
                        HostResponse::error("Daemon connection offline")
                    }
                    Err(e) => HostResponse::error(format!("Daemon error: {e}")),
                }
            }
            BrowserRequest::QueryStatus { task_id } => {
                let ipc_req = IpcRequest::QueryTask { task_id };
                match self.send_request(&ipc_req).await {
                    Ok(IpcResponse::TaskDetail { task }) => HostResponse::task_status(task),
                    Ok(IpcResponse::Error { message, .. }) => HostResponse::error(message),
                    Ok(other) => HostResponse::error(format!("Unexpected IPC response: {other:?}")),
                    Err(HostError::DaemonOffline(_)) => {
                        HostResponse::error("Daemon connection offline")
                    }
                    Err(e) => HostResponse::error(format!("Daemon error: {e}")),
                }
            }
            BrowserRequest::PauseTask { task_id } => {
                let ipc_req = IpcRequest::PauseTask { task_id };
                match self.send_request(&ipc_req).await {
                    Ok(IpcResponse::TaskUpdated { .. }) => HostResponse::task_updated(),
                    Ok(IpcResponse::Error { message, .. }) => HostResponse::error(message),
                    Ok(other) => HostResponse::error(format!("Unexpected IPC response: {other:?}")),
                    Err(HostError::DaemonOffline(_)) => {
                        HostResponse::error("Daemon connection offline")
                    }
                    Err(e) => HostResponse::error(format!("Daemon error: {e}")),
                }
            }
            BrowserRequest::ResumeTask { task_id } => {
                let ipc_req = IpcRequest::ResumeTask { task_id };
                match self.send_request(&ipc_req).await {
                    Ok(IpcResponse::TaskUpdated { .. }) => HostResponse::task_updated(),
                    Ok(IpcResponse::Error { message, .. }) => HostResponse::error(message),
                    Ok(other) => HostResponse::error(format!("Unexpected IPC response: {other:?}")),
                    Err(HostError::DaemonOffline(_)) => {
                        HostResponse::error("Daemon connection offline")
                    }
                    Err(e) => HostResponse::error(format!("Daemon error: {e}")),
                }
            }
            BrowserRequest::CancelTask {
                task_id,
                delete_file,
            } => {
                let ipc_req = IpcRequest::CancelTask {
                    task_id,
                    delete_file,
                };
                match self.send_request(&ipc_req).await {
                    Ok(IpcResponse::TaskCancelled { .. }) | Ok(IpcResponse::TaskUpdated { .. }) => {
                        HostResponse::task_updated()
                    }
                    Ok(IpcResponse::Error { message, .. }) => HostResponse::error(message),
                    Ok(other) => HostResponse::error(format!("Unexpected IPC response: {other:?}")),
                    Err(HostError::DaemonOffline(_)) => {
                        HostResponse::error("Daemon connection offline")
                    }
                    Err(e) => HostResponse::error(format!("Daemon error: {e}")),
                }
            }
        }
    }
}
