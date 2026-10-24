use std::time::{Duration, Instant};

use fetchtrek_ipc::{
    decode_response, decode_telemetry, encode_request, IpcClient, IpcRequest, IpcResponse,
    IpcStream, MessageType, TaskSummary, TelemetrySnapshot,
};
use fetchtrek_types::TaskState;

use crate::error::CliError;

pub struct FetchTrekClient {
    stream: IpcStream,
    correlation_seq: u32,
}

impl FetchTrekClient {
    pub async fn connect(endpoint: Option<&str>) -> Result<Self, CliError> {
        let target_endpoint = endpoint
            .map(|s| s.to_string())
            .unwrap_or_else(fetchtrek_ipc::default_endpoint);
        let stream = IpcClient::connect(&target_endpoint).await?;
        Ok(Self {
            stream,
            correlation_seq: 1,
        })
    }

    pub fn new(stream: IpcStream) -> Self {
        Self {
            stream,
            correlation_seq: 1,
        }
    }

    async fn send_request(&mut self, request: &IpcRequest) -> Result<IpcResponse, CliError> {
        let correlation_id = self.correlation_seq;
        self.correlation_seq = self.correlation_seq.wrapping_add(1);

        let frame = encode_request(request, correlation_id)?;
        self.stream.write_frame(&frame).await?;

        let response_frame = tokio::time::timeout(Duration::from_millis(5000), async {
            loop {
                let f = self.stream.read_frame().await?;
                if f.header.message_type == MessageType::Response.as_u8() {
                    return Ok::<_, fetchtrek_ipc::IpcError>(f);
                }
            }
        })
        .await
        .map_err(|_| CliError::Timeout)??;

        let (resp, _corr) = decode_response(&response_frame)?;
        match resp {
            IpcResponse::Error { code, message } => Err(CliError::Server { code, message }),
            other => Ok(other),
        }
    }

    pub async fn ping(&mut self) -> Result<Duration, CliError> {
        let start = Instant::now();
        let resp = self.send_request(&IpcRequest::Ping).await?;
        match resp {
            IpcResponse::Pong => Ok(start.elapsed()),
            other => Err(CliError::UnexpectedResponse(format!("{:?}", other))),
        }
    }

    pub async fn add_task(
        &mut self,
        url: &str,
        destination_path: Option<&str>,
        filename: Option<&str>,
        connections: Option<usize>,
    ) -> Result<(i64, String), CliError> {
        let req = IpcRequest::AddTask {
            url: url.to_string(),
            destination_path: destination_path.map(String::from),
            filename: filename.map(String::from),
            connections,
        };
        let resp = self.send_request(&req).await?;
        match resp {
            IpcResponse::TaskCreated { task_id, uuid } => Ok((task_id, uuid)),
            other => Err(CliError::UnexpectedResponse(format!("{:?}", other))),
        }
    }

    pub async fn list_tasks(
        &mut self,
        status: Option<TaskState>,
    ) -> Result<Vec<TaskSummary>, CliError> {
        let req = IpcRequest::ListTasks {
            status_filter: status,
        };
        let resp = self.send_request(&req).await?;
        match resp {
            IpcResponse::TaskList { tasks } => Ok(tasks),
            other => Err(CliError::UnexpectedResponse(format!("{:?}", other))),
        }
    }

    pub async fn pause_task(&mut self, task_id: i64) -> Result<(), CliError> {
        let req = IpcRequest::PauseTask { task_id };
        let resp = self.send_request(&req).await?;
        match resp {
            IpcResponse::TaskUpdated { .. } => Ok(()),
            other => Err(CliError::UnexpectedResponse(format!("{:?}", other))),
        }
    }

    pub async fn resume_task(&mut self, task_id: i64) -> Result<(), CliError> {
        let req = IpcRequest::ResumeTask { task_id };
        let resp = self.send_request(&req).await?;
        match resp {
            IpcResponse::TaskUpdated { .. } => Ok(()),
            other => Err(CliError::UnexpectedResponse(format!("{:?}", other))),
        }
    }

    pub async fn cancel_task(&mut self, task_id: i64, delete_file: bool) -> Result<(), CliError> {
        let req = IpcRequest::CancelTask {
            task_id,
            delete_file,
        };
        let resp = self.send_request(&req).await?;
        match resp {
            IpcResponse::TaskCancelled { .. } => Ok(()),
            other => Err(CliError::UnexpectedResponse(format!("{:?}", other))),
        }
    }

    pub async fn query_task(&mut self, task_id: i64) -> Result<TaskSummary, CliError> {
        let req = IpcRequest::QueryTask { task_id };
        let resp = self.send_request(&req).await?;
        match resp {
            IpcResponse::TaskDetail { task } => Ok(task),
            other => Err(CliError::UnexpectedResponse(format!("{:?}", other))),
        }
    }

    pub async fn read_telemetry_event(&mut self) -> Result<TelemetrySnapshot, CliError> {
        let event_frame = tokio::time::timeout(Duration::from_millis(5000), async {
            loop {
                let f = self.stream.read_frame().await?;
                if f.header.message_type == MessageType::Event.as_u8() {
                    return Ok::<_, fetchtrek_ipc::IpcError>(f);
                }
            }
        })
        .await
        .map_err(|_| CliError::Timeout)??;

        let snapshot = decode_telemetry(&event_frame)?;
        Ok(snapshot)
    }
}
