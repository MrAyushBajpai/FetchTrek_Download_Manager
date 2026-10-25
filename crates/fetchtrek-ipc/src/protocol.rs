use fetchtrek_types::TaskState;
use serde::{Deserialize, Serialize};

use crate::error::IpcError;
use crate::framing::{IpcFrame, MessageType};

pub const OP_PING: u16 = 0x0001;
pub const OP_PONG: u16 = 0x0002;
pub const OP_ADD_TASK_REQ: u16 = 0x0010;
pub const OP_ADD_TASK_RESP: u16 = 0x0011;
pub const OP_PAUSE_TASK_REQ: u16 = 0x0020;
pub const OP_PAUSE_TASK_RESP: u16 = 0x0021;
pub const OP_RESUME_TASK_REQ: u16 = 0x0030;
pub const OP_RESUME_TASK_RESP: u16 = 0x0031;
pub const OP_CANCEL_TASK_REQ: u16 = 0x0040;
pub const OP_CANCEL_TASK_RESP: u16 = 0x0041;
pub const OP_LIST_TASKS_REQ: u16 = 0x0050;
pub const OP_LIST_TASKS_RESP: u16 = 0x0051;
pub const OP_QUERY_TASK_REQ: u16 = 0x0060;
pub const OP_QUERY_TASK_RESP: u16 = 0x0061;
pub const OP_ERROR_RESP: u16 = 0x00FF;
pub const OP_TELEMETRY_EVENT: u16 = 0x0100;

pub const HEARTBEAT_INTERVAL_MS: u64 = 2_000; // 2 seconds
pub const HEARTBEAT_TIMEOUT_MS: u64 = 6_000; // 3 missed pings = 6 seconds

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskSummary {
    pub id: i64,
    pub uuid: String,
    pub url: String,
    pub destination_path: String,
    pub filename: String,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub status: TaskState,
    pub speed_bps: u64,
    pub progress_percent: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IpcRequest {
    Ping,
    AddTask {
        url: String,
        destination_path: Option<String>,
        filename: Option<String>,
        connections: Option<usize>,
        #[serde(default)]
        headers: Option<Vec<(String, String)>>,
        #[serde(default)]
        cookies: Option<String>,
        #[serde(default)]
        user_agent: Option<String>,
        #[serde(default)]
        referer: Option<String>,
    },
    PauseTask {
        task_id: i64,
    },
    ResumeTask {
        task_id: i64,
    },
    CancelTask {
        task_id: i64,
        delete_file: bool,
    },
    ListTasks {
        status_filter: Option<TaskState>,
    },
    QueryTask {
        task_id: i64,
    },
}

impl IpcRequest {
    pub fn opcode(&self) -> u16 {
        match self {
            Self::Ping => OP_PING,
            Self::AddTask { .. } => OP_ADD_TASK_REQ,
            Self::PauseTask { .. } => OP_PAUSE_TASK_REQ,
            Self::ResumeTask { .. } => OP_RESUME_TASK_REQ,
            Self::CancelTask { .. } => OP_CANCEL_TASK_REQ,
            Self::ListTasks { .. } => OP_LIST_TASKS_REQ,
            Self::QueryTask { .. } => OP_QUERY_TASK_REQ,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IpcResponse {
    Pong,
    TaskCreated { task_id: i64, uuid: String },
    TaskUpdated { task_id: i64, status: TaskState },
    TaskCancelled { task_id: i64 },
    TaskList { tasks: Vec<TaskSummary> },
    TaskDetail { task: TaskSummary },
    Error { code: u32, message: String },
}

impl IpcResponse {
    pub fn opcode(&self) -> u16 {
        match self {
            Self::Pong => OP_PONG,
            Self::TaskCreated { .. } => OP_ADD_TASK_RESP,
            Self::TaskUpdated { .. } => OP_PAUSE_TASK_RESP,
            Self::TaskCancelled { .. } => OP_CANCEL_TASK_RESP,
            Self::TaskList { .. } => OP_LIST_TASKS_RESP,
            Self::TaskDetail { .. } => OP_QUERY_TASK_RESP,
            Self::Error { .. } => OP_ERROR_RESP,
        }
    }
}

pub fn encode_request(req: &IpcRequest, correlation_id: u32) -> Result<IpcFrame, IpcError> {
    let opcode = req.opcode();
    let payload = bincode::serialize(req).map_err(|e| IpcError::Codec(e.to_string()))?;
    IpcFrame::new(
        opcode,
        MessageType::Request.as_u8(),
        0,
        correlation_id,
        payload,
    )
}

pub fn decode_request(frame: &IpcFrame) -> Result<(IpcRequest, u32), IpcError> {
    if frame.header.message_type != MessageType::Request.as_u8() {
        return Err(IpcError::Codec(format!(
            "Expected MessageType::Request (0), got {}",
            frame.header.message_type
        )));
    }
    let req: IpcRequest =
        bincode::deserialize(&frame.payload).map_err(|e| IpcError::Codec(e.to_string()))?;
    Ok((req, frame.header.correlation_id))
}

pub fn encode_response(resp: &IpcResponse, correlation_id: u32) -> Result<IpcFrame, IpcError> {
    let opcode = resp.opcode();
    let payload = bincode::serialize(resp).map_err(|e| IpcError::Codec(e.to_string()))?;
    IpcFrame::new(
        opcode,
        MessageType::Response.as_u8(),
        0,
        correlation_id,
        payload,
    )
}

pub fn decode_response(frame: &IpcFrame) -> Result<(IpcResponse, u32), IpcError> {
    if frame.header.message_type != MessageType::Response.as_u8() {
        return Err(IpcError::Codec(format!(
            "Expected MessageType::Response (1), got {}",
            frame.header.message_type
        )));
    }
    let resp: IpcResponse =
        bincode::deserialize(&frame.payload).map_err(|e| IpcError::Codec(e.to_string()))?;
    Ok((resp, frame.header.correlation_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatTracker {
    pub last_ping_ms: u64,
    pub last_pong_ms: u64,
}

impl HeartbeatTracker {
    pub fn new(now_ms: u64) -> Self {
        Self {
            last_ping_ms: now_ms,
            last_pong_ms: now_ms,
        }
    }

    pub fn record_ping(&mut self, now_ms: u64) {
        self.last_ping_ms = now_ms;
    }

    pub fn record_pong(&mut self, now_ms: u64) {
        self.last_pong_ms = now_ms;
    }

    pub fn should_ping(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.last_ping_ms) >= HEARTBEAT_INTERVAL_MS
    }

    pub fn is_timed_out(&self, now_ms: u64) -> bool {
        now_ms.saturating_sub(self.last_pong_ms) >= HEARTBEAT_TIMEOUT_MS
    }
}
