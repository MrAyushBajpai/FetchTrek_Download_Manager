use fetchtrek_types::TaskState;
use serde::{Deserialize, Serialize};

use crate::error::IpcError;
use crate::framing::{IpcFrame, MessageType};
use crate::protocol::OP_TELEMETRY_EVENT;

pub const TELEMETRY_INTERVAL_MS: u64 = 100; // 10 Hz rate limit (every 100ms)

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskProgress {
    pub task_id: i64,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub speed_bps: u64,
    pub active_connections: u16,
    pub status: TaskState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetrySnapshot {
    pub timestamp_ms: u64,
    pub global_download_speed_bps: u64,
    pub global_upload_speed_bps: u64,
    pub active_task_count: u32,
    pub tasks: Vec<TaskProgress>,
}

pub fn encode_telemetry(snapshot: &TelemetrySnapshot) -> Result<IpcFrame, IpcError> {
    let payload = bincode::serialize(snapshot).map_err(|e| IpcError::Codec(e.to_string()))?;
    IpcFrame::new(
        OP_TELEMETRY_EVENT,
        MessageType::Event.as_u8(),
        0,
        0,
        payload,
    )
}

pub fn decode_telemetry(frame: &IpcFrame) -> Result<TelemetrySnapshot, IpcError> {
    if frame.header.message_type != MessageType::Event.as_u8() {
        return Err(IpcError::Codec(format!(
            "Expected MessageType::Event (2), got {}",
            frame.header.message_type
        )));
    }
    if frame.header.message_id != OP_TELEMETRY_EVENT {
        return Err(IpcError::Codec(format!(
            "Expected OP_TELEMETRY_EVENT (0x{:04X}), got 0x{:04X}",
            OP_TELEMETRY_EVENT, frame.header.message_id
        )));
    }
    let snapshot: TelemetrySnapshot =
        bincode::deserialize(&frame.payload).map_err(|e| IpcError::Codec(e.to_string()))?;
    Ok(snapshot)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TelemetryBroadcaster {
    last_emit_ms: u64,
}

impl TelemetryBroadcaster {
    pub fn new() -> Self {
        Self { last_emit_ms: 0 }
    }

    pub fn with_start(start_ms: u64) -> Self {
        Self {
            last_emit_ms: start_ms,
        }
    }

    pub fn should_emit(&mut self, now_ms: u64) -> bool {
        if self.last_emit_ms == 0
            || now_ms.saturating_sub(self.last_emit_ms) >= TELEMETRY_INTERVAL_MS
        {
            self.last_emit_ms = now_ms;
            true
        } else {
            false
        }
    }
}
