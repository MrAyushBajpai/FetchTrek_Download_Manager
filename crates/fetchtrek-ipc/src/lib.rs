//! IPC transport and binary framing protocols.

pub mod error;
pub mod framing;
pub mod protocol;
pub mod telemetry;
pub mod transport;

pub use error::IpcError;
pub use framing::{
    read_frame, write_frame, IpcFrame, IpcHeader, MessageType, HEADER_LEN, IPC_MAGIC,
    MAX_PAYLOAD_LEN,
};
pub use protocol::{
    decode_request, decode_response, encode_request, encode_response, HeartbeatTracker, IpcRequest,
    IpcResponse, TaskSummary, HEARTBEAT_INTERVAL_MS, HEARTBEAT_TIMEOUT_MS, OP_ADD_TASK_REQ,
    OP_ADD_TASK_RESP, OP_CANCEL_TASK_REQ, OP_CANCEL_TASK_RESP, OP_ERROR_RESP, OP_LIST_TASKS_REQ,
    OP_LIST_TASKS_RESP, OP_PAUSE_TASK_REQ, OP_PAUSE_TASK_RESP, OP_PING, OP_PONG, OP_QUERY_TASK_REQ,
    OP_QUERY_TASK_RESP, OP_RESUME_TASK_REQ, OP_RESUME_TASK_RESP, OP_TELEMETRY_EVENT,
};
pub use telemetry::{
    decode_telemetry, encode_telemetry, TaskProgress, TelemetryBroadcaster, TelemetrySnapshot,
    TELEMETRY_INTERVAL_MS,
};
pub use transport::{default_endpoint, endpoint_with_name, IpcClient, IpcServer, IpcStream};
