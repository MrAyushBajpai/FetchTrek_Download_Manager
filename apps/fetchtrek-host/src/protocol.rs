use fetchtrek_ipc::TaskSummary;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum BrowserRequest {
    Ping,
    InterceptDownload {
        url: String,
        #[serde(default)]
        filename: Option<String>,
        #[serde(default)]
        destination_path: Option<String>,
        #[serde(default)]
        headers: Option<HashMap<String, String>>,
        #[serde(default)]
        cookies: Option<String>,
        #[serde(default)]
        connections: Option<usize>,
    },
    QueryStatus {
        task_id: i64,
    },
    PauseTask {
        task_id: i64,
    },
    ResumeTask {
        task_id: i64,
    },
    CancelTask {
        task_id: i64,
        #[serde(default)]
        delete_file: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HostResponse {
    Pong {
        status: String,
        action: String,
        version: String,
    },
    TaskCreated {
        status: String,
        action: String,
        task_id: i64,
        uuid: String,
    },
    TaskStatus {
        status: String,
        action: String,
        task: TaskSummary,
    },
    TaskUpdated {
        status: String,
        action: String,
    },
    Error {
        status: String,
        message: String,
    },
}

impl HostResponse {
    pub fn pong(version: impl Into<String>) -> Self {
        Self::Pong {
            status: "ok".into(),
            action: "pong".into(),
            version: version.into(),
        }
    }

    pub fn task_created(task_id: i64, uuid: impl Into<String>) -> Self {
        Self::TaskCreated {
            status: "ok".into(),
            action: "task_created".into(),
            task_id,
            uuid: uuid.into(),
        }
    }

    pub fn task_status(task: TaskSummary) -> Self {
        Self::TaskStatus {
            status: "ok".into(),
            action: "task_status".into(),
            task,
        }
    }

    pub fn task_updated() -> Self {
        Self::TaskUpdated {
            status: "ok".into(),
            action: "task_updated".into(),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            status: "error".into(),
            message: message.into(),
        }
    }
}
