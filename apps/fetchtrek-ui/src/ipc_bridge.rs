use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fetchtrek_ipc::{
    decode_response, decode_telemetry, encode_request, IpcClient, IpcRequest, IpcResponse,
    MessageType,
};
use slint::SharedString;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::store::with_store_mut;
use crate::MainWindow;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum UiCommand {
    AddTask {
        url: String,
        destination_path: Option<String>,
        filename: Option<String>,
        connections: Option<usize>,
    },
    PauseTask(i64),
    ResumeTask(i64),
    CancelTask {
        task_id: i64,
        delete_file: bool,
    },
    PauseAll,
    ResumeAll,
}

#[derive(Clone)]
pub struct IpcBridgeHandle {
    pub command_tx: mpsc::Sender<UiCommand>,
}

pub fn start_ipc_bridge(weak_window: slint::Weak<MainWindow>) -> IpcBridgeHandle {
    let (command_tx, command_rx) = mpsc::channel(64);
    let handle = IpcBridgeHandle { command_tx };

    std::thread::Builder::new()
        .name("fetchtrek-ipc-worker".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("Failed to build Tokio runtime for IPC bridge");

            rt.block_on(run_bridge_worker(weak_window, command_rx));
        })
        .expect("Failed to spawn IPC bridge background thread");

    handle
}

async fn run_bridge_worker(
    weak_window: slint::Weak<MainWindow>,
    mut command_rx: mpsc::Receiver<UiCommand>,
) {
    let endpoint = fetchtrek_ipc::default_endpoint();
    let correlation_gen = Arc::new(AtomicU32::new(1));
    let mut backoff = Duration::from_millis(500);

    loop {
        set_ui_daemon_status(&weak_window, "Connecting...");

        match IpcClient::connect(&endpoint).await {
            Ok(mut stream) => {
                backoff = Duration::from_millis(500);
                set_ui_daemon_status(&weak_window, "Connected");

                // Request initial task list
                let list_req = IpcRequest::ListTasks {
                    status_filter: None,
                };
                let corr = correlation_gen.fetch_add(1, Ordering::Relaxed);
                if let Ok(frame) = encode_request(&list_req, corr) {
                    let _ = stream.write_frame(&frame).await;
                }

                // Session loop
                loop {
                    tokio::select! {
                        frame_res = stream.read_frame() => {
                            match frame_res {
                                Ok(frame) => {
                                    if frame.header.message_type == MessageType::Event.as_u8() {
                                        if let Ok(snapshot) = decode_telemetry(&frame) {
                                            let weak = weak_window.clone();
                                            let _ = slint::invoke_from_event_loop(move || {
                                                if let Some(win) = weak.upgrade() {
                                                    with_store_mut(|st| {
                                                        st.apply_telemetry(&snapshot, &win);
                                                    });
                                                }
                                            });
                                        }
                                    } else if frame.header.message_type == MessageType::Response.as_u8() {
                                        if let Ok((IpcResponse::TaskList { tasks }, _)) = decode_response(&frame) {
                                            let weak = weak_window.clone();
                                            let _ = slint::invoke_from_event_loop(move || {
                                                if let Some(win) = weak.upgrade() {
                                                    with_store_mut(|st| {
                                                        st.set_initial_tasks(tasks, &win);
                                                    });
                                                }
                                            });
                                        }
                                    }
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                        cmd_opt = command_rx.recv() => {
                            match cmd_opt {
                                Some(cmd) => {
                                    if !handle_ui_command(cmd, &mut stream, &correlation_gen).await {
                                        break;
                                    }
                                }
                                None => return,
                            }
                        }
                    }
                }

                set_ui_daemon_status(&weak_window, "Disconnected");
            }
            Err(_) => {
                set_ui_daemon_status(&weak_window, "Connecting...");
            }
        }

        sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_millis(4000));
    }
}

async fn handle_ui_command(
    cmd: UiCommand,
    stream: &mut fetchtrek_ipc::IpcStream,
    correlation_gen: &AtomicU32,
) -> bool {
    let requests: Vec<IpcRequest> = match cmd {
        UiCommand::AddTask {
            url,
            destination_path,
            filename,
            connections,
        } => vec![IpcRequest::AddTask {
            url,
            destination_path,
            filename,
            connections,
            headers: None,
            cookies: None,
            user_agent: None,
            referer: None,
        }],
        UiCommand::PauseTask(task_id) => vec![IpcRequest::PauseTask { task_id }],
        UiCommand::ResumeTask(task_id) => vec![IpcRequest::ResumeTask { task_id }],
        UiCommand::CancelTask {
            task_id,
            delete_file,
        } => vec![IpcRequest::CancelTask {
            task_id,
            delete_file,
        }],
        UiCommand::PauseAll => vec![],
        UiCommand::ResumeAll => vec![],
    };

    for req in requests {
        let corr = correlation_gen.fetch_add(1, Ordering::Relaxed);
        if let Ok(frame) = encode_request(&req, corr) {
            if stream.write_frame(&frame).await.is_err() {
                return false;
            }
        }
    }

    true
}

fn set_ui_daemon_status(weak_window: &slint::Weak<MainWindow>, status: &'static str) {
    let weak = weak_window.clone();
    let _ = slint::invoke_from_event_loop(move || {
        if let Some(win) = weak.upgrade() {
            win.set_daemon_status(SharedString::from(status));
        }
    });
}
