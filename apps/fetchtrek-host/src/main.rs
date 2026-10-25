use fetchtrek_host::{
    read_native_message, write_native_message, BrowserRequest, HostError, HostRelay, HostResponse,
    HOST_VERSION,
};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--version" || a == "-v") {
        println!("fetchtrek-host {}", HOST_VERSION);
        return;
    }

    if args.iter().any(|a| a == "--ping-daemon") {
        let mut relay = HostRelay::default_endpoint();
        match relay.ping_daemon().await {
            Ok(duration) => {
                println!("Daemon ping ok ({:?})", duration);
                return;
            }
            Err(e) => {
                eprintln!("Failed to ping daemon: {e}");
                std::process::exit(1);
            }
        }
    }

    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let mut relay = HostRelay::default_endpoint();

    loop {
        match read_native_message(&mut stdin).await {
            Ok(msg) => {
                let response = match serde_json::from_str::<BrowserRequest>(&msg) {
                    Ok(req) => relay.dispatch(req).await,
                    Err(e) => {
                        eprintln!("[fetchtrek-host] Invalid JSON request: {e}");
                        HostResponse::error(format!("Invalid JSON request: {e}"))
                    }
                };

                let resp_json = match serde_json::to_string(&response) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[fetchtrek-host] Failed to serialize response: {e}");
                        r#"{"status":"error","message":"Internal serialization error"}"#.to_string()
                    }
                };

                if let Err(e) = write_native_message(&mut stdout, &resp_json).await {
                    eprintln!("[fetchtrek-host] Failed to write response to stdout: {e}");
                    break;
                }
            }
            Err(HostError::Eof) => {
                // Extension closed stdin (clean shutdown)
                break;
            }
            Err(e) => {
                eprintln!("[fetchtrek-host] Framing error on stdin: {e}");
                let err_resp = HostResponse::error(format!("Framing error: {e}"));
                if let Ok(resp_json) = serde_json::to_string(&err_resp) {
                    let _ = write_native_message(&mut stdout, &resp_json).await;
                }
                break;
            }
        }
    }
}
