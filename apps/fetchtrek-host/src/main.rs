use std::path::Path;

use fetchtrek_host::{
    check_status, parse_cli_command, print_help, read_native_message, resolve_host_exe_path,
    write_native_message, BrowserRequest, CliCommand, HostError, HostRelay, HostResponse,
    HOST_VERSION,
};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg_refs: Vec<&str> = args.iter().skip(1).map(String::as_str).collect();

    let cmd = match parse_cli_command(&arg_refs) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    match cmd {
        CliCommand::Version => {
            println!("fetchtrek-host {}", HOST_VERSION);
        }
        CliCommand::Help => {
            print_help();
        }
        CliCommand::PingDaemon => {
            let mut relay = HostRelay::default_endpoint();
            match relay.ping_daemon().await {
                Ok(duration) => {
                    println!("Daemon ping ok ({:?})", duration);
                }
                Err(e) => {
                    eprintln!("Failed to ping daemon: {e}");
                    std::process::exit(1);
                }
            }
        }
        CliCommand::Status => {
            let statuses = check_status();
            println!("FetchTrek Native Messaging Host Registration Status:");
            println!("{:-<76}", "");
            println!(
                "{:<18} {:<12} {:<8} Manifest Path",
                "Browser", "Registered", "Valid"
            );
            println!("{:-<76}", "");
            for s in &statuses {
                let reg_str = if s.registered { "Yes" } else { "No" };
                let valid_str = if s.valid { "Yes" } else { "No" };
                let path_str = s
                    .manifest_path
                    .as_ref()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|| "-".to_string());
                println!(
                    "{:<18} {:<12} {:<8} {}",
                    s.browser.display_name(),
                    reg_str,
                    valid_str,
                    path_str
                );
            }
            println!("{:-<76}", "");
            let exe_path = resolve_host_exe_path()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| "unknown".to_string());
            println!(
                "Host Executable: {exe_path} (Exists: {})",
                Path::new(&exe_path).exists()
            );
        }
        CliCommand::Register {
            browser,
            extension_id,
            host_path,
        } => {
            let host_exe = match host_path {
                Some(p) => p,
                None => match resolve_host_exe_path() {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("Failed to resolve host executable path: {e}");
                        std::process::exit(1);
                    }
                },
            };

            match fetchtrek_host::register(browser, &host_exe, extension_id.as_deref()) {
                Ok(results) => {
                    for (b, manifest) in &results {
                        println!("Registered {} native messaging host.", b.display_name());
                        println!("  Manifest: {}", manifest.display());
                        println!("  Host Exe: {}", host_exe.display());
                    }
                    println!(
                        "Registration complete for {} browser target(s).",
                        results.len()
                    );
                }
                Err(e) => {
                    eprintln!("Registration failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        CliCommand::Unregister { browser } => match fetchtrek_host::unregister(browser) {
            Ok(unregistered) => {
                for b in &unregistered {
                    println!("Unregistered {} native messaging host.", b.display_name());
                }
                println!(
                    "Unregistration complete for {} browser target(s).",
                    unregistered.len()
                );
            }
            Err(e) => {
                eprintln!("Unregistration failed: {e}");
                std::process::exit(1);
            }
        },
        CliCommand::StdioLoop => {
            run_stdio_loop().await;
        }
    }
}

async fn run_stdio_loop() {
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
