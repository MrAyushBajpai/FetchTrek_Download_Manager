use std::path::PathBuf;
use std::str::FromStr;

use crate::registration::BrowserTarget;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliCommand {
    StdioLoop,
    Version,
    PingDaemon,
    Status,
    Register {
        browser: BrowserTarget,
        extension_id: Option<String>,
        host_path: Option<PathBuf>,
    },
    Unregister {
        browser: BrowserTarget,
    },
    Help,
}

pub fn parse_cli_command(args: &[&str]) -> Result<CliCommand, String> {
    if args.is_empty() {
        return Ok(CliCommand::StdioLoop);
    }

    match args[0] {
        "--version" | "-v" => Ok(CliCommand::Version),
        "--ping-daemon" => Ok(CliCommand::PingDaemon),
        "--help" | "-h" | "help" => Ok(CliCommand::Help),
        "status" => Ok(CliCommand::Status),
        "register" => {
            let mut browser = BrowserTarget::All;
            let mut extension_id = None;
            let mut host_path = None;
            let mut idx = 1;
            while idx < args.len() {
                match args[idx] {
                    "--browser" => {
                        idx += 1;
                        if idx >= args.len() {
                            return Err("Missing argument for --browser".to_string());
                        }
                        browser = BrowserTarget::from_str(args[idx])?;
                    }
                    "--extension-id" => {
                        idx += 1;
                        if idx >= args.len() {
                            return Err("Missing argument for --extension-id".to_string());
                        }
                        extension_id = Some(args[idx].to_string());
                    }
                    "--host-path" => {
                        idx += 1;
                        if idx >= args.len() {
                            return Err("Missing argument for --host-path".to_string());
                        }
                        host_path = Some(PathBuf::from(args[idx]));
                    }
                    unknown => {
                        return Err(format!("Unknown option for 'register': '{unknown}'"));
                    }
                }
                idx += 1;
            }
            Ok(CliCommand::Register {
                browser,
                extension_id,
                host_path,
            })
        }
        "unregister" => {
            let mut browser = BrowserTarget::All;
            let mut idx = 1;
            while idx < args.len() {
                match args[idx] {
                    "--browser" => {
                        idx += 1;
                        if idx >= args.len() {
                            return Err("Missing argument for --browser".to_string());
                        }
                        browser = BrowserTarget::from_str(args[idx])?;
                    }
                    unknown => {
                        return Err(format!("Unknown option for 'unregister': '{unknown}'"));
                    }
                }
                idx += 1;
            }
            Ok(CliCommand::Unregister { browser })
        }
        unknown => Err(format!(
            "Unknown command: '{unknown}'. Available commands: register, unregister, status, --version, --ping-daemon"
        )),
    }
}

pub fn print_help() {
    println!("FetchTrek Native Messaging Host");
    println!("Usage: fetchtrek-host [COMMAND] [OPTIONS]");
    println!();
    println!("Commands:");
    println!("  status                        Check browser registration status");
    println!("  register   [--browser <name>] Register host with browser(s) (default: all)");
    println!("             [--extension-id <ID>] Specific extension ID to whitelist");
    println!("             [--host-path <path>] Override host executable path");
    println!("  unregister [--browser <name>] Unregister host from browser(s) (default: all)");
    println!("  --version, -v                 Print version");
    println!("  --ping-daemon                 Test IPC connection to daemon");
    println!("  help, --help, -h              Print this help menu");
    println!();
    println!("Supported browsers: chrome, edge, brave, firefox, all");
    println!("When invoked without arguments, runs in stdio native messaging mode.");
}
