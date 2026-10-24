use clap::{Parser, Subcommand, ValueEnum};
use fetchtrek_cli::{CliError, FetchTrekClient};
use fetchtrek_types::TaskState;

#[derive(Parser, Debug)]
#[command(name = "fetchtrek", about = "FetchTrek Download Manager CLI", version)]
struct Cli {
    #[arg(
        short,
        long,
        global = true,
        help = "Custom IPC endpoint pipe or socket path"
    )]
    endpoint: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Add a new download task
    Add {
        /// Target download URL
        url: String,

        /// Destination file path
        #[arg(short, long)]
        dest: Option<String>,

        /// Destination filename
        #[arg(short, long)]
        name: Option<String>,

        /// Number of parallel socket connections
        #[arg(short, long)]
        connections: Option<usize>,
    },

    /// List download tasks
    List {
        /// Filter by task state
        #[arg(short, long)]
        status: Option<CliTaskState>,
    },

    /// Pause an active download task
    Pause {
        /// Task ID
        task_id: i64,
    },

    /// Resume a paused download task
    Resume {
        /// Task ID
        task_id: i64,
    },

    /// Cancel a download task
    Cancel {
        /// Task ID
        task_id: i64,

        /// Delete destination file from disk
        #[arg(short, long)]
        delete_file: bool,
    },

    /// Query detailed task status
    Status {
        /// Task ID
        task_id: i64,
    },

    /// Ping the daemon to test latency and connectivity
    Ping,

    /// Live 10 Hz telemetry monitor
    Monitor,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum CliTaskState {
    Queued,
    Active,
    Paused,
    Completed,
    Failed,
}

impl From<CliTaskState> for TaskState {
    fn from(c: CliTaskState) -> Self {
        match c {
            CliTaskState::Queued => TaskState::Queued,
            CliTaskState::Active => TaskState::Active,
            CliTaskState::Paused => TaskState::Paused,
            CliTaskState::Completed => TaskState::Completed,
            CliTaskState::Failed => TaskState::FatalError,
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let cli = Cli::parse();
    let mut client = FetchTrekClient::connect(cli.endpoint.as_deref()).await?;

    match cli.command {
        Commands::Add {
            url,
            dest,
            name,
            connections,
        } => {
            let (task_id, uuid) = client
                .add_task(&url, dest.as_deref(), name.as_deref(), connections)
                .await?;
            println!("Task created: ID = {}, UUID = {}", task_id, uuid);
        }
        Commands::List { status } => {
            let tasks = client.list_tasks(status.map(Into::into)).await?;
            println!(
                "{:<5} | {:<12} | {:<25} | {:<12} | {:<8} | FILENAME",
                "ID", "STATUS", "DOWNLOADED / TOTAL", "SPEED", "PROGRESS"
            );
            println!(
                "{:-<5}-+-{:-<12}-+-{:-<25}-+-{:-<12}-+-{:-<8}-+-{:-<20}",
                "", "", "", "", "", ""
            );
            for t in tasks {
                let speed_str = if t.speed_bps > 1024 * 1024 {
                    format!("{:.2} MB/s", t.speed_bps as f64 / (1024.0 * 1024.0))
                } else {
                    format!("{:.1} KB/s", t.speed_bps as f64 / 1024.0)
                };
                let progress_str = format!("{:.1}%", t.progress_percent);
                let bytes_str = format!(
                    "{} / {}",
                    format_bytes(t.downloaded_bytes),
                    format_bytes(t.total_bytes)
                );
                println!(
                    "{:<5} | {:<12?} | {:<25} | {:<12} | {:<8} | {}",
                    t.id, t.status, bytes_str, speed_str, progress_str, t.filename
                );
            }
        }
        Commands::Pause { task_id } => {
            client.pause_task(task_id).await?;
            println!("Task {} paused", task_id);
        }
        Commands::Resume { task_id } => {
            client.resume_task(task_id).await?;
            println!("Task {} resumed", task_id);
        }
        Commands::Cancel {
            task_id,
            delete_file,
        } => {
            client.cancel_task(task_id, delete_file).await?;
            println!("Task {} cancelled", task_id);
        }
        Commands::Status { task_id } => {
            let task = client.query_task(task_id).await?;
            println!("Task ID:          {}", task.id);
            println!("UUID:             {}", task.uuid);
            println!("URL:              {}", task.url);
            println!("Filename:         {}", task.filename);
            println!("Destination:      {}", task.destination_path);
            println!("Status:           {:?}", task.status);
            println!(
                "Progress:         {:.1}% ({} / {})",
                task.progress_percent,
                format_bytes(task.downloaded_bytes),
                format_bytes(task.total_bytes)
            );
            println!(
                "Speed:            {:.2} MB/s",
                task.speed_bps as f64 / (1024.0 * 1024.0)
            );
        }
        Commands::Ping => {
            let duration = client.ping().await?;
            println!("Pong received in {} µs", duration.as_micros());
        }
        Commands::Monitor => {
            println!("Streaming live telemetry (Ctrl+C to stop)...");
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        println!("\nMonitoring stopped.");
                        break;
                    }
                    event_res = client.read_telemetry_event() => {
                        match event_res {
                            Ok(snapshot) => {
                                let mb_s = snapshot.global_download_speed_bps as f64 / (1024.0 * 1024.0);
                                println!(
                                    "[{}] Active: {} | Speed: {:.2} MB/s | Running tasks: {}",
                                    snapshot.timestamp_ms,
                                    snapshot.active_task_count,
                                    mb_s,
                                    snapshot.tasks.len()
                                );
                            }
                            Err(e) => {
                                eprintln!("Error reading telemetry: {}", e);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
