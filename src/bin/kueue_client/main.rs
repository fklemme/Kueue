use clap::{Parser, Subcommand};
use console::style;
use kueue::{
    constants::{DEFAULT_PORT, DEFAULT_SERVER_ADDR},
    messages::stream::MessageStream,
    messages::{ClientToServerMessage, HelloMessage, ServerToClientMessage},
    structs::{JobInfo, JobStatus, WorkerInfo},
};
use simple_logger::SimpleLogger;
use std::{net::Ipv4Addr, str::FromStr};
use terminal_size::terminal_size;
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Address of server to connect to.
    #[arg(short = 'a', long, default_value_t = DEFAULT_SERVER_ADDR.to_string())]
    server_address: String,
    /// Port of server to connect to.
    #[arg(short = 'p', long, default_value_t = DEFAULT_PORT)]
    server_port: u16,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Issue command to be off-loaded to remote workers.
    #[command(external_subcommand)]
    Cmd(Vec<String>), // TODO: missing in help!
    /// Query information about scheduled and running jobs.
    ListJobs,
    /// Query infromation about available workers.
    ListWorkers,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    // Read command line arguments
    let args = Args::parse();
    log::debug!("{:?}", args);

    // Connect to server
    let server_addr = (Ipv4Addr::from_str(&args.server_address)?, args.server_port);
    let stream = TcpStream::connect(server_addr).await?;
    let mut stream = MessageStream::new(stream);

    // Send hello from client
    stream.send(&HelloMessage::HelloFromClient).await?;

    // Await welcoming response from server
    match stream.receive::<ServerToClientMessage>().await? {
        ServerToClientMessage::WelcomeClient => log::trace!("Established connection to server!"), // continue
        other => return Err(format!("Expected WelcomeClient, received: {:?}", other).into()),
    }

    // TODO: Implement encryption & authentification

    // Process subcommand
    match args.command {
        Command::Cmd(cmd) => {
            // Issue job
            assert!(!cmd.is_empty());
            let cwd = std::env::current_dir()?;
            let message = ClientToServerMessage::IssueJob(JobInfo::new(cmd, cwd));
            stream.send(&message).await?;

            // Await acceptance
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::AcceptJob(job_info) => {
                    log::debug!("Job submitted successfully!");
                    log::info!("Job ID: {}", job_info.id);
                }
                other => {
                    return Err(format!("Expected AcceptJob, received: {:?}", other).into());
                }
            }
        }
        Command::ListJobs => {
            // Query jobs
            let message = ClientToServerMessage::ListJobs;
            stream.send(&message).await?;

            // Await results
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::JobList(job_list) => {
                    print_job_list(job_list);
                }
                other => {
                    return Err(format!("Expected JobList, received: {:?}", other).into());
                }
            }
        }
        Command::ListWorkers => {
            // Query workers
            let message = ClientToServerMessage::ListWorkers;
            stream.send(&message).await?;

            // Await results
            match stream.receive::<ServerToClientMessage>().await? {
                ServerToClientMessage::WorkerList(worker_list) => {
                    print_worker_list(worker_list);
                }
                other => {
                    return Err(format!("Expected WorkerList, received: {:?}", other).into());
                }
            }
        }
    }

    // Say bye to gracefully shut down connection.
    stream.send(&ClientToServerMessage::Bye).await?;

    Ok(())
}

/// Print jobs to screen.
fn print_job_list(job_list: Vec<JobInfo>) {
    if job_list.is_empty() {
        println!("No jobs listed on server!");
    } else {
        let default_col_space: usize = 20;
        let space_other_cols: usize = 17;

        // Try to detect terminal size
        let term_size = terminal_size();
        let (cwd_col, cmd_col, status_col) = if let Some(size) = term_size {
            let term_width = size.0 .0 as usize;
            if term_width > space_other_cols + 3 * 15 {
                let available_space = term_width - space_other_cols;
                let cwd_col = available_space / 4;
                let status_col = (available_space - cwd_col) / 2;
                let cmd_col = available_space - cwd_col - status_col;
                (cwd_col, cmd_col, status_col)
            } else {
                (default_col_space, default_col_space, default_col_space)
            }
        } else {
            (default_col_space, default_col_space, default_col_space)
        };

        // Print header
        println!(
            "| {: ^4} | {: <cwd_col$} | {: <cmd_col$} | {: <status_col$} |",
            style("id").bold().underlined(),
            style("working dir").bold().underlined(),
            style("command").bold().underlined(),
            style("status").bold().underlined(),
        );

        for job_info in job_list {
            // working dir
            let working_dir = job_info.cwd.to_string_lossy().to_string();
            let working_dir = if working_dir.len() <= cwd_col {
                working_dir
            } else {
                "...".to_string() + &working_dir[(working_dir.len() - (cwd_col - 3))..]
            };

            // command
            let command = job_info.cmd.join(" ");
            let command = if command.len() <= cmd_col {
                command
            } else {
                command[..(cmd_col - 3)].to_string() + "..."
            };

            // status
            let resize_status = |status: String| {
                if status.len() <= status_col {
                    status
                } else {
                    status[..(status_col - 3)].to_string() + "..."
                }
            };
            let status = match job_info.status {
                JobStatus::Pending { issued } => style(resize_status(format!(
                    "pending, issued {}",
                    issued.format("%Y-%m-%d %H:%M:%S").to_string()
                ))),
                JobStatus::Offered {
                    issued: _,
                    offered: _,
                    to,
                } => style(resize_status(format!("offered to {}", to))).dim(),
                JobStatus::Running {
                    issued: _,
                    started,
                    on,
                } => style(resize_status(format!(
                    "running on {}, started {}",
                    on,
                    started.format("%Y-%m-%d %H:%M:%S").to_string()
                )))
                .blue(),
                JobStatus::Finished {
                    finished: _,
                    return_code,
                    on: _,
                    run_time_seconds,
                } => {
                    if return_code == 0 {
                        let h = run_time_seconds / 3600;
                        let m = (run_time_seconds % 3600) / 60;
                        let s = run_time_seconds % 60;
                        style(resize_status(format!(
                            "finished, took {h}h:{m:02}m:{s:02}s"
                        )))
                        .green()
                    } else {
                        style(resize_status(format!("failed, code {return_code}",))).red()
                    }
                }
            };

            // Print line
            println!(
                "| {: >4} | {: <cwd_col$} | {: <cmd_col$} | {: <status_col$} |",
                job_info.id, working_dir, command, status
            );
        }
    }
}

/// Print workers to screen.
fn print_worker_list(worker_list: Vec<WorkerInfo>) {
    if worker_list.is_empty() {
        println!("No workers registered on server!");
    } else {
        let default_col_space: usize = 20;
        let space_other_cols: usize = 55;

        // Try to detect terminal size
        let term_size = terminal_size();
        let (worker_col, os_col) = if let Some(size) = term_size {
            let term_width = size.0 .0 as usize;
            if term_width > space_other_cols + 2 * 16 {
                let available_space = term_width - space_other_cols;
                let worker_col = available_space / 2;
                let os_col = available_space - worker_col;
                (worker_col, os_col)
            } else {
                (default_col_space, default_col_space)
            }
        } else {
            (default_col_space, default_col_space)
        };

        // Print header
        println!(
            "| {: <worker_col$} | {: <os_col$} | {: ^5} | {: ^10} | {: ^7} | {: ^14} |",
            style("worker name").bold().underlined(),
            style("operating system").bold().underlined(),
            style("cpu").bold().underlined(),
            style("memory").bold().underlined(),
            style("jobs").bold().underlined(),
            style("load 1/5/15m").bold().underlined(),
        );

        for info in worker_list {
            // worker name
            let worker_name = if info.name.len() <= worker_col {
                info.name.clone()
            } else {
                info.name[..(worker_col - 3)].to_string() + "..."
            };

            // operating system
            let operation_system = if info.hw.distribution.len() <= os_col {
                info.hw.distribution.clone()
            } else {
                info.hw.distribution[..(os_col - 3)].to_string() + "..."
            };

            // cpu cores
            let cpu_cores = format!("{} x", info.hw.cpu_cores);

            // memory
            let memory_mb = info.hw.total_memory / 1024 / 1024;
            let memory_mb = format!("{} MB", memory_mb);

            // running jobs
            let running_jobs = format!("{} / {}", info.jobs_total(), info.max_parallel_jobs);
            let running_jobs = if info.jobs_total() * 2 < info.max_parallel_jobs {
                style(running_jobs).green()
            } else if info.jobs_total() < info.max_parallel_jobs {
                style(running_jobs).yellow()
            } else {
                style(running_jobs).red()
            };

            // loads
            let load_style = |load| {
                let load_fmt = format!("{:.1}", load);
                if load < 1.0 {
                    style(load_fmt).green()
                } else if load < 10.0 {
                    style(load_fmt).yellow()
                } else {
                    style(load_fmt).red()
                }
            };

            let load_one = load_style(info.load.one);
            let load_five = load_style(info.load.five);
            let load_fifteen = load_style(info.load.fifteen);

            // Print line
            println!(
                "| {: <worker_col$} | {: <os_col$} | {: >5} | {: >10} | {: ^7} | {: >4} {: >4} {: >4} |",
                worker_name,
                operation_system,
                cpu_cores,
                memory_mb,
                running_jobs,
                load_one,
                load_five,
                load_fifteen
            );
        }
    }
}
