use clap::{Parser, Subcommand};
use console::style;
use kueue::{
    constants::DEFAULT_PORT,
    messages::stream::MessageStream,
    messages::{ClientToServerMessage, HelloMessage, ServerToClientMessage},
    structs::{JobInfo, WorkerInfo},
};
use simple_logger::SimpleLogger;
use std::{net::Ipv4Addr, str::FromStr};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Address of server to connect to.
    #[arg(short = 'a', long, default_value_t = String::from("127.0.0.1"))]
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
    SimpleLogger::new().init().unwrap();

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
        for job_info in job_list {
            println!("{:?}", job_info);
        }
    }
}

/// Print workers to screen.
fn print_worker_list(worker_list: Vec<WorkerInfo>) {
    if worker_list.is_empty() {
        println!("No workers registered on server!");
    } else {
        // Try to detect terminal size (TODO: needs fine tuning)
        let (term_width, _term_height) = console::Term::stdout().size();
        let space_other_cols = 50;
        let (worker_col, os_col) = if term_width > space_other_cols + 20 {
            let col_space = (term_width - space_other_cols) as usize / 2;
            (col_space, col_space)
        } else {
            (20, 20)
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
            let worker_name = if info.name.len() <= 25 {
                info.name.clone()
            } else {
                info.name[..22].to_string() + "..."
            };

            // operating system
            let operation_system = if info.hw.distribution.len() <= 25 {
                info.hw.distribution.clone()
            } else {
                info.hw.distribution[..22].to_string() + "..."
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
