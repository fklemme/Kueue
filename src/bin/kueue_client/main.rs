use clap::{Parser, Subcommand};
use kueue::{
    constants::DEFAULT_PORT,
    messages::stream::MessageStream,
    messages::{ClientToServerMessage, HelloMessage, ServerToClientMessage},
    structs::{JobInfo, WorkerInfo},
};
use simple_logger::SimpleLogger;
use std::{io::Write, net::Ipv4Addr, str::FromStr};
use termcolor::{Color, ColorChoice, StandardStream};
use termcolor_output::colored;
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
    let mut stdout = StandardStream::stdout(ColorChoice::Auto);

    if job_list.is_empty() {
        writeln!(&mut stdout, "No jobs listed on server!").unwrap();
    } else {
        for job_info in job_list {
            writeln!(&mut stdout, "{:?}", job_info).unwrap();
        }
    }
}

/// Print workers to screen.
fn print_worker_list(worker_list: Vec<WorkerInfo>) {
    let mut stdout = StandardStream::stdout(ColorChoice::Auto);

    // Set color
    // color_spec.set_fg(Some(Color::Green));
    // stdout.set_color(&color_spec).unwrap();

    if worker_list.is_empty() {
        writeln!(&mut stdout, "No workers registered on server!").unwrap();
    } else {
        // Print header
        colored!(
            &mut stdout,
            "| {}{}worker name{}{}               ",
            bold!(true),
            underline!(true),
            bold!(false),
            underline!(false)
        )
        .unwrap();
        colored!(
            &mut stdout,
            "| {}{}operating system{}{}          ",
            bold!(true),
            underline!(true),
            bold!(false),
            underline!(false)
        )
        .unwrap();
        colored!(
            &mut stdout,
            "|  {}{}cpu{}{}  ",
            bold!(true),
            underline!(true),
            bold!(false),
            underline!(false)
        )
        .unwrap();
        colored!(
            &mut stdout,
            "|  {}{}memory{}{}   ",
            bold!(true),
            underline!(true),
            bold!(false),
            underline!(false)
        )
        .unwrap();
        colored!(
            &mut stdout,
            "|  {}{}jobs{}{}   ",
            bold!(true),
            underline!(true),
            bold!(false),
            underline!(false)
        )
        .unwrap();
        colored!(
            &mut stdout,
            "|  {}{}load{}{} (1/5/15) |\n",
            bold!(true),
            underline!(true),
            bold!(false),
            underline!(false)
        )
        .unwrap();

        for info in worker_list {
            // Print worker name
            let worker_name = if info.name.len() <= 25 {
                info.name.clone()
            } else {
                info.name[..22].to_string() + "..."
            };
            write!(&mut stdout, "| {: <25} ", worker_name).unwrap();

            // Print os
            let dist = if info.hw.distribution.len() <= 25 {
                info.hw.distribution.clone()
            } else {
                info.hw.distribution[..22].to_string() + "..."
            };
            write!(&mut stdout, "| {: <25} ", dist).unwrap();

            // Print cpu
            let cpu_cores = format!("{} x", info.hw.cpu_cores);
            write!(&mut stdout, "| {: >5} ", cpu_cores).unwrap();

            // Print memory
            let mem_mb = info.hw.total_memory / 1024 / 1024;
            let mem_mb = format!("{} MB", mem_mb);
            write!(&mut stdout, "| {: >9} ", mem_mb).unwrap();

            // Running jobs color
            let color = if info.jobs_total() * 2 < info.max_parallel_jobs {
                Some(Color::Green)
            } else if info.jobs_total() < info.max_parallel_jobs {
                Some(Color::Yellow)
            } else {
                Some(Color::Red)
            };

            // Print running jobs
            let running = format!("{} / {}", info.jobs_total(), info.max_parallel_jobs);
            colored!(
                &mut stdout,
                "| {}{: ^7}{} | ",
                fg!(color),
                running,
                fg!(None)
            )
            .unwrap();

            for load in [info.load.one, info.load.five, info.load.fifteen] {
                // Load color
                let color = if load < 1.0 {
                    Some(Color::Green)
                } else if load < 10.0 {
                    Some(Color::Yellow)
                } else {
                    Some(Color::Red)
                };

                // Print load
                let load = format!("{:.1}", load);
                colored!(&mut stdout, "{}{: >4}{} ", fg!(color), load, fg!(None)).unwrap();
            }

            // End of line
            writeln!(&mut stdout, "|").unwrap();
        }
    }
}
