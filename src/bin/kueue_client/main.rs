use clap::{Parser, Subcommand};
use kueue::{
    constants::DEFAULT_PORT,
    messages::stream::MessageStream,
    messages::{ClientToServerMessage, HelloMessage, ServerToClientMessage},
    structs::{JobInfo, WorkerInfo},
};
use simple_logger::SimpleLogger;
use std::{io::Write, net::Ipv4Addr, str::FromStr};
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
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

fn print_job_list(job_list: Vec<JobInfo>) {
    let mut stdout = StandardStream::stdout(ColorChoice::Auto);
    let mut color_spec = ColorSpec::new();

    // Set color.
    color_spec.set_fg(Some(Color::Green));
    stdout.set_color(&color_spec).unwrap();

    if job_list.is_empty() {
        writeln!(&mut stdout, "No jobs listed on server!").unwrap();
    } else {
        for job_info in job_list {
            writeln!(&mut stdout, "{:?}", job_info).unwrap();
        }
    }

    // Reset color.
    color_spec.set_fg(None);
    stdout.set_color(&color_spec).unwrap();
}

fn print_worker_list(worker_list: Vec<WorkerInfo>) {
    let mut stdout = StandardStream::stdout(ColorChoice::Auto);
    let mut color_spec = ColorSpec::new();

    // Set color.
    color_spec.set_fg(Some(Color::Green));
    stdout.set_color(&color_spec).unwrap();

    if worker_list.is_empty() {
        writeln!(&mut stdout, "No workers registered on server!").unwrap();
    } else {
        for worker_info in worker_list {
            writeln!(&mut stdout, "{:?}", worker_info).unwrap();
        }
    }

    // Reset color.
    color_spec.set_fg(None);
    stdout.set_color(&color_spec).unwrap();
}
