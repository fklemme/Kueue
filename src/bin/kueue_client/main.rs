use clap::{Parser, Subcommand};
use kueue::constants::DEFAULT_PORT;
use kueue::message::stream::MessageStream;
use kueue::message::{ClientMessage, HelloMessage, ServerMessage};
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
    match stream.receive::<ServerMessage>().await? {
        ServerMessage::WelcomeClient => log::trace!("Established connection to server..."), // continue
        other => return Err(format!("Expected WelcomeClient, received: {:?}", other).into()),
    }

    // TODO: Implement encryption & authentification

    // Process subcommand
    match args.command {
        Command::Cmd(cmd) => {
            // Issue job
            assert!(!cmd.is_empty());
            let cmd = cmd.join(" ");
            let cwd = std::env::current_dir()?;
            let job = ClientMessage::IssueJob { cmd, cwd };
            stream.send(&job).await?;

            // Await acceptance
            match stream.receive::<ServerMessage>().await? {
                ServerMessage::AcceptJob => {
                    log::debug!("Job submitted successfully!")
                }
                other => return Err(format!("Expected AcceptJob, received: {:?}", other).into()),
            }
        }
        Command::ListJobs => {
            // Query jobs
            let list_jobs = ClientMessage::ListJobs;
            stream.send(&list_jobs).await?;

            // Await results
            match stream.receive::<ServerMessage>().await? {
                ServerMessage::JobList(job_list) => {
                    for job_info in job_list {
                        println!("Job: {}, State: {}", job_info.cmd, job_info.status);
                    }
                }
                other => return Err(format!("Expected JobList, received: {:?}", other).into()),
            }
        }
        Command::ListWorkers => log::warn!("TODO: implement list workers..."),
    }

    // Say bye to gracefully shut down connection.
    stream.send(&ClientMessage::Bye).await?;

    Ok(())
}
