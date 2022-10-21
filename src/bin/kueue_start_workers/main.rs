use std::{error::Error, io::Read, net::TcpStream};

use kueue::config::Config;
use ssh2::Session;

fn main() {
    // Read configuration from file or defaults.
    let config = Config::new().expect("Failed to load config!");
    let ssh_user = config.restart_workers.ssh_user;
    let workers: Vec<_> = config
        .restart_workers
        .hostnames
        .split_whitespace()
        .collect();

    for worker in workers {
        if let Err(e) = process_worker(worker, &ssh_user) {
            log::error!("Failed processing worker {}: {}", worker, e);
        }
    }
}

fn process_worker(worker: &str, ssh_user: &str) -> Result<(), Box<dyn Error>> {
    log::trace!("Processing worker {}...", worker);

    // TODO: Requires some kind of "ssh-add ~/.ssh/id_rsa"?
    let mut session = Session::new()?;

    // Connect to worker
    let tcp_stream = TcpStream::connect(format!("{}:{}", worker, 22))?;
    session.set_tcp_stream(tcp_stream);
    session.handshake()?;
    session.userauth_agent(ssh_user).unwrap();

    // Check if worker is running (in screen session)
    let mut screen_ls = String::new();
    let mut channel = session.channel_session()?;
    channel.exec("screen -ls")?;
    channel.read_to_string(&mut screen_ls)?;
    channel.wait_close()?;

    if screen_ls.contains("kueue_worker") {
        log::info!("Worker {} appears to be running.", worker);
    } else {
        log::warn!("Worker {} appears to be down! Restart...", worker);

        // Restart worker in detached screen
        let cmd = "screen -dmS kueue_worker bash -c kueue_worker";
        let mut output = String::new();
        let mut channel = session.channel_session()?;
        channel.exec(cmd)?;
        channel.read_to_string(&mut output)?;
        channel.wait_close()?;

        log::debug!("Output after restarting: {}", output)
    }

    Ok(()) // done
}
