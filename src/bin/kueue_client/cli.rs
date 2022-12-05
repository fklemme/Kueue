use crate::print::term_size;
use clap::{Parser, Subcommand};
use kueue::config::default_path;

#[derive(Parser, Debug)]
#[command(version, author, about)]
pub struct Args {
    /// Path to config file.
    #[arg(short, long, default_value_t = default_path().to_string_lossy().into())]
    pub config: String,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Issue command to be off-loaded to remote workers.
    #[command(external_subcommand)]
    Cmd(Vec<String>), // TODO: missing in help!
    /// Query information about scheduled and running jobs.
    ListJobs {
        /// Number of latest jobs to query.
        #[arg(short, long, default_value_t = term_size().1 - 4)]
        num_jobs: usize,
        /// Show pending jobs.
        #[arg(short, long)]
        pending: bool,
        /// Show offered jobs.
        #[arg(short, long)]
        offered: bool,
        /// Show running jobs.
        #[arg(short, long)]
        running: bool,
        /// Show finished jobs.
        #[arg(short, long)]
        finished: bool,
        /// Show failed jobs.
        #[arg(short = 'e', long)]
        failed: bool,
        /// Show canceled jobs.
        #[arg(short, long)]
        canceled: bool,
    },
    /// Query information about available workers.
    ListWorkers,
    /// Show information about a specific job.
    ShowJob {
        /// ID of the job to be queried.
        id: usize,
    },
    /// Remove job from the queue.
    RemoveJob {
        /// ID of the job to be removed.
        id: usize,
        /// If the jobs has already been started, kill the process on the
        /// worker. Otherwise, the job will continue without any effect.
        #[arg(short, long, default_value_t = false)]
        kill: bool,
    },
}
