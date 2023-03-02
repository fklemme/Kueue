//! Command line interface for the client application.

use clap::{Parser, Subcommand};
use clap_complete::Shell;
use std::path::PathBuf;

#[derive(Clone, Parser, Debug)]
#[command(version, author, about)]
pub struct Cli {
    /// Path to config file.
    #[arg(short, long, id = "PATH")]
    pub config: Option<PathBuf>,
    /// Subcommands for Kueue.
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Clone, Subcommand, Debug)]
pub enum Command {
    /// Issue command to be off-loaded to remote workers.
    Cmd {
        /// Required/reserved CPU cores to run the command.
        #[arg(short, long)]
        cpus: Option<u64>,
        /// Required/reserved RAM (in megabytes) to run the command.
        #[arg(short, long)]
        ram_mb: Option<u64>,
        /// Redirect stdout to the given file path. If "null" is provided, stdout is discarded.
        #[arg(short = 'o', long)]
        stdout: Option<String>,
        /// Redirect stderr to the given file path. If "null" is provided, stderr is discarded.
        #[arg(short = 'e', long)]
        stderr: Option<String>,
        /// Positional arguments that define the passed command.
        #[command(subcommand)]
        args: CmdArgs,
    },
    /// Query information about scheduled, running, and finished jobs.
    ListJobs {
        /// Number of most recent jobs to query.
        #[arg(short, long)]
        num_jobs: Option<u64>,
        /// Show pending jobs.
        #[arg(short, long)]
        pending: bool,
        /// Show offered jobs.
        #[arg(short, long)]
        offered: bool,
        /// Show running jobs.
        #[arg(short, long)]
        running: bool,
        /// Show finished jobs that succeeded.
        #[arg(short, long)]
        succeeded: bool,
        /// Show finished jobs that failed.
        #[arg(short, long)]
        failed: bool,
        /// Show canceled jobs.
        #[arg(short, long)]
        canceled: bool,
    },
    /// Query information about a specific job.
    ShowJob {
        /// ID of the job to be queried.
        id: u64,
    },
    /// Remove a job from the queue.
    RemoveJob {
        /// ID of the job to be removed.
        id: u64,
        /// If the jobs has already been started, kill the process on the
        /// worker. Otherwise, the job will continue without any effect.
        #[arg(short, long, default_value_t = false)]
        kill: bool,
    },
    /// Remove finished and canceled jobs from the server.
    ///
    /// By default, only successfully finished jobs are cleaned up. Use the
    /// `--all` flag to also remove failed jobs as well.
    CleanJobs {
        /// Remove all finished jobs, including failed ones.
        #[arg(short, long, default_value_t = false)]
        all: bool,
    },
    /// Query information about available workers.
    ListWorkers,
    /// Query information about a specific worker.
    ShowWorker {
        /// ID of the worker to be queried.
        id: u64,
    },
    /// Generate shell completion script for bash, zsh, etc.
    ///
    /// An easy long-term solution is to put `eval "$(kueue complete bash)"`
    /// into your `~/.bashrc`, or the respective start-up script, depending on
    /// your system. Alternatively, you can save the output of
    /// `kueue complete bash` to a script file and source that file either from
    /// your `~/.bashrc` or wherever needed.
    Complete {
        /// Shell to generate the completion script for.
        shell: Shell,
    },
}

#[derive(Clone, Subcommand, Debug)]
pub enum CmdArgs {
    #[command(external_subcommand)]
    Args(Vec<String>), // never shows up in help
}
