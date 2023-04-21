//! Command line interface for the client application.

use clap::{Parser, Subcommand};
use clap_complete::Shell;
use std::path::PathBuf;

/// Command line interface for the client.
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

/// Subcommands for the command line interface.
#[derive(Clone, Subcommand, Debug)]
pub enum Command {
    /// Issue command to be off-loaded to remote workers.
    ///
    /// After the command has been issued, the ID of the newly created job is
    /// printed to stdout. If `--wait` has been given as additional argument,
    /// the return code of the remotely executed job is printed to stdout instead.
    Cmd {
        /// Job slots occupied by this command.
        #[arg(short, long, default_value_t = 1)]
        job_slots: u64,
        /// Required CPU cores to run the command.
        #[arg(short, long)]
        cpus: Option<u64>,
        /// Required RAM memory (in megabytes) to run the command.
        #[arg(short, long)]
        ram_mb: Option<u64>,
        /// Additional resources, such as licenses.
        #[arg(id = "resource", long)]
        resources: Vec<String>,
        /// Redirect stdout to the given file path. If "null" is provided, stdout is discarded.
        #[arg(short = 'o', long)]
        stdout: Option<String>,
        /// Redirect stderr to the given file path. If "null" is provided, stderr is discarded.
        #[arg(short = 'e', long)]
        stderr: Option<String>,
        /// Block until the job has been finished or canceled.
        #[arg(short, long)]
        wait: bool,
        /// Positional arguments that define the command.
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
        job_id: u64,
    },
    /// Block until a certain job has finished.
    WaitJob {
        /// ID of the job to be waited for.
        job_id: u64,
    },
    /// Remove a job from the queue.
    ///
    /// Be default, already running jobs will not be interrupted.
    /// To cancel a running job, pass the `--kill` flag as well.
    RemoveJob {
        /// ID of the job to be removed.
        job_id: u64,
        /// If the jobs has already been started, also kill the process on the
        /// worker. Otherwise, the job will continue without any effect.
        #[arg(short, long, default_value_t = false)]
        kill: bool,
    },
    /// Remove finished and canceled jobs from the server.
    ///
    /// By default, only successfully finished and canceled jobs are cleaned up.
    /// Use the `--all` flag to remove failed jobs as well.
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
        worker_id: u64,
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

/// Arbitrary command captured from positional arguments.
#[derive(Clone, Subcommand, Debug)]
pub enum CmdArgs {
    #[command(external_subcommand)]
    /// Captured arguments. This comment does not show up in help.
    Args(Vec<String>),
}
