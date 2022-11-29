use chrono::Utc;
use console::style;
use kueue::structs::{JobInfo, JobStatus, WorkerInfo};
use terminal_size::terminal_size;

/// Returns the terminal's width and height.
pub fn term_size() -> (usize, usize) {
    // Try to detect terminal size
    if let Some(term_size) = terminal_size() {
        (term_size.0 .0 as usize, term_size.1 .0 as usize)
    } else {
        (80, 25) // default VGA terminal size
    }
}

/// Print jobs to screen.
pub fn job_list(
    jobs_pending: usize,
    jobs_offered: usize,
    jobs_running: usize,
    jobs_finished: usize,
    any_job_failed: bool,
    job_infos: Vec<JobInfo>,
) {
    if !job_infos.is_empty() {
        let default_col_space: usize = 20;
        let space_other_cols: usize = 19;

        let (cwd_col, cmd_col, status_col) = {
            let term_width = term_size().0;
            if term_width > space_other_cols + 3 * 15 {
                let available_space = term_width - space_other_cols;
                let cwd_col = available_space / 4;
                let status_col = (available_space - cwd_col) / 2;
                let cmd_col = available_space - cwd_col - status_col;
                (cwd_col, cmd_col, status_col)
            } else {
                (default_col_space, default_col_space, default_col_space)
            }
        };

        // Print header
        println!(
            "| {: ^6} | {: <cwd_col$} | {: <cmd_col$} | {: <status_col$} |",
            style("id").bold().underlined(),
            style("working dir").bold().underlined(),
            style("command").bold().underlined(),
            style("status").bold().underlined(),
        );

        for job_info in job_infos {
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
                    "pending since {}",
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
                } => {
                    let run_time_seconds = (Utc::now() - started).num_seconds();
                    let h = run_time_seconds / 3600;
                    let m = (run_time_seconds % 3600) / 60;
                    let s = run_time_seconds % 60;
                    style(resize_status(format!(
                        "running for {}h:{:02}m:{:02}s on {}",
                        h, m, s, on
                    )))
                    .blue()
                }
                JobStatus::Finished {
                    finished: _,
                    return_code,
                    on,
                    run_time_seconds,
                } => {
                    if return_code == 0 {
                        let h = run_time_seconds / 3600;
                        let m = (run_time_seconds % 3600) / 60;
                        let s = run_time_seconds % 60;
                        style(resize_status(format!(
                            "finished after {}h:{:02}m:{:02}s on {}",
                            h, m, s, on
                        )))
                        .green()
                    } else {
                        style(resize_status(format!(
                            "failed with code {} on {}",
                            return_code, on
                        )))
                        .red()
                    }
                }
            };

            // Print line
            println!(
                "| {: >6} | {: <cwd_col$} | {: <cmd_col$} | {: <status_col$} |",
                job_info.id, working_dir, command, status
            );
        }
    }

    // Print summary line
    println!("{}", style("--- job status summary ---").bold());
    println!(
        "pending: {}, offered: {}, running: {}, finished: {}",
        jobs_pending,
        style(jobs_offered).dim(),
        style(jobs_running).blue(),
        if any_job_failed {
            style(jobs_finished).red()
        } else {
            style(jobs_finished).green()
        }
    );
}

/// Print workers to screen.
pub fn worker_list(worker_list: Vec<WorkerInfo>) {
    if worker_list.is_empty() {
        println!("No workers registered on server!");
    } else {
        let default_col_space: usize = 20;
        let space_other_cols: usize = 66;

        let (worker_col, os_col) = {
            let term_width = term_size().0;
            if term_width > space_other_cols + 2 * 16 {
                let available_space = term_width - space_other_cols;
                let worker_col = available_space / 2;
                let os_col = available_space - worker_col;
                (worker_col, os_col)
            } else {
                (default_col_space, default_col_space)
            }
        };

        // Print header
        println!(
            "| {: <worker_col$} | {: <os_col$} | {: ^5} | {: ^10} | {: ^7} | {: ^14} | {: ^8} |",
            style("worker name").bold().underlined(),
            style("operating system").bold().underlined(),
            style("cpu").bold().underlined(),
            style("memory").bold().underlined(),
            style("jobs").bold().underlined(),
            style("load 1/5/15m").bold().underlined(),
            style("uptime").bold().underlined(),
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

            let uptime = Utc::now() - info.connected_since;
            let uptime = {
                let hours = uptime.num_hours() % 24;
                format!("{}d {: >2}h", uptime.num_days(), hours)
            };

            // Print line
            println!(
                "| {: <worker_col$} | {: <os_col$} | {: >5} | {: >10} | {: ^7} | {: >4} {: >4} {: >4} | {: >8} |",
                worker_name,
                operation_system,
                cpu_cores,
                memory_mb,
                running_jobs,
                load_one,
                load_five,
                load_fifteen,
                uptime
            );
        }
    }
}

pub fn job_info(job_info: Option<JobInfo>, stdout: Option<String>, stderr: Option<String>) {
    if let Some(job_info) = job_info {
        println!("=== {} ===", style("job information").bold().underlined());
        println!("job id: {}", job_info.id);
        println!("command: {}", job_info.cmd.join(" "));
        println!("working directory: {}", job_info.cwd.to_string_lossy());
        match job_info.status {
            JobStatus::Pending { issued } => {
                println!("job status: pending");
                println!("   issued on: {}", issued);
            }
            JobStatus::Offered {
                issued,
                offered,
                to,
            } => {
                println!("job status: {}", style("pending").dim());
                println!("   issued on: {}", issued);
                println!("   offered on: {}", offered);
                println!("   offered to: {}", to);
            }
            JobStatus::Running {
                issued,
                started,
                on,
            } => {
                println!("job status: {}", style("running").blue());
                println!("   issued on: {}", issued);
                println!("   started on: {}", started);
                println!("   running on: {}", on);
            }
            JobStatus::Finished {
                finished,
                return_code,
                on,
                run_time_seconds,
            } => {
                if return_code == 0 {
                    println!("job status: {}", style("finished").green());
                } else {
                    println!("job status: {}", style("failed").red());
                }
                println!("   finished on: {}", finished);
                println!("   return code: {}", return_code);
                println!("   executed on: {}", on);
                println!("   runtime: {} seconds", run_time_seconds);
            }
        }

        if let Some(stdout) = stdout {
            println!("=== {} ===\n{}", style("stdout").bold(), stdout);
        }

        if let Some(stderr) = stderr {
            println!("=== {} ===\n{}", style("stderr").red(), stderr);
        }
    } else {
        println!("{}", style("Job not found!").red());
    }
}
