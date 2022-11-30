use chrono::Utc;
use console::style;
use kueue::structs::{JobInfo, JobStatus, WorkerInfo};
use std::cmp::max;
use terminal_size::terminal_size;

/// Returns the terminal's width and height.
pub fn term_size() -> (usize, usize) {
    // Try to detect terminal size
    if let Some(term_size) = terminal_size() {
        (term_size.0 .0 as usize, term_size.1 .0 as usize)
    } else {
        // Default VGA terminal size is: (80, 25)
        // But large size is more useful as it is used by tools like "grep".
        (1_000_000, 1_000)
    }
}

/// Calculate column widths based on content.
fn get_col_widths(min_col_widths: Vec<usize>, max_col_widths: Vec<usize>) -> Vec<usize> {
    assert!(!min_col_widths.is_empty() && !max_col_widths.is_empty());
    assert!(min_col_widths.len() == max_col_widths.len());

    // TODO: Min col widths are not considered correctly!

    // Final and target column widths.
    let mut col_widths = vec![0 as usize; min_col_widths.len()];
    let target_col_widths: Vec<usize> = min_col_widths
        .iter()
        .zip(max_col_widths.iter())
        .map(|(a, b)| max(a, b).to_owned())
        .collect();

    // Column space available and assigned.
    let total_col_width_available = term_size().0 - (3 * target_col_widths.len() + 1);

    // Assign column widths as long as there is free space.
    'outer: loop {
        let total_col_width_occupied: usize = col_widths.iter().sum();
        let remaining_col_width_available = total_col_width_available - total_col_width_occupied;

        let num_cols_not_fixed = col_widths.iter().filter(|width| **width == 0).count();
        if num_cols_not_fixed == 0 {
            // All columns have a width assigned. Done.
            return col_widths;
        }

        // For all columns targeting <= average col width, assign target width.
        let average_col_width = remaining_col_width_available / num_cols_not_fixed;
        for index in 0..col_widths.len() {
            // Column has not been fixed yet.
            if col_widths[index] == 0 {
                if target_col_widths[index] <= average_col_width {
                    col_widths[index] = target_col_widths[index];
                    // Remaining space and thus average column widths will change.
                    continue 'outer;
                }
            }
        }

        // If we reach this point, no columns could be fixed by applying their target width.
        // Instead, proceed to assign average width to any unfixed column.
        for index in 0..col_widths.len() {
            // Column has not been fixed yet.
            if col_widths[index] == 0 {
                col_widths[index] = average_col_width;
                // Average could be rounded down, so only fix one column at a time.
                continue 'outer;
            }
        }
    }
}

fn dots_front(text: String, len: usize) -> String {
    if text.len() <= len {
        text
    } else {
        "...".to_string() + &text[(text.len() - (len - 3))..]
    }
}

fn dots_back(text: String, len: usize) -> String {
    if text.len() <= len {
        text
    } else {
        text[..(len - 3)].to_owned() + "..."
    }
}

fn format_status(job_info: &JobInfo) -> String {
    match &job_info.status {
        JobStatus::Pending { issued } => format!(
            "pending since {}",
            issued.format("%Y-%m-%d %H:%M:%S").to_string()
        ),
        JobStatus::Offered {
            issued: _,
            offered: _,
            to,
        } => format!("offered to {}", to),
        JobStatus::Running {
            issued: _,
            started,
            on,
        } => {
            let run_time_seconds = (Utc::now() - *started).num_seconds();
            let h = run_time_seconds / 3600;
            let m = (run_time_seconds % 3600) / 60;
            let s = run_time_seconds % 60;
            format!("running for {}h:{:02}m:{:02}s on {}", h, m, s, on)
        }
        JobStatus::Finished {
            finished: _,
            return_code,
            on,
            run_time_seconds,
        } => {
            if *return_code == 0 {
                let h = run_time_seconds / 3600;
                let m = (run_time_seconds % 3600) / 60;
                let s = run_time_seconds % 60;
                format!("finished after {}h:{:02}m:{:02}s on {}", h, m, s, on)
            } else {
                format!("failed with code {} on {}", return_code, on)
            }
        }
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
        // Calculate spacing for columns.
        let max_id_col_width = format!("{}", job_infos.last().unwrap().id).len();
        let max_cwd_col_width = job_infos
            .iter()
            .map(|job_info| job_info.cwd.to_string_lossy().len())
            .max()
            .unwrap();
        let max_cmd_col_width = job_infos
            .iter()
            .map(|job_info| job_info.cmd.join(" ").len())
            .max()
            .unwrap();
        let max_status_col_width = job_infos
            .iter()
            .map(|job_info| format_status(job_info).len())
            .max()
            .unwrap();

        let min_col_widths = vec![
            "id".len(),
            "working dir".len(),
            "command".len(),
            "status".len(),
        ];
        let max_col_widths = vec![
            max_id_col_width,
            max_cwd_col_width,
            max_cmd_col_width,
            max_status_col_width,
        ];

        let col_widths = get_col_widths(min_col_widths, max_col_widths);
        let (id_col, cwd_col, cmd_col, status_col) =
            (col_widths[0], col_widths[1], col_widths[2], col_widths[3]);

        // Print header
        println!(
            "| {: ^id_col$} | {: <cwd_col$} | {: <cmd_col$} | {: <status_col$} |",
            style("id").bold().underlined(),
            style("working dir").bold().underlined(),
            style("command").bold().underlined(),
            style("status").bold().underlined(),
        );

        for job_info in job_infos {
            // working dir
            let working_dir = job_info.cwd.to_string_lossy();
            let working_dir = dots_front(working_dir.to_string(), cwd_col);

            // command
            let command = job_info.cmd.join(" ");
            let command = dots_back(command, cmd_col);

            // status
            let status = dots_back(format_status(&job_info), status_col);
            let status = match job_info.status {
                JobStatus::Pending { .. } => style(status),
                JobStatus::Offered { .. } => style(status).dim(),
                JobStatus::Running { .. } => style(status).blue(),
                JobStatus::Finished { return_code, .. } => {
                    if return_code == 0 {
                        style(status).green()
                    } else {
                        style(status).red()
                    }
                }
            };

            // Print line.
            println!(
                "| {: >id_col$} | {: <cwd_col$} | {: <cmd_col$} | {: <status_col$} |",
                job_info.id, working_dir, command, status
            );
        }
    }

    // Print summary line.
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
        // Calculate spacing for columns.
        let max_worker_col_width = worker_list
            .iter()
            .map(|worker_info| worker_info.name.len())
            .max()
            .unwrap();
        let max_os_col_width = worker_list
            .iter()
            .map(|worker_info| worker_info.hw.distribution.len())
            .max()
            .unwrap();

        let min_col_widths = vec![
            "worker name".len(),
            "operating system".len(),
            "cpu".len(),
            "memory".len(),
            "jobs".len(),
            "load 1/5/15m".len(),
            "uptime".len(),
        ];
        let max_col_widths = vec![
            max_worker_col_width,
            max_os_col_width,
            5,  // cpu
            10, // memory
            7,  // jobs
            14, // load
            8,  // uptime
        ];

        let col_widths = get_col_widths(min_col_widths, max_col_widths);
        let (worker_col, os_col, cpu_col, memory_col, jobs_col, load_col, uptime_col) = (
            col_widths[0],
            col_widths[1],
            col_widths[2],
            col_widths[3],
            col_widths[4],
            col_widths[5],
            col_widths[6],
        );

        // TODO: col widths not consistently used in code below!

        // Print header
        println!(
            "| {: <worker_col$} | {: <os_col$} | {: ^cpu_col$} | {: ^memory_col$} | {: ^jobs_col$} | {: ^load_col$} | {: ^uptime_col$} |",
            style("worker name").bold().underlined(),
            style("operating system").bold().underlined(),
            style("cpu").bold().underlined(),
            style("memory").bold().underlined(),
            style("jobs").bold().underlined(),
            style("load 1/5/15m").bold().underlined(),
            style("uptime").bold().underlined(),
        );

        for info in worker_list {
            let worker_name = dots_back(info.name.clone(), worker_col);
            let operation_system = dots_back(info.hw.distribution.clone(), os_col);
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
                "| {: <worker_col$} | {: <os_col$} | {: >cpu_col$} | {: >memory_col$} | {: ^jobs_col$} | {: >4} {: >4} {: >4} | {: >uptime_col$} |",
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
