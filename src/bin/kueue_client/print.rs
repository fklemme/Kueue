use chrono::{DateTime, Utc};
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

    // Make sure that max >= min col width.
    let max_col_widths: Vec<usize> = max_col_widths
        .iter()
        .zip(min_col_widths.iter())
        .map(|(a, b)| max(a, b).to_owned())
        .collect();

    // Total column space available for assignment.
    let total_col_width_available = term_size().0 - (3 * min_col_widths.len() + 1);

    // Return immediately if we have enough space for every column at max width.
    if max_col_widths.iter().sum::<usize>() <= total_col_width_available {
        return max_col_widths;
    }

    // Also return immediately if there is not even enough space to provide the min col widths.
    if total_col_width_available <= min_col_widths.iter().sum::<usize>() {
        return min_col_widths;
    }

    // Final column widths to be returned later. Start with minimum column widths.
    let mut col_widths = min_col_widths.clone();

    // Grow column widths as long as there is free space.
    let mut remaining_col_width_available =
        total_col_width_available - col_widths.iter().sum::<usize>();
    'outer: while remaining_col_width_available > 0 {
        // Sort columns by size.
        let mut indices: Vec<usize> = (0..col_widths.len()).collect();
        indices.sort_by(|i1, i2| col_widths[*i1].cmp(&col_widths[*i2]));

        // Increment smallest column.
        for index in indices {
            if col_widths[index] < max_col_widths[index] {
                col_widths[index] += 1;
                remaining_col_width_available -= 1;
                continue 'outer;
            }
        }

        // This point should never be reached, because if there were enough
        // space for all columns, we would have returned even before the loop.
        assert!(false);
    }

    // No more space for increments available.
    col_widths
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
            comment: _,
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
        JobStatus::Canceled { canceled } => format!(
            "canceled on {}",
            canceled.format("%Y-%m-%d %H:%M:%S").to_string()
        ),
    }
}

/// Print jobs to screen.
pub fn job_list(
    jobs_pending: usize,
    jobs_offered: usize,
    jobs_running: usize,
    jobs_succeeded: usize,
    jobs_failed: usize,
    jobs_canceled: usize,
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
                JobStatus::Canceled { .. } => style(status).yellow(),
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

    print!("pending: {}", jobs_pending);
    if jobs_offered > 0 {
        print!(", offered: {}", style(jobs_offered).dim());
    }
    if jobs_running > 0 {
        print!(", running: {}", style(jobs_running).blue());
    }
    if jobs_succeeded > 0 {
        print!(", succeeded: {}", style(jobs_succeeded).green());
    }
    if jobs_failed > 0 {
        print!(", failed: {}", style(jobs_failed).red());
    }
    if jobs_canceled > 0 {
        print!(", canceled: {}", style(jobs_canceled).yellow());
    }
    println!(""); // end line
}

fn format_cores(cpu_cores: usize) -> String {
    format!("{} x", cpu_cores)
}

fn format_frequency(cpu_frequency: u64) -> String {
    format!("{} MHz", cpu_frequency)
}

fn format_memory_mb(memory_bytes: u64) -> String {
    format!("{} MB", memory_bytes / 1024 / 1024)
}

fn format_uptime(connected_since: DateTime<Utc>) -> String {
    let uptime = Utc::now() - connected_since;
    let hours = uptime.num_hours() % 24;
    format!("{}d {:02}h", uptime.num_days(), hours)
}

/// Print workers to screen.
pub fn worker_list(worker_list: Vec<WorkerInfo>) {
    if worker_list.is_empty() {
        println!("No workers registered on server!");
    } else {
        // Calculate spacing for columns.
        let max_worker_col_width = worker_list
            .iter()
            .map(|info| info.name.len())
            .max()
            .unwrap();
        let max_os_col_width = worker_list
            .iter()
            .map(|info| info.hw.distribution.len())
            .max()
            .unwrap();
        let max_cores_col_width = worker_list
            .iter()
            .map(|info| format_cores(info.hw.cpu_cores).len())
            .max()
            .unwrap();
        let max_freq_col_width = worker_list
            .iter()
            .map(|info| format_frequency(info.hw.cpu_frequency).len())
            .max()
            .unwrap();
        let max_memory_col_width = worker_list
            .iter()
            .map(|info| format_memory_mb(info.hw.total_memory).len())
            .max()
            .unwrap();
        let max_run_jobs_col_width = worker_list
            .iter()
            .map(|info| format!("{}", info.jobs_total()).len())
            .max()
            .unwrap();
        let max_max_jobs_col_width = worker_list
            .iter()
            .map(|info| format!("{}", info.max_parallel_jobs).len())
            .max()
            .unwrap();
        let max_load_1_col_width = worker_list
            .iter()
            .map(|info| format!("{:.1}", info.load.one).len())
            .max()
            .unwrap();
        let max_load_5_col_width = worker_list
            .iter()
            .map(|info| format!("{:.1}", info.load.five).len())
            .max()
            .unwrap();
        let max_load_15_col_width = worker_list
            .iter()
            .map(|info| format!("{:.1}", info.load.fifteen).len())
            .max()
            .unwrap();
        let max_uptime_col_width = worker_list
            .iter()
            .map(|info| format_uptime(info.connected_since).len())
            .max()
            .unwrap();

        let min_col_widths = vec![
            "worker name".len(),
            "operating system".len(),
            max("cpus".len(), max_cores_col_width),
            max("avg freq".len(), max_freq_col_width),
            max("memory".len(), max_memory_col_width),
            max_run_jobs_col_width,
            max_max_jobs_col_width,
            max_load_1_col_width,
            max_load_5_col_width,
            max_load_15_col_width,
            max("uptime".len(), max_uptime_col_width),
        ];
        let max_col_widths = vec![
            max_worker_col_width,
            max_os_col_width,
            0, // cpu cores
            0, // cpu frequency
            0, // memory
            0, // running jobs
            0, // max jobs
            0, // load 1
            0, // load 5
            0, // load 15
            0, // uptime
        ];

        let col_widths = get_col_widths(min_col_widths, max_col_widths);
        let (
            worker_col,
            os_col,
            cores_col,
            freq_col,
            memory_col,
            run_jobs_col,
            max_jobs_col,
            load_1_col,
            load_5_col,
            load_15_col,
            uptime_col,
        ) = (
            col_widths[0],
            col_widths[1],
            col_widths[2],
            col_widths[3],
            col_widths[4],
            col_widths[5],
            col_widths[6],
            col_widths[7],
            col_widths[8],
            col_widths[9],
            col_widths[10],
        );

        let jobs_col = run_jobs_col + max_jobs_col + 3; // " / " seperator
        let load_col = max(
            "load 1/5/15m".len(),
            load_1_col + load_5_col + load_15_col + 2,
        );

        let load_1_col = if load_1_col + load_5_col + load_15_col + 2 < load_col {
            let diff = load_col - (load_1_col + load_5_col + load_15_col + 2);
            load_1_col + diff
        } else {
            load_1_col
        };

        // Print header
        println!(
            "| {: <worker_col$} | {: <os_col$} \
            | {: ^cores_col$} | {: ^freq_col$} \
            | {: ^memory_col$} | {: ^jobs_col$} \
            | {: ^load_col$} | {: ^uptime_col$} |",
            style("worker name").bold().underlined(),
            style("operating system").bold().underlined(),
            style("cpus").bold().underlined(),
            style("avg freq").bold().underlined(),
            style("memory").bold().underlined(),
            style("jobs").bold().underlined(),
            style("load 1/5/15m").bold().underlined(),
            style("uptime").bold().underlined(),
        );

        for info in worker_list {
            let worker_name = dots_back(info.name.clone(), worker_col);
            let operation_system = dots_back(info.hw.distribution.clone(), os_col);
            let cpu_cores = format_cores(info.hw.cpu_cores);
            let cpu_frequency = format_frequency(info.hw.cpu_frequency);
            let memory_mb = format_memory_mb(info.hw.total_memory);

            // jobs
            let (running_jobs, max_jobs) = if info.jobs_total() * 2 < info.max_parallel_jobs {
                (
                    style(info.jobs_total()).green(),
                    style(info.max_parallel_jobs).green(),
                )
            } else if info.jobs_total() < info.max_parallel_jobs {
                (
                    style(info.jobs_total()).yellow(),
                    style(info.max_parallel_jobs).yellow(),
                )
            } else {
                (
                    style(info.jobs_total()).red(),
                    style(info.max_parallel_jobs).red(),
                )
            };

            // loads
            let load_style = |load| {
                let load_fmt = format!("{:.1}", load);
                if load < (0.25 * info.hw.cpu_cores as f64) {
                    style(load_fmt).green()
                } else if load < (0.75 * info.hw.cpu_cores as f64) {
                    style(load_fmt).yellow()
                } else {
                    style(load_fmt).red()
                }
            };

            let load_one = load_style(info.load.one);
            let load_five = load_style(info.load.five);
            let load_fifteen = load_style(info.load.fifteen);

            let uptime = format_uptime(info.connected_since);

            // Print line
            println!(
                "| {: <worker_col$} | {: <os_col$} \
                | {: >cores_col$} | {: >freq_col$} | {: >memory_col$} \
                | {: >run_jobs_col$} / {: >max_jobs_col$} \
                | {: >load_1_col$} {: >load_5_col$} {: >load_15_col$} \
                | {: >uptime_col$} |",
                worker_name,
                operation_system,
                cpu_cores,
                cpu_frequency,
                memory_mb,
                running_jobs,
                max_jobs,
                load_one,
                load_five,
                load_fifteen,
                uptime
            );
        }
    }
}

pub fn job_info(job_info: JobInfo, stdout_text: Option<String>, stderr_text: Option<String>) {
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
            comment,
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
            println!("   comment: {}", comment);
        }
        JobStatus::Canceled { canceled } => {
            println!("job status: {}", style("canceled").yellow());
            println!("   canceled on: {}", canceled);
        }
    }

    if let Some(text) = stdout_text {
        println!("=== {} ===\n{}", style("stdout").bold(), text);
    }

    if let Some(text) = stderr_text {
        println!("=== {} ===\n{}", style("stderr").red(), text);
    }
}
