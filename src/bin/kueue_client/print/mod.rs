pub mod format;

use chrono::{DateTime, Utc};
use console::{style, StyledObject};
use kueue_lib::structs::{JobInfo, JobStatus, WorkerInfo};
use std::{cmp::max, collections::BTreeSet};

fn format_cpu_cores(cpu_cores: u64) -> String {
    format!("{} x", cpu_cores)
}

fn format_memory_mb(memory_mb: u64) -> String {
    format!("{} MB", memory_mb)
}

fn format_worker(job_status: &JobStatus) -> String {
    match job_status {
        JobStatus::Offered { worker, .. } => worker.clone(),
        JobStatus::Running { worker, .. } => worker.clone(),
        JobStatus::Finished { worker, .. } => worker.clone(),
        _ => "---".to_string(),
    }
}

fn format_status(job_status: &JobStatus) -> String {
    match job_status {
        JobStatus::Pending { issued } => {
            format!("pending since {}", format::date(issued))
        }
        JobStatus::Offered { offered, .. } => {
            format!("offered on {}", format::date(offered))
        }
        JobStatus::Running { started, .. } => {
            format!("running for {}", format::elapsed_since(started))
        }
        JobStatus::Finished {
            return_code,
            run_time_seconds,
            ..
        } => {
            if *return_code == 0 {
                format!(
                    "finished after {}",
                    format::elapsed_seconds(*run_time_seconds)
                )
            } else {
                format!("failed with code {}", return_code)
            }
        }
        JobStatus::Canceled { canceled, .. } => {
            format!("canceled on {}", format::date(canceled))
        }
    }
}

/// Print jobs to screen.
#[allow(clippy::too_many_arguments)]
pub fn job_list(
    job_infos: Vec<JobInfo>,
    jobs_pending: u64,
    jobs_offered: u64,
    jobs_running: u64,
    jobs_succeeded: u64,
    jobs_failed: u64,
    jobs_canceled: u64,
    job_avg_run_time_seconds: i64,
    remaining_jobs_eta_seconds: i64,
) {
    let mut footer_width = format::term_size().0;

    if !job_infos.is_empty() {
        // Get maximum column widths for space calculation.
        let max_id_col_width = format!("{}", job_infos.last().unwrap().job_id).len();
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
        let max_cores_col_width = job_infos
            .iter()
            .map(|job_info| format_cpu_cores(job_info.local_resources.cpus).len())
            .max()
            .unwrap();
        let max_memory_col_width = job_infos
            .iter()
            .map(|job_info| format_memory_mb(job_info.local_resources.ram_mb).len())
            .max()
            .unwrap();
        let max_worker_col_width = job_infos
            .iter()
            .map(|job_info| format_worker(&job_info.status).len())
            .max()
            .unwrap();
        let max_status_col_width = job_infos
            .iter()
            .map(|job_info| format_status(&job_info.status).len())
            .max()
            .unwrap();

        let min_col_widths = vec![
            max("id".len(), max_id_col_width),
            "working dir".len(),
            "command".len(),
            max("cpus".len(), max_cores_col_width),
            max("memory".len(), max_memory_col_width),
            "worker".len(),
            "status".len(),
        ];
        let max_col_widths = vec![
            0, // job id
            max_cwd_col_width,
            max_cmd_col_width,
            0, // cpu cores
            0, // memory
            max_worker_col_width,
            max_status_col_width,
        ];

        let col_widths = format::col_widths(min_col_widths, max_col_widths);
        let (id_col, cwd_col, cmd_col, cores_col, memory_col, worker_col, status_col) = (
            col_widths[0],
            col_widths[1],
            col_widths[2],
            col_widths[3],
            col_widths[4],
            col_widths[5],
            col_widths[6],
        );

        footer_width = col_widths.iter().sum::<usize>() + 3 * col_widths.len() + 1;

        // Print header
        println!(
            "| {: ^id_col$} | {: <cwd_col$} | {: <cmd_col$} | {: ^cores_col$} | {: ^memory_col$} | {: <worker_col$} | {: <status_col$} |",
            style("id").bold().underlined(),
            style("working dir").bold().underlined(),
            style("command").bold().underlined(),
            style("cpus").bold().underlined(),
            style("memory").bold().underlined(),
            style("worker").bold().underlined(),
            style("status").bold().underlined(),
        );

        for job_info in job_infos {
            // working dir
            let working_dir = job_info.cwd.to_string_lossy();
            let working_dir = format::dots_front(working_dir.to_string(), cwd_col);

            // command
            let command = job_info.cmd.join(" ");
            let command = format::dots_back(command, cmd_col);

            let cpu_cores = format_cpu_cores(job_info.local_resources.cpus);
            let memory_mb = format_memory_mb(job_info.local_resources.ram_mb);

            // worker
            let worker = format_worker(&job_info.status);
            let worker = format::dots_back(worker, worker_col);

            // status
            let status = format_status(&job_info.status);
            let status = format::dots_back(status, status_col);
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
                "| {: >id_col$} | {: <cwd_col$} | {: <cmd_col$} | {: >cores_col$} | {: >memory_col$} | {: <worker_col$} | {: <status_col$} |",
                job_info.job_id, working_dir, command, cpu_cores, memory_mb, worker, status
            );
        }
    }

    // Print bottom lines with overview information.
    // First line.
    let summary_heading = "--- job status summary ---";
    let avg_run_time = format!(
        "average job runtime: {}",
        format::elapsed_seconds(job_avg_run_time_seconds)
    );

    if footer_width > summary_heading.len() + avg_run_time.len() {
        let spaces = footer_width - summary_heading.len() - avg_run_time.len();
        println!(
            "{}{: <spaces$}{}",
            style(summary_heading).bold(),
            "",
            avg_run_time
        );
    } else {
        println!("{} {}", style(summary_heading).bold(), avg_run_time);
    }

    // Second line.
    print!("pending: {}", jobs_pending);
    let mut printed = format!("pending: {}", jobs_pending).len();
    if jobs_offered > 0 {
        print!(", offered: {}", style(jobs_offered).dim());
        printed += format!(", offered: {}", jobs_offered).len();
    }
    if jobs_running > 0 {
        print!(", running: {}", style(jobs_running).blue());
        printed += format!(", running: {}", jobs_running).len();
    }
    if jobs_succeeded > 0 {
        print!(", succeeded: {}", style(jobs_succeeded).green());
        printed += format!(", succeeded: {}", jobs_succeeded).len();
    }
    if jobs_failed > 0 {
        print!(", failed: {}", style(jobs_failed).red());
        printed += format!(", failed: {}", jobs_failed).len();
    }
    if jobs_canceled > 0 {
        print!(", canceled: {}", style(jobs_canceled).yellow());
        printed += format!(", canceled: {}", jobs_canceled).len();
    }

    let remaining_jobs_eta = format!(
        "remaining jobs ETA: {}",
        format::elapsed_seconds(remaining_jobs_eta_seconds)
    );

    if footer_width > printed + remaining_jobs_eta.len() {
        let spaces = footer_width - printed - remaining_jobs_eta.len();
        println!("{: <spaces$}{}", "", remaining_jobs_eta);
    } else {
        println!(" {}", remaining_jobs_eta);
    }
}

pub fn job_info(job_info: JobInfo, stdout_text: Option<String>, stderr_text: Option<String>) {
    println!("=== {} ===", style("job information").bold().underlined());
    println!("job id: {}", job_info.job_id);
    println!("command: {}", job_info.cmd.join(" "));
    println!("working directory: {}", job_info.cwd.to_string_lossy());
    println!("required job slots: {}", job_info.local_resources.job_slots);
    println!("required CPU cores: {}", job_info.local_resources.cpus);
    println!("required RAM: {} megabytes", job_info.local_resources.ram_mb);
    println!(); // line break

    if let Some(global_resources) = job_info.global_resources {
        println!("{}", style("additional requirements:").bold());
        for (resource, amount) in global_resources {
            println!("   {amount}x {resource}");
        }
        println!(); // line break
    }

    match &job_info.status {
        JobStatus::Pending { issued } => {
            println!("{}: pending", style("job status").bold());
            println!("   issued on: {}", format::date(issued));
        }
        JobStatus::Offered {
            issued,
            offered,
            worker,
        } => {
            println!("{}: {}", style("job status").bold(), style("pending").dim());
            println!("   issued on: {}", format::date(issued));
            println!("   offered on: {}", offered);
            println!("   offered to: {}", worker);
        }
        JobStatus::Running {
            issued,
            started,
            worker,
        } => {
            println!(
                "{}: {}",
                style("job status").bold(),
                style("running").blue()
            );
            println!("   issued on: {}", format::date(issued));
            println!("   started on: {}", format::date(started));
            println!("   running on: {}", worker);
        }
        JobStatus::Finished {
            issued,
            started,
            finished,
            return_code,
            worker,
            run_time_seconds,
            comment,
        } => {
            if *return_code == 0 {
                println!(
                    "{}: {}",
                    style("job status").bold(),
                    style("finished").green()
                );
            } else {
                println!("{}: {}", style("job status").bold(), style("failed").red());
            }
            println!("   issued on: {}", format::date(issued));
            println!("   started on: {}", format::date(started));
            println!("   finished on: {}", format::date(finished));
            println!("   return code: {}", return_code);
            println!("   executed on: {}", worker);
            println!("   runtime: {}", format::elapsed_seconds(*run_time_seconds));
            println!("   comment: {}", comment);
        }
        JobStatus::Canceled { issued, canceled } => {
            println!(
                "{}: {}",
                style("job status").bold(),
                style("canceled").yellow()
            );
            println!("   issued on: {}", format::date(issued));
            println!("   canceled on: {}", format::date(canceled));
        }
    }

    if let Some(text) = stdout_text {
        println!("\n=== {} ===\n{}", style("stdout").bold(), text);
    }

    if let Some(text) = stderr_text {
        println!("\n=== {} ===\n{}", style("stderr").red(), text);
    }
}

fn format_frequency(cpu_frequency: u64) -> String {
    format!("{} MHz", cpu_frequency)
}

fn format_jobs(jobs_offered: &BTreeSet<u64>, jobs_running: &BTreeSet<u64>) -> String {
    let jobs: Vec<String> = jobs_running
        .iter()
        .chain(jobs_offered.iter())
        .map(|job_id| format!("{job_id}"))
        .collect();
    if jobs.is_empty() {
        String::from("---")
    } else {
        jobs.join(", ")
    }
}

fn format_resource_load(load: f64, decimal: usize) -> StyledObject<String> {
    let load_fmt = format!("{:.decimal$} %", load * 100.0);
    match load {
        x if x < 0.25 => style(load_fmt).green(),
        x if x < 0.75 => style(load_fmt).yellow(),
        _ => style(load_fmt).red(), // else
    }
}

fn format_cpu_load(load: f64, cpu_cores: u64) -> StyledObject<String> {
    let load_fmt = format!("{:.1}", load);
    if load < (0.25 * cpu_cores as f64) {
        style(load_fmt).green()
    } else if load < (0.75 * cpu_cores as f64) {
        style(load_fmt).yellow()
    } else {
        style(load_fmt).red()
    }
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
        // Get maximum column widths for space calculation.
        let max_id_col_width = format!("{}", worker_list.last().unwrap().worker_id).len();
        let max_worker_col_width = worker_list
            .iter()
            .map(|info| info.worker_name.len())
            .max()
            .unwrap();
        let max_os_col_width = worker_list
            .iter()
            .map(|info| info.system_info.distribution.len())
            .max()
            .unwrap();
        let max_cores_col_width = worker_list
            .iter()
            .map(|info| format_cpu_cores(info.system_info.cpu_cores).len())
            .max()
            .unwrap();
        let max_freq_col_width = worker_list
            .iter()
            .map(|info| format_frequency(info.system_info.cpu_frequency).len())
            .max()
            .unwrap();
        let max_memory_col_width = worker_list
            .iter()
            .map(|info| format_memory_mb(info.system_info.total_ram_mb).len())
            .max()
            .unwrap();
        let max_jobs_col_width = worker_list
            .iter()
            .map(|info| format_jobs(&info.jobs_offered, &info.jobs_running).len())
            .max()
            .unwrap();
        let max_busy_col_width = worker_list
            .iter()
            .map(|info| format!("{:.0} %", info.resource_load() * 100.0).len())
            .max()
            .unwrap();
        let max_load_1_col_width = worker_list
            .iter()
            .map(|info| format!("{:.1}", info.system_info.load_info.one).len())
            .max()
            .unwrap();
        let max_load_5_col_width = worker_list
            .iter()
            .map(|info| format!("{:.1}", info.system_info.load_info.five).len())
            .max()
            .unwrap();
        let max_load_15_col_width = worker_list
            .iter()
            .map(|info| format!("{:.1}", info.system_info.load_info.fifteen).len())
            .max()
            .unwrap();
        let max_uptime_col_width = worker_list
            .iter()
            .map(|info| format_uptime(info.connected_since).len())
            .max()
            .unwrap();

        let min_col_widths = vec![
            max("id".len(), max_id_col_width),
            "worker name".len(),
            "operating system".len(),
            max("cpus".len(), max_cores_col_width),
            max("avg freq".len(), max_freq_col_width),
            max("memory".len(), max_memory_col_width),
            "jobs".len(),
            max("busy".len(), max_busy_col_width),
            max_load_1_col_width,
            max_load_5_col_width,
            max_load_15_col_width,
            max("uptime".len(), max_uptime_col_width),
        ];
        let max_col_widths = vec![
            0, // id
            max_worker_col_width,
            max_os_col_width,
            0, // cpu cores
            0, // cpu frequency
            0, // memory
            max_jobs_col_width,
            0, // busy
            0, // load 1
            0, // load 5
            0, // load 15
            0, // uptime
        ];

        let col_widths = format::col_widths(min_col_widths, max_col_widths);
        let (
            id_col,
            worker_col,
            os_col,
            cores_col,
            freq_col,
            memory_col,
            jobs_col,
            busy_col,
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
            col_widths[11],
        );

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
            "| {: ^id_col$} | {: <worker_col$} | {: <os_col$} \
            | {: ^cores_col$} | {: ^freq_col$} | {: ^memory_col$} \
            | {: ^jobs_col$} | {: ^busy_col$} | {: ^load_col$} \
            | {: ^uptime_col$} |",
            style("id").bold().underlined(),
            style("worker name").bold().underlined(),
            style("operating system").bold().underlined(),
            style("cpus").bold().underlined(),
            style("avg freq").bold().underlined(),
            style("memory").bold().underlined(),
            style("jobs").bold().underlined(),
            style("busy").bold().underlined(),
            style("load 1/5/15m").bold().underlined(),
            style("uptime").bold().underlined(),
        );

        for info in worker_list {
            let worker_name = format::dots_back(info.worker_name.clone(), worker_col);
            let operation_system = format::dots_back(info.system_info.distribution.clone(), os_col);
            let cpu_cores = format_cpu_cores(info.system_info.cpu_cores);
            let cpu_frequency = format_frequency(info.system_info.cpu_frequency);
            let memory_mb = format_memory_mb(info.system_info.total_ram_mb);

            let jobs = format::dots_back(
                format_jobs(&info.jobs_offered, &info.jobs_running),
                jobs_col,
            );

            let busy = format_resource_load(info.resource_load(), 0);

            let load_one =
                format_cpu_load(info.system_info.load_info.one, info.system_info.cpu_cores);
            let load_five =
                format_cpu_load(info.system_info.load_info.five, info.system_info.cpu_cores);
            let load_fifteen = format_cpu_load(
                info.system_info.load_info.fifteen,
                info.system_info.cpu_cores,
            );

            let uptime = format_uptime(info.connected_since);

            // Print line
            println!(
                "| {: >id_col$} | {: <worker_col$} | {: <os_col$} \
                | {: >cores_col$} | {: >freq_col$} | {: >memory_col$} \
                | {: <jobs_col$} | {: >busy_col$} \
                | {: >load_1_col$} {: >load_5_col$} {: >load_15_col$} \
                | {: >uptime_col$} |",
                info.worker_id,
                worker_name,
                operation_system,
                cpu_cores,
                cpu_frequency,
                memory_mb,
                jobs,
                busy,
                load_one,
                load_five,
                load_fifteen,
                uptime
            );
        }
    }
}

pub fn worker_info(worker_info: WorkerInfo) {
    println!(
        "=== {} ===",
        style("worker information").bold().underlined()
    );
    println!("worker id: {}", worker_info.worker_id);
    println!("name: {}", worker_info.worker_name);
    println!(
        "connected since: {}",
        format::date(&worker_info.connected_since)
    );
    println!(); // line break

    println!("{}", style("system information").bold().underlined());
    println!("   kernel: {}", worker_info.system_info.kernel);
    println!("   distribution: {}", worker_info.system_info.distribution);
    println!("   cpu cores: {}", worker_info.system_info.cpu_cores);
    println!(
        "   cpu frequency: {} megahertz",
        worker_info.system_info.cpu_frequency
    );
    println!(
        "   total memory: {} megabytes",
        worker_info.system_info.total_ram_mb
    );
    println!("last updated: {}", format::date(&worker_info.last_updated));
    println!(); // line break

    println!("{}", style("system load and resources").bold().underlined());
    let load_one = format_cpu_load(
        worker_info.system_info.load_info.one,
        worker_info.system_info.cpu_cores,
    );
    let load_five = format_cpu_load(
        worker_info.system_info.load_info.five,
        worker_info.system_info.cpu_cores,
    );
    let load_fifteen = format_cpu_load(
        worker_info.system_info.load_info.fifteen,
        worker_info.system_info.cpu_cores,
    );
    println!("   load: {} / {} / {}", load_one, load_five, load_fifteen);

    let jobs_offered: Vec<String> = worker_info
        .jobs_offered
        .iter()
        .map(|job_id| format!("{}", job_id))
        .collect();
    let jobs_offered = if jobs_offered.is_empty() {
        String::from("---")
    } else {
        jobs_offered.join(", ")
    };
    println!("   jobs offered: {}", jobs_offered);

    let jobs_running: Vec<String> = worker_info
        .jobs_running
        .iter()
        .map(|job_id| format!("{}", job_id))
        .collect();
    let jobs_running = if jobs_running.is_empty() {
        String::from("---")
    } else {
        jobs_running.join(", ")
    };
    println!("   jobs running: {}", jobs_running);

    println!(
        "   free jobs slots: {}",
        worker_info.free_resources.job_slots
    );
    println!(
        "   free cpus: {} / {}",
        worker_info.free_resources.cpus, worker_info.system_info.cpu_cores
    );
    println!(
        "   free ram: {} / {} megabytes",
        worker_info.free_resources.ram_mb, worker_info.system_info.total_ram_mb
    );

    println!(
        "{}: {}",
        style("total occupation").bold(),
        format_resource_load(worker_info.resource_load(), 2)
    );
}
