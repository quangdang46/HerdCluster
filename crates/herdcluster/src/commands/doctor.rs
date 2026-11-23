//! `herdcluster doctor` — cluster health checks with optional fixes.
//!
//! Thin wrapper over [`cluster_node::doctor::run_doctor`]: the service
//! owns the check suite (config, discovery, TLS, pairing, per-kind CLIs,
//! resources, orchestration skill — PLAN.md §3.9); this module only
//! renders the [`DoctorReport`] and maps `ok` to the exit code.

use cluster_node::doctor::{run_doctor, CheckStatus, DoctorOptions, DoctorReport};

use super::render::render_table;
use super::{block_on, print_json, vlog};

const HEADERS: &[&str] = &["CHECK", "STATUS", "DETAIL", "FIX"];

/// `herdcluster doctor [--fix] [--yes] [--machine <label>] [--json]`
pub fn run(fix: bool, yes: bool, machine: Option<String>, json: bool, verbose: bool) -> i32 {
    let opts = DoctorOptions {
        machine,
        fix,
        yes,
        json,
    };

    vlog(verbose, "running doctor check suite");
    let fut = async move { run_doctor(opts).await };

    let report = match block_on(fut) {
        Ok(Ok(report)) => report,
        Ok(Err(e)) => {
            eprintln!("error: doctor failed: {e}");
            return 1;
        }
        Err(code) => return code,
    };

    if json {
        print_json(&report.to_json());
    } else {
        print_human(&report);
    }

    if report.ok {
        0
    } else {
        1
    }
}

fn print_human(report: &DoctorReport) {
    let rows: Vec<Vec<String>> = report
        .checks
        .iter()
        .map(|c| {
            vec![
                c.name.clone(),
                status_label(&c.status).to_string(),
                c.detail.clone(),
                c.fix_hint.clone().unwrap_or_default(),
            ]
        })
        .collect();
    println!("{}", render_table(HEADERS, &rows));
    if !report.fixed.is_empty() {
        println!("fixed: {}", report.fixed.join(", "));
    }
    println!(
        "{}",
        if report.ok {
            "doctor: ok"
        } else {
            "doctor: problems found"
        }
    );
}

fn status_label(status: &CheckStatus) -> &'static str {
    match status {
        CheckStatus::Pass => "PASS",
        CheckStatus::Warn => "WARN",
        CheckStatus::Fail => "FAIL",
    }
}
