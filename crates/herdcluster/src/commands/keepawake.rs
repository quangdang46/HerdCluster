//! `herdcluster keepawake` — inspect the keep-awake sleep-inhibition
//! feature (`cluster_node::keepawake`).
//!
//! The actual assertion lives inside the long-running controller/worker
//! daemons, so `status` is a one-shot report: the `cluster.json` flag,
//! platform backend availability, and a single live `agent.list` probe
//! of how many local agents are currently `working`.

use cluster_node::keepawake::{self, StatusReport};

use crate::cli::KeepawakeCommands;

use super::{print_json, vlog};

/// `herdcluster keepawake <subcommand>`
pub fn run(command: KeepawakeCommands, verbose: bool) -> i32 {
    match command {
        KeepawakeCommands::Status { json } => status(json, verbose),
    }
}

fn status(json: bool, verbose: bool) -> i32 {
    vlog(
        verbose,
        "probing keep-awake config, backend, and local agent.list",
    );
    let report = keepawake::report_status();
    if json {
        print_json(&report);
        return 0;
    }
    print_report(&report);
    0
}

fn print_report(r: &StatusReport) {
    println!(
        "keep-awake: {}",
        if r.enabled { "enabled" } else { "disabled" }
    );
    println!(
        "  config: cluster.json `keepawake` (toggle: `herdcluster config set keepawake <true|false>`)"
    );
    println!(
        "  backend: {} ({})",
        r.backend,
        if r.backend_supported {
            "available"
        } else {
            "unavailable"
        }
    );
    match r.working_agents {
        Some(n) => println!("  local agents working: {n}"),
        None => println!(
            "  local agent probe: unavailable ({})",
            r.probe_error.as_deref().unwrap_or("unknown error")
        ),
    }
    println!("  note: the live assertion is held by the running controller/worker daemon");
}
