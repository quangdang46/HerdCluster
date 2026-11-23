//! `herdcluster join` — pair this machine as a worker, then run the
//! worker daemon.
//!
//! Wraps [`cluster_node::worker::join`] for the pairing half; on
//! [`JoinOutcome::Paired`] it falls straight into
//! [`cluster_node::worker::run_daemon`] — `join` IS the daemon starter.
//! Pairing-only reconnects use `herdcluster worker` instead.

use cluster_node::worker::{self, JoinConfig, JoinOutcome, WorkerDaemonConfig};

use super::{block_on, print_json, report_node_error, vlog};
use serde_json::json;

/// `herdcluster join [--controller <hint>] [--yes] [--json]`
pub fn run(controller: Option<String>, yes: bool, json_out: bool, verbose: bool) -> i32 {
    let cfg = JoinConfig {
        controller,
        yes,
        json: json_out,
    };

    vlog(verbose, "running worker pairing flow");
    let fut = async move {
        match worker::join(cfg).await {
            Ok(JoinOutcome::Paired {
                worker_label,
                cluster_name,
            }) => {
                if json_out {
                    print_json(&json!({
                        "paired": true,
                        "worker_label": worker_label,
                        "cluster_name": cluster_name,
                    }));
                } else {
                    println!("paired as worker '{worker_label}' in cluster '{cluster_name}'");
                    println!("starting worker daemon (Ctrl-C to stop)…");
                }

                let state_dir = match cluster_core::config::state_dir() {
                    Ok(d) => d,
                    Err(e) => {
                        eprintln!("error: cannot resolve herdcluster state dir: {e}");
                        return 1;
                    }
                };
                match worker::run_daemon(WorkerDaemonConfig::new(state_dir)).await {
                    Ok(()) => 0,
                    Err(e) => report_node_error("worker_daemon_failed", &e, json_out),
                }
            }
            Ok(JoinOutcome::Declined) => {
                if !json_out {
                    println!("pairing declined");
                }
                1
            }
            Ok(JoinOutcome::Rejected { reason }) => {
                report_node_error("pairing_rejected", &reason, json_out)
            }
            Err(e) => report_node_error("join_failed", &e, json_out),
        }
    };

    block_on(fut).unwrap_or(1)
}
