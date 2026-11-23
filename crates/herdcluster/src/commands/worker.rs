//! `herdcluster worker` — run the worker daemon without re-pairing.
//!
//! Loads the stored pairing credential + `worker.json` controller
//! address (or a `--controller` override) and calls
//! [`cluster_node::worker::run_daemon`]. Unlike `join`, this never
//! touches discovery or the pairing FSM — it is the "already paired,
//! just run" path used by autostart and manual restarts.

use std::net::SocketAddr;
use std::path::PathBuf;

use cluster_node::worker::{self, WorkerDaemonConfig};

use super::{block_on, report_node_error, vlog};

/// `<state_dir>/worker_credential.json` — must exist before the daemon
/// can do anything useful (mirrors `cluster-net`'s credential path).
const CREDENTIAL_FILE: &str = "worker_credential.json";

/// `herdcluster worker [--controller <addr>]`
pub fn run(controller: Option<String>, verbose: bool) -> i32 {
    let state_dir = match cluster_core::config::state_dir() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: cannot resolve herdcluster state dir: {e}");
            return 1;
        }
    };

    // Fail fast before run_daemon gets as far as installing/starting the
    // herdr runtime: without a pairing credential the daemon can never
    // connect, so don't spawn anything.
    let cred_path: PathBuf = state_dir.join(CREDENTIAL_FILE);
    if !cred_path.exists() {
        eprintln!(
            "error: worker is not paired (missing credential at {}); \
             run `herdcluster join` first",
            cred_path.display()
        );
        return 1;
    }

    vlog(verbose, "starting worker daemon");
    let fut = async move {
        let mut cfg = WorkerDaemonConfig::new(state_dir);
        if let Some(raw) = controller {
            match resolve_controller_addr(&raw).await {
                Ok(addr) => cfg.controller = Some(addr),
                Err(msg) => {
                    eprintln!("error: invalid --controller '{raw}': {msg}");
                    return 1;
                }
            }
        }
        match worker::run_daemon(cfg).await {
            Ok(()) => 0,
            Err(e) => report_node_error("worker_daemon_failed", &e, false),
        }
    };

    block_on(fut).unwrap_or(1)
}

/// Parse `--controller`: a literal `SocketAddr` (`1.2.3.4:47820`) or a
/// `host:port` pair resolved via the system resolver.
async fn resolve_controller_addr(raw: &str) -> Result<SocketAddr, String> {
    if let Ok(addr) = raw.parse::<SocketAddr>() {
        return Ok(addr);
    }
    let (host, port) = raw
        .rsplit_once(':')
        .ok_or_else(|| "expected ip:port or host:port".to_string())?;
    let port: u16 = port.parse().map_err(|_| format!("invalid port '{port}'"))?;
    let mut addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| format!("cannot resolve '{host}': {e}"))?;
    addrs
        .next()
        .ok_or_else(|| format!("no address found for '{host}'"))
}
