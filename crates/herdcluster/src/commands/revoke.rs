//! `herdcluster revoke <label>` — remove a worker's pairing.
//!
//! Thin wrapper over [`cluster_node::revoke::revoke_worker`]: drops the
//! registry entry, the issued credential, and disconnects the worker if
//! it is currently live.

use cluster_node::revoke::revoke_worker;
use serde_json::json;

use super::{print_json, render::render_table};

/// `herdcluster revoke <label> [--json]`
pub fn run(label: &str, json_out: bool) -> i32 {
    match revoke_worker(label) {
        Ok(outcome) => {
            if json_out {
                print_json(&json!({
                    "label": label,
                    "removed_registry": outcome.removed_registry,
                    "removed_credential": outcome.removed_credential,
                    "disconnected_live": outcome.disconnected_live,
                }));
            } else {
                let rows = vec![vec![
                    label.to_string(),
                    bool_cell(outcome.removed_registry),
                    bool_cell(outcome.removed_credential),
                    bool_cell(outcome.disconnected_live),
                ]];
                println!(
                    "{}",
                    render_table(&["WORKER", "REGISTRY", "CREDENTIAL", "DISCONNECTED"], &rows)
                );
            }
            0
        }
        Err(e) => {
            if json_out {
                let envelope = json!({
                    "error": { "code": "revoke_failed", "message": e.to_string() }
                });
                eprintln!("{}", envelope);
            } else {
                eprintln!("error: revoke failed: {e}");
            }
            1
        }
    }
}

fn bool_cell(v: bool) -> String {
    if v {
        "yes".into()
    } else {
        "no".into()
    }
}
