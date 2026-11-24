//! `herdcluster config` — read/edit `cluster.json` machine overrides.
//!
//! Thin wrapper over [`cluster_node::cluster_config`]: the service owns
//! key parsing, validation ([`cluster_core::registry::MachineOverride::
//! problems`]), and persistence; this module only renders output and
//! maps errors to exit codes.

use cluster_node::cluster_config::{self, ConfigView};

use crate::cli::ConfigCommands;

use super::render::render_table;
use super::{print_json, report_node_error, vlog};

/// Dispatch `herdcluster config <get|set|unset>`.
pub fn run(command: ConfigCommands, verbose: bool) -> i32 {
    match command {
        ConfigCommands::Get { json } => get(json, verbose),
        ConfigCommands::Set { key, value, json } => set(&key, &value, json, verbose),
        ConfigCommands::Unset { key, json } => unset(&key, json, verbose),
    }
}

fn get(json: bool, verbose: bool) -> i32 {
    vlog(verbose, "loading cluster config");
    match cluster_config::get() {
        Ok(view) => {
            if json {
                print_json(&view);
            } else {
                print_view(&view);
            }
            0
        }
        Err(e) => report_node_error("config_error", &e, json),
    }
}

fn set(key: &str, value: &str, json: bool, verbose: bool) -> i32 {
    vlog(verbose, "setting config {key} = {value}");
    match cluster_config::set(key, value) {
        Ok(()) => {
            if json {
                print_json(&serde_json::json!({ "ok": true, "key": key, "value": value }));
            } else {
                println!("set {key} = {value}");
            }
            0
        }
        Err(e) => report_node_error("config_error", &e, json),
    }
}

fn unset(key: &str, json: bool, verbose: bool) -> i32 {
    vlog(verbose, "unsetting config {key}");
    match cluster_config::unset(key) {
        Ok(()) => {
            if json {
                print_json(&serde_json::json!({ "ok": true, "key": key }));
            } else {
                println!("unset {key}");
            }
            0
        }
        Err(e) => report_node_error("config_error", &e, json),
    }
}

fn print_view(view: &ConfigView) {
    println!("cluster: {} ({})", view.name, view.cluster_id);
    println!("port: {}  protocol v{}", view.port, view.protocol_version);
    if view.machines.is_empty() {
        println!("machine overrides: none");
        return;
    }
    let rows: Vec<Vec<String>> = view
        .machines
        .iter()
        .map(|(label, o)| {
            vec![
                label.clone(),
                o.max_agents
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "-".into()),
                o.kinds_override
                    .as_ref()
                    .map(|k| k.join(","))
                    .unwrap_or_else(|| "-".into()),
            ]
        })
        .collect();
    println!("machine overrides:");
    println!(
        "{}",
        render_table(&["LABEL", "MAX_AGENTS", "KINDS_OVERRIDE"], &rows)
    );
}
