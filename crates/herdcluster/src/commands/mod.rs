//! Command handlers for the `herdcluster` CLI.
//!
//! `cli.rs` owns argv parsing only; every subcommand dispatches here.
//! Handlers compose `cluster-fleet` (fleet ops, spawn, machines view),
//! `api-client` (local herdr), `cluster-core` (config/registry), and
//! `cluster-node` (controller/worker daemons, doctor, skills, revoke,
//! autostart). Daemon commands are async — they run on a per-command
//! tokio runtime via [`block_on`].

pub mod agent;
pub mod autostart;
pub mod clean;
pub mod config;
pub mod doctor;
pub mod init;
pub mod join;
pub mod keepawake;
pub mod machines;
pub mod render;
pub mod revoke;
pub mod skills;
pub mod spawn;
pub mod status;
pub mod update;
pub mod worker;

use std::fmt::Display;
use std::future::Future;

use cluster_fleet::{FleetContext, FleetError, SpawnError};
use serde::Serialize;
use serde_json::Value;

use crate::cli::{Commands, SkillsCommands};

/// Dispatch a parsed command. Returns the process exit code:
/// 0 ok, 1 runtime error, 2 usage/not-implemented.
pub fn dispatch(command: Commands, verbose: bool) -> i32 {
    match command {
        Commands::Init {
            name,
            no_skills,
            port,
            yes,
        } => init::run(name, no_skills, port, yes, verbose),
        Commands::Join {
            controller,
            yes,
            json,
        } => join::run(controller, yes, json, verbose),
        Commands::Worker { controller } => worker::run(controller, verbose),
        Commands::Doctor {
            fix,
            yes,
            machine,
            repo,
            json,
        } => doctor::run(fix, yes, machine, repo, json, verbose),
        Commands::Config { command } => config::run(command, verbose),
        Commands::Revoke { label, json } => revoke::run(&label, json),
        Commands::Autostart { command } => autostart::run(command),
        Commands::Machines { json } => machines::run(json, verbose),
        Commands::Spawn {
            name,
            kind,
            machine,
            repo,
            branch,
            json,
            agent_args,
        } => spawn::run(
            &name, &kind, &machine, repo, branch, json, agent_args, verbose,
        ),
        Commands::Clean {
            machine,
            yes,
            dry_run,
            close_workspaces,
        } => clean::run(machine, yes, dry_run, close_workspaces, verbose, false),
        Commands::Agent { command } => agent::run(command, verbose),
        Commands::Status { all, json } => status::run(all, json, verbose),
        Commands::Keepawake { command } => keepawake::run(command, verbose),
        Commands::Update { version, yes, json } => update::run(version, yes, json, verbose),
        Commands::Skills { command } => match command {
            SkillsCommands::Install { yes, overwrite } => skills::install(yes, overwrite),
            SkillsCommands::Status => skills::status(),
        },
    }
}

/// Run `fut` on a fresh multi-thread tokio runtime. Daemon commands are
/// one-shot processes, so a per-command runtime is simpler than threading
/// one through `dispatch` (which is sync by design for the CLI surface).
///
/// Returns `Err(exit_code)` when the runtime itself cannot start.
pub(crate) fn block_on<F: Future>(fut: F) -> Result<F::Output, i32> {
    match tokio::runtime::Runtime::new() {
        Ok(rt) => Ok(rt.block_on(fut)),
        Err(e) => {
            eprintln!("error: failed to start async runtime: {e}");
            Err(1)
        }
    }
}

/// Print a `cluster-node` service error and return exit code 1.
///
/// `code` is the stable snake_case tag used inside the `--json` error
/// envelope (cluster-node errors don't carry machine-readable codes).
pub(crate) fn report_node_error(code: &str, e: &impl Display, json: bool) -> i32 {
    if json {
        let envelope = serde_json::json!({
            "error": { "code": code, "message": e.to_string() }
        });
        eprintln!(
            "{}",
            serde_json::to_string(&envelope).unwrap_or_else(|_| envelope.to_string())
        );
    } else {
        eprintln!("error: {e}");
    }
    1
}

/// `--verbose` diagnostics on stderr (no logging dep — keep it simple).
pub(crate) fn vlog(verbose: bool, msg: impl Display) {
    if verbose {
        eprintln!("[herdcluster] {msg}");
    }
}

/// Load the fleet context, reporting config errors in the requested
/// output mode. Returns `Err(exit_code)` on failure.
///
/// `ensure_layout` runs first: `FleetContext::load` persists a fresh
/// `cluster.json` on first use, which fails if the state dir itself does
/// not exist yet (fresh install before `init` lands).
pub(crate) fn load_ctx(verbose: bool, json: bool) -> Result<FleetContext, i32> {
    vlog(
        verbose,
        "loading herdcluster state (cluster.json + workers.json)",
    );
    if let Err(e) = cluster_core::config::ensure_layout() {
        return Err(report_fleet_error(
            &FleetError::Config {
                message: e.to_string(),
            },
            json,
        ));
    }
    FleetContext::load().map_err(|e| report_fleet_error(&e, json))
}

/// Print a [`FleetError`] and return exit code 1.
pub(crate) fn report_fleet_error(e: &FleetError, json: bool) -> i32 {
    if json {
        // fleet provides the canonical {"error":{code,message}} envelope.
        eprintln!(
            "{}",
            serde_json::to_string(&e.to_json()).unwrap_or_else(|_| e.to_string())
        );
    } else {
        eprintln!("error: {e}");
    }
    1
}

/// Print a [`SpawnError`] and return exit code 1.
pub(crate) fn report_spawn_error(e: &SpawnError, json: bool) -> i32 {
    if json {
        eprintln!(
            "{}",
            serde_json::to_string(&e.to_json()).unwrap_or_else(|_| e.to_string())
        );
    } else {
        eprintln!("error: {e}");
    }
    1
}

/// Print a successful `serde_json::Value` result: pretty JSON in `--json`
/// mode, best-effort human text otherwise.
pub(crate) fn emit_result(value: &Value, json: bool) {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
        );
    } else {
        println!("{}", display_value(value));
    }
}

/// Best-effort human rendering of an API result value: strings pass
/// through, string arrays join with newlines, and objects contribute the
/// first text-ish field found (`content`, `text`, `output`, `screen` —
/// searched a few levels deep to unwrap `{"read": {"text": …}}`-style
/// envelopes). Everything else pretty-prints as JSON.
pub(crate) fn display_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Array(items) if items.iter().all(Value::is_string) => items
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("\n"),
        _ => find_text_field(value, &["content", "text", "output", "screen"], 4).unwrap_or_else(
            || serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string()),
        ),
    }
}

/// Depth-limited search for a non-empty string under `keys`, checking
/// each object level before recursing into children (mirrors the
/// defensive style of `cluster-fleet`'s response parsing).
fn find_text_field(value: &Value, keys: &[&str], depth: usize) -> Option<String> {
    if depth == 0 {
        return None;
    }
    if let Value::Object(map) = value {
        for key in keys {
            if let Some(s) = map.get(*key).and_then(Value::as_str) {
                if !s.is_empty() {
                    return Some(s.to_string());
                }
            }
        }
        for child in map.values() {
            if let Some(found) = find_text_field(child, keys, depth - 1) {
                return Some(found);
            }
        }
    } else if let Value::Array(items) = value {
        for child in items {
            if let Some(found) = find_text_field(child, keys, depth - 1) {
                return Some(found);
            }
        }
    }
    None
}

/// Serialize `value` to stdout as pretty JSON. Serialization of a
/// `Serialize` value into `Value`/string cannot realistically fail; the
/// fallback keeps a static message rather than panicking.
pub(crate) fn print_json<T: Serialize>(value: &T) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| "null".to_string())
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn display_value_extracts_text_fields() {
        assert_eq!(display_value(&json!("hi")), "hi");
        assert_eq!(
            display_value(&json!({"content": "line1\nline2"})),
            "line1\nline2"
        );
        // Text fields are found through wrapper objects (agent.read shape).
        assert_eq!(
            display_value(&json!({"read": {"text": "pane contents"}, "type": "x"})),
            "pane contents"
        );
        assert_eq!(display_value(&json!({"pane": {"output": "abc"}})), "abc");
        // No text fields → JSON fallback.
        assert!(display_value(&json!({"agent": {"status": "idle"}})).contains("status"));
        // Array of strings joins with newlines.
        assert_eq!(display_value(&json!(["a", "b"])), "a\nb");
        // Mixed array falls back to JSON.
        assert!(display_value(&json!(["a", 1])).contains('['));
    }

    #[test]
    fn fleet_error_json_envelope_shape() {
        let e = FleetError::UnknownMachine {
            label: "ghost".into(),
        };
        let v = e.to_json();
        assert_eq!(v["error"]["code"], "unknown_machine");
        assert!(v["error"]["message"].is_string());
    }
}
