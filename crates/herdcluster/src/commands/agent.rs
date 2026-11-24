//! `herdcluster agent <sub>` — agent operations across the cluster.
//!
//! `list` fans out to every machine (`--all`) or queries only the local
//! herdr (`agent list` without `--all`, addressed as `local/<name>`).
//! `get`/`read`/`prompt`/`send-keys`/`rename` take a global agent
//! ID (`{machine}/{agent}`) and forward through `cluster-fleet` ops.
//! `wait`/`watch` use herdr's event APIs (`agent.wait` / `events.subscribe`)
//! fleet-aware via [`cluster_fleet::agent_wait_live`] /
//! [`cluster_fleet::agent_watch_live`], falling back to `agent.get`
//! polling on endpoints that can't express them.

use api_client::methods::ReadSource;
use cluster_fleet::{
    agent_get, agent_prompt, agent_read, agent_rename, agent_send_keys, agent_wait_live,
    agent_watch_live, format_global_id, list_all_agents, parse_duration, parse_global_id,
    parse_until, AgentListRow, FleetContext, FleetError, PollEvent, PollResult, WatchTuning,
    LOCAL_LABEL,
};
use cluster_node::project_state;

use super::render::render_table;
use super::{emit_result, load_ctx, report_fleet_error, vlog};
use crate::cli::AgentCommands;

const LIST_HEADERS: &[&str] = &["ID", "MACHINE", "KIND", "STATUS", "NAME"];

/// `herdcluster agent ...`
pub fn run(command: AgentCommands, verbose: bool) -> i32 {
    match command {
        AgentCommands::List { all, json } => list(all, json, verbose),
        AgentCommands::Get { id, json } => with_ctx(verbose, json, |ctx| agent_get(ctx, &id)),
        AgentCommands::Read { id, lines, json } => with_ctx(verbose, json, |ctx| {
            // `recent` (upstream herdr default) returns real scrollback and
            // honors `--lines`; `visible` only sees the alt-screen viewport,
            // which for TUIs like Devin is just the footer (issue #9).
            agent_read(ctx, &id, ReadSource::Recent, None, lines, None)
        }),
        AgentCommands::Wait {
            id,
            until,
            timeout,
            stuck_after,
            json,
        } => wait_agent(
            &id,
            &until,
            timeout.as_deref(),
            stuck_after.as_deref(),
            json,
            verbose,
        ),
        AgentCommands::Watch {
            id,
            json,
            stuck_after,
        } => watch_agent(&id, stuck_after.as_deref(), json, verbose),
        AgentCommands::Prompt { id, text } => {
            with_ctx(verbose, false, |ctx| agent_prompt(ctx, &id, &text, None))
        }
        AgentCommands::SendKeys { id, keys } => {
            let key_list: Vec<String> = keys.split_whitespace().map(str::to_string).collect();
            if key_list.is_empty() {
                eprintln!("error: send-keys requires at least one key name");
                return 2;
            }
            with_ctx(verbose, false, |ctx| agent_send_keys(ctx, &id, &key_list))
        }
        AgentCommands::Rename { id, name } => rename(&id, &name, verbose),
    }
}

/// Shared op wrapper: load ctx, run the fleet op, emit result or error.
fn with_ctx(
    verbose: bool,
    json: bool,
    op: impl FnOnce(&FleetContext) -> Result<serde_json::Value, FleetError>,
) -> i32 {
    let ctx = match load_ctx(verbose, json) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };
    match op(&ctx) {
        Ok(value) => {
            emit_result(&value, json);
            0
        }
        Err(e) => report_fleet_error(&e, json),
    }
}

/// RFC 3339 millisecond timestamp for watch/event output.
fn now_ts() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Build a [`WatchTuning`] from the `--timeout`/`--stuck-after` flag
/// values. `stuck_after` accepts a duration (`2m`) or `off`/`0` to
/// disable; `None` keeps the 10-minute default.
fn watch_tuning(
    timeout: Option<&str>,
    stuck_after: Option<&str>,
) -> Result<WatchTuning, FleetError> {
    let mut tuning = WatchTuning::default();
    if let Some(t) = timeout {
        tuning.timeout = Some(parse_duration(t)?);
    }
    if let Some(raw) = stuck_after {
        let raw = raw.trim();
        if raw.eq_ignore_ascii_case("off") || raw == "0" {
            tuning.stuck_after = None;
        } else {
            let d = parse_duration(raw)?;
            tuning.stuck_after = (!d.is_zero()).then_some(d);
        }
    }
    Ok(tuning)
}

/// Print a stuck warning. Shared by wait (stderr always) and watch text
/// mode; watch `--json` emits it as an event instead.
fn stuck_warning(id: &str, status: &str, elapsed_secs: u64) {
    eprintln!(
        "warning: {id} has been '{status}' unchanged for {}s",
        elapsed_secs
    );
}

/// `agent wait` — poll `agent.get` until the status enters `--until`,
/// the agent disappears, or `--timeout` elapses. Exit 0 on match/gone,
/// 1 on timeout or fleet error, 2 on bad flags.
fn wait_agent(
    id: &str,
    until: &str,
    timeout: Option<&str>,
    stuck_after: Option<&str>,
    json: bool,
    verbose: bool,
) -> i32 {
    let until_set = match parse_until(until) {
        Ok(set) => set,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    let tuning = match watch_tuning(timeout, stuck_after) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    let ctx = match load_ctx(verbose, json) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };

    let id_owned = id.to_string();
    let mut emit = |ev: PollEvent| match ev {
        PollEvent::Status { from, to } => vlog(
            verbose,
            format!("{id_owned}: {} -> {to}", from.unwrap_or_else(|| "-".into())),
        ),
        PollEvent::Stuck {
            status,
            elapsed_secs,
        } => stuck_warning(&id_owned, &status, elapsed_secs),
    };

    match agent_wait_live(&ctx, &id_owned, &until_set, &tuning, &mut emit) {
        Ok(PollResult::Matched { status, agent }) => {
            if json {
                emit_result(&agent, true);
            } else {
                println!("{id_owned}: {status}");
            }
            0
        }
        Ok(PollResult::Gone) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({ "id": id_owned, "outcome": "agent_gone" })
                );
            } else {
                println!("{id_owned}: agent is gone");
            }
            0
        }
        Ok(PollResult::TimedOut { last_status }) => {
            let msg = format!(
                "timed out waiting for {id_owned} \
                 (last status: {})",
                last_status.as_deref().unwrap_or("unknown")
            );
            if json {
                eprintln!(
                    "{}",
                    serde_json::json!({
                        "error": { "code": "wait_timeout", "message": msg }
                    })
                );
            } else {
                eprintln!("error: {msg}");
            }
            1
        }
        Err(e) => report_fleet_error(&e, json),
    }
}

/// `agent watch` — stream status transitions until the agent reaches a
/// terminal status, disappears, or the process is interrupted.
/// `--json` emits one JSON object per line.
fn watch_agent(id: &str, stuck_after: Option<&str>, json: bool, verbose: bool) -> i32 {
    let mut tuning = match watch_tuning(None, stuck_after) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("error: {e}");
            return 2;
        }
    };
    tuning.timeout = None; // watch runs until interrupted or terminal
    let ctx = match load_ctx(verbose, json) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };

    let id_owned = id.to_string();
    let mut emit = |ev: PollEvent| match ev {
        PollEvent::Status { from, to } => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "ts": now_ts(),
                        "id": id_owned,
                        "event": "status",
                        "from": from,
                        "to": to,
                    })
                );
            } else {
                match from {
                    Some(prev) => println!("{} {id_owned}: {prev} -> {to}", now_ts()),
                    None => println!("{} {id_owned}: {to}", now_ts()),
                }
            }
        }
        PollEvent::Stuck {
            status,
            elapsed_secs,
        } => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "ts": now_ts(),
                        "id": id_owned,
                        "event": "stuck",
                        "status": status,
                        "elapsed_secs": elapsed_secs,
                    })
                );
            } else {
                stuck_warning(&id_owned, &status, elapsed_secs);
            }
        }
    };

    let end_status = match agent_watch_live(&ctx, &id_owned, &tuning, &mut emit) {
        Ok(PollResult::Matched { status, .. }) => Some(status),
        Ok(PollResult::Gone) => None,
        Ok(PollResult::TimedOut { .. }) => unreachable!("watch sets no timeout"),
        Err(e) => return report_fleet_error(&e, json),
    };

    // Terminal marker so scripts can detect clean stream end.
    if json {
        println!(
            "{}",
            serde_json::json!({
                "ts": now_ts(),
                "id": id_owned,
                "event": "end",
                "status": end_status,
            })
        );
    } else if let Some(status) = &end_status {
        println!("{} {id_owned}: finished ({status})", now_ts());
    } else {
        println!("{} {id_owned}: agent is gone", now_ts());
    }
    0
}

/// `agent rename` — forward the rename to the hosting machine, then
/// keep `.herdcluster/agents.json` in sync so `herdcluster clean` still
/// tracks the live agent under its new global id (a stale record would
/// leave the renamed agent running forever). The record update is
/// best-effort: an unrecorded agent or an unwritable state file never
/// fails the rename itself.
fn rename(id: &str, name: &str, verbose: bool) -> i32 {
    let ctx = match load_ctx(verbose, false) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };
    match agent_rename(&ctx, id, Some(name)) {
        Ok(value) => {
            sync_rename_record(id, name, verbose);
            emit_result(&value, false);
            0
        }
        Err(e) => report_fleet_error(&e, false),
    }
}

/// Best-effort: after a successful `agent rename`, move the spawn
/// record from the old global id to `{machine}/{new name}`.
fn sync_rename_record(old_id: &str, new_name: &str, verbose: bool) {
    let Some(project) = std::env::current_dir()
        .ok()
        .and_then(|cwd| project_state::find_project_dir(&cwd))
    else {
        return;
    };
    // The machine half is unchanged; the agent half becomes the new
    // name. `agent_rename` already validated `old_id`'s syntax.
    let Ok(gid) = parse_global_id(old_id) else {
        return;
    };
    let new_id = format_global_id(&gid.machine, new_name);
    match project_state::rename_agent(&project, old_id, &new_id, new_name) {
        Ok(true) => vlog(
            verbose,
            format!("renamed spawn record {old_id} -> {new_id}"),
        ),
        // Not a recorded spawn — nothing to update.
        Ok(false) => {}
        Err(e) => vlog(
            verbose,
            format!("could not update agents.json after rename: {e}"),
        ),
    }
}

/// `agent list` — local only, or `--all` fleet-wide fan-out.
fn list(all: bool, json: bool, verbose: bool) -> i32 {
    let ctx = match load_ctx(verbose, json) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };

    let rows: Vec<AgentListRow> = if all {
        vlog(verbose, "fanning out agent.list to all machines");
        list_all_agents(&ctx)
    } else {
        vlog(verbose, "listing agents on the local machine only");
        match local_agent_rows(&ctx) {
            Ok(rows) => rows,
            Err(e) => return report_fleet_error(&e, json),
        }
    };

    if json {
        super::print_json(&rows);
        return 0;
    }
    let table_rows: Vec<Vec<String>> = rows.iter().map(agent_row).collect();
    println!("{}", render_table(LIST_HEADERS, &table_rows));
    0
}

/// Local-only `agent.list` → [`AgentListRow`]s (same shape as `--all`).
fn local_agent_rows(ctx: &FleetContext) -> Result<Vec<AgentListRow>, FleetError> {
    let client = ctx.local().client()?;
    let value = client.agent_list().map_err(|e| FleetError::Api {
        message: e.to_string(),
    })?;
    Ok(cluster_fleet::fleet::parse_agent_list(LOCAL_LABEL, &value)
        .into_iter()
        .map(AgentListRow::Agent)
        .collect())
}

/// One table row per [`AgentListRow`]; unreachable machines render as a
/// placeholder row instead of being hidden.
fn agent_row(row: &AgentListRow) -> Vec<String> {
    match row {
        AgentListRow::Agent(a) => vec![
            a.id.clone(),
            a.machine.clone(),
            a.kind.clone().unwrap_or_else(|| "-".into()),
            agent_status(a),
            agent_display_name(a),
        ],
        AgentListRow::Unreachable(p) => vec![
            "-".into(),
            p.machine.clone(),
            "-".into(),
            "unreachable".into(),
            p.reason.clone(),
        ],
    }
}

/// Status for display: parsed `status`/`state` first, then common
/// herdr 0.9.5 field aliases found in the flattened `extra` map.
pub(crate) fn agent_status(a: &cluster_fleet::FleetAgent) -> String {
    a.status
        .clone()
        .or_else(|| extra_str(a, "agent_status"))
        .or_else(|| extra_str(a, "state"))
        .unwrap_or_else(|| "-".into())
}

/// Display name for an agent row: parsed `name`, then the terminal title
/// aliases real herdr responses carry.
pub(crate) fn agent_display_name(a: &cluster_fleet::FleetAgent) -> String {
    a.name
        .clone()
        .or_else(|| extra_str(a, "terminal_title_stripped"))
        .or_else(|| extra_str(a, "terminal_title"))
        .unwrap_or_else(|| "-".into())
}

fn extra_str(a: &cluster_fleet::FleetAgent, key: &str) -> Option<String> {
    a.extra
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster_fleet::fleet::MachinePlaceholder;
    use cluster_fleet::FleetAgent;
    use serde_json::Map;

    #[test]
    fn agent_row_renders_agent_and_placeholder() {
        let agent = AgentListRow::Agent(FleetAgent {
            machine: "local".into(),
            id: "local/claude-1".into(),
            agent: "claude-1".into(),
            name: Some("claude-1".into()),
            kind: Some("claude".into()),
            status: Some("idle".into()),
            extra: Map::new(),
        });
        let row = agent_row(&agent);
        assert_eq!(row[0], "local/claude-1");
        assert_eq!(row[3], "idle");

        let ph = AgentListRow::Unreachable(MachinePlaceholder {
            machine: "wsl".into(),
            status: "unreachable".into(),
            reason: "socket missing".into(),
        });
        let row = agent_row(&ph);
        assert_eq!(row[1], "wsl");
        assert_eq!(row[3], "unreachable");
        assert_eq!(row[4], "socket missing");
    }

    #[test]
    fn send_keys_whitespace_split() {
        let keys = "C-c Enter"
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["C-c".to_string(), "Enter".to_string()]);
    }
}
