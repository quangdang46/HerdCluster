//! `herdcluster agent <sub>` — agent operations across the cluster.
//!
//! `list` fans out to every machine (`--all`) or queries only the local
//! herdr (`agent list` without `--all`, addressed as `local/<name>`).
//! `get`/`read`/`wait`/`prompt`/`send-keys`/`rename` take a global agent
//! ID (`{machine}/{agent}`) and forward through `cluster-fleet` ops.

use api_client::methods::ReadSource;
use cluster_fleet::{
    agent_get, agent_prompt, agent_read, agent_rename, agent_send_keys, agent_wait,
    list_all_agents, AgentListRow, FleetContext, FleetError, LOCAL_LABEL,
};

use super::render::render_table;
use super::{emit_result, load_ctx, report_fleet_error, vlog};
use crate::cli::AgentCommands;

const LIST_HEADERS: &[&str] = &["ID", "MACHINE", "KIND", "STATUS", "NAME"];

/// `herdcluster agent ...`
pub fn run(command: AgentCommands, verbose: bool) -> i32 {
    match command {
        AgentCommands::List { all, json } => list(all, json, verbose),
        AgentCommands::Get { id, json } => with_ctx(verbose, json, |ctx| agent_get(ctx, &id)),
        AgentCommands::Read { id, json } => with_ctx(verbose, json, |ctx| {
            agent_read(ctx, &id, ReadSource::Visible, None, None, None)
        }),
        AgentCommands::Wait { id, json } => {
            with_ctx(verbose, json, |ctx| agent_wait(ctx, &id, None))
        }
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
        AgentCommands::Rename { id, name } => {
            with_ctx(verbose, false, |ctx| agent_rename(ctx, &id, Some(&name)))
        }
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
