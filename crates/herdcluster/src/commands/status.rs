//! `herdcluster status [--all] [--json]` — cluster summary.
//!
//! Thin composition over `cluster-fleet`: the `machines` view supplies
//! per-machine reachability and live agent counts; `--all` additionally
//! merges the full `agent list --all` rows (including unreachable
//! placeholders) so scripts get the same stable schema.

use cluster_fleet::{list_all_agents, machines, AgentListRow, FleetContext, MachineInfo};
use serde::Serialize;

use super::render::render_table;
use super::{load_ctx, print_json, vlog};

#[derive(Debug, Serialize)]
struct StatusReport {
    /// Cluster display name from `cluster.json`.
    cluster: String,
    /// Per-machine view (same objects as `machines --json`).
    machines: Vec<MachineInfo>,
    /// Machines whose live probe succeeded this call.
    reachable_machines: usize,
    /// Total machines known (local + registered workers).
    total_machines: usize,
    /// Sum of live-probed agent counts across reachable machines.
    agents: usize,
    /// Full merged agent rows — present only with `--all`.
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_rows: Option<Vec<AgentListRow>>,
}

/// `herdcluster status`
pub fn run(all: bool, json: bool, verbose: bool) -> i32 {
    let ctx: FleetContext = match load_ctx(verbose, json) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };

    vlog(verbose, "collecting machine + agent status");
    let infos = machines(&ctx);
    let reachable = infos.iter().filter(|m| m.reachable).count();
    let agents: usize = infos
        .iter()
        .filter_map(|m| m.agents)
        .map(|n| n as usize)
        .sum();

    let agent_rows = if all {
        Some(list_all_agents(&ctx))
    } else {
        None
    };

    let report = StatusReport {
        cluster: ctx.cluster_name.clone(),
        total_machines: infos.len(),
        reachable_machines: reachable,
        agents,
        machines: infos,
        agent_rows,
    };

    if json {
        print_json(&report);
        return 0;
    }
    print_human(&report);
    0
}

fn print_human(report: &StatusReport) {
    println!("cluster: {}", report.cluster);
    println!(
        "machines: {}/{} reachable, {} agents",
        report.reachable_machines, report.total_machines, report.agents
    );
    let rows: Vec<Vec<String>> = report
        .machines
        .iter()
        .map(|m| {
            vec![
                m.label.clone(),
                match m.kind {
                    cluster_fleet::MachineKind::Local => "local".into(),
                    cluster_fleet::MachineKind::Remote => "remote".into(),
                },
                m.status.clone(),
                m.agents
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "-".into()),
            ]
        })
        .collect();
    println!(
        "{}",
        render_table(&["NAME", "TYPE", "STATUS", "AGENTS"], &rows)
    );

    if let Some(rows) = &report.agent_rows {
        println!("agents:");
        let agent_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|r| match r {
                AgentListRow::Agent(a) => vec![
                    a.id.clone(),
                    a.kind.clone().unwrap_or_else(|| "-".into()),
                    super::agent::agent_status(a),
                ],
                AgentListRow::Unreachable(p) => vec![
                    format!("{}/*", p.machine),
                    "unreachable".into(),
                    p.reason.clone(),
                ],
            })
            .collect();
        println!("{}", render_table(&["ID", "KIND", "STATUS"], &agent_rows));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster_fleet::MachineKind;

    fn machine(label: &str, reachable: bool, agents: Option<u64>) -> MachineInfo {
        MachineInfo {
            label: label.into(),
            kind: if label == "local" {
                MachineKind::Local
            } else {
                MachineKind::Remote
            },
            status: if reachable { "online" } else { "offline" }.into(),
            reachable,
            agents,
            agent_count: None,
            max_agents: None,
            mem_available_mb: None,
            kinds: vec![],
            resources: None,
            last_seen: None,
        }
    }

    #[test]
    fn status_json_shape_without_all() {
        let report = StatusReport {
            cluster: "test".into(),
            machines: vec![machine("local", true, Some(2)), machine("wsl", false, None)],
            reachable_machines: 1,
            total_machines: 2,
            agents: 2,
            agent_rows: None,
        };
        let v = serde_json::to_value(&report).unwrap();
        assert_eq!(v["cluster"], "test");
        assert_eq!(v["reachable_machines"], 1);
        assert_eq!(v["total_machines"], 2);
        assert_eq!(v["agents"], 2);
        assert!(v.get("agent_rows").is_none()); // skipped when None
        assert_eq!(v["machines"][0]["label"], "local");
    }

    #[test]
    fn status_json_shape_with_all() {
        let report = StatusReport {
            cluster: "test".into(),
            machines: vec![],
            reachable_machines: 0,
            total_machines: 0,
            agents: 0,
            agent_rows: Some(vec![]),
        };
        let v = serde_json::to_value(&report).unwrap();
        assert_eq!(v["agent_rows"], serde_json::json!([]));
    }
}
