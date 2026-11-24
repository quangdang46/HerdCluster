//! `herdcluster clean` — tear down agents recorded in
//! `.herdcluster/agents.json`.
//!
//! Reads the project state under the cwd's project directory
//! (`.git`/`.herdcluster` marker, found by walking up), selects the
//! recorded agents (all, or `--machine <label>` only), and for each one
//! sends `pane.close` through the fleet endpoint routing — the pane
//! hosts the agent, so closing it stops the agent. With
//! `--close-workspaces` the agent's workspace is closed afterwards via
//! `workspace.close` (Herdr state only — files on disk are untouched).
//!
//! Entries are removed from `agents.json` only after a successful
//! teardown; failures keep their entry and are reported. The
//! `.herdcluster/` directory itself is never deleted. `--dry-run`
//! prints the plan without touching any machine or file.

use std::collections::BTreeSet;

use cluster_fleet::{machine_pane_close, machine_workspace_close, FleetContext};
use cluster_node::join_ux::confirm;
use cluster_node::project_state::{self, SpawnedAgent};

/// `herdcluster clean [--machine <label>] [--yes] [--dry-run] [--close-workspaces]`
pub fn run(
    machine: Option<String>,
    yes: bool,
    dry_run: bool,
    close_workspaces: bool,
    verbose: bool,
    json: bool,
) -> i32 {
    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: cannot determine current directory: {e}");
            return 1;
        }
    };
    let Some(project_dir) = project_state::find_project_dir(&cwd) else {
        println!(
            "no project state (.git/.herdcluster) found at or above {}",
            cwd.display()
        );
        println!("nothing to clean");
        return 0;
    };
    let agents_path = project_state::agents_path(&project_dir);
    let file = match project_state::load_agents(&project_dir) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: cannot read {}: {e}", agents_path.display());
            return 1;
        }
    };

    let selected = project_state::select_agents(&file, machine.as_deref());
    if selected.is_empty() {
        println!("no recorded agents to clean");
        return 0;
    }

    if dry_run {
        println!(
            "dry run — would clean {} agent(s) from {}",
            selected.len(),
            agents_path.display()
        );
        for (id, agent) in &selected {
            println!(
                "  {id}  machine={} pane={} workspace={}",
                agent.machine, agent.pane_id, agent.workspace_id
            );
        }
        if close_workspaces {
            for (m, ws) in workspace_close_plan(&selected) {
                println!("  + close workspace {ws} on {m}");
            }
        }
        return 0;
    }

    let proceed = yes
        || confirm(
            &format!(
                "Tear down {} recorded agent(s){}?",
                selected.len(),
                if close_workspaces {
                    " and their workspaces"
                } else {
                    ""
                }
            ),
            false,
        );
    if !proceed {
        println!("aborted (use --yes to skip this prompt)");
        return 0;
    }

    let ctx: FleetContext = match super::load_ctx(verbose, json) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };

    let mut cleaned = 0usize;
    let mut failed: Vec<(String, String)> = Vec::new();
    // (machine, workspace_id) pairs whose pane closes succeeded — only
    // those are candidates for workspace.close.
    let mut closed_pane_workspaces: Vec<(String, String)> = Vec::new();

    for (id, agent) in &selected {
        match machine_pane_close(&ctx, &agent.machine, &agent.pane_id) {
            Ok(_) => {
                if let Err(e) = project_state::remove_agent(&project_dir, id) {
                    eprintln!("warning: {id} torn down but agents.json update failed: {e}");
                }
                cleaned += 1;
                if close_workspaces {
                    closed_pane_workspaces
                        .push((agent.machine.clone(), agent.workspace_id.clone()));
                }
                println!("cleaned {id} (pane {})", agent.pane_id);
            }
            Err(e) => {
                eprintln!("error: failed to clean {id}: {e}");
                failed.push((id.to_string(), e.to_string()));
            }
        }
    }

    if close_workspaces {
        for (machine, workspace_id) in dedup_workspaces(&closed_pane_workspaces) {
            match machine_workspace_close(&ctx, &machine, &workspace_id, None) {
                Ok(_) => println!("closed workspace {workspace_id} on {machine}"),
                Err(e) => {
                    eprintln!("warning: workspace.close {workspace_id} on {machine} failed: {e}");
                    failed.push((format!("{machine}/{workspace_id}"), e.to_string()));
                }
            }
        }
    }

    println!(
        "cleaned {cleaned}/{} agent(s){}",
        selected.len(),
        if failed.is_empty() {
            String::new()
        } else {
            format!("; {} failure(s)", failed.len())
        }
    );
    if failed.is_empty() {
        0
    } else {
        1
    }
}

/// Pure: `(machine, workspace_id)` pairs to close for the selected
/// agents — deduplicated, preserving first-seen order.
fn workspace_close_plan(selected: &[(&String, &SpawnedAgent)]) -> Vec<(String, String)> {
    dedup_workspaces(
        &selected
            .iter()
            .map(|(_, a)| (a.machine.clone(), a.workspace_id.clone()))
            .collect::<Vec<_>>(),
    )
}

fn dedup_workspaces(pairs: &[(String, String)]) -> Vec<(String, String)> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for (m, w) in pairs {
        if seen.insert((m.clone(), w.clone())) {
            out.push((m.clone(), w.clone()));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster_node::project_state::AgentsFile;

    fn agent(machine: &str, workspace_id: &str) -> SpawnedAgent {
        SpawnedAgent {
            machine: machine.into(),
            name: "a".into(),
            kind: "claude".into(),
            workspace_id: workspace_id.into(),
            pane_id: "w:p".into(),
            spawned_at: "t".into(),
        }
    }

    #[test]
    fn dry_run_plan_lists_selected_agents() {
        let mut file = AgentsFile::default();
        file.agents.insert("local/a".into(), agent("local", "w1"));
        file.agents.insert("w1/b".into(), agent("w1", "w2"));
        let selected = project_state::select_agents(&file, Some("w1"));
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].0.as_str(), "w1/b");
        // Workspace plan dedupes shared workspaces.
        let plan = workspace_close_plan(&selected);
        assert_eq!(plan, vec![("w1".to_string(), "w2".to_string())]);
    }

    #[test]
    fn workspace_plan_dedupes() {
        let a = agent("local", "w1");
        let b = agent("local", "w1");
        let c = agent("w2", "w9");
        let ids: Vec<String> = vec!["local/a".into(), "local/b".into(), "w2/c".into()];
        let selected: Vec<(&String, &SpawnedAgent)> = ids
            .iter()
            .zip([&a, &b, &c].iter())
            .map(|(id, ag)| (id, *ag))
            .collect();
        let plan = workspace_close_plan(&selected);
        assert_eq!(
            plan,
            vec![
                ("local".to_string(), "w1".to_string()),
                ("w2".to_string(), "w9".to_string())
            ]
        );
    }
}
