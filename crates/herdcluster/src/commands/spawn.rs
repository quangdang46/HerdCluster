//! `herdcluster spawn` — compose and run a cross-machine agent spawn.
//!
//! Thin wrapper over [`cluster_fleet::spawn`]: resolves the machine target
//! (`auto` → scheduler placement, anything else → explicit label), builds
//! a [`SpawnRequest`], and renders the [`SpawnResult`].

use cluster_fleet::{spawn, FleetContext, MachineTarget, SpawnRequest};
use cluster_node::project_state::{self, SpawnedAgent};

use super::{load_ctx, report_spawn_error, vlog};

/// `herdcluster spawn <name> --kind <k> --machine <m|auto> [--repo <path>]
/// [--branch <b>] [-- <agent args>...]`
///
/// `agent_args` are extra CLI arguments forwarded to the agent itself,
/// placed after a literal `--` so clap does not eat agent flags (e.g.
/// `herdcluster spawn x --kind devin -- --permission-mode dangerous`).
#[allow(clippy::too_many_arguments)]
pub fn run(
    name: &str,
    kind: &str,
    machine: &str,
    repo: Option<String>,
    branch: Option<String>,
    json: bool,
    agent_args: Vec<String>,
    verbose: bool,
) -> i32 {
    let ctx: FleetContext = match load_ctx(verbose, json) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };

    // `--repo` omitted → a `.herdcluster/config.json` mapping for the
    // target machine wins over the fleet default (sole workspace /
    // server-default cwd). `auto` has no target yet, so it is skipped.
    let cwd = std::env::current_dir().ok();
    let repo = repo.or_else(|| {
        cwd.as_deref()
            .and_then(|c| project_state::repo_override_for(c, machine))
    });

    let req = SpawnRequest {
        name: name.to_string(),
        kind: kind.to_string(),
        machine: parse_machine_target(machine),
        repo_path: repo,
        branch,
        extra_agent_args: agent_args,
        ready_timeout: None, // fleet default (35s, PLAN.md §3.8)
    };
    vlog(
        verbose,
        format!("spawning {kind} agent {name:?} on {machine}"),
    );

    match spawn(&ctx, &req) {
        Ok(result) => {
            record_spawn(&result, cwd.as_deref(), verbose);
            if json {
                super::print_json(&result);
            } else {
                println!(
                    "spawned {} ({}) on {}\n  id:        {}\n  pane:      {}\n  workspace: {}",
                    result.agent,
                    result.kind,
                    result.machine,
                    result.global_id,
                    result.pane_id,
                    result.workspace_id
                );
            }
            0
        }
        Err(e) => report_spawn_error(&e, json),
    }
}

/// Best-effort: append the spawn to `.herdcluster/agents.json` when the
/// cwd sits inside a project (`.git`/`.herdcluster` marker). Never fails
/// the spawn — state bookkeeping is advisory.
fn record_spawn(result: &cluster_fleet::SpawnResult, cwd: Option<&std::path::Path>, verbose: bool) {
    let Some(cwd) = cwd else { return };
    let Some(project) = project_state::find_project_dir(cwd) else {
        return;
    };
    let agent = SpawnedAgent::now(
        &result.machine,
        &result.agent,
        &result.kind,
        &result.workspace_id,
        &result.pane_id,
    );
    match project_state::record_spawn(&project, &result.global_id, agent) {
        Ok(()) => vlog(
            verbose,
            format!(
                "recorded {} in {}",
                result.global_id,
                project_state::agents_path(&project).display()
            ),
        ),
        Err(e) => vlog(
            verbose,
            format!("could not record spawn in agents.json: {e}"),
        ),
    }
}

/// `--machine auto` selects scheduler placement; every other value is an
/// explicit machine label (`local` or a worker label). Case-insensitive
/// for `auto` only — labels are matched exactly downstream.
pub(crate) fn parse_machine_target(s: &str) -> MachineTarget {
    if s.eq_ignore_ascii_case("auto") {
        MachineTarget::Auto
    } else {
        MachineTarget::Label(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_target_auto_is_case_insensitive() {
        assert_eq!(parse_machine_target("auto"), MachineTarget::Auto);
        assert_eq!(parse_machine_target("AUTO"), MachineTarget::Auto);
        assert_eq!(parse_machine_target("Auto"), MachineTarget::Auto);
    }

    #[test]
    fn machine_target_otherwise_is_label() {
        assert_eq!(
            parse_machine_target("local"),
            MachineTarget::Label("local".into())
        );
        assert_eq!(
            parse_machine_target("desktop-win"),
            MachineTarget::Label("desktop-win".into())
        );
        // "auto" as a prefix is still a label.
        assert_eq!(
            parse_machine_target("autopilot"),
            MachineTarget::Label("autopilot".into())
        );
    }
}
