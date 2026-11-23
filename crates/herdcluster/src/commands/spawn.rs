//! `herdcluster spawn` — compose and run a cross-machine agent spawn.
//!
//! Thin wrapper over [`cluster_fleet::spawn`]: resolves the machine target
//! (`auto` → scheduler placement, anything else → explicit label), builds
//! a [`SpawnRequest`], and renders the [`SpawnResult`].

use cluster_fleet::{spawn, FleetContext, MachineTarget, SpawnRequest};

use super::{load_ctx, report_spawn_error, vlog};

/// `herdcluster spawn <name> --kind <k> --machine <m|auto> --repo <path> ...`
#[allow(clippy::too_many_arguments)]
pub fn run(
    name: &str,
    kind: &str,
    machine: &str,
    repo: &str,
    branch: Option<String>,
    json: bool,
    agent_args: Vec<String>,
    verbose: bool,
) -> i32 {
    let ctx: FleetContext = match load_ctx(verbose, json) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };

    let req = SpawnRequest {
        name: name.to_string(),
        kind: kind.to_string(),
        machine: parse_machine_target(machine),
        repo_path: repo.to_string(),
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
