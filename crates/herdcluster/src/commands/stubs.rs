//! Stubbed commands: init / join / worker / skills.
//!
//! These depend on `cluster-node` (daemons), `cluster-net` (TLS/mux/
//! discovery), and `cluster-bridge` (API socket proxy) which are being
//! built in parallel. They keep the "not implemented yet" contract —
//! exit code 2 — with a pointer to the owning area so users know where
//! the feature lands.

use crate::cli::SkillsCommands;

/// `herdcluster init` — controller service (cluster-node, PLAN.md M3).
pub fn init() -> i32 {
    not_implemented(
        "init",
        "cluster controller service + pairing (cluster-node/cluster-net, PLAN.md §3.5, milestone M3)",
    )
}

/// `herdcluster join` — worker pairing client (cluster-node/cluster-net).
pub fn join() -> i32 {
    not_implemented(
        "join",
        "worker pairing + discovery (cluster-node/cluster-net, PLAN.md §3.5, milestone M3)",
    )
}

/// `herdcluster worker` — worker daemon mode.
pub fn worker() -> i32 {
    not_implemented(
        "worker",
        "worker daemon (cluster-node + cluster-bridge, PLAN.md §3.3, milestone M3)",
    )
}

/// `herdcluster skills install|status` — orchestration skill provisioning.
pub fn skills(command: SkillsCommands) -> i32 {
    let sub = match command {
        SkillsCommands::Install => "install",
        SkillsCommands::Status => "status",
    };
    not_implemented(
        &format!("skills {sub}"),
        "orchestration skill provisioning (PLAN.md §3.10)",
    )
}

fn not_implemented(cmd: &str, area: &str) -> i32 {
    eprintln!(
        "herdcluster {cmd}: not implemented yet ({area}; tracked in .beads — \
         see docs/herdcluster/PLAN.md)"
    );
    2
}
