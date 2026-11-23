//! `herdcluster skills install|status` — orchestration skill provisioning.
//!
//! Thin wrapper over [`cluster_node::skills`]: the service owns the
//! bundled skill payload, divergence detection, and install paths
//! (`.agents/skills/herdr-orchestrator`, `.claude/skills/herdr-orchestrator`
//! per the M3 bead); this module only prompts and renders.

use std::path::Path;

use cluster_node::join_ux::confirm;
use cluster_node::skills::{self, InstallOutcome, SkillStatus};

/// Canonical project-relative install location (also probed under
/// `.claude/skills/` by the provisioning code).
const SKILL_REL: &str = ".agents/skills/herdr-orchestrator";

/// `herdcluster skills install [--yes] [--overwrite]`
pub fn install(yes: bool, overwrite: bool) -> i32 {
    let dir = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: cannot determine current directory: {e}");
            return 1;
        }
    };
    match skills::skill_status(&dir) {
        SkillStatus::Installed => {
            println!("orchestration skill already installed");
            println!("  path: {}", dir.join(SKILL_REL).display());
            0
        }
        SkillStatus::Missing => install_at(&dir, false),
        SkillStatus::Diverged => {
            // Diverged = a local copy exists but differs from the bundled
            // version. Never clobber it silently.
            let proceed = overwrite
                || yes
                || confirm(
                    "A diverged local skill copy exists. Overwrite it with the bundled version?",
                    false,
                );
            if proceed {
                install_at(&dir, true)
            } else {
                println!("skipped: diverged skill left untouched (use --overwrite to replace)");
                0
            }
        }
    }
}

fn install_at(dir: &Path, overwrite_diverged: bool) -> i32 {
    match skills::install_skill(dir, overwrite_diverged) {
        Ok(InstallOutcome {
            path,
            created,
            skipped_reason,
        }) => {
            if let Some(reason) = skipped_reason {
                println!("skill install skipped: {reason}");
            } else if created {
                println!("installed orchestration skill");
                println!("  path: {}", path.display());
            } else {
                println!("skill already present");
                println!("  path: {}", path.display());
            }
            0
        }
        Err(e) => {
            eprintln!("error: skill install failed: {e}");
            1
        }
    }
}

/// `herdcluster skills status`
pub fn status() -> i32 {
    let dir = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: cannot determine current directory: {e}");
            return 1;
        }
    };

    let (label, detail) = match skills::skill_status(&dir) {
        SkillStatus::Installed => ("installed", "matches bundled version"),
        SkillStatus::Missing => ("missing", "not provisioned in this project"),
        SkillStatus::Diverged => ("diverged", "local copy differs from bundled version"),
    };
    println!("orchestration skill: {label} ({detail})");
    println!("  expected path: {}", dir.join(SKILL_REL).display());
    0
}

/// Consent-gated skill offer used by `herdcluster init` (and reusable by
/// `join` flows). Returns 0 always — provisioning must never fail the
/// daemon startup that triggered it.
pub(crate) fn offer(project_dir: &Path, assume_yes: bool) -> i32 {
    match skills::skill_status(project_dir) {
        SkillStatus::Installed => 0,
        SkillStatus::Missing => {
            let proceed = assume_yes
                || confirm(
                    "Install the herdcluster orchestration skill into this project?",
                    true,
                );
            if proceed {
                install_at(project_dir, false)
            } else {
                println!("skipped skill install (run `herdcluster skills install` later)");
                0
            }
        }
        SkillStatus::Diverged => {
            let proceed = assume_yes
                || confirm(
                    "A diverged orchestration skill exists here. Overwrite with the bundled version?",
                    false,
                );
            if proceed {
                install_at(project_dir, true)
            } else {
                0
            }
        }
    }
}
