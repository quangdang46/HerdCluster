//! `herdcluster skills install|status` — bundled skill provisioning.
//!
//! Thin wrapper over [`cluster_node::skills`]: the service owns the
//! bundled skill payloads (`herdr-orchestrator` + `herdcluster`),
//! divergence detection, and install paths
//! (`.agents/skills/<skill>`, `.claude/skills/<skill>` per the M3 bead);
//! this module only prompts and renders.

use std::path::Path;

use cluster_node::join_ux::confirm;
use cluster_node::skills::{self, InstallOutcome, SkillStatus, SKILL_NAMES};

/// `herdcluster skills install [--yes] [--overwrite]`
///
/// Installs every bundled skill ([`SKILL_NAMES`]). Skills that are
/// already `Installed` are skipped; `Diverged` copies are only replaced
/// with consent (`--overwrite`, `--yes`, or an interactive prompt) —
/// matching the previous single-skill semantics.
pub fn install(yes: bool, overwrite: bool) -> i32 {
    let dir = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: cannot determine current directory: {e}");
            return 1;
        }
    };

    // One consent decision covers every diverged skill — the prompt
    // names the skills so the user knows what would be replaced.
    let statuses: Vec<(&str, SkillStatus)> = SKILL_NAMES
        .iter()
        .map(|name| (*name, skills::skill_status(&dir, name)))
        .collect();
    let diverged: Vec<&str> = statuses
        .iter()
        .filter(|(_, s)| *s == SkillStatus::Diverged)
        .map(|(n, _)| *n)
        .collect();

    let mut overwrite_diverged = false;
    if !diverged.is_empty() {
        // Diverged = a local copy exists but differs from the bundled
        // version. Never clobber it silently.
        let proceed = overwrite
            || yes
            || confirm(
                &format!(
                    "Diverged local skill copy/copies exist ({}). Overwrite with the bundled version?",
                    diverged.join(", ")
                ),
                false,
            );
        if !proceed {
            println!(
                "skipped: diverged skill(s) left untouched (use --overwrite to replace): {}",
                diverged.join(", ")
            );
        }
        overwrite_diverged = proceed;
    }

    let mut rc = 0;
    for (name, status) in &statuses {
        match status {
            SkillStatus::Installed => {
                println!("{name}: already installed");
                println!(
                    "  path: {}",
                    dir.join(".agents").join("skills").join(name).display()
                );
            }
            SkillStatus::Missing => {
                rc = rc.max(install_at(&dir, name, false));
            }
            SkillStatus::Diverged if overwrite_diverged => {
                rc = rc.max(install_at(&dir, name, true));
            }
            SkillStatus::Diverged => {}
        }
    }
    rc
}

fn install_at(dir: &Path, skill: &str, overwrite_diverged: bool) -> i32 {
    match skills::install_skill(dir, skill, overwrite_diverged) {
        Ok(InstallOutcome {
            path,
            created,
            skipped_reason,
        }) => {
            if let Some(reason) = skipped_reason {
                println!("{skill}: install skipped: {reason}");
            } else if created {
                println!("{skill}: installed");
                println!("  path: {}", path.display());
            } else {
                println!("{skill}: already present");
                println!("  path: {}", path.display());
            }
            0
        }
        Err(e) => {
            eprintln!("error: {skill} skill install failed: {e}");
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

    for name in SKILL_NAMES {
        let (label, detail) = match skills::skill_status(&dir, name) {
            SkillStatus::Installed => ("installed", "matches bundled version"),
            SkillStatus::Missing => ("missing", "not provisioned in this project"),
            SkillStatus::Diverged => ("diverged", "local copy differs from bundled version"),
        };
        println!("{name} skill: {label} ({detail})");
        println!(
            "  expected path: {}",
            dir.join(".agents").join("skills").join(name).display()
        );
    }
    0
}

/// Consent-gated skill offer used by `herdcluster init` (and reusable by
/// `join` flows). Returns 0 always — provisioning must never fail the
/// daemon startup that triggered it.
///
/// Installs every bundled skill: missing ones are offered once (a single
/// prompt covers all); diverged copies are replaced only after consent
/// (`assume_yes` or an interactive prompt), preserving the previous
/// single-skill consent boundary.
pub(crate) fn offer(project_dir: &Path, assume_yes: bool) -> i32 {
    let statuses: Vec<(&str, SkillStatus)> = SKILL_NAMES
        .iter()
        .map(|name| (*name, skills::skill_status(project_dir, name)))
        .collect();
    let missing: Vec<&str> = statuses
        .iter()
        .filter(|(_, s)| *s == SkillStatus::Missing)
        .map(|(n, _)| *n)
        .collect();
    let diverged: Vec<&str> = statuses
        .iter()
        .filter(|(_, s)| *s == SkillStatus::Diverged)
        .map(|(n, _)| *n)
        .collect();

    if !missing.is_empty() {
        let proceed =
            assume_yes || confirm("Install the herdcluster skills into this project?", true);
        if proceed {
            for name in &missing {
                install_at(project_dir, name, false);
            }
        } else {
            println!("skipped skill install (run `herdcluster skills install` later)");
        }
    }

    if !diverged.is_empty() {
        let proceed = assume_yes
            || confirm(
                &format!(
                    "Diverged skill copy/copies exist ({}). Overwrite with the bundled version?",
                    diverged.join(", ")
                ),
                false,
            );
        if proceed {
            for name in &diverged {
                install_at(project_dir, name, true);
            }
        }
    }
    0
}
