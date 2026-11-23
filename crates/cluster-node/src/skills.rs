//! `skills` — the bundled `herdr-orchestrator` skill.
//!
//! HerdCluster ships a copy of the orchestration skill that herdr's
//! plugin system expects under `.agents/skills/` (and, for Claude
//! Code discovery, `.claude/skills/`). This module owns:
//!
//! - [`bundled_skill_files`] — the skill tree baked into the binary via
//!   `include_str!` (paths relative to the skill root);
//! - [`skill_status`] — compare a project's installed copies against the
//!   bundle (`Installed` / `Missing` / `Diverged`);
//! - [`install_skill`] — materialize the bundle into
//!   `<project>/.agents/skills/herdr-orchestrator/`.
//!
//! **Consent boundary:** this module never decides *whether* to
//! overwrite. `install_skill` refuses to clobber a diverged file unless
//! the caller passes `overwrite_diverged = true` — the CLI layer turns
//! that flag into a `--yes`/prompt decision. Agent credentials and API
//! keys are never touched here.

use std::path::{Path, PathBuf};

use thiserror::Error;

/// Skill directory name under `.agents/skills/` and `.claude/skills/`.
pub const SKILL_NAME: &str = "herdr-orchestrator";

/// Status of the bundled skill inside a project directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillStatus {
    /// At least one recognized location holds a byte-identical copy.
    Installed,
    /// Neither `.agents/skills/` nor `.claude/skills/` has the skill.
    Missing,
    /// A copy exists but differs from the bundled content.
    Diverged,
}

/// Result of a successful (or partially skipped) [`install_skill`].
#[derive(Debug, Clone)]
pub struct InstallOutcome {
    /// Directory the skill was installed into
    /// (`<project>/.agents/skills/herdr-orchestrator`).
    pub path: PathBuf,
    /// `true` when at least one file was written this call.
    pub created: bool,
    /// Human-readable reason nothing was written (e.g. diverged files
    /// protected without `overwrite_diverged`).
    pub skipped_reason: Option<String>,
}

/// Errors from [`install_skill`].
#[derive(Debug, Error)]
pub enum SkillError {
    /// Filesystem operation failed (create/write/rename).
    #[error("skill io error: {0}")]
    Io(#[from] std::io::Error),
    /// `project_dir` does not exist or is not a directory.
    #[error("project dir '{}' is not a directory", .0.display())]
    NotADirectory(PathBuf),
}

/// The bundled skill tree: `(relative_path, contents)` pairs.
///
/// Paths are relative to the skill root (no leading `herdr-orchestrator/`)
/// and use `/` separators on every platform. The list is fixed at compile
/// time — adding a file to `.agents/skills/herdr-orchestrator/` requires
/// adding a matching `include_str!` entry here.
pub fn bundled_skill_files() -> Vec<(&'static str, &'static str)> {
    vec![
        ("SKILL.md", include_str!("../../../.agents/skills/herdr-orchestrator/SKILL.md")),
        (
            "AGENTS.md",
            include_str!("../../../.agents/skills/herdr-orchestrator/AGENTS.md"),
        ),
        (
            "ANTI_PATTERN.md",
            include_str!("../../../.agents/skills/herdr-orchestrator/ANTI_PATTERN.md"),
        ),
        (
            "HERDR_ORCHESTRATOR.md",
            include_str!("../../../.agents/skills/herdr-orchestrator/HERDR_ORCHESTRATOR.md"),
        ),
        (
            "agent-lifecycle.md",
            include_str!("../../../.agents/skills/herdr-orchestrator/agent-lifecycle.md"),
        ),
        (
            "delegation.md",
            include_str!("../../../.agents/skills/herdr-orchestrator/delegation.md"),
        ),
        (
            "model-routing-and-context.md",
            include_str!(
                "../../../.agents/skills/herdr-orchestrator/model-routing-and-context.md"
            ),
        ),
        (
            "parallel-worktrees.md",
            include_str!("../../../.agents/skills/herdr-orchestrator/parallel-worktrees.md"),
        ),
        (
            "verification-and-safety.md",
            include_str!("../../../.agents/skills/herdr-orchestrator/verification-and-safety.md"),
        ),
        (
            "roles/root.md",
            include_str!("../../../.agents/skills/herdr-orchestrator/roles/root.md"),
        ),
        (
            "roles/implementer.md",
            include_str!("../../../.agents/skills/herdr-orchestrator/roles/implementer.md"),
        ),
        (
            "roles/reviewer.md",
            include_str!("../../../.agents/skills/herdr-orchestrator/roles/reviewer.md"),
        ),
        (
            "roles/peer.md",
            include_str!("../../../.agents/skills/herdr-orchestrator/roles/peer.md"),
        ),
    ]
}

/// `.agents` copy path for a project.
pub fn agents_skill_dir(project_dir: &Path) -> PathBuf {
    project_dir
        .join(".agents")
        .join("skills")
        .join(SKILL_NAME)
}

/// `.claude` copy path for a project.
pub fn claude_skill_dir(project_dir: &Path) -> PathBuf {
    project_dir
        .join(".claude")
        .join("skills")
        .join(SKILL_NAME)
}

fn skill_md_path(dir: &Path) -> PathBuf {
    dir.join("SKILL.md")
}

/// Classify the skill's presence in `project_dir`.
///
/// Both recognized locations are inspected:
///
/// - `Installed` — at least one location exists **and every file it
///   contains that the bundle also ships is byte-identical** to the
///   bundle (extra files the user added are ignored).
/// - `Diverged` — a location exists but at least one bundled file
///   differs or is missing.
/// - `Missing` — neither location has a `SKILL.md`.
pub fn skill_status(project_dir: &Path) -> SkillStatus {
    let mut saw_any = false;
    let mut diverged = false;
    for dir in [agents_skill_dir(project_dir), claude_skill_dir(project_dir)] {
        if !skill_md_path(&dir).is_file() {
            continue;
        }
        saw_any = true;
        if !dir_matches_bundle(&dir) {
            diverged = true;
        }
    }
    if !saw_any {
        SkillStatus::Missing
    } else if diverged {
        SkillStatus::Diverged
    } else {
        SkillStatus::Installed
    }
}

/// `true` when every bundled file exists under `dir` with identical
/// bytes. Extra on-disk files do not count as divergence — users may
/// add project-local notes next to the skill.
fn dir_matches_bundle(dir: &Path) -> bool {
    bundled_skill_files().iter().all(|(rel, contents)| {
        let path = dir.join(rel);
        match std::fs::read(&path) {
            Ok(bytes) => bytes == contents.as_bytes(),
            Err(_) => false,
        }
    })
}

/// Install the bundled skill into `<project_dir>/.agents/skills/`.
///
/// Behavior:
///
/// - Creates `.agents/skills/herdr-orchestrator/` (and `roles/`) as
///   needed.
/// - Writes each bundled file; identical existing files are left alone.
/// - Existing files that **differ** are overwritten only when
///   `overwrite_diverged` is `true`; otherwise they are kept and the
///   outcome reports `skipped_reason`.
/// - Files are written via a sibling temp file + rename so a crash
///   mid-write never leaves a truncated skill file.
///
/// The `.claude/skills/` mirror is *not* written — `skill_status`
/// treats either location as installed, and `.agents/` is the
/// canonical HerdCluster path; callers that also want the Claude mirror
/// can copy the tree themselves.
pub fn install_skill(
    project_dir: &Path,
    overwrite_diverged: bool,
) -> Result<InstallOutcome, SkillError> {
    if !project_dir.is_dir() {
        return Err(SkillError::NotADirectory(project_dir.to_path_buf()));
    }
    let target = agents_skill_dir(project_dir);
    std::fs::create_dir_all(&target)?;

    let mut created = false;
    let mut skipped: Vec<String> = Vec::new();
    for (rel, contents) in bundled_skill_files() {
        let path = target.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::read(&path) {
            Ok(existing) if existing == contents.as_bytes() => continue, // up to date
            Ok(_) if !overwrite_diverged => {
                skipped.push(rel.to_string());
                continue;
            }
            _ => {}
        }
        write_atomic(&path, contents.as_bytes())?;
        created = true;
    }

    let skipped_reason = if skipped.is_empty() {
        None
    } else {
        Some(format!(
            "kept {} diverged file(s) (pass overwrite/--yes to replace): {}",
            skipped.len(),
            skipped.join(", ")
        ))
    };
    Ok(InstallOutcome {
        path: target,
        created,
        skipped_reason,
    })
}

/// Write `bytes` to `path` atomically-ish: sibling temp file + rename.
/// On Unix the rename replaces the destination atomically; on Windows
/// `rename` fails if the target exists, so remove-then-rename is used
/// there (still no partial-file window for readers of the final path).
fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("hcinstall.tmp");
    std::fs::write(&tmp, bytes)?;
    #[cfg(windows)]
    {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "hc-skill-{}-{}",
            tag,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn bundled_files_cover_skill_tree() {
        let files = bundled_skill_files();
        let names: Vec<&str> = files.iter().map(|(p, _)| *p).collect();
        for expected in [
            "SKILL.md",
            "AGENTS.md",
            "ANTI_PATTERN.md",
            "HERDR_ORCHESTRATOR.md",
            "agent-lifecycle.md",
            "delegation.md",
            "model-routing-and-context.md",
            "parallel-worktrees.md",
            "verification-and-safety.md",
            "roles/root.md",
            "roles/implementer.md",
            "roles/reviewer.md",
            "roles/peer.md",
        ] {
            assert!(names.contains(&expected), "missing bundled file {expected}");
        }
        // Contents are non-empty and SKILL.md looks like a skill.
        let skill = files.iter().find(|(p, _)| *p == "SKILL.md").unwrap();
        assert!(skill.1.contains("herdr-orchestrator"));
    }

    #[test]
    fn status_missing_then_installed() {
        let dir = tempdir("status");
        assert_eq!(skill_status(&dir), SkillStatus::Missing);

        let outcome = install_skill(&dir, false).unwrap();
        assert!(outcome.created);
        assert!(outcome.skipped_reason.is_none());
        assert_eq!(skill_status(&dir), SkillStatus::Installed);

        // Second install is a no-op.
        let outcome = install_skill(&dir, false).unwrap();
        assert!(!outcome.created);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn diverged_detected_and_protected() {
        let dir = tempdir("diverged");
        install_skill(&dir, false).unwrap();

        // Mutate one bundled file.
        let skill_md = agents_skill_dir(&dir).join("SKILL.md");
        std::fs::write(&skill_md, "user edits").unwrap();
        assert_eq!(skill_status(&dir), SkillStatus::Diverged);

        // Without consent the file is preserved.
        let outcome = install_skill(&dir, false).unwrap();
        assert!(!outcome.created);
        assert!(outcome.skipped_reason.is_some());
        assert_eq!(std::fs::read_to_string(&skill_md).unwrap(), "user edits");

        // With consent it is restored.
        let outcome = install_skill(&dir, true).unwrap();
        assert!(outcome.created);
        assert_eq!(skill_status(&dir), SkillStatus::Installed);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn claude_location_counts_as_installed() {
        let dir = tempdir("claude");
        // Copy the bundle into .claude/skills instead of .agents/.
        let target = claude_skill_dir(&dir);
        std::fs::create_dir_all(&target).unwrap();
        for (rel, contents) in bundled_skill_files() {
            let p = target.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(&p, contents).unwrap();
        }
        assert_eq!(skill_status(&dir), SkillStatus::Installed);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn install_requires_project_dir() {
        let missing = std::env::temp_dir().join(format!("hc-nope-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&missing);
        assert!(matches!(
            install_skill(&missing, false),
            Err(SkillError::NotADirectory(_))
        ));
    }
}
