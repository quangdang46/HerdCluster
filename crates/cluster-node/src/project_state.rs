//! `project_state` — per-project `.herdcluster/` state.
//!
//! When `herdcluster` runs inside a project directory (one containing
//! `.git` or `.herdcluster`, found by walking up from the cwd), it
//! maintains a small state dir:
//!
//! ```text
//! <project>/.herdcluster/
//! ├── config.json   # project-scoped config (machine → repo path map)
//! └── agents.json   # agents spawned via `herdcluster spawn`
//! ```
//!
//! `config.json` maps machine labels to the repo path on that machine —
//! e.g. `{"machines": {"local": "/Users/me/mac", "DESKTOP-X": "/home/me/wsl"}}`.
//! `herdcluster spawn --repo` omitted consults this map before falling
//! back to the historical behavior (sole workspace / server default).
//!
//! `agents.json` records every agent `herdcluster spawn` created, keyed
//! by global id (`{machine}/{name}`), so `herdcluster clean` can tear
//! them down later. The `.herdcluster` directory itself is never
//! deleted by `clean` — only the agent entries inside `agents.json`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Directory name created at the project root.
pub const PROJECT_DIR_NAME: &str = ".herdcluster";
/// Project config file (`config.json`).
pub const CONFIG_FILE_NAME: &str = "config.json";
/// Spawned-agents registry file (`agents.json`).
pub const AGENTS_FILE_NAME: &str = "agents.json";

/// `config.json` contents: project-scoped configuration.
///
/// `machines` maps a machine label (`"local"` or a worker label) to the
/// repository path on that machine — the value `spawn` should use for
/// `--repo` when the flag is omitted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectConfig {
    /// Machine label → repo path on that machine.
    #[serde(default)]
    pub machines: BTreeMap<String, String>,
}

/// One spawned-agent record in `agents.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpawnedAgent {
    /// Machine label the agent runs on (`"local"` or a worker label).
    pub machine: String,
    /// Machine-local agent name (the `name` passed to `agent.start`).
    pub name: String,
    /// Agent kind (e.g. `"claude"`).
    pub kind: String,
    /// Workspace hosting the agent's pane.
    pub workspace_id: String,
    /// Pane hosting the agent.
    pub pane_id: String,
    /// RFC 3339 timestamp of the spawn.
    pub spawned_at: String,
}

impl SpawnedAgent {
    /// Build a record stamped with the current UTC time (RFC 3339).
    #[allow(clippy::too_many_arguments)]
    pub fn now(machine: &str, name: &str, kind: &str, workspace_id: &str, pane_id: &str) -> Self {
        SpawnedAgent {
            machine: machine.to_string(),
            name: name.to_string(),
            kind: kind.to_string(),
            workspace_id: workspace_id.to_string(),
            pane_id: pane_id.to_string(),
            spawned_at: chrono::Utc::now().to_rfc3339(),
        }
    }
}

/// `agents.json` contents: global agent id → spawn record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentsFile {
    /// Spawned agents keyed by global id (`{machine}/{name}`).
    #[serde(default)]
    pub agents: BTreeMap<String, SpawnedAgent>,
}

/// Errors from project-state reads/writes.
#[derive(Debug, Error)]
pub enum ProjectStateError {
    /// Filesystem operation failed.
    #[error("project state io error: {0}")]
    Io(#[from] std::io::Error),
    /// A state file exists but is not valid JSON / wrong shape.
    #[error("project state file '{}' is corrupt: {message}", path.display())]
    Corrupt {
        /// Parse error message.
        message: String,
        /// Path of the corrupt file.
        path: PathBuf,
    },
}

/// Find the project directory for `start`: the nearest ancestor
/// (including `start` itself) that contains `.git` or `.herdcluster`.
///
/// Returns `None` when no ancestor qualifies — callers then skip all
/// project-state work. `.git` may be a file (worktrees/submodules) —
/// presence of either form counts.
pub fn find_project_dir(start: &Path) -> Option<PathBuf> {
    let mut dir = if start.is_dir() {
        start.to_path_buf()
    } else {
        start.parent()?.to_path_buf()
    };
    loop {
        if dir.join(".git").exists() || dir.join(PROJECT_DIR_NAME).exists() {
            return Some(dir);
        }
        dir = dir.parent()?.to_path_buf();
    }
}

/// `<project>/.herdcluster`.
pub fn state_dir(project_dir: &Path) -> PathBuf {
    project_dir.join(PROJECT_DIR_NAME)
}

/// `<project>/.herdcluster/config.json`.
pub fn config_path(project_dir: &Path) -> PathBuf {
    state_dir(project_dir).join(CONFIG_FILE_NAME)
}

/// `<project>/.herdcluster/agents.json`.
pub fn agents_path(project_dir: &Path) -> PathBuf {
    state_dir(project_dir).join(AGENTS_FILE_NAME)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, ProjectStateError> {
    match std::fs::read_to_string(path) {
        Ok(text) => serde_json::from_str(&text).map_err(|e| ProjectStateError::Corrupt {
            message: e.to_string(),
            path: path.to_path_buf(),
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Missing file = empty state, not an error. `{}` relies on
            // every field having a serde default.
            serde_json::from_str("{}").map_err(|e| ProjectStateError::Corrupt {
                message: e.to_string(),
                path: path.to_path_buf(),
            })
        }
        Err(e) => Err(ProjectStateError::Io(e)),
    }
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), ProjectStateError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let text = serde_json::to_string_pretty(value).map_err(|e| ProjectStateError::Corrupt {
        message: e.to_string(),
        path: path.to_path_buf(),
    })?;
    // Write via sibling temp + rename so a crash never leaves a
    // truncated registry file.
    let tmp = path.with_extension("hctmp");
    std::fs::write(&tmp, format!("{text}\n"))?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Load `config.json`; a missing file yields the default (empty map).
pub fn load_config(project_dir: &Path) -> Result<ProjectConfig, ProjectStateError> {
    read_json(&config_path(project_dir))
}

/// Persist `config.json` (creates `.herdcluster/` as needed).
pub fn save_config(project_dir: &Path, config: &ProjectConfig) -> Result<(), ProjectStateError> {
    write_json(&config_path(project_dir), config)
}

/// Load `agents.json`; a missing file yields an empty registry.
pub fn load_agents(project_dir: &Path) -> Result<AgentsFile, ProjectStateError> {
    read_json(&agents_path(project_dir))
}

/// Persist `agents.json` (creates `.herdcluster/` as needed).
pub fn save_agents(project_dir: &Path, agents: &AgentsFile) -> Result<(), ProjectStateError> {
    write_json(&agents_path(project_dir), agents)
}

/// Create `.herdcluster/` and any missing state files (`config.json`,
/// `agents.json`) so the directory is always well-formed after a spawn.
/// Existing files are left untouched.
pub fn ensure_files(project_dir: &Path) -> Result<(), ProjectStateError> {
    let dir = state_dir(project_dir);
    std::fs::create_dir_all(&dir)?;
    let config = config_path(project_dir);
    if !config.exists() {
        write_json(&config, &ProjectConfig::default())?;
    }
    let agents = agents_path(project_dir);
    if !agents.exists() {
        write_json(&agents, &AgentsFile::default())?;
    }
    Ok(())
}

/// Record a successful spawn into `agents.json`.
///
/// Creates `.herdcluster/` (and `config.json`/`agents.json` when absent)
/// via [`ensure_files`]. `global_id` is the map key — callers pass
/// [`cluster_fleet::format_global_id`] output. Overwrites any existing
/// entry for the same id (a respawn of the same name is a fresh record).
pub fn record_spawn(
    project_dir: &Path,
    global_id: &str,
    agent: SpawnedAgent,
) -> Result<(), ProjectStateError> {
    ensure_files(project_dir)?;
    let mut file = load_agents(project_dir)?;
    file.agents.insert(global_id.to_string(), agent);
    save_agents(project_dir, &file)
}

/// Remove one entry from `agents.json` (used by `clean` after a
/// successful teardown). Returns `true` when an entry was removed.
pub fn remove_agent(project_dir: &Path, global_id: &str) -> Result<bool, ProjectStateError> {
    let mut file = load_agents(project_dir)?;
    let removed = file.agents.remove(global_id).is_some();
    if removed {
        save_agents(project_dir, &file)?;
    }
    Ok(removed)
}

/// The repo path configured for `machine`, if any.
///
/// Used by `spawn` when `--repo` is omitted: an explicit mapping in
/// `.herdcluster/config.json` wins over the historical fallback
/// (sole-workspace reuse / server default cwd).
pub fn repo_for_machine(project_dir: &Path, machine: &str) -> Option<String> {
    load_config(project_dir)
        .ok()
        .and_then(|c| c.machines.get(machine).cloned())
        .filter(|p| !p.trim().is_empty())
}

/// Convenience: resolve the `--repo` override for `machine` from the
/// project state under `cwd` — `find_project_dir(cwd)` →
/// [`repo_for_machine`]. Returns `None` outside a project or when no
/// mapping exists (including `machine == "auto"`, whose target is only
/// known after placement).
pub fn repo_override_for(cwd: &Path, machine: &str) -> Option<String> {
    if machine.eq_ignore_ascii_case("auto") {
        return None;
    }
    let project = find_project_dir(cwd)?;
    repo_for_machine(&project, machine)
}

/// Agents selected for cleanup: every recorded agent, or only those on
/// `machine` when a label filter is given. Pure — the caller decides
/// what to do with the selection (dry-run lists it, `clean` tears it
/// down).
pub fn select_agents<'a>(
    file: &'a AgentsFile,
    machine: Option<&str>,
) -> Vec<(&'a String, &'a SpawnedAgent)> {
    file.agents
        .iter()
        .filter(|(_, a)| machine.is_none_or(|m| a.machine == m))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hc-pstate-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn config_roundtrip() {
        let dir = tempdir("config");
        let mut config = ProjectConfig::default();
        config
            .machines
            .insert("local".into(), "/Users/me/mac".into());
        config
            .machines
            .insert("DESKTOP-X".into(), "/home/me/wsl".into());
        save_config(&dir, &config).unwrap();

        let loaded = load_config(&dir).unwrap();
        assert_eq!(loaded, config);
        assert_eq!(
            repo_for_machine(&dir, "DESKTOP-X").as_deref(),
            Some("/home/me/wsl")
        );
        assert_eq!(repo_for_machine(&dir, "ghost"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_files_load_as_empty() {
        let dir = tempdir("empty");
        assert_eq!(load_config(&dir).unwrap(), ProjectConfig::default());
        assert_eq!(load_agents(&dir).unwrap(), AgentsFile::default());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn agents_roundtrip_and_record() {
        let dir = tempdir("agents");
        let agent = SpawnedAgent {
            machine: "local".into(),
            name: "probe".into(),
            kind: "claude".into(),
            workspace_id: "w1".into(),
            pane_id: "w1:p2".into(),
            spawned_at: "2025-01-01T00:00:00Z".into(),
        };
        record_spawn(&dir, "local/probe", agent.clone()).unwrap();
        // .herdcluster/ was created by record_spawn.
        assert!(state_dir(&dir).is_dir());

        let file = load_agents(&dir).unwrap();
        assert_eq!(file.agents["local/probe"], agent);

        // Re-record same id overwrites.
        let mut updated = agent.clone();
        updated.pane_id = "w1:p9".into();
        record_spawn(&dir, "local/probe", updated.clone()).unwrap();
        assert_eq!(load_agents(&dir).unwrap().agents["local/probe"], updated);

        // remove_agent drops the entry.
        assert!(remove_agent(&dir, "local/probe").unwrap());
        assert!(!remove_agent(&dir, "local/probe").unwrap());
        assert!(load_agents(&dir).unwrap().agents.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn find_project_dir_walks_up() {
        let dir = tempdir("find");
        // No markers → None.
        assert_eq!(find_project_dir(&dir), None);
        // .git file (worktree style) counts.
        std::fs::write(dir.join(".git"), "gitdir: elsewhere").unwrap();
        let nested = dir.join("a").join("b");
        std::fs::create_dir_all(&nested).unwrap();
        assert_eq!(find_project_dir(&nested), Some(dir.clone()));
        // Existing .herdcluster also counts.
        let other = tempdir("find2");
        std::fs::create_dir_all(other.join(PROJECT_DIR_NAME)).unwrap();
        assert_eq!(find_project_dir(&other), Some(other.clone()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&other);
    }

    #[test]
    fn repo_override_for_skips_auto_and_unknown() {
        let dir = tempdir("override");
        std::fs::write(dir.join(".git"), "").unwrap();
        let nested = dir.join("sub");
        std::fs::create_dir_all(&nested).unwrap();
        let mut config = ProjectConfig::default();
        config.machines.insert("wsl".into(), "/home/me/repo".into());
        save_config(&dir, &config).unwrap();

        assert_eq!(
            repo_override_for(&nested, "wsl").as_deref(),
            Some("/home/me/repo")
        );
        assert_eq!(repo_override_for(&nested, "auto"), None);
        assert_eq!(repo_override_for(&nested, "local"), None);
        // Outside any project → None.
        let bare = tempdir("bare");
        assert_eq!(repo_override_for(&bare, "wsl"), None);
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&bare);
    }

    #[test]
    fn select_agents_filters_by_machine() {
        let mut file = AgentsFile::default();
        for (id, machine) in [("local/a", "local"), ("w1/b", "w1"), ("w2/c", "w2")] {
            file.agents.insert(
                id.to_string(),
                SpawnedAgent {
                    machine: machine.into(),
                    name: id.rsplit('/').next().unwrap().into(),
                    kind: "claude".into(),
                    workspace_id: "w".into(),
                    pane_id: "p".into(),
                    spawned_at: "t".into(),
                },
            );
        }
        assert_eq!(select_agents(&file, None).len(), 3);
        let w1: Vec<&str> = select_agents(&file, Some("w1"))
            .into_iter()
            .map(|(id, _)| id.as_str())
            .collect();
        assert_eq!(w1, vec!["w1/b"]);
    }

    #[test]
    fn corrupt_file_is_typed_error() {
        let dir = tempdir("corrupt");
        std::fs::create_dir_all(state_dir(&dir)).unwrap();
        std::fs::write(agents_path(&dir), "{not json").unwrap();
        assert!(matches!(
            load_agents(&dir),
            Err(ProjectStateError::Corrupt { .. })
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
