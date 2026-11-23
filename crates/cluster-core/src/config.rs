//! Cluster config + state directory layout — the single place other crates
//! get filesystem paths from.
//!
//! Layout under the state dir:
//!
//! ```text
//! <state_dir>/
//!   identity.key      # Ed25519 keypair (hex seed, 0600) — see identity.rs
//!   cluster.json      # ClusterInfo — see registry.rs
//!   workers.json      # WorkersFile — see registry.rs
//!   credentials/      # per-worker issued credentials (0600)
//!   runtime/          # herdr runtime supervisor state
//! ```
//!
//! State dir resolution:
//! 1. `HERDCLUSTER_STATE_DIR` env var (tests, portable installs).
//! 2. Platform default:
//!    - macOS: `~/Library/Application Support/herdcluster`
//!    - Linux/other Unix: `$XDG_CONFIG_HOME/herdcluster` or
//!      `~/.config/herdcluster`
//!    - Windows: `%APPDATA%/herdcluster` (fallback `%USERPROFILE%/.herdcluster`)
//!
//! `HERDCLUSTER_PORT` overrides the port returned by [`load_cluster`] in
//! memory (not persisted) — useful for tests and port-conflict recovery.

use crate::fsutil;
use crate::registry::{self, ClusterInfo, WorkerRegistry};
use std::path::PathBuf;
use thiserror::Error;

/// Default controller port (PLAN.md §3.4).
pub const DEFAULT_PORT: u16 = 47820;
/// Env var overriding the state dir.
pub const ENV_STATE_DIR: &str = "HERDCLUSTER_STATE_DIR";
/// Env var overriding the listen port.
pub const ENV_PORT: &str = "HERDCLUSTER_PORT";

/// Errors from config/state-dir operations.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Filesystem error.
    #[error("config io error: {0}")]
    Io(#[from] std::io::Error),
    /// Registry load/save error.
    #[error("registry error: {0}")]
    Registry(#[from] registry::RegistryError),
    /// No home directory could be determined for the platform default.
    #[error("could not determine home directory for state dir")]
    NoHomeDir,
    /// `HERDCLUSTER_PORT` was set but not a valid port number.
    #[error("invalid {ENV_PORT} value: {0}")]
    InvalidPort(String),
}

/// Resolve the herdcluster state directory. See module docs for the
/// resolution order. Does not create anything — call [`ensure_layout`] for
/// that (kept lazy so read-only commands don't touch the disk).
pub fn state_dir() -> Result<PathBuf, ConfigError> {
    if let Ok(v) = std::env::var(ENV_STATE_DIR) {
        if !v.trim().is_empty() {
            return Ok(PathBuf::from(v));
        }
    }
    platform_state_dir()
}

/// Platform-specific default state dir (no env override applied).
fn platform_state_dir() -> Result<PathBuf, ConfigError> {
    #[cfg(target_os = "macos")]
    {
        let home = home_dir()?;
        Ok(home
            .join("Library")
            .join("Application Support")
            .join("herdcluster"))
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
            if !xdg.trim().is_empty() {
                return Ok(PathBuf::from(xdg).join("herdcluster"));
            }
        }
        Ok(home_dir()?.join(".config").join("herdcluster"))
    }
    #[cfg(windows)]
    {
        if let Ok(appdata) = std::env::var("APPDATA") {
            if !appdata.trim().is_empty() {
                return Ok(PathBuf::from(appdata).join("herdcluster"));
            }
        }
        Ok(home_dir()?.join(".herdcluster"))
    }
    #[cfg(not(any(unix, windows)))]
    {
        Ok(home_dir()?.join(".herdcluster"))
    }
}

fn home_dir() -> Result<PathBuf, ConfigError> {
    #[cfg(windows)]
    {
        if let Ok(p) = std::env::var("USERPROFILE") {
            if !p.trim().is_empty() {
                return Ok(PathBuf::from(p));
            }
        }
    }
    #[cfg(unix)]
    {
        if let Ok(p) = std::env::var("HOME") {
            if !p.trim().is_empty() {
                return Ok(PathBuf::from(p));
            }
        }
    }
    Err(ConfigError::NoHomeDir)
}

// ---- Path accessors (all lazy — no dirs created) ----

/// `<state_dir>/identity.key` — Ed25519 keypair file.
pub fn identity_key_path() -> Result<PathBuf, ConfigError> {
    Ok(state_dir()?.join("identity.key"))
}

/// `<state_dir>/cluster.json`.
pub fn cluster_json_path() -> Result<PathBuf, ConfigError> {
    Ok(state_dir()?.join(registry::CLUSTER_FILE_NAME))
}

/// `<state_dir>/workers.json`.
pub fn workers_json_path() -> Result<PathBuf, ConfigError> {
    Ok(state_dir()?.join(registry::WORKERS_FILE_NAME))
}

/// `<state_dir>/credentials/` — per-worker issued credentials.
pub fn credentials_dir() -> Result<PathBuf, ConfigError> {
    Ok(state_dir()?.join("credentials"))
}

/// `<state_dir>/runtime/` — herdr runtime supervisor state.
pub fn runtime_dir() -> Result<PathBuf, ConfigError> {
    Ok(state_dir()?.join("runtime"))
}

/// Create the state dir and the `credentials/` + `runtime/` subdirs
/// (mode `0700` on Unix). Idempotent.
pub fn ensure_layout() -> Result<PathBuf, ConfigError> {
    let dir = state_dir()?;
    fsutil::ensure_dir(&dir)?;
    fsutil::ensure_dir(&credentials_dir()?)?;
    fsutil::ensure_dir(&runtime_dir()?)?;
    Ok(dir)
}

/// Load `cluster.json`, creating a fresh cluster identity on first run.
/// Returns the [`registry::LoadOutcome`] so callers can surface quarantine
/// warnings. `HERDCLUSTER_PORT`, when set and valid, overrides `port` on the
/// returned copy in memory only (never persisted).
pub fn load_cluster() -> Result<registry::LoadOutcome<ClusterInfo>, ConfigError> {
    let path = cluster_json_path()?;
    let default_name = default_cluster_name();
    let mut outcome = registry::load_or_init_cluster(&path, &default_name)?;
    if outcome.created {
        // First run (or recovered from corruption): persist the fresh
        // cluster identity so `init` produces a stable cluster_id.
        let persisted = outcome.value.clone();
        registry::save_cluster_info(&path, &persisted)?;
    }
    if let Ok(raw) = std::env::var(ENV_PORT) {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            let port: u16 = trimmed
                .parse()
                .map_err(|_| ConfigError::InvalidPort(raw.clone()))?;
            outcome.value.port = port;
        }
    }
    Ok(outcome)
}

/// Save `cluster.json` under the state dir.
pub fn save_cluster(info: &ClusterInfo) -> Result<(), ConfigError> {
    registry::save_cluster_info(&cluster_json_path()?, info)?;
    Ok(())
}

/// Open the worker registry at `<state_dir>/workers.json`.
pub fn open_workers() -> Result<WorkerRegistry, ConfigError> {
    Ok(WorkerRegistry::open(workers_json_path()?)?)
}

/// Default cluster name: hostname, falling back to `"herdcluster"`.
fn default_cluster_name() -> String {
    hostname().unwrap_or_else(|| "herdcluster".to_string())
}

fn hostname() -> Option<String> {
    // std has no hostname API; read the conventional env vars first.
    for var in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(v) = std::env::var(var) {
            if !v.trim().is_empty() {
                return Some(v.trim().to_string());
            }
        }
    }
    #[cfg(unix)]
    {
        // `hostname` syscall via /etc or `uname -n` is overkill; try the
        // gethostname approach through libc-free path: read /proc/sys/kernel/hostname
        // on Linux, else fall back.
        if let Ok(s) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
            let s = s.trim().to_string();
            if !s.is_empty() {
                return Some(s);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Env-mutating tests share this lock to stay deterministic.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn tmpdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hc-config-{}-{}", tag, std::process::id()));
        let _ = fsutil::ensure_dir(&dir);
        dir
    }

    #[test]
    fn state_dir_env_override() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tmpdir("env");
        std::env::set_var(ENV_STATE_DIR, &dir);
        std::env::remove_var(ENV_PORT);
        assert_eq!(state_dir().unwrap(), dir);
        // Path accessors resolve under the override.
        assert_eq!(identity_key_path().unwrap(), dir.join("identity.key"));
        assert_eq!(cluster_json_path().unwrap(), dir.join("cluster.json"));
        assert_eq!(workers_json_path().unwrap(), dir.join("workers.json"));
        assert_eq!(credentials_dir().unwrap(), dir.join("credentials"));
        assert_eq!(runtime_dir().unwrap(), dir.join("runtime"));
        std::env::remove_var(ENV_STATE_DIR);
    }

    #[test]
    fn state_dir_empty_env_falls_through() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var(ENV_STATE_DIR, "   ");
        // Should not panic; result is platform default (HOME must exist in
        // test envs, but tolerate NoHomeDir).
        let _ = state_dir();
        std::env::remove_var(ENV_STATE_DIR);
    }

    #[test]
    fn ensure_layout_creates_tree() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tmpdir("layout");
        std::env::set_var(ENV_STATE_DIR, &dir);
        let root = ensure_layout().unwrap();
        assert_eq!(root, dir);
        assert!(dir.join("credentials").is_dir());
        assert!(dir.join("runtime").is_dir());
        std::env::remove_var(ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_cluster_creates_then_reuses() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tmpdir("cluster");
        std::env::set_var(ENV_STATE_DIR, &dir);
        std::env::remove_var(ENV_PORT);

        let first = load_cluster().unwrap().value;
        assert_eq!(first.port, DEFAULT_PORT);
        assert!(dir.join("cluster.json").exists());

        let second = load_cluster().unwrap().value;
        assert_eq!(first.cluster_id, second.cluster_id);
        std::env::remove_var(ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn port_env_override_is_in_memory_only() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tmpdir("port");
        std::env::set_var(ENV_STATE_DIR, &dir);
        std::env::set_var(ENV_PORT, "49999");

        let loaded = load_cluster().unwrap().value;
        assert_eq!(loaded.port, 49999);
        // Persisted file keeps the default.
        let on_disk: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(dir.join("cluster.json")).unwrap())
                .unwrap();
        assert_eq!(on_disk["port"], DEFAULT_PORT as u64);

        // Invalid port → error.
        std::env::set_var(ENV_PORT, "not-a-port");
        assert!(matches!(load_cluster(), Err(ConfigError::InvalidPort(_))));
        std::env::remove_var(ENV_STATE_DIR);
        std::env::remove_var(ENV_PORT);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_cluster_json_warns_and_recovers() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tmpdir("corrupt");
        std::env::set_var(ENV_STATE_DIR, &dir);
        std::env::remove_var(ENV_PORT);
        std::fs::write(dir.join("cluster.json"), "{bad").unwrap();

        let outcome = load_cluster().unwrap();
        assert!(outcome.warning.is_some());
        assert!(dir.join("cluster.json.bad").exists());
        std::env::remove_var(ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_workers_roundtrip() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tmpdir("workers");
        std::env::set_var(ENV_STATE_DIR, &dir);
        let mut reg = open_workers().unwrap();
        reg.upsert(registry::WorkerEntry {
            worker_id: "w1".into(),
            label: "m".into(),
            pubkey: "ab".repeat(32),
            paired_at: chrono::Utc::now(),
            last_seen: None,
            status: registry::WorkerStatus::Offline,
            resources: None,
        });
        reg.save().unwrap();
        let reg2 = open_workers().unwrap();
        assert_eq!(reg2.list().len(), 1);
        std::env::remove_var(ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
