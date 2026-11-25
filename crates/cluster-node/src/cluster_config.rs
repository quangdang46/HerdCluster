//! `herdcluster config` — read and edit `cluster.json` machine overrides
//! (PLAN.md §3.10 `config get|set`, beads `m6-caps`).
//!
//! Supported keys (MVP — `config set` for other fields can come later):
//!
//! - `keepawake <true|false>` — cluster-wide keep-awake flag (daemons
//!   inhibit sleep while local agents work; see `cluster_node::keepawake`)
//! - `machines.<label>.max_agents <0..=8>` — per-machine agent cap
//! - `machines.<label>.kinds_override <k1,k2,...>` — replace reported kinds
//! - `config unset machines.<label>.<field>` — drop the whole override
//!   for `<label>` (same key syntax as `set`; the field only selects
//!   the label — unset always removes the whole override)
//!
//! Every write is validated through
//! [`cluster_core::registry::MachineOverride::problems`] *before* saving,
//! so invalid values are rejected at the CLI, not just flagged by
//! `doctor`. Labels may contain dots (`mac.local`); keys are parsed from
//! the right.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use cluster_core::config as core_config;
use cluster_core::registry::{ClusterInfo, MachineOverride};
use serde::Serialize;
use thiserror::Error;

/// Errors from config get/set/unset.
#[derive(Debug, Error)]
pub enum ClusterConfigError {
    /// Underlying state-dir/config failure.
    #[error("config error: {0}")]
    Core(#[from] core_config::ConfigError),
    /// `cluster.json` does not exist — mutations only make sense on the
    /// controller (workers have no cluster config of their own).
    #[error("no cluster.json at {0} — run `herdcluster config` on the controller machine")]
    NoCluster(PathBuf),
    /// Key is not one of the supported `set` forms (`keepawake`,
    /// `machines.<label>.<field>`).
    #[error("unknown config key '{0}' (supported: keepawake, machines.<label>.max_agents, machines.<label>.kinds_override)")]
    UnknownKey(String),
    /// `unset` key is not a `machines.<label>.<field>` form — unset
    /// removes the whole override for the label, not a single field.
    #[error("unknown config unset key '{0}' (supported: machines.<label>.<field> — unset removes the whole override for the label)")]
    UnsetKey(String),
    /// Value failed to parse for the given key.
    #[error("invalid value for {key}: {reason}")]
    BadValue {
        /// The config key being set.
        key: String,
        /// Why the value was rejected.
        reason: String,
    },
    /// The resulting override still fails [`MachineOverride::problems`].
    #[error("invalid override for machines.{label}: {}", problems.join("; "))]
    Invalid {
        /// Machine label the override applies to.
        label: String,
        /// Validation problems.
        problems: Vec<String>,
    },
    /// `unset` named a label with no override.
    #[error("no machines.{0} override exists")]
    NoOverride(String),
    /// `machines.<label>` names no known machine — likely a typo; the
    /// override would be dead config.
    #[error("unknown machine '{label}' (known: {known})")]
    UnknownMachine {
        /// The label that was set.
        label: String,
        /// Comma-separated known labels (`local` + registered workers).
        known: String,
    },
}

/// Serializable view of `cluster.json` for `config get --json`.
#[derive(Debug, Clone, Serialize)]
pub struct ConfigView {
    /// Cluster display name.
    pub name: String,
    /// Cluster id.
    pub cluster_id: String,
    /// Controller listen port.
    pub port: u16,
    /// Wire protocol version.
    pub protocol_version: u32,
    /// Per-machine overrides, keyed by label.
    pub machines: BTreeMap<String, MachineOverride>,
    /// Keep-awake sleep-inhibition flag (cluster-wide).
    pub keepawake: bool,
}

impl From<&ClusterInfo> for ConfigView {
    fn from(info: &ClusterInfo) -> Self {
        Self {
            name: info.name.clone(),
            cluster_id: info.cluster_id.clone(),
            port: info.port,
            protocol_version: info.protocol_version,
            machines: info.machines.clone(),
            keepawake: info.keepawake,
        }
    }
}

/// Load `cluster.json` for reads. Unlike mutations this may create a
/// fresh cluster identity — same behavior as other fleet commands.
pub fn get() -> Result<ConfigView, ClusterConfigError> {
    core_config::ensure_layout()?;
    Ok(ConfigView::from(&core_config::load_cluster()?.value))
}

/// Lock file serializing `cluster.json` read-modify-write cycles
/// (`config set`/`unset`), same pattern as `project_state`'s
/// `agents.json.lock`. Without it two concurrent `config set`
/// invocations each load→mutate→save the whole file and one update is
/// silently lost (issue #90).
const CLUSTER_LOCK_FILE_NAME: &str = "cluster.json.lock";
/// How long a mutation waits for the `cluster.json` lock before failing.
const LOCK_TIMEOUT: Duration = Duration::from_secs(10);
/// Poll interval while spinning on the lock.
const LOCK_POLL: Duration = Duration::from_millis(10);
/// A lock file older than this is assumed abandoned (crashed writer)
/// and stolen — mutations are sub-millisecond, so this is generous.
const LOCK_STALE_AFTER: Duration = Duration::from_secs(60);

/// Set `machines.<label>.<field>` to `value`, validate, and persist.
pub fn set(key: &str, value: &str) -> Result<(), ClusterConfigError> {
    // Hold the mutation lock for the whole load→mutate→save cycle so
    // concurrent `config set|unset` processes can't drop each other's
    // updates.
    let _lock = acquire_cluster_lock()?;
    let mut info = load_existing()?;

    // Top-level scalar flags (no machines.<label> prefix).
    if key == "keepawake" {
        let v: bool = value
            .trim()
            .parse()
            .map_err(|_| ClusterConfigError::BadValue {
                key: key.to_string(),
                reason: format!("'{value}' is not a boolean (true/false)"),
            })?;
        info.keepawake = v;
        core_config::save_cluster(&info)?;
        return Ok(());
    }

    let (label, field) = parse_key(key)?;
    ensure_known_machine(&label)?;
    let entry = info.machines.entry(label.clone()).or_default();

    match field {
        Field::MaxAgents => {
            let n: u32 = value
                .trim()
                .parse()
                .map_err(|_| ClusterConfigError::BadValue {
                    key: key.to_string(),
                    reason: format!("'{value}' is not an integer"),
                })?;
            entry.max_agents = Some(n);
        }
        Field::KindsOverride => {
            let kinds: Vec<String> = value
                .split(',')
                .map(|k| k.trim().to_string())
                .filter(|k| !k.is_empty())
                .collect();
            if kinds.is_empty() {
                return Err(ClusterConfigError::BadValue {
                    key: key.to_string(),
                    reason: "kinds_override needs at least one kind (comma-separated)".into(),
                });
            }
            entry.kinds_override = Some(kinds);
        }
    }

    validate_and_save(&info, &label)?;
    Ok(())
}

/// Remove the whole override for the label in
/// `machines.<label>.<field>` — same key syntax as [`set`]; the field
/// suffix only selects the label (unset always drops the whole
/// override, never a single field).
pub fn unset(key: &str) -> Result<(), ClusterConfigError> {
    let _lock = acquire_cluster_lock()?;
    let mut info = load_existing()?;
    let label = parse_unset_key(key)?;
    if info.machines.remove(&label).is_none() {
        return Err(ClusterConfigError::NoOverride(label));
    }
    core_config::save_cluster(&info)?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Field {
    MaxAgents,
    KindsOverride,
}

/// Reject `machines.<label>` overrides for labels that match no known
/// machine (`local` + registered workers in `workers.json`). Prevents
/// dead config from typos; an empty/missing registry still allows
/// `local`.
fn ensure_known_machine(label: &str) -> Result<(), ClusterConfigError> {
    let registry = core_config::open_workers()?;
    let mut known: Vec<String> = vec!["local".to_string()];
    known.extend(registry.data().workers.iter().map(|w| w.label.clone()));
    if known.iter().any(|k| k == label) {
        return Ok(());
    }
    Err(ClusterConfigError::UnknownMachine {
        label: label.to_string(),
        known: known.join(", "),
    })
}

/// Split `machines.<label>.<field>` into `(label, field)`. Labels may
/// contain dots, so the field is matched as a suffix.
fn parse_key(key: &str) -> Result<(String, Field), ClusterConfigError> {
    let rest = key
        .strip_prefix("machines.")
        .ok_or_else(|| ClusterConfigError::UnknownKey(key.to_string()))?;
    if let Some(label) = rest.strip_suffix(".max_agents") {
        return non_empty_label(label, key).map(|l| (l.to_string(), Field::MaxAgents));
    }
    if let Some(label) = rest.strip_suffix(".kinds_override") {
        return non_empty_label(label, key).map(|l| (l.to_string(), Field::KindsOverride));
    }
    Err(ClusterConfigError::UnknownKey(key.to_string()))
}

/// `unset` takes the same `machines.<label>.<field>` key forms as
/// [`parse_key`] and resolves the label identically — labels may
/// contain dots and may themselves end in `.max_agents` /
/// `.kinds_override` (issue #91). The parsed field is discarded:
/// unset always removes the whole override for the label. Keys `set`
/// would reject get the unset-specific [`ClusterConfigError::UnsetKey`].
fn parse_unset_key(key: &str) -> Result<String, ClusterConfigError> {
    parse_key(key)
        .map(|(label, _field)| label)
        .map_err(|_| ClusterConfigError::UnsetKey(key.to_string()))
}

fn non_empty_label<'a>(label: &'a str, key: &str) -> Result<&'a str, ClusterConfigError> {
    if label.is_empty() {
        Err(ClusterConfigError::UnknownKey(key.to_string()))
    } else {
        Ok(label)
    }
}

/// Load `cluster.json` for mutation; refuses to create one (workers must
/// not grow a stray cluster config).
fn load_existing() -> Result<ClusterInfo, ClusterConfigError> {
    core_config::ensure_layout()?;
    let path = core_config::cluster_json_path()?;
    if !path.exists() {
        return Err(ClusterConfigError::NoCluster(path));
    }
    Ok(core_config::load_cluster()?.value)
}

/// Held `cluster.json` lock: the file's existence is the mutex;
/// dropping the guard removes it. See [`acquire_cluster_lock`].
struct ClusterLock {
    path: PathBuf,
}

impl Drop for ClusterLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Map a lock-filesystem failure into the config error type (via
/// [`core_config::ConfigError::Io`]).
fn lock_err(e: std::io::Error) -> ClusterConfigError {
    ClusterConfigError::Core(core_config::ConfigError::Io(e))
}

/// Take the `cluster.json` mutation lock, spinning on `create_new`
/// until [`LOCK_TIMEOUT`] elapses — the same pattern as
/// `project_state::acquire_agents_lock`. A lock file older than
/// [`LOCK_STALE_AFTER`] is treated as abandoned (its writer crashed)
/// and removed so a stale lock can never wedge `config` permanently.
fn acquire_cluster_lock() -> Result<ClusterLock, ClusterConfigError> {
    let path = core_config::cluster_json_path()?.with_file_name(CLUSTER_LOCK_FILE_NAME);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(lock_err)?;
    }
    let deadline = Instant::now() + LOCK_TIMEOUT;
    loop {
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(_) => return Ok(ClusterLock { path }),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let stale = std::fs::metadata(&path)
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|t| t.elapsed().ok())
                    .is_some_and(|age| age > LOCK_STALE_AFTER);
                if stale {
                    let _ = std::fs::remove_file(&path);
                    continue;
                }
                if Instant::now() >= deadline {
                    return Err(lock_err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            "timed out waiting for {} (another herdcluster process holds it)",
                            path.display()
                        ),
                    )));
                }
                std::thread::sleep(LOCK_POLL);
            }
            Err(e) => return Err(lock_err(e)),
        }
    }
}

/// Run the override validator on the touched label, then persist.
fn validate_and_save(info: &ClusterInfo, label: &str) -> Result<(), ClusterConfigError> {
    let problems = info
        .machines
        .get(label)
        .map(|o| o.problems())
        .unwrap_or_default();
    if !problems.is_empty() {
        return Err(ClusterConfigError::Invalid {
            label: label.to_string(),
            problems,
        });
    }
    core_config::save_cluster(info)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::STATE_DIR_ENV_LOCK;

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn tmpdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hc-cfg-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn seed_cluster(dir: &std::path::Path) {
        let info = ClusterInfo {
            cluster_id: "c1".into(),
            name: "c1".into(),
            port: 47820,
            protocol_version: 1,
            created_at: chrono::Utc::now(),
            machines: BTreeMap::new(),
            keepawake: true,
        };
        cluster_core::registry::save_cluster_info(&dir.join("cluster.json"), &info).unwrap();
    }

    fn seed_workers(dir: &std::path::Path, labels: &[&str]) {
        use cluster_core::registry::{WorkerEntry, WorkerStatus, WorkersFile};
        let workers = labels
            .iter()
            .map(|label| WorkerEntry {
                worker_id: format!("id-{label}"),
                label: label.to_string(),
                pubkey: "00".repeat(32),
                paired_at: chrono::Utc::now(),
                last_seen: None,
                status: WorkerStatus::Offline,
                resources: None,
            })
            .collect();
        let file = WorkersFile {
            version: cluster_core::registry::WORKERS_FILE_VERSION,
            workers,
        };
        cluster_core::registry::save_workers(&dir.join("workers.json"), &file).unwrap();
    }

    #[test]
    fn parse_key_handles_dotted_labels() {
        let (label, field) = parse_key("machines.mac.local.max_agents").unwrap();
        assert_eq!(label, "mac.local");
        assert_eq!(field, Field::MaxAgents);
        let (label, field) = parse_key("machines.wsl.kinds_override").unwrap();
        assert_eq!(label, "wsl");
        assert_eq!(field, Field::KindsOverride);
        assert!(matches!(
            parse_key("machines.wsl"),
            Err(ClusterConfigError::UnknownKey(_))
        ));
        assert!(matches!(
            parse_key("name"),
            Err(ClusterConfigError::UnknownKey(_))
        ));
        assert!(matches!(
            parse_key("machines..max_agents"),
            Err(ClusterConfigError::UnknownKey(_))
        ));
    }

    #[test]
    fn parse_unset_key_mirrors_set_parsing() {
        // Same `machines.<label>.<field>` forms as `set`; the field is
        // parsed but discarded (unset removes the whole override).
        assert_eq!(parse_unset_key("machines.wsl.max_agents").unwrap(), "wsl");
        assert_eq!(
            parse_unset_key("machines.wsl.kinds_override").unwrap(),
            "wsl"
        );
        // Dotted labels are fine.
        assert_eq!(
            parse_unset_key("machines.mac.local.max_agents").unwrap(),
            "mac.local"
        );
        // A label literally ending in a field suffix is addressed by
        // appending a field, exactly like `set` (issue #91).
        assert_eq!(
            parse_unset_key("machines.foo.max_agents.max_agents").unwrap(),
            "foo.max_agents"
        );
        assert_eq!(
            parse_unset_key("machines.foo.max_agents.kinds_override").unwrap(),
            "foo.max_agents"
        );
        // Bare `machines.<label>` and non-machine keys are rejected the
        // same way `set` rejects them, with the unset-specific error
        // (issue #21).
        for key in [
            "machines.wsl",
            "machines.",
            "machines..max_agents",
            "keepawake",
            "name",
        ] {
            match parse_unset_key(key) {
                Err(ClusterConfigError::UnsetKey(k)) => assert_eq!(k, key),
                other => panic!("expected UnsetKey for {key}, got {other:?}"),
            }
        }
    }

    #[test]
    fn unset_error_message_describes_unset_syntax() {
        let err = parse_unset_key("machines.wsl").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("machines.<label>"), "{msg}");
        assert!(msg.contains("whole override"), "{msg}");
        // Must not leak the `set`-oriented key list.
        assert!(!msg.contains("max_agents,"), "{msg}");
    }

    #[tokio::test]
    async fn set_validates_and_persists() {
        let _g1 = STATE_DIR_ENV_LOCK.lock().await;
        let _g2 = ENV_LOCK.lock().await;
        let dir = tmpdir("set");
        seed_cluster(&dir);
        seed_workers(&dir, &["wsl"]);
        std::env::set_var(core_config::ENV_STATE_DIR, &dir);

        set("machines.wsl.max_agents", "6").unwrap();
        set("machines.wsl.kinds_override", "claude, codex").unwrap();
        let view = get().unwrap();
        let o = &view.machines["wsl"];
        assert_eq!(o.max_agents, Some(6));
        assert_eq!(
            o.kinds_override.as_deref(),
            Some(&["claude".to_string(), "codex".to_string()][..])
        );

        // Invalid value rejected before save.
        let err = set("machines.wsl.max_agents", "42").unwrap_err();
        assert!(matches!(err, ClusterConfigError::Invalid { .. }));
        assert_eq!(get().unwrap().machines["wsl"].max_agents, Some(6));

        // Bad scalar rejected.
        assert!(matches!(
            set("machines.wsl.max_agents", "abc"),
            Err(ClusterConfigError::BadValue { .. })
        ));

        unset("machines.wsl.max_agents").unwrap();
        assert!(get().unwrap().machines.is_empty());
        assert!(matches!(
            unset("machines.wsl.kinds_override"),
            Err(ClusterConfigError::NoOverride(_))
        ));

        std::env::remove_var(core_config::ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn set_refuses_to_create_cluster_json() {
        let _g1 = STATE_DIR_ENV_LOCK.lock().await;
        let _g2 = ENV_LOCK.lock().await;
        let dir = tmpdir("nocluster");
        std::env::set_var(core_config::ENV_STATE_DIR, &dir);

        let err = set("machines.wsl.max_agents", "4").unwrap_err();
        assert!(matches!(err, ClusterConfigError::NoCluster(_)));
        assert!(!dir.join("cluster.json").exists());

        std::env::remove_var(core_config::ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn set_rejects_unknown_machine_label() {
        let _g1 = STATE_DIR_ENV_LOCK.lock().await;
        let _g2 = ENV_LOCK.lock().await;
        let dir = tmpdir("badlabel");
        seed_cluster(&dir);
        seed_workers(&dir, &["wsl"]);
        std::env::set_var(core_config::ENV_STATE_DIR, &dir);

        // "local" and registered workers are accepted...
        set("machines.local.max_agents", "4").unwrap();
        // ...anything else is rejected before touching cluster.json.
        let err = set("machines.NOSUCHBOX.max_agents", "3").unwrap_err();
        match err {
            ClusterConfigError::UnknownMachine { label, known } => {
                assert_eq!(label, "NOSUCHBOX");
                assert!(known.contains("local") && known.contains("wsl"), "{known}");
            }
            other => panic!("expected UnknownMachine, got {other}"),
        }
        assert!(!get().unwrap().machines.contains_key("NOSUCHBOX"));

        std::env::remove_var(core_config::ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn unset_addresses_labels_ending_in_field_suffix() {
        let _g1 = STATE_DIR_ENV_LOCK.lock().await;
        let _g2 = ENV_LOCK.lock().await;
        let dir = tmpdir("suffixlabel");
        seed_cluster(&dir);
        seed_workers(&dir, &["foo", "foo.max_agents"]);
        std::env::set_var(core_config::ENV_STATE_DIR, &dir);

        // `foo.max_agents` is a legitimate label; `set` reaches it via
        // the doubled suffix, and `unset` must too (issue #91).
        set("machines.foo.max_agents.max_agents", "3").unwrap();
        set("machines.foo.max_agents", "5").unwrap();
        let view = get().unwrap();
        assert_eq!(view.machines["foo.max_agents"].max_agents, Some(3));
        assert_eq!(view.machines["foo"].max_agents, Some(5));

        // Unsetting the doubled-suffix key drops only that label's
        // override; the `foo` override is untouched.
        unset("machines.foo.max_agents.max_agents").unwrap();
        let view = get().unwrap();
        assert!(!view.machines.contains_key("foo.max_agents"));
        assert_eq!(view.machines["foo"].max_agents, Some(5));

        // The bare-looking key still means label `foo` + field — same
        // resolution as `set` — so it removes `foo`'s override.
        unset("machines.foo.max_agents").unwrap();
        assert!(get().unwrap().machines.is_empty());

        std::env::remove_var(core_config::ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn concurrent_set_unset_lose_no_updates() {
        let _g1 = STATE_DIR_ENV_LOCK.lock().await;
        let _g2 = ENV_LOCK.lock().await;
        let dir = tmpdir("race");
        seed_cluster(&dir);
        let labels: Vec<String> = (0..8).map(|i| format!("w{i}")).collect();
        let refs: Vec<&str> = labels.iter().map(String::as_str).collect();
        seed_workers(&dir, &refs);
        std::env::set_var(core_config::ENV_STATE_DIR, &dir);

        // Each thread performs several load→mutate→save cycles on
        // distinct labels. Without the cluster.json lock these
        // whole-file read-modify-writes silently drop each other's
        // entries (issue #90).
        let mut handles = Vec::new();
        for (i, label) in labels.iter().enumerate() {
            let label = label.clone();
            handles.push(std::thread::spawn(move || {
                for round in 0..3 {
                    let n = ((i + round) % 8 + 1) as u32;
                    set(&format!("machines.{label}.max_agents"), &n.to_string()).unwrap();
                    set(&format!("machines.{label}.kinds_override"), "claude,codex").unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let view = get().unwrap();
        assert_eq!(view.machines.len(), labels.len());
        for label in &labels {
            let o = &view.machines[label];
            assert!(o.max_agents.is_some(), "missing max_agents for {label}");
            assert_eq!(
                o.kinds_override.as_deref(),
                Some(&["claude".to_string(), "codex".to_string()][..]),
                "missing kinds_override for {label}"
            );
        }

        // Concurrent unsets are serialized too: each thread removes a
        // distinct label and every removal must land.
        let mut handles = Vec::new();
        for label in labels.iter().take(4) {
            let label = label.clone();
            handles.push(std::thread::spawn(move || {
                unset(&format!("machines.{label}.max_agents")).unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let view = get().unwrap();
        assert_eq!(view.machines.len(), labels.len() - 4);
        for label in labels.iter().skip(4) {
            assert!(view.machines.contains_key(label), "lost {label}");
        }

        std::env::remove_var(core_config::ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
