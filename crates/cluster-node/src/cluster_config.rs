//! `herdcluster config` — read and edit `cluster.json` machine overrides
//! (PLAN.md §3.10 `config get|set`, beads `m6-caps`).
//!
//! Supported keys (MVP — `config set` for other fields can come later):
//!
//! - `machines.<label>.max_agents <0..=8>` — per-machine agent cap
//! - `machines.<label>.kinds_override <k1,k2,...>` — replace reported kinds
//! - `config unset machines.<label>` — drop the whole override
//!
//! Every write is validated through
//! [`cluster_core::registry::MachineOverride::problems`] *before* saving,
//! so invalid values are rejected at the CLI, not just flagged by
//! `doctor`. Labels may contain dots (`mac.local`); keys are parsed from
//! the right.

use std::collections::BTreeMap;
use std::path::PathBuf;

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
    /// Key is not one of the supported `machines.<label>.<field>` forms.
    #[error("unknown config key '{0}' (supported: machines.<label>.max_agents, machines.<label>.kinds_override)")]
    UnknownKey(String),
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
}

impl From<&ClusterInfo> for ConfigView {
    fn from(info: &ClusterInfo) -> Self {
        Self {
            name: info.name.clone(),
            cluster_id: info.cluster_id.clone(),
            port: info.port,
            protocol_version: info.protocol_version,
            machines: info.machines.clone(),
        }
    }
}

/// Load `cluster.json` for reads. Unlike mutations this may create a
/// fresh cluster identity — same behavior as other fleet commands.
pub fn get() -> Result<ConfigView, ClusterConfigError> {
    core_config::ensure_layout()?;
    Ok(ConfigView::from(&core_config::load_cluster()?.value))
}

/// Set `machines.<label>.<field>` to `value`, validate, and persist.
pub fn set(key: &str, value: &str) -> Result<(), ClusterConfigError> {
    let mut info = load_existing()?;
    let (label, field) = parse_key(key)?;
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

/// Remove the whole `machines.<label>` override.
pub fn unset(key: &str) -> Result<(), ClusterConfigError> {
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

/// `unset` takes the bare `machines.<label>` form (labels may contain
/// dots). Field-suffixed keys are rejected — unset removes the whole
/// override for the label.
fn parse_unset_key(key: &str) -> Result<String, ClusterConfigError> {
    let label = key
        .strip_prefix("machines.")
        .filter(|l| !l.is_empty())
        .ok_or_else(|| ClusterConfigError::UnknownKey(key.to_string()))?;
    if label.ends_with(".max_agents") || label.ends_with(".kinds_override") {
        return Err(ClusterConfigError::UnknownKey(key.to_string()));
    }
    Ok(label.to_string())
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
        };
        cluster_core::registry::save_cluster_info(&dir.join("cluster.json"), &info).unwrap();
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

    #[tokio::test]
    async fn set_validates_and_persists() {
        let _g1 = STATE_DIR_ENV_LOCK.lock().await;
        let _g2 = ENV_LOCK.lock().await;
        let dir = tmpdir("set");
        seed_cluster(&dir);
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

        unset("machines.wsl").unwrap();
        assert!(get().unwrap().machines.is_empty());
        assert!(matches!(
            unset("machines.wsl"),
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
}
