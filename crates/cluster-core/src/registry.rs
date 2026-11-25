//! Worker registry + `cluster.json` schema and persistence.
//!
//! Two JSON files live under the herdcluster state dir (paths come from
//! [`crate::config`]; this module only sees explicit paths so it stays
//! testable):
//!
//! - `cluster.json` — [`ClusterInfo`]: cluster identity created by `init`,
//!   including per-machine scheduler overrides (`machines.<label>`).
//! - `workers.json` — [`WorkersFile`]: the controller-side registry of paired
//!   workers, their pubkeys, last-seen timestamps, derived status, and the
//!   latest resource snapshot.
//!
//! All writes are atomic (temp + rename + fsync) with `0600` permissions.
//! A corrupt file is quarantined by renaming it to `<name>.bad` and a fresh
//! default is returned together with a human-readable warning — callers
//! should surface the warning (e.g. via `doctor`).
//!
//! Status FSM: `last_seen` age vs. the heartbeat cadence (5 s beats,
//! degraded after 3 misses ≈ 15 s, offline after 30 s — PLAN.md §3.5).

use crate::fsutil;
use crate::protocol::Resources;
use chrono::{DateTime, Duration, Utc};
use rand_core::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Current `workers.json` schema version.
pub const WORKERS_FILE_VERSION: u32 = 1;
/// Heartbeat interval workers are expected to sustain (5 s).
pub const HEARTBEAT_INTERVAL_SECS: i64 = 5;
/// Consecutive missed heartbeats (≈15 s) before `degraded`.
pub const MISSED_HEARTBEATS_DEGRADED: i64 = 3;
/// Age of `last_seen` beyond which a worker is `offline` (30 s).
pub const OFFLINE_AFTER_SECS: i64 = 30;
/// Derived degraded threshold: `HEARTBEAT_INTERVAL_SECS * MISSED_HEARTBEATS_DEGRADED`.
pub const DEGRADED_AFTER_SECS: i64 = HEARTBEAT_INTERVAL_SECS * MISSED_HEARTBEATS_DEGRADED;

/// Registry file name for `cluster.json`.
pub const CLUSTER_FILE_NAME: &str = "cluster.json";
/// Registry file name for `workers.json`.
pub const WORKERS_FILE_NAME: &str = "workers.json";

/// Errors from registry load/save/migration.
#[derive(Debug, Error)]
pub enum RegistryError {
    /// Underlying filesystem error.
    #[error("registry io error: {0}")]
    Io(#[from] std::io::Error),
    /// File parsed but its schema version is newer than this build supports.
    #[error("unsupported registry schema version {found} (max {max})")]
    UnsupportedVersion {
        /// Version found in the file.
        found: u32,
        /// Highest version this build understands.
        max: u32,
    },
    /// JSON was valid but did not match the schema (and was quarantined).
    #[error("registry file corrupt: {0}")]
    Corrupt(String),
}

/// Result of a load that may have quarantined a corrupt file.
#[derive(Debug)]
pub struct LoadOutcome<T> {
    /// The loaded (or freshly-created default) value.
    pub value: T,
    /// Non-fatal warning, e.g. "workers.json was corrupt and has been
    /// quarantined to workers.json.bad".
    pub warning: Option<String>,
    /// True when `value` is a fresh default (file missing or quarantined)
    /// rather than parsed content — callers may want to persist it.
    pub created: bool,
}

fn default_worker_status() -> WorkerStatus {
    WorkerStatus::Offline
}

fn default_keepawake() -> bool {
    true
}

/// Worker lifecycle status, derived from `last_seen` age.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    /// Seen within [`DEGRADED_AFTER_SECS`].
    #[default]
    Online,
    /// Seen within [`OFFLINE_AFTER_SECS`] but older than degraded threshold.
    Degraded,
    /// Never seen or older than [`OFFLINE_AFTER_SECS`].
    Offline,
}

/// Derive the status for a `last_seen` timestamp at time `now`.
/// `None` (never seen) → [`WorkerStatus::Offline`].
pub fn derive_status(last_seen: Option<DateTime<Utc>>, now: DateTime<Utc>) -> WorkerStatus {
    let Some(seen) = last_seen else {
        return WorkerStatus::Offline;
    };
    let age = now.signed_duration_since(seen);
    if age <= Duration::seconds(DEGRADED_AFTER_SECS) {
        WorkerStatus::Online
    } else if age <= Duration::seconds(OFFLINE_AFTER_SECS) {
        WorkerStatus::Degraded
    } else {
        WorkerStatus::Offline
    }
}

/// Per-machine scheduler override stored in `cluster.json` under
/// `machines.<label>` (PLAN.md §3.7 caps).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MachineOverride {
    /// Cap on concurrent agents for this machine. `0` is valid and means
    /// the machine accepts no agents (the scheduler reports it at
    /// capacity). Values above [`crate::scheduler::MAX_AGENTS_CEILING`]
    /// are invalid — see [`MachineOverride::problems`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_agents: Option<u32>,
    /// If set, replaces the machine's reported `kinds[]` for placement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kinds_override: Option<Vec<String>>,
}

impl MachineOverride {
    /// Validate override values against scheduler policy (PLAN.md §3.7).
    ///
    /// Returns human-readable problems — an empty vec means the override
    /// is sane. This is the "reject invalid values" layer: JSON parsing
    /// accepts any `u32`, so range/semantic checks live here and are
    /// surfaced by `herdcluster doctor` (the scheduler still clamps
    /// defensively at scoring time).
    pub fn problems(&self) -> Vec<String> {
        let mut problems = Vec::new();
        if let Some(max) = self.max_agents {
            if max > crate::scheduler::MAX_AGENTS_CEILING {
                problems.push(format!(
                    "max_agents {max} exceeds the hard ceiling of {}",
                    crate::scheduler::MAX_AGENTS_CEILING
                ));
            }
        }
        if let Some(kinds) = &self.kinds_override {
            if kinds.is_empty() {
                problems.push(
                    "kinds_override is an empty list — the machine would match no agent kinds"
                        .to_string(),
                );
            }
            for kind in kinds {
                if kind.trim().is_empty() {
                    problems.push("kinds_override contains an empty kind".to_string());
                    break;
                }
            }
        }
        problems
    }
}

/// `cluster.json` — cluster identity created by `init`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterInfo {
    /// Random cluster identifier (hex).
    pub cluster_id: String,
    /// Human-facing cluster/machine name (default: hostname).
    pub name: String,
    /// Port the controller listens on.
    pub port: u16,
    /// Wire protocol version this cluster speaks.
    pub protocol_version: u32,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Per-machine scheduler overrides keyed by machine label.
    #[serde(default)]
    pub machines: BTreeMap<String, MachineOverride>,
    /// Keep-awake: daemons hold a platform sleep-inhibition assertion
    /// while local agents are `working`. `false` disables; absent in
    /// older `cluster.json` files → `true`.
    #[serde(default = "default_keepawake")]
    pub keepawake: bool,
}

impl ClusterInfo {
    /// Validate every `machines.<label>` override. Returns
    /// `(label, problem)` pairs in label order — empty when all
    /// overrides are sane. Surfaced by `herdcluster doctor`.
    pub fn machine_override_problems(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for (label, o) in &self.machines {
            for p in o.problems() {
                out.push((label.clone(), p));
            }
        }
        out
    }

    /// Labels in `machines` that match no known machine — dead config
    /// the scheduler will never apply. Callers supply the known-label
    /// set (`local` + `workers.json` labels); this stays a pure value
    /// check so it is unit-testable without I/O.
    pub fn unknown_machine_overrides(&self, known_labels: &BTreeSet<String>) -> Vec<String> {
        self.machines
            .keys()
            .filter(|label| !known_labels.contains(*label))
            .cloned()
            .collect()
    }
}

/// One row of `workers.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerEntry {
    /// Stable worker identifier (assigned at pairing time).
    pub worker_id: String,
    /// Human-facing machine label, e.g. `"desktop-win"`.
    pub label: String,
    /// Worker Ed25519 public key, hex-encoded.
    pub pubkey: String,
    /// When pairing completed.
    pub paired_at: DateTime<Utc>,
    /// Most recent heartbeat/resources timestamp; `None` if never seen.
    #[serde(default)]
    pub last_seen: Option<DateTime<Utc>>,
    /// Derived status (recomputed by [`WorkerRegistry::mark_seen`] and
    /// [`WorkerRegistry::refresh_statuses`]). This is only a persisted
    /// cache of `last_seen` age — readers that make placement decisions
    /// (e.g. `cluster-fleet`'s `FleetContext`) recompute it on load rather
    /// than trusting the stored value. Defaults to `offline` when
    /// absent so never-seen workers don't look alive.
    #[serde(default = "default_worker_status")]
    pub status: WorkerStatus,
    /// Latest resource snapshot, if the worker has reported one.
    #[serde(default)]
    pub resources: Option<Resources>,
}

/// `workers.json` top-level schema (versioned for future migrations).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkersFile {
    /// Schema version; must be `<= WORKERS_FILE_VERSION`.
    #[serde(default)]
    pub version: u32,
    /// Registered workers.
    #[serde(default)]
    pub workers: Vec<WorkerEntry>,
}

impl WorkersFile {
    /// An empty registry at the current schema version.
    pub fn new() -> Self {
        Self {
            version: WORKERS_FILE_VERSION,
            workers: Vec::new(),
        }
    }
}

impl Default for WorkersFile {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a random hex identifier (cluster_id, worker_id, …) of
/// exactly `hex_chars` characters. Odd lengths round up to whole bytes
/// and the extra nibble is truncated.
pub fn random_id(hex_chars: usize) -> String {
    let bytes = hex_chars.div_ceil(2).max(1);
    let mut buf = vec![0u8; bytes];
    rand_core::OsRng.fill_bytes(&mut buf);
    let mut s = fsutil::hex_encode(&buf);
    s.truncate(hex_chars);
    s
}

/// Rename `path` to `<full-name>.bad` (e.g. `workers.json` →
/// `workers.json.bad`), replacing any previous quarantine file.
fn quarantine(path: &Path) -> std::io::Result<PathBuf> {
    let mut bad = path.as_os_str().to_os_string();
    bad.push(".bad");
    let bad = PathBuf::from(bad);
    let _ = std::fs::remove_file(&bad);
    std::fs::rename(path, &bad)?;
    Ok(bad)
}

/// Load and parse a JSON file; on parse failure quarantine it to `<name>.bad`
/// and invoke `fallback` to produce a fresh default.
fn load_or_quarantine<T, F>(path: &Path, fallback: F) -> Result<LoadOutcome<T>, RegistryError>
where
    T: for<'de> Deserialize<'de>,
    F: FnOnce() -> T,
{
    let raw = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LoadOutcome {
                value: fallback(),
                warning: None,
                created: true,
            })
        }
        Err(e) => return Err(RegistryError::Io(e)),
    };
    match serde_json::from_str::<T>(&raw) {
        Ok(v) => Ok(LoadOutcome {
            value: v,
            warning: None,
            created: false,
        }),
        Err(e) => {
            let bad = quarantine(path)?;
            Ok(LoadOutcome {
                value: fallback(),
                warning: Some(format!(
                    "{} was corrupt ({e}) and has been quarantined to {}",
                    path.display(),
                    bad.display()
                )),
                created: true,
            })
        }
    }
}

fn check_version(version: u32, max: u32) -> Result<(), RegistryError> {
    if version > max {
        Err(RegistryError::UnsupportedVersion {
            found: version,
            max,
        })
    } else {
        Ok(())
    }
}

/// Load `cluster.json`, or create a fresh [`ClusterInfo`] if missing.
/// `default_name` is used for new clusters (typically the hostname).
/// A corrupt file is quarantined and a fresh cluster created with a warning.
pub fn load_or_init_cluster(
    path: &Path,
    default_name: &str,
) -> Result<LoadOutcome<ClusterInfo>, RegistryError> {
    load_or_quarantine::<ClusterInfo, _>(path, || ClusterInfo {
        cluster_id: random_id(32),
        name: default_name.to_string(),
        port: crate::config::DEFAULT_PORT,
        protocol_version: crate::protocol::PROTOCOL_VERSION,
        created_at: Utc::now(),
        machines: BTreeMap::new(),
        keepawake: true,
    })
}

/// Save `cluster.json` atomically with `0600` perms.
pub fn save_cluster_info(path: &Path, info: &ClusterInfo) -> Result<(), RegistryError> {
    let json =
        serde_json::to_vec_pretty(info).map_err(|e| RegistryError::Corrupt(e.to_string()))?;
    fsutil::atomic_write(path, &json)?;
    Ok(())
}

/// Load `workers.json`; missing file → empty registry; corrupt file →
/// quarantined to `workers.json.bad` and an empty registry + warning.
pub fn load_workers(path: &Path) -> Result<LoadOutcome<WorkersFile>, RegistryError> {
    let outcome = load_or_quarantine::<WorkersFile, _>(path, WorkersFile::new)?;
    check_version(outcome.value.version, WORKERS_FILE_VERSION)?;
    // Migration path: version 0 (field absent in a legacy file) is accepted
    // and normalized to the current version on next save.
    Ok(outcome)
}

/// Save `workers.json` atomically with `0600` perms.
pub fn save_workers(path: &Path, file: &WorkersFile) -> Result<(), RegistryError> {
    let json =
        serde_json::to_vec_pretty(file).map_err(|e| RegistryError::Corrupt(e.to_string()))?;
    fsutil::atomic_write(path, &json)?;
    Ok(())
}

/// Stateful handle over a `workers.json` file: load once, mutate, save.
pub struct WorkerRegistry {
    path: PathBuf,
    data: WorkersFile,
    /// Warning produced while loading (e.g. quarantine notice).
    pub load_warning: Option<String>,
}

impl WorkerRegistry {
    /// Open (or create) the registry at `path`.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, RegistryError> {
        let path = path.into();
        let outcome = load_workers(&path)?;
        Ok(Self {
            path,
            data: outcome.value,
            load_warning: outcome.warning,
        })
    }

    /// Path of the backing file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The underlying file data (read-only view).
    pub fn data(&self) -> &WorkersFile {
        &self.data
    }

    /// Insert or update a worker. Matches first by `worker_id`, then by
    /// `pubkey` (the cryptographic identity); a pubkey match under a
    /// different id updates that row in place.
    ///
    /// A pubkey match is a *re-pair* of the same worker: the stable
    /// `worker_id` and all liveness state (`paired_at`, `last_seen`,
    /// `status`, `resources`) are preserved — only `label` (and any other
    /// caller-supplied identity fields) are refreshed. Rotating
    /// `worker_id` here would orphan the controller's per-worker bridge
    /// map (keyed by `worker_id`): the stale disconnect would resolve the
    /// *new* id, never remove the old bridge, and the replacement bind
    /// would hit `SocketInUse` — permanently bridgeless (issue #98).
    pub fn upsert(&mut self, mut entry: WorkerEntry) {
        if let Some(pos) = self
            .data
            .workers
            .iter()
            .position(|w| w.worker_id == entry.worker_id)
        {
            self.data.workers[pos] = entry;
            return;
        }
        if let Some(pos) = self
            .data
            .workers
            .iter()
            .position(|w| w.pubkey == entry.pubkey)
        {
            let existing = &mut self.data.workers[pos];
            entry.worker_id = existing.worker_id.clone();
            entry.paired_at = existing.paired_at;
            entry.last_seen = existing.last_seen;
            entry.status = existing.status;
            entry.resources = existing.resources.clone();
            *existing = entry;
            return;
        }
        self.data.workers.push(entry);
    }

    /// Remove a worker by `worker_id`. Returns whether one was removed.
    pub fn remove(&mut self, worker_id: &str) -> bool {
        let before = self.data.workers.len();
        self.data.workers.retain(|w| w.worker_id != worker_id);
        self.data.workers.len() != before
    }

    /// Look up a worker by `worker_id`.
    pub fn get(&self, worker_id: &str) -> Option<&WorkerEntry> {
        self.data.workers.iter().find(|w| w.worker_id == worker_id)
    }

    /// All registered workers.
    pub fn list(&self) -> &[WorkerEntry] {
        &self.data.workers
    }

    /// Record a heartbeat/resource report: updates `last_seen`, derives
    /// `status`, and stores the resource snapshot.
    pub fn mark_seen(
        &mut self,
        worker_id: &str,
        resources: Option<Resources>,
        now: DateTime<Utc>,
    ) -> Result<(), RegistryError> {
        let entry = self
            .data
            .workers
            .iter_mut()
            .find(|w| w.worker_id == worker_id)
            .ok_or_else(|| RegistryError::Corrupt(format!("unknown worker {worker_id}")))?;
        entry.last_seen = Some(now);
        if resources.is_some() {
            entry.resources = resources;
        }
        entry.status = derive_status(entry.last_seen, now);
        Ok(())
    }

    /// Recompute every worker's stored `status` against `now` without
    /// touching `last_seen` (e.g. for `machines` display).
    pub fn refresh_statuses(&mut self, now: DateTime<Utc>) {
        for w in &mut self.data.workers {
            w.status = derive_status(w.last_seen, now);
        }
    }

    /// Persist to disk (atomic, `0600`). Normalizes `version` to current.
    pub fn save(&mut self) -> Result<(), RegistryError> {
        self.data.version = WORKERS_FILE_VERSION;
        save_workers(&self.path, &self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hc-registry-{}-{}", tag, std::process::id()));
        let _ = fsutil::ensure_dir(&dir);
        dir
    }

    /// Deterministic unique-looking pubkey per worker id (tests only).
    fn pubkey_for(id: &str) -> String {
        let mut buf = [0u8; 32];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = id.as_bytes()[i % id.len()] ^ (i as u8);
        }
        fsutil::hex_encode(&buf)
    }

    fn entry(id: &str, label: &str) -> WorkerEntry {
        WorkerEntry {
            worker_id: id.into(),
            label: label.into(),
            pubkey: pubkey_for(id),
            paired_at: Utc::now(),
            last_seen: None,
            status: WorkerStatus::Offline,
            resources: None,
        }
    }

    fn resources(agent_count: u32) -> Resources {
        Resources {
            cpu_cores: 8,
            mem_total_mb: 16384,
            mem_available_mb: 4096,
            agent_count,
            max_agents: 4,
            load_avg: vec![0.1],
            kinds: vec!["claude".into()],
        }
    }

    #[test]
    fn random_id_returns_exact_length() {
        for n in [1usize, 2, 5, 16, 31, 32, 33] {
            let id = random_id(n);
            assert_eq!(id.len(), n, "random_id({n}) gave {id:?}");
            assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
        }
        // Distinct calls produce distinct ids (sanity, not a guarantee).
        assert_ne!(random_id(16), random_id(16));
    }

    #[test]
    fn derive_status_thresholds() {
        let now = Utc::now();
        assert_eq!(derive_status(None, now), WorkerStatus::Offline);
        assert_eq!(
            derive_status(Some(now - Duration::seconds(10)), now),
            WorkerStatus::Online
        );
        assert_eq!(
            derive_status(Some(now - Duration::seconds(20)), now),
            WorkerStatus::Degraded
        );
        assert_eq!(
            derive_status(Some(now - Duration::seconds(31)), now),
            WorkerStatus::Offline
        );
    }

    #[test]
    fn workers_roundtrip_and_crud() {
        let dir = tmpdir("crud");
        let path = dir.join(WORKERS_FILE_NAME);
        let mut reg = WorkerRegistry::open(&path).unwrap();
        assert!(reg.load_warning.is_none());

        reg.upsert(entry("w1", "desktop"));
        reg.upsert(entry("w2", "wsl"));
        reg.save().unwrap();

        // Reopen and verify persistence.
        let mut reg2 = WorkerRegistry::open(&path).unwrap();
        assert_eq!(reg2.list().len(), 2);
        assert!(reg2.get("w1").is_some());
        assert!(reg2.get("nope").is_none());

        // Upsert by pubkey with a different id updates in place and
        // preserves the stable worker_id (issue #98).
        let mut e = entry("w3", "renamed");
        e.pubkey = pubkey_for("w1"); // same pubkey as w1
        reg2.upsert(e);
        reg2.save().unwrap();
        let reg3 = WorkerRegistry::open(&path).unwrap();
        assert_eq!(reg3.list().len(), 2);
        assert_eq!(reg3.list()[0].worker_id, "w1");
        assert_eq!(reg3.list()[0].label, "renamed");

        // Removal.
        let mut reg4 = WorkerRegistry::open(&path).unwrap();
        assert!(reg4.remove("w1"));
        assert!(!reg4.remove("w1"));
        reg4.save().unwrap();
        let reg5 = WorkerRegistry::open(&path).unwrap();
        assert_eq!(reg5.list().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn upsert_same_pubkey_preserves_worker_id_and_state() {
        // Issue #98: re-pairing the same pubkey must keep worker_id and
        // liveness state — the controller's bridge map is keyed by
        // worker_id, so rotating it orphans the old bridge entry.
        let dir = tmpdir("upsert-preserve");
        let path = dir.join(WORKERS_FILE_NAME);
        let mut reg = WorkerRegistry::open(&path).unwrap();
        let mut w = entry("w1", "old-label");
        w.last_seen = Some(Utc::now());
        w.status = WorkerStatus::Online;
        w.resources = Some(resources(3));
        let paired_at = w.paired_at;
        reg.upsert(w);

        // Re-pair: same pubkey, fresh random id + label, reset state.
        let mut e = entry("w9-newid", "new-label");
        e.pubkey = pubkey_for("w1");
        e.last_seen = None;
        e.status = WorkerStatus::Offline;
        e.resources = None;
        reg.upsert(e);

        assert_eq!(reg.list().len(), 1);
        assert!(reg.get("w9-newid").is_none());
        let w = reg.get("w1").expect("worker_id preserved");
        assert_eq!(w.label, "new-label");
        assert_eq!(w.paired_at, paired_at);
        assert!(w.last_seen.is_some());
        assert_eq!(w.status, WorkerStatus::Online);
        assert_eq!(w.resources.as_ref().unwrap().agent_count, 3);

        // worker_id match still replaces wholesale (explicit id update).
        let mut by_id = entry("w1", "id-update");
        by_id.pubkey = "ff".repeat(32);
        by_id.last_seen = None;
        reg.upsert(by_id);
        let w = reg.get("w1").unwrap();
        assert_eq!(w.label, "id-update");
        assert_eq!(w.pubkey, "ff".repeat(32));
        assert!(w.last_seen.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mark_seen_updates_status_and_resources() {
        let dir = tmpdir("seen");
        let path = dir.join(WORKERS_FILE_NAME);
        let mut reg = WorkerRegistry::open(&path).unwrap();
        reg.upsert(entry("w1", "m"));
        let now = Utc::now();
        reg.mark_seen("w1", Some(resources(2)), now).unwrap();
        let w = reg.get("w1").unwrap();
        assert_eq!(w.last_seen, Some(now));
        assert_eq!(w.status, WorkerStatus::Online);
        assert_eq!(w.resources.as_ref().unwrap().agent_count, 2);

        // Age it past degraded and offline thresholds.
        let later = now + Duration::seconds(OFFLINE_AFTER_SECS + 1);
        reg.refresh_statuses(later);
        assert_eq!(reg.get("w1").unwrap().status, WorkerStatus::Offline);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_workers_file_quarantined() {
        let dir = tmpdir("corrupt");
        let path = dir.join(WORKERS_FILE_NAME);
        std::fs::write(&path, "{ not json !!!").unwrap();
        let outcome = load_workers(&path).unwrap();
        assert!(outcome.warning.is_some());
        assert!(outcome.value.workers.is_empty());
        // Quarantine file exists.
        let bad = dir.join("workers.json.bad");
        assert!(bad.exists());
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_version_field_migrates() {
        let dir = tmpdir("migrate");
        let path = dir.join(WORKERS_FILE_NAME);
        // Legacy file without `version`.
        std::fs::write(
            &path,
            r#"{"workers":[{"worker_id":"w1","label":"m","pubkey":"aa","paired_at":"2024-01-01T00:00:00Z","last_seen":null,"status":"online","resources":null}]}"#,
        )
        .unwrap();
        let outcome = load_workers(&path).unwrap();
        assert_eq!(outcome.value.workers.len(), 1);
        // version defaults to 0; save normalizes to current.
        let mut reg = WorkerRegistry {
            path: path.clone(),
            data: outcome.value,
            load_warning: None,
        };
        reg.save().unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["version"], WORKERS_FILE_VERSION);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn future_version_rejected() {
        let dir = tmpdir("future");
        let path = dir.join(WORKERS_FILE_NAME);
        std::fs::write(&path, r#"{"version":999,"workers":[]}"#).unwrap();
        assert!(matches!(
            load_workers(&path),
            Err(RegistryError::UnsupportedVersion { found: 999, .. })
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cluster_info_roundtrip_with_machines() {
        let dir = tmpdir("cluster");
        let path = dir.join(CLUSTER_FILE_NAME);
        let outcome = load_or_init_cluster(&path, "test-host").unwrap();
        let info = outcome.value;
        assert_eq!(info.name, "test-host");
        assert_eq!(info.port, crate::config::DEFAULT_PORT);
        assert_eq!(info.protocol_version, crate::protocol::PROTOCOL_VERSION);
        assert!(!info.cluster_id.is_empty());
        save_cluster_info(&path, &info).unwrap();

        // Second load reuses the same cluster_id.
        let again = load_or_init_cluster(&path, "other-name").unwrap().value;
        assert_eq!(again.cluster_id, info.cluster_id);
        assert_eq!(again.name, "test-host"); // persisted, not overwritten

        // Machines overrides survive a roundtrip.
        let mut info2 = again.clone();
        info2.machines.insert(
            "desktop".into(),
            MachineOverride {
                max_agents: Some(6),
                kinds_override: Some(vec!["claude".into()]),
            },
        );
        save_cluster_info(&path, &info2).unwrap();
        let loaded = load_or_init_cluster(&path, "x").unwrap().value;
        assert_eq!(loaded.machines["desktop"].max_agents, Some(6));
        assert_eq!(
            loaded.machines["desktop"].kinds_override.as_deref(),
            Some(&["claude".to_string()][..])
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn machine_override_problems_reject_invalid_values() {
        use crate::scheduler::MAX_AGENTS_CEILING;

        // Valid: absent fields, in-range cap, real kinds, and 0 (pause).
        assert!(MachineOverride::default().problems().is_empty());
        assert!(MachineOverride {
            max_agents: Some(MAX_AGENTS_CEILING),
            kinds_override: Some(vec!["claude".into(), "codex".into()]),
        }
        .problems()
        .is_empty());
        assert!(MachineOverride {
            max_agents: Some(0),
            kinds_override: None,
        }
        .problems()
        .is_empty());

        // Over the hard ceiling → rejected.
        let p = MachineOverride {
            max_agents: Some(MAX_AGENTS_CEILING + 1),
            kinds_override: None,
        }
        .problems();
        assert_eq!(p.len(), 1);
        assert!(p[0].contains("ceiling"));

        // Empty kinds_override and blank kind entries → rejected.
        let p = MachineOverride {
            max_agents: None,
            kinds_override: Some(vec![]),
        }
        .problems();
        assert_eq!(p.len(), 1);
        assert!(p[0].contains("empty list"));

        let p = MachineOverride {
            max_agents: None,
            kinds_override: Some(vec!["claude".into(), "  ".into()]),
        }
        .problems();
        assert_eq!(p.len(), 1);
        assert!(p[0].contains("empty kind"));
    }

    #[test]
    fn cluster_info_machine_override_problems_carry_labels() {
        let mut info = ClusterInfo {
            cluster_id: "x".into(),
            name: "x".into(),
            port: 1,
            protocol_version: 1,
            created_at: Utc::now(),
            machines: BTreeMap::new(),
            keepawake: true,
        };
        assert!(info.machine_override_problems().is_empty());

        info.machines.insert(
            "wsl".into(),
            MachineOverride {
                max_agents: Some(99),
                kinds_override: None,
            },
        );
        info.machines.insert(
            "mac".into(),
            MachineOverride {
                max_agents: Some(4),
                kinds_override: None,
            },
        );
        let problems = info.machine_override_problems();
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].0, "wsl"); // BTreeMap order: mac first but only wsl has problems
        assert!(problems[0].1.contains("99"));
    }

    #[test]
    fn cluster_info_unknown_machine_overrides_flags_dead_labels() {
        let mut info = ClusterInfo {
            cluster_id: "x".into(),
            name: "x".into(),
            port: 1,
            protocol_version: 1,
            created_at: Utc::now(),
            machines: BTreeMap::new(),
            keepawake: true,
        };
        for label in ["local", "wsl", "ghost"] {
            info.machines
                .insert(label.into(), MachineOverride::default());
        }
        let known: BTreeSet<String> = ["local".to_string(), "wsl".to_string()]
            .into_iter()
            .collect();
        assert_eq!(info.unknown_machine_overrides(&known), vec!["ghost"]);

        // Everything known → no dead config. Empty machines → empty.
        let known: BTreeSet<String> = ["local".into(), "wsl".into(), "ghost".into()]
            .into_iter()
            .collect();
        assert!(info.unknown_machine_overrides(&known).is_empty());
        info.machines.clear();
        assert!(info.unknown_machine_overrides(&known).is_empty());
    }

    #[test]
    fn corrupt_cluster_file_quarantined_and_recreated() {
        let dir = tmpdir("cluster-corrupt");
        let path = dir.join(CLUSTER_FILE_NAME);
        std::fs::write(&path, "garbage{{{").unwrap();
        let outcome = load_or_init_cluster(&path, "fresh").unwrap();
        assert!(outcome.warning.is_some());
        assert_eq!(outcome.value.name, "fresh");
        assert!(dir.join("cluster.json.bad").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sequential_saves_are_consistent() {
        let dir = tmpdir("seq");
        let path = dir.join(WORKERS_FILE_NAME);
        for i in 0..5 {
            let mut reg = WorkerRegistry::open(&path).unwrap();
            reg.upsert(entry(&format!("w{i}"), "m"));
            reg.save().unwrap();
        }
        let reg = WorkerRegistry::open(&path).unwrap();
        assert_eq!(reg.list().len(), 5);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_permissions_are_private() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let dir = tmpdir("perms");
            let path = dir.join(WORKERS_FILE_NAME);
            let mut reg = WorkerRegistry::open(&path).unwrap();
            reg.upsert(entry("w1", "m"));
            reg.save().unwrap();
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
