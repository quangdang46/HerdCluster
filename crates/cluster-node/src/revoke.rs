//! `revoke` — `herdcluster revoke <label>` worker removal
//! (bead `m3-revoke`, PLAN.md §3.4/§3.5).
//!
//! # What revocation means here
//!
//! There is no per-worker secret stored on the controller to delete:
//! worker auth is "possession of the worker's Ed25519 keypair" plus a
//! **registry membership check** — `controller.rs::on_worker_connected`
//! looks the authenticated pubkey up in `workers.json` and closes the
//! session when it is absent. Revocation therefore works by removing
//! that row; a revoked worker's *next* connect is refused server-side
//! (not merely hidden), while its worker-side `worker_credential.json`
//! (which only pins the cluster CA, not a per-worker secret) remains
//! harmless — re-pairing with a fresh code issues a new identity row.
//!
//! # Live disconnect — file-based, no controller API change
//!
//! `revoke_worker` cannot reach into a running `herdcluster init`
//! process, so it uses a **revocation marker file** convention:
//!
//! ```text
//! <state_dir>/revocations/<worker_id>.json
//! ```
//!
//! written before the registry row is removed. `controller.rs` runs a
//! small watch task (`revocation_watch_loop`, additive) that polls the
//! directory, closes any live [`cluster_net::conn::Session`] for the
//! marker's pubkey, drops the registry row + bridge in memory (the
//! running controller caches `workers.json` in memory and would
//! otherwise resurrect the row on its next debounced save), and deletes
//! the marker. When no controller is running the marker is simply
//! consumed on next startup — idempotent either way.
//!
//! The controller-side `credentials/` dir is scanned for per-worker
//! files named after the worker id/label/pubkey as a defensive cleanup
//! (the directory is currently unused by pairing, but the layout
//! reserves it for issued credentials).

use std::path::{Path, PathBuf};

use chrono::Utc;
use cluster_core::registry::{self, WorkerRegistry};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use cluster_fleet::endpoint::{bridge_socket_path, sanitize_label};

/// Directory (under the state dir) holding revocation marker files —
/// the contract between [`revoke_worker`] and `controller.rs`'s watch
/// loop. See module docs.
pub const REVOCATIONS_DIR_NAME: &str = "revocations";
/// Schema version for [`RevocationMarker`].
pub const REVOCATION_MARKER_VERSION: u32 = 1;

/// What [`revoke_worker`] actually did.
#[derive(Debug, Clone, Default)]
pub struct RevokeOutcome {
    /// The `workers.json` row was found and removed.
    pub removed_registry: bool,
    /// Credential material stored under `<state_dir>/credentials/`
    /// matching this worker was deleted. (The worker-side
    /// `worker_credential.json` lives on the remote machine and is not
    /// reachable from the controller — see module docs.)
    pub removed_credential: bool,
    /// Live disconnect was effected or requested: the revocation marker
    /// was persisted (a running controller closes the session within
    /// ~500 ms) and any stale bridge socket file was removed.
    pub disconnected_live: bool,
}

/// Errors from [`revoke_worker`].
#[derive(Debug, Error)]
pub enum RevokeError {
    /// No registry entry matched `label` (or `worker_id`).
    #[error("unknown worker '{0}': not in workers.json (paired workers: run `herdcluster machines` to list)")]
    UnknownWorker(String),
    /// Herdcluster state-dir resolution failed.
    #[error("config error: {0}")]
    Config(String),
    /// Filesystem error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Registry load/save error.
    #[error("registry error: {0}")]
    Registry(#[from] registry::RegistryError),
}

/// Revocation marker consumed by the controller's watch loop.
///
/// Carries everything needed to kill the live session without a
/// registry lookup first: the pubkey identifies the
/// [`cluster_net::conn::Session`] in `conn::Controller`'s session map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevocationMarker {
    /// Marker schema version.
    pub version: u32,
    /// Registry `worker_id` the row was removed under.
    pub worker_id: String,
    /// Worker label at revoke time (display only).
    pub label: String,
    /// Worker Ed25519 pubkey, hex — keys the live session lookup.
    pub pubkey: String,
    /// Revoke time, seconds since UNIX epoch.
    pub revoked_at_unix: u64,
}

/// Revoke `label` against the default herdcluster state dir.
pub fn revoke_worker(label: &str) -> Result<RevokeOutcome, RevokeError> {
    let state_dir =
        cluster_core::config::state_dir().map_err(|e| RevokeError::Config(e.to_string()))?;
    revoke_worker_in_state_dir(label, &state_dir)
}

/// [`revoke_worker`] against an explicit state dir — the testable core.
///
/// `label` matches a registry entry's `label` **or** `worker_id`
/// (accepting either keeps scripted revocations forgiving; labels are
/// the documented interface).
pub fn revoke_worker_in_state_dir(
    label: &str,
    state_dir: &Path,
) -> Result<RevokeOutcome, RevokeError> {
    let workers_path = state_dir.join(registry::WORKERS_FILE_NAME);
    let mut reg = WorkerRegistry::open(&workers_path)?;

    // Match by label first, then worker_id.
    let entry = reg
        .list()
        .iter()
        .find(|w| w.label == label)
        .or_else(|| reg.list().iter().find(|w| w.worker_id == label))
        .cloned()
        .ok_or_else(|| RevokeError::UnknownWorker(label.to_string()))?;

    // 1. Persist the revocation marker BEFORE touching the registry so a
    //    running controller sees it even if our registry save races its
    //    debounced persist.
    let marker = RevocationMarker {
        version: REVOCATION_MARKER_VERSION,
        worker_id: entry.worker_id.clone(),
        label: entry.label.clone(),
        pubkey: entry.pubkey.clone(),
        revoked_at_unix: unix_now(),
    };
    write_marker(state_dir, &marker)?;

    // 2. Remove the registry row and persist immediately (revocation is
    //    rare — bypass the controller's debounced dirty-flag path; the
    //    watch loop reconciles the in-memory copy in a running
    //    controller).
    let removed_registry = reg.remove(&entry.worker_id);
    if removed_registry {
        reg.save()?;
    }

    // 3. Delete any controller-side credential files for this worker.
    let removed_credential = remove_worker_credentials(state_dir, &entry);

    // 4. Remove the stale bridge socket so new local API clients get a
    //    clean "no socket" instead of a dead listener path. (A live
    //    bridge inside a running controller is shut down by the watch
    //    loop via the session close → WorkerDisconnected path.)
    let bridge_sock = bridge_socket_path(state_dir, &entry.label);
    if bridge_sock.exists() {
        let _ = std::fs::remove_file(&bridge_sock);
    }

    // 5. disconnected_live: the marker is persisted, so a running
    //    controller will close the live session momentarily; when no
    //    controller runs there is simply nothing live to disconnect.
    let disconnected_live = true;

    tracing::info!(
        worker = %entry.label,
        worker_id = %entry.worker_id,
        removed_registry,
        removed_credential,
        "worker revoked"
    );

    Ok(RevokeOutcome {
        removed_registry,
        removed_credential,
        disconnected_live,
    })
}

/// `<state_dir>/revocations/`.
pub fn revocations_dir(state_dir: &Path) -> PathBuf {
    state_dir.join(REVOCATIONS_DIR_NAME)
}

/// Marker path for a worker id.
pub fn revocation_marker_path(state_dir: &Path, worker_id: &str) -> PathBuf {
    // worker_id is registry-generated hex, but sanitize defensively so a
    // crafted label/id can never escape the directory.
    revocations_dir(state_dir).join(format!("{}.json", sanitize_label(worker_id)))
}

/// Write the revocation marker atomically (temp + rename).
fn write_marker(state_dir: &Path, marker: &RevocationMarker) -> Result<(), RevokeError> {
    let dir = revocations_dir(state_dir);
    std::fs::create_dir_all(&dir)?;
    let path = revocation_marker_path(state_dir, &marker.worker_id);
    let json = serde_json::to_vec_pretty(marker)
        .map_err(|e| RevokeError::Config(format!("marker encode: {e}")))?;
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Consume (delete) a revocation marker after the controller acted on
/// it. Exposed for the controller watch loop and tests.
pub fn consume_marker(state_dir: &Path, worker_id: &str) -> std::io::Result<()> {
    let path = revocation_marker_path(state_dir, worker_id);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Delete files under `<state_dir>/credentials/` that look like they
/// belong to `entry` (name contains worker_id, label, or pubkey prefix).
/// Returns true when at least one file was removed.
fn remove_worker_credentials(state_dir: &Path, entry: &registry::WorkerEntry) -> bool {
    let dir = state_dir.join("credentials");
    let Ok(read) = std::fs::read_dir(&dir) else {
        return false;
    };
    let mut removed = false;
    for item in read.flatten() {
        let name = item.file_name().to_string_lossy().to_string();
        let matches = name.contains(&entry.worker_id)
            || name.contains(&entry.label)
            || (!entry.pubkey.is_empty()
                && name.contains(&entry.pubkey[..8.min(entry.pubkey.len())]));
        if matches && std::fs::remove_file(item.path()).is_ok() {
            removed = true;
        }
    }
    removed
}

fn unix_now() -> u64 {
    Utc::now().timestamp().max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster_core::protocol::Resources;

    fn tmpdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hc-revoke-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn entry(id: &str, label: &str, pubkey: &str) -> registry::WorkerEntry {
        registry::WorkerEntry {
            worker_id: id.into(),
            label: label.into(),
            pubkey: pubkey.into(),
            paired_at: Utc::now(),
            last_seen: None,
            status: registry::WorkerStatus::Online,
            resources: Some(Resources {
                cpu_cores: 4,
                mem_total_mb: 8192,
                mem_available_mb: 4096,
                agent_count: 0,
                max_agents: 4,
                load_avg: vec![],
                kinds: vec![],
            }),
        }
    }

    fn seed_registry(dir: &Path, workers: Vec<registry::WorkerEntry>) {
        let mut reg = WorkerRegistry::open(dir.join(registry::WORKERS_FILE_NAME)).unwrap();
        for w in workers {
            reg.upsert(w);
        }
        reg.save().unwrap();
    }

    #[test]
    fn revoke_removes_registry_and_writes_marker() {
        let dir = tmpdir("basic");
        seed_registry(
            &dir,
            vec![
                entry("w1", "desktop-win", &"ab".repeat(32)),
                entry("w2", "wsl", &"cd".repeat(32)),
            ],
        );

        let outcome = revoke_worker_in_state_dir("desktop-win", &dir).unwrap();
        assert!(outcome.removed_registry);
        assert!(outcome.disconnected_live);

        // Registry row gone, other worker untouched.
        let reg = WorkerRegistry::open(dir.join(registry::WORKERS_FILE_NAME)).unwrap();
        assert!(reg.get("w1").is_none());
        assert!(reg.get("w2").is_some());

        // Marker exists and round-trips.
        let marker_path = revocation_marker_path(&dir, "w1");
        assert!(marker_path.exists());
        let raw = std::fs::read_to_string(&marker_path).unwrap();
        let marker: RevocationMarker = serde_json::from_str(&raw).unwrap();
        assert_eq!(marker.worker_id, "w1");
        assert_eq!(marker.label, "desktop-win");
        assert_eq!(marker.pubkey, "ab".repeat(32));

        // worker_id also accepted as the selector.
        let outcome2 = revoke_worker_in_state_dir("w2", &dir).unwrap();
        assert!(outcome2.removed_registry);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn revoke_unknown_worker_is_typed_error() {
        let dir = tmpdir("unknown");
        seed_registry(&dir, vec![entry("w1", "desktop-win", &"ab".repeat(32))]);
        let err = revoke_worker_in_state_dir("ghost", &dir).unwrap_err();
        assert!(matches!(err, RevokeError::UnknownWorker(_)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn revoke_deletes_matching_credential_files() {
        let dir = tmpdir("creds");
        seed_registry(&dir, vec![entry("w1", "desktop-win", &"ab".repeat(32))]);
        let creds = dir.join("credentials");
        std::fs::create_dir_all(&creds).unwrap();
        std::fs::write(creds.join("w1-issued.json"), b"{}").unwrap();
        std::fs::write(creds.join("other.json"), b"{}").unwrap();

        let outcome = revoke_worker_in_state_dir("desktop-win", &dir).unwrap();
        assert!(outcome.removed_credential);
        assert!(!creds.join("w1-issued.json").exists());
        assert!(creds.join("other.json").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn revoke_removes_bridge_socket() {
        let dir = tmpdir("bridge");
        seed_registry(&dir, vec![entry("w1", "desktop win", &"ab".repeat(32))]);
        let sock = bridge_socket_path(&dir, "desktop win");
        std::fs::create_dir_all(sock.parent().unwrap()).unwrap();
        std::fs::write(&sock, b"").unwrap();

        let outcome = revoke_worker_in_state_dir("desktop win", &dir).unwrap();
        assert!(outcome.removed_registry);
        assert!(!sock.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn marker_roundtrip_and_consume() {
        let dir = tmpdir("marker");
        seed_registry(&dir, vec![entry("w9", "m", &"ef".repeat(32))]);
        revoke_worker_in_state_dir("m", &dir).unwrap();
        assert!(revocation_marker_path(&dir, "w9").exists());
        consume_marker(&dir, "w9").unwrap();
        assert!(!revocation_marker_path(&dir, "w9").exists());
        // Consuming a missing marker is not an error.
        consume_marker(&dir, "w9").unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn re_pair_after_revoke_works() {
        // Re-pairing = a fresh registry row; revocation must not leave
        // state that blocks a new entry with the same label.
        let dir = tmpdir("repair");
        seed_registry(&dir, vec![entry("w1", "desktop-win", &"ab".repeat(32))]);
        revoke_worker_in_state_dir("desktop-win", &dir).unwrap();

        let mut reg = WorkerRegistry::open(dir.join(registry::WORKERS_FILE_NAME)).unwrap();
        reg.upsert(entry("w2", "desktop-win", &"ff".repeat(32)));
        reg.save().unwrap();
        let reg = WorkerRegistry::open(dir.join(registry::WORKERS_FILE_NAME)).unwrap();
        assert_eq!(reg.list().len(), 1);
        assert_eq!(reg.list()[0].worker_id, "w2");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
