//! Machine endpoints and the fleet context (registry + endpoints).
//!
//! [`MachineEndpoint`] is the fleet's view of "one herdr API socket": the
//! local machine uses herdr's own socket path; each worker uses the
//! cluster-bridge socket convention documented on [`bridge_socket_path`].
//! Endpoints are cheap values (a label + a path) — an
//! [`api_client::ApiClient`] is constructed lazily per call so missing
//! sockets surface as typed errors at call time rather than at context
//! load time.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use api_client::{platform, ApiClient, ApiError};
use chrono::Utc;
use cluster_core::config;
use cluster_core::registry::{
    derive_status, ClusterInfo, MachineOverride, WorkerEntry, WorkerStatus,
};
use serde::{Deserialize, Serialize};

use crate::error::FleetError;

/// Reserved machine label for the controller's local herdr instance.
///
/// Global agent IDs on the local machine look like `local/<agent>`.
/// A `workers.json` entry whose label is `"local"` is ignored — the label
/// is reserved so global IDs stay unambiguous.
pub const LOCAL_LABEL: &str = "local";

/// Directory (under the herdcluster state dir) where `cluster-bridge`
/// exposes one unix socket per paired worker.
///
/// **Bridge socket convention** (implemented by `cluster-bridge`, consumed
/// here): for a worker with registry label `L`, the controller-side bridge
/// listens on
///
/// ```text
/// <state_dir>/bridge/<sanitize(L)>.sock
/// ```
///
/// where `sanitize` is [`sanitize_label`]. The socket speaks the exact
/// herdr NDJSON API protocol (one request per connection), so
/// [`api_client::ApiClient`] works against it unchanged.
pub const BRIDGE_DIR_NAME: &str = "bridge";
/// File extension for bridge sockets.
pub const BRIDGE_SOCKET_SUFFIX: &str = ".sock";

/// Which side of the cluster an endpoint lives on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MachineKind {
    /// The controller's own herdr server.
    Local,
    /// A paired worker reached through the cluster bridge.
    Remote,
}

/// One addressable herdr API endpoint (local socket or worker bridge).
#[derive(Debug, Clone)]
pub struct MachineEndpoint {
    /// Machine label — the first half of every global agent ID on it.
    pub label: String,
    /// Unix socket path the endpoint's `ApiClient` connects to.
    pub socket_path: PathBuf,
    /// Local vs. bridged-remote.
    pub kind: MachineKind,
}

impl MachineEndpoint {
    /// Build an [`ApiClient`] for this endpoint.
    ///
    /// A missing socket is mapped to a typed error: [`FleetError::Api`] for
    /// the local machine (herdr simply isn't running),
    /// [`FleetError::WorkerUnreachable`] for workers (bridge down or worker
    /// offline).
    pub fn client(&self) -> Result<ApiClient, FleetError> {
        match ApiClient::connect(&self.socket_path) {
            Ok(client) => Ok(client),
            Err(ApiError::ServerNotRunning { .. }) => {
                let reason = format!(
                    "no API socket at {} (worker offline or cluster bridge not running)",
                    self.socket_path.display()
                );
                if self.kind == MachineKind::Local {
                    Err(FleetError::Api { message: reason })
                } else {
                    Err(FleetError::WorkerUnreachable {
                        machine: self.label.clone(),
                        reason,
                    })
                }
            }
            Err(other) => Err(FleetError::Api {
                message: other.to_string(),
            }),
        }
    }
}

/// Sanitize a worker label into a filesystem-safe bridge socket basename.
///
/// Keeps `[A-Za-z0-9._-]`, maps everything else to `_`, trims leading/trailing
/// dots and dashes, and falls back to `"worker"` if nothing usable remains.
/// Two distinct labels can theoretically sanitize to the same basename —
/// pairing should reject such labels; this is documented, not enforced here.
pub fn sanitize_label(label: &str) -> String {
    let mut out: String = label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    while out.starts_with('.') || out.starts_with('-') || out.starts_with('_') {
        out.remove(0);
    }
    while out.ends_with('.') || out.ends_with('-') || out.ends_with('_') {
        out.pop();
    }
    if out.is_empty() {
        "worker".to_string()
    } else {
        out
    }
}

/// The bridge socket path for a worker label — the convention
/// `cluster-bridge` must implement (see [`BRIDGE_DIR_NAME`]).
pub fn bridge_socket_path(state_dir: &Path, label: &str) -> PathBuf {
    state_dir
        .join(BRIDGE_DIR_NAME)
        .join(format!("{}{BRIDGE_SOCKET_SUFFIX}", sanitize_label(label)))
}

/// Endpoint for the controller's local herdr server.
///
/// Uses [`api_client::platform::socket_path`] discovery (honors
/// `HERDR_SOCKET_PATH` and herdr session env vars). Fails only on
/// platforms where discovery is unsupported.
pub fn local_endpoint() -> Result<MachineEndpoint, FleetError> {
    let socket_path = platform::socket_path().map_err(|e| FleetError::Api {
        message: e.to_string(),
    })?;
    Ok(MachineEndpoint {
        label: LOCAL_LABEL.to_string(),
        socket_path,
        kind: MachineKind::Local,
    })
}

/// Endpoint for a remote worker: its cluster-bridge socket under
/// `<state_dir>/bridge/`. The socket is *not* checked for existence here —
/// reachability is determined lazily at call time.
pub fn remote_endpoint(label: &str, state_dir: &Path) -> MachineEndpoint {
    MachineEndpoint {
        label: label.to_string(),
        socket_path: bridge_socket_path(state_dir, label),
        kind: MachineKind::Remote,
    }
}

/// The fleet view: every addressable machine plus the registry data needed
/// for status display, placement, and offline short-circuiting.
///
/// Construct via [`FleetContext::load`] (reads herdcluster state) or
/// [`FleetContext::build`] (explicit inputs — used by tests).
#[derive(Debug, Clone)]
pub struct FleetContext {
    /// Herdcluster state dir the bridge sockets live under.
    pub state_dir: PathBuf,
    /// Cluster display name from `cluster.json`.
    pub cluster_name: String,
    /// Per-machine scheduler overrides from `cluster.json`.
    pub machines_overrides: BTreeMap<String, MachineOverride>,
    /// All endpoints: local first, then workers sorted by label.
    pub endpoints: Vec<MachineEndpoint>,
    /// Worker registry entries keyed by label (excludes the local machine
    /// and any worker whose label collided with [`LOCAL_LABEL`]).
    pub workers: BTreeMap<String, WorkerEntry>,
}

impl FleetContext {
    /// Load the fleet context from herdcluster state (`cluster.json` +
    /// `workers.json` under the resolved state dir).
    ///
    /// Side effect: like `cluster-core`'s config loaders, a missing
    /// `cluster.json` is created with a fresh cluster identity — run
    /// `herdcluster init` first for a stable name.
    ///
    /// Worker `status` values are recomputed from `last_seen` at load
    /// time (see [`FleetContext::build`]), so stale persisted statuses
    /// never leak into scheduling or reachability decisions.
    pub fn load() -> Result<Self, FleetError> {
        let state_dir = config::state_dir().map_err(|e| FleetError::Config {
            message: e.to_string(),
        })?;
        let cluster = config::load_cluster()
            .map_err(|e| FleetError::Config {
                message: e.to_string(),
            })?
            .value;
        let registry = config::open_workers().map_err(|e| FleetError::Config {
            message: e.to_string(),
        })?;
        Self::build(state_dir, cluster, registry.data().workers.clone())
    }

    /// Build a context from explicit inputs (no disk access beyond what the
    /// caller already did). `workers` may be in any order; endpoints come
    /// out sorted by label with the local machine first.
    ///
    /// Each worker's persisted `status` is treated as a cache and
    /// recomputed from `last_seen` at load time (issue #41) — the same
    /// derivation `doctor` and `WorkerRegistry::refresh_statuses` apply —
    /// so a `workers.json` written hours ago cannot report a dead worker
    /// as online. Pass `last_seen` values that reflect reality; the
    /// `status` field of the input entries is ignored.
    pub fn build(
        state_dir: PathBuf,
        cluster: ClusterInfo,
        workers: Vec<WorkerEntry>,
    ) -> Result<Self, FleetError> {
        let local = local_endpoint()?;
        let mut sorted: Vec<WorkerEntry> = workers;
        sorted.sort_by(|a, b| a.label.cmp(&b.label));

        let now = Utc::now();
        let mut endpoints = vec![local];
        let mut worker_map = BTreeMap::new();
        for mut w in sorted {
            if w.label == LOCAL_LABEL {
                // Reserved label — a worker can never own "local/..." IDs.
                continue;
            }
            w.status = derive_status(w.last_seen, now);
            endpoints.push(remote_endpoint(&w.label, &state_dir));
            worker_map.insert(w.label.clone(), w);
        }

        Ok(FleetContext {
            state_dir,
            cluster_name: cluster.name,
            machines_overrides: cluster.machines,
            endpoints,
            workers: worker_map,
        })
    }

    /// Look up an endpoint by machine label (`"local"` or a worker label).
    pub fn endpoint_for(&self, label: &str) -> Option<&MachineEndpoint> {
        self.endpoints.iter().find(|e| e.label == label)
    }

    /// Registry entry for a worker label (never the local machine).
    pub fn worker_entry(&self, label: &str) -> Option<&WorkerEntry> {
        self.workers.get(label)
    }

    /// The local endpoint (always present — index 0 by construction, but
    /// looked up by kind for clarity).
    pub fn local(&self) -> &MachineEndpoint {
        self.endpoints
            .iter()
            .find(|e| e.kind == MachineKind::Local)
            .expect("FleetContext always contains a local endpoint")
    }

    /// Registry status for a machine label. The local machine has no
    /// registry entry; callers should treat it as always "potentially
    /// online" and let the live call decide.
    pub fn registry_status(&self, label: &str) -> Option<WorkerStatus> {
        self.workers.get(label).map(|w| w.status)
    }

    /// True when `label` is a remote worker whose last heartbeat is stale
    /// enough to be `offline`. Offline workers are never called — fan-out
    /// emits placeholder rows instead.
    pub fn is_offline(&self, label: &str) -> bool {
        self.registry_status(label) == Some(WorkerStatus::Offline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster_core::protocol::Resources;

    /// Build a worker fixture. `last_seen` drives the status the context
    /// derives — the persisted `status` field is overwritten by
    /// [`FleetContext::build`] (issue #41), so fixtures set `last_seen`
    /// to express the intended liveness.
    fn worker(label: &str, last_seen: Option<chrono::DateTime<Utc>>) -> WorkerEntry {
        WorkerEntry {
            worker_id: format!("id-{label}"),
            label: label.into(),
            pubkey: "ab".repeat(32),
            paired_at: chrono::Utc::now(),
            last_seen,
            status: WorkerStatus::Offline, // placeholder; build() recomputes
            resources: Some(Resources {
                cpu_cores: 4,
                mem_total_mb: 8192,
                mem_available_mb: 4096,
                agent_count: 1,
                max_agents: 4,
                load_avg: vec![],
                kinds: vec!["claude".into()],
            }),
        }
    }

    fn cluster() -> ClusterInfo {
        ClusterInfo {
            cluster_id: "c".into(),
            name: "test-cluster".into(),
            port: 47820,
            protocol_version: 1,
            created_at: chrono::Utc::now(),
            machines: BTreeMap::new(),
            keepawake: true,
        }
    }

    #[test]
    fn sanitize_label_cases() {
        assert_eq!(sanitize_label("desktop-win"), "desktop-win");
        assert_eq!(sanitize_label("wsl2 box!"), "wsl2_box");
        assert_eq!(sanitize_label("..weird.."), "weird");
        assert_eq!(sanitize_label("!!!"), "worker");
        assert_eq!(sanitize_label("a/b\\c"), "a_b_c");
    }

    #[test]
    fn bridge_socket_path_convention() {
        let dir = Path::new("/tmp/hc-state");
        assert_eq!(
            bridge_socket_path(dir, "desktop-win"),
            dir.join("bridge").join("desktop-win.sock")
        );
        assert_eq!(
            bridge_socket_path(dir, "wsl 2"),
            dir.join("bridge").join("wsl_2.sock")
        );
    }

    #[test]
    fn build_orders_endpoints_and_skips_local_label_worker() {
        // local_endpoint() needs a real platform socket path — on unix it
        // resolves without checking existence, so this works in tests.
        let now = chrono::Utc::now();
        let workers = vec![
            worker("zeta", Some(now)),
            worker(LOCAL_LABEL, Some(now)), // must be dropped
            worker("alpha", None),
        ];
        let ctx = FleetContext::build(PathBuf::from("/tmp/hc"), cluster(), workers).unwrap();
        let labels: Vec<&str> = ctx.endpoints.iter().map(|e| e.label.as_str()).collect();
        assert_eq!(labels, vec!["local", "alpha", "zeta"]);
        assert_eq!(ctx.endpoints[0].kind, MachineKind::Local);
        assert_eq!(ctx.endpoints[1].kind, MachineKind::Remote);
        assert_eq!(
            ctx.endpoints[1].socket_path,
            PathBuf::from("/tmp/hc/bridge/alpha.sock")
        );
        assert!(ctx.workers.contains_key("alpha"));
        assert!(!ctx.workers.contains_key(LOCAL_LABEL));
        assert!(ctx.is_offline("alpha"));
        assert!(!ctx.is_offline("zeta"));
        assert!(!ctx.is_offline("local")); // local has no registry entry
        assert!(ctx.endpoint_for("local").is_some());
        assert!(ctx.endpoint_for("ghost").is_none());
    }

    #[test]
    fn build_recomputes_status_from_last_seen() {
        // Issue #41: `workers.json` persists `status` verbatim, but it is
        // only a cache of `last_seen` age. A worker the file still marks
        // `online` whose last heartbeat is hours old must be treated as
        // offline; a recently-seen worker stays online.
        let now = chrono::Utc::now();
        let mut stale_online = worker("stale-online", Some(now - chrono::Duration::hours(1)));
        stale_online.status = WorkerStatus::Online; // persisted lie
        let mut never_seen = worker("never-seen", None);
        never_seen.status = WorkerStatus::Online; // persisted lie
        let workers = vec![
            stale_online,
            never_seen,
            worker("fresh", Some(now)),
            worker("degraded", Some(now - chrono::Duration::seconds(20))),
        ];
        let ctx = FleetContext::build(PathBuf::from("/tmp/hc"), cluster(), workers).unwrap();

        // Stale last_seen → Offline regardless of the persisted status.
        assert_eq!(
            ctx.registry_status("stale-online"),
            Some(WorkerStatus::Offline)
        );
        assert!(ctx.is_offline("stale-online"));
        assert_eq!(
            ctx.workers.get("stale-online").unwrap().status,
            WorkerStatus::Offline
        );
        // Never seen → Offline even if the file claimed online.
        assert!(ctx.is_offline("never-seen"));
        // Fresh heartbeat → Online.
        assert_eq!(ctx.registry_status("fresh"), Some(WorkerStatus::Online));
        assert!(!ctx.is_offline("fresh"));
        // ~20 s old → Degraded: not offline, so not short-circuited.
        assert_eq!(
            ctx.registry_status("degraded"),
            Some(WorkerStatus::Degraded)
        );
        assert!(!ctx.is_offline("degraded"));
    }
}
