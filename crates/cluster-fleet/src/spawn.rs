//! `herdcluster spawn` composer (PLAN.md §3.8).
//!
//! `spawn(name, kind, machine, repo_path, branch, extra_agent_args)`:
//!
//! 1. **Resolve machine** — explicit label → registry/endpoint lookup;
//!    `auto` → [`cluster_core::scheduler::place`] over [`WorkerCaps`]
//!    built from registry resources + `cluster.json` overrides + a live
//!    local probe. The local machine competes equally.
//! 2. **Pre-flight** — `server.agent_manifests` on the target must know
//!    `kind` (unioned with the worker's reported `resources.kinds`, which
//!    doubles as the agent-CLI-presence hint). When neither source yields
//!    any kinds the check is skipped and `agent.start` remains the
//!    authority — a clear `agent_start_failed` error, never silent.
//! 3. **Ensure workspace** — `workspace.list` searched by repo path, else
//!    `workspace.create {cwd}`.
//! 4. **Ensure dedicated pane** — see [`ensure_pane`] for the chosen
//!    strategy.
//! 5. **`agent.start`** in that pane, then poll `agent.get` until the
//!    agent reports a ready status or `ready_timeout` expires.
//!
//! ## `--repo` semantics
//!
//! `repo_path` is a path **on the target machine's filesystem** — for a
//! remote worker that is a WSL2/Linux path, not a macOS path (PLAN.md
//! §3.8, `--repo-map` is a later option).
//!
//! ## `--branch` semantics
//!
//! `branch` is accepted for forward compatibility (a future `--worktree`
//! option will consume it). Today it is recorded in [`SpawnResult`] but
//! not passed to the agent CLI — agent CLIs have no uniform branch flag.
//! It is never silently *dropped from the result*.
//!
//! Sync only: `api-client` is synchronous; the CLI calls [`spawn`]
//! directly.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use api_client::methods::{
    AgentStartParams, PaneSplitParams, SplitDirection, WorkspaceCreateParams,
};
use api_client::ApiError;
use cluster_core::registry::WorkerStatus;
use cluster_core::scheduler::{self, PlacementRequest, Rejection, WorkerCaps};
use serde::Serialize;
use serde_json::Value;

use crate::endpoint::{FleetContext, MachineEndpoint, MachineKind, LOCAL_LABEL};
use crate::error::FleetError;
use crate::ids::format_global_id;
use crate::parse;

/// Default bound for the post-`agent.start` readiness wait (PLAN.md §3.8:
/// 35 s).
pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(35);
/// `agent.start` upstream `timeout_ms` bound used for the launch call.
const AGENT_START_TIMEOUT_MS: u64 = 30_000;
/// Poll interval while waiting for the agent to become ready.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(400);
/// Per-`agent.get` poll request timeout (must be < ready_timeout to allow
/// retries).
const READY_POLL_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Fallback `mem_available_mb` for the local machine's scheduler caps.
///
/// std has no portable memory API and the local resource reporter is a
/// later milestone (m6-resources); until then the local machine is scored
/// with this conservative constant so it neither always wins nor always
/// loses on headroom alone.
pub const LOCAL_MEM_AVAILABLE_FALLBACK_MB: u64 = 4096;

/// Where `spawn` should place the agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MachineTarget {
    /// Explicit machine label (`"local"` or a worker label).
    Label(String),
    /// Scheduler auto-placement (`--machine auto`).
    Auto,
}

/// Parameters for [`spawn`].
#[derive(Debug, Clone)]
pub struct SpawnRequest {
    /// Agent display name / target on the machine.
    pub name: String,
    /// Agent kind, e.g. `"claude"` — see `server.agent_manifests`.
    pub kind: String,
    /// Machine selection.
    pub machine: MachineTarget,
    /// Repository path **on the target machine** (see module docs).
    pub repo_path: String,
    /// Optional branch — recorded, not yet consumed (see module docs).
    pub branch: Option<String>,
    /// Extra CLI args forwarded to `agent.start`.
    pub extra_agent_args: Vec<String>,
    /// Max time to wait for the agent to report ready after `agent.start`.
    /// `None` → [`DEFAULT_READY_TIMEOUT`].
    pub ready_timeout: Option<Duration>,
}

/// Successful spawn result.
#[derive(Debug, Clone, Serialize)]
pub struct SpawnResult {
    /// Global agent ID (`{machine}/{name}`).
    pub global_id: String,
    /// Machine the agent was spawned on.
    pub machine: String,
    /// Machine-local agent target (the `name` passed to `agent.start`).
    pub agent: String,
    /// Agent kind.
    pub kind: String,
    /// Pane hosting the agent.
    pub pane_id: String,
    /// Workspace hosting the pane.
    pub workspace_id: String,
    /// Branch that was requested (recorded for the future `--worktree`
    /// option; not consumed today).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// Typed errors for each spawn step. `code()` values are a stable
/// `--json` contract.
#[derive(Debug)]
pub enum SpawnError {
    /// Machine label unknown, or auto-placement found no candidate
    /// (`rejections` carries per-machine reasons).
    NoPlacement {
        /// Machine label that was not found (explicit-label case).
        machine: Option<String>,
        /// Per-machine scheduler rejections (auto case).
        rejections: Vec<Rejection>,
    },
    /// Target machine is a worker with a stale heartbeat.
    WorkerUnreachable {
        /// Machine label.
        machine: String,
        /// Cause.
        reason: String,
    },
    /// `kind` is not supported on the target machine.
    KindUnsupported {
        /// Machine label.
        machine: String,
        /// Requested kind.
        kind: String,
        /// Kinds the machine reported supporting (may be empty when the
        /// check was skipped — see module docs).
        available: Vec<String>,
    },
    /// Workspace list/create failed.
    WorkspaceFailed {
        /// Machine label.
        machine: String,
        /// Cause.
        reason: String,
    },
    /// Pane split/list failed or no pane id could be determined.
    PaneFailed {
        /// Machine label.
        machine: String,
        /// Cause.
        reason: String,
    },
    /// `agent.start` failed.
    AgentStartFailed {
        /// Machine label.
        machine: String,
        /// Cause.
        reason: String,
    },
    /// The agent never reported a ready status within `ready_timeout`.
    TimeoutWaitingReady {
        /// Machine label.
        machine: String,
        /// Agent name.
        agent: String,
        /// Seconds waited.
        waited_secs: u64,
    },
    /// Underlying API/config failure not attributable to a step.
    Api {
        /// Machine label, if known.
        machine: Option<String>,
        /// Cause.
        reason: String,
    },
}

impl SpawnError {
    /// Stable snake_case code for `--json` error output.
    pub fn code(&self) -> &'static str {
        match self {
            SpawnError::NoPlacement { .. } => "no_placement",
            SpawnError::WorkerUnreachable { .. } => "worker_unreachable",
            SpawnError::KindUnsupported { .. } => "kind_unsupported",
            SpawnError::WorkspaceFailed { .. } => "workspace_failed",
            SpawnError::PaneFailed { .. } => "pane_failed",
            SpawnError::AgentStartFailed { .. } => "agent_start_failed",
            SpawnError::TimeoutWaitingReady { .. } => "timeout_waiting_ready",
            SpawnError::Api { .. } => "api_error",
        }
    }

    /// Canonical `{"error": {...}}` JSON envelope.
    pub fn to_json(&self) -> Value {
        let mut error = serde_json::json!({
            "code": self.code(),
            "message": self.to_string(),
        });
        match self {
            SpawnError::NoPlacement { rejections, .. } => {
                error["rejections"] = serde_json::json!(rejections);
            }
            SpawnError::KindUnsupported { available, .. } => {
                error["available"] = serde_json::json!(available);
            }
            _ => {}
        }
        serde_json::json!({ "error": error })
    }
}

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpawnError::NoPlacement {
                machine,
                rejections,
            } => {
                if let Some(m) = machine {
                    write!(f, "unknown machine {m:?}")
                } else {
                    let detail = rejections
                        .iter()
                        .map(|r| format!("{}: {:?}", r.machine, r.reason))
                        .collect::<Vec<_>>()
                        .join(", ");
                    write!(f, "no machine can host the agent ({detail})")
                }
            }
            SpawnError::WorkerUnreachable { machine, reason } => {
                write!(f, "machine {machine:?} is unreachable: {reason}")
            }
            SpawnError::KindUnsupported {
                machine,
                kind,
                available,
            } => write!(
                f,
                "agent kind {kind:?} is not supported on {machine:?} \
                 (available: {})",
                if available.is_empty() {
                    "unknown".to_string()
                } else {
                    available.join(", ")
                }
            ),
            SpawnError::WorkspaceFailed { machine, reason } => {
                write!(f, "workspace step failed on {machine:?}: {reason}")
            }
            SpawnError::PaneFailed { machine, reason } => {
                write!(f, "pane step failed on {machine:?}: {reason}")
            }
            SpawnError::AgentStartFailed { machine, reason } => {
                write!(f, "agent.start failed on {machine:?}: {reason}")
            }
            SpawnError::TimeoutWaitingReady {
                machine,
                agent,
                waited_secs,
            } => write!(
                f,
                "agent {agent:?} on {machine:?} did not become ready within {waited_secs}s"
            ),
            SpawnError::Api { machine, reason } => match machine {
                Some(m) => write!(f, "API error on {m:?}: {reason}"),
                None => write!(f, "API error: {reason}"),
            },
        }
    }
}

impl std::error::Error for SpawnError {}

impl Serialize for SpawnError {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        // Serialize the inner error object (code/message/…) — callers wrap
        // it under "error" via [`SpawnError::to_json`].
        let mut error = serde_json::json!({
            "code": self.code(),
            "message": self.to_string(),
        });
        match self {
            SpawnError::NoPlacement { rejections, .. } => {
                error["rejections"] = serde_json::json!(rejections);
            }
            SpawnError::KindUnsupported { available, .. } => {
                error["available"] = serde_json::json!(available);
            }
            _ => {}
        }
        error.serialize(s)
    }
}

impl From<FleetError> for SpawnError {
    fn from(e: FleetError) -> Self {
        match e {
            FleetError::UnknownMachine { label } => SpawnError::NoPlacement {
                machine: Some(label),
                rejections: Vec::new(),
            },
            FleetError::WorkerUnreachable { machine, reason } => {
                SpawnError::WorkerUnreachable { machine, reason }
            }
            other => SpawnError::Api {
                machine: None,
                reason: other.to_string(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Machine resolution + scheduler caps
// ---------------------------------------------------------------------------

/// Resolve the target machine label.
fn resolve_machine(ctx: &FleetContext, req: &SpawnRequest) -> Result<String, SpawnError> {
    match &req.machine {
        MachineTarget::Label(label) => {
            if ctx.endpoint_for(label).is_none() {
                return Err(SpawnError::NoPlacement {
                    machine: Some(label.clone()),
                    rejections: Vec::new(),
                });
            }
            if ctx.is_offline(label) {
                return Err(SpawnError::WorkerUnreachable {
                    machine: label.clone(),
                    reason: "worker offline (stale heartbeat)".to_string(),
                });
            }
            Ok(label.clone())
        }
        MachineTarget::Auto => {
            let caps = build_worker_caps(ctx);
            let placement = scheduler::place(
                &caps,
                &PlacementRequest {
                    kind: req.kind.clone(),
                    machine_hint: None,
                },
            )
            .map_err(|rejections| SpawnError::NoPlacement {
                machine: None,
                rejections,
            })?;
            Ok(placement.machine)
        }
    }
}

/// Effective `max_agents` for a machine: `cluster.json` override wins,
/// then the worker-reported cap, then the scheduler default — clamped to
/// the hard ceiling.
fn effective_max_agents(override_max: Option<u32>, reported: Option<u32>) -> u32 {
    let v = override_max
        .or(reported)
        .unwrap_or(scheduler::DEFAULT_MAX_AGENTS);
    v.min(scheduler::MAX_AGENTS_CEILING)
}

/// Effective `kinds` for a machine: `kinds_override` replaces the
/// reported/manifest set entirely (PLAN.md §3.7).
fn effective_kinds(override_kinds: &Option<Vec<String>>, reported: &[String]) -> Vec<String> {
    override_kinds.clone().unwrap_or_else(|| reported.to_vec())
}

/// Build [`WorkerCaps`] for every machine (local included — it competes
/// equally, PLAN.md §3.7).
///
/// The local probe (`agent.list` count + `server.agent_manifests` kinds)
/// is best-effort: if local herdr is down the local entry is simply
/// `online: false` and the scheduler rejects it with `Offline`.
fn build_worker_caps(ctx: &FleetContext) -> Vec<WorkerCaps> {
    let mut caps = Vec::with_capacity(ctx.endpoints.len());

    // --- local machine ---
    let local_override = ctx.machines_overrides.get(LOCAL_LABEL);
    let (local_online, local_count, local_kinds) = probe_local(ctx);
    caps.push(WorkerCaps {
        label: LOCAL_LABEL.to_string(),
        online: local_online,
        // No per-machine "enabled" flag exists in the registry schema yet;
        // every known machine is enabled. (Scheduler supports the flag for
        // when config grows one.)
        enabled: true,
        kinds: effective_kinds(
            &local_override.and_then(|o| o.kinds_override.clone()),
            &local_kinds,
        ),
        agent_count: local_count,
        max_agents: effective_max_agents(local_override.and_then(|o| o.max_agents), None),
        mem_available_mb: LOCAL_MEM_AVAILABLE_FALLBACK_MB,
    });

    // --- remote workers ---
    for (label, entry) in &ctx.workers {
        let resources = entry.resources.as_ref();
        let reported_kinds: Vec<String> = resources.map(|r| r.kinds.clone()).unwrap_or_default();
        let override_ = ctx.machines_overrides.get(label);
        caps.push(WorkerCaps {
            label: label.clone(),
            online: entry.status != WorkerStatus::Offline,
            enabled: true,
            kinds: effective_kinds(
                &override_.and_then(|o| o.kinds_override.clone()),
                &reported_kinds,
            ),
            agent_count: resources.map(|r| r.agent_count).unwrap_or(0),
            max_agents: effective_max_agents(
                override_.and_then(|o| o.max_agents),
                resources.map(|r| r.max_agents),
            ),
            mem_available_mb: resources.map(|r| r.mem_available_mb).unwrap_or(0),
        });
    }
    caps
}

/// Best-effort local probe: `(reachable, agent_count, manifest_kinds)`.
fn probe_local(ctx: &FleetContext) -> (bool, u32, Vec<String>) {
    let client = match ctx.local().client() {
        Ok(c) => c,
        Err(_) => return (false, 0, Vec::new()),
    };
    let count = client
        .request_with_timeout("agent.list", &serde_json::json!({}), Duration::from_secs(5))
        .map(|v| parse::items_array(&v).len() as u32)
        .unwrap_or(0);
    let kinds = client
        .server_agent_manifests()
        .map(|v| manifest_kinds(&v))
        .unwrap_or_default();
    (true, count, kinds)
}

/// Extract supported agent kinds from a `server.agent_manifests` response.
///
/// Defensive: recursively collects string values under `kind`,
/// `agent_kind`, `id`, or `slug` keys (objects only), plus any string
/// array under a `kinds` key. Upstream shape drift is expected to be
/// covered by the live e2e bead; when nothing parses we return empty and
/// callers fall back to other signals.
fn manifest_kinds(value: &Value) -> Vec<String> {
    fn walk(v: &Value, out: &mut BTreeSet<String>) {
        match v {
            Value::Object(map) => {
                for key in ["kind", "agent_kind", "id", "slug"] {
                    if let Some(s) = map.get(key).and_then(Value::as_str) {
                        if !s.is_empty() {
                            out.insert(s.to_string());
                        }
                    }
                }
                if let Some(arr) = map.get("kinds").and_then(Value::as_array) {
                    for k in arr.iter().filter_map(Value::as_str) {
                        if !k.is_empty() {
                            out.insert(k.to_string());
                        }
                    }
                }
                for child in map.values() {
                    walk(child, out);
                }
            }
            Value::Array(items) => {
                for child in items {
                    walk(child, out);
                }
            }
            _ => {}
        }
    }
    let mut out = BTreeSet::new();
    walk(value, &mut out);
    out.into_iter().collect()
}

// ---------------------------------------------------------------------------
// Pre-flight, workspace, pane, agent.start, readiness
// ---------------------------------------------------------------------------

/// Verify `kind` is supported on the target. Union of live
/// `server.agent_manifests` kinds and the worker-reported
/// `resources.kinds` (the CLI-presence hint). Skipped when both are empty.
fn check_kind_supported(
    ctx: &FleetContext,
    ep: &MachineEndpoint,
    client: &api_client::ApiClient,
    kind: &str,
) -> Result<(), SpawnError> {
    let manifests = client
        .server_agent_manifests()
        .map_err(|e| step_error(ep, "agent_manifests", e))?;
    let manifest_kinds = manifest_kinds(&manifests);

    let reported: Vec<String> = ctx
        .worker_entry(&ep.label)
        .and_then(|w| w.resources.as_ref())
        .map(|r| r.kinds.clone())
        .unwrap_or_default();

    let mut available: Vec<String> = manifest_kinds;
    for k in &reported {
        if !available.contains(k) {
            available.push(k.clone());
        }
    }

    if available.is_empty() {
        // Cannot verify — proceed; agent.start is the authority and will
        // produce a clear agent_start_failed if the kind is bogus.
        return Ok(());
    }
    if available.iter().any(|k| k == kind) {
        Ok(())
    } else {
        Err(SpawnError::KindUnsupported {
            machine: ep.label.clone(),
            kind: kind.to_string(),
            available,
        })
    }
}

/// Path field aliases on workspace list items.
const WORKSPACE_PATH_KEYS: &[&str] = &["cwd", "root", "path", "repo_path", "worktree_path"];
/// Path field aliases on pane list items (unused today; kept for the
/// workspace-root fallback if pane ids ever need path matching).
#[allow(dead_code)]
const PANE_PATH_KEYS: &[&str] = &["cwd", "path"];

/// Find a workspace id whose cwd matches `repo_path`.
///
/// Matches when the workspace path equals `repo_path` or lies inside it
/// (`<repo>/…`). Pure — unit-tested.
fn find_workspace_by_path(list: &Value, repo_path: &str) -> Option<String> {
    let repo = repo_path.trim_end_matches('/');
    for item in parse::items_array(list) {
        let Some(ws_path) = parse::str_field(item, WORKSPACE_PATH_KEYS) else {
            continue;
        };
        let ws = ws_path.trim_end_matches('/');
        if ws == repo || ws.starts_with(&format!("{repo}/")) {
            if let Some(id) = parse::find_id(item, &["workspace_id", "id"], 2) {
                return Some(id);
            }
        }
    }
    None
}

/// Ensure a workspace exists for `repo_path`; returns `(workspace_id,
/// created)`.
fn ensure_workspace(
    client: &api_client::ApiClient,
    ep: &MachineEndpoint,
    repo_path: &str,
    label: &str,
) -> Result<(String, bool), SpawnError> {
    let list = client
        .workspace_list()
        .map_err(|e| step_error(ep, "workspace.list", e))?;
    if let Some(id) = find_workspace_by_path(&list, repo_path) {
        return Ok((id, false));
    }
    let created = client
        .workspace_create(&WorkspaceCreateParams {
            cwd: Some(repo_path.to_string()),
            label: Some(label.to_string()),
            focus: Some(false),
            env: None,
            source_workspace_id: None,
        })
        .map_err(|e| step_error(ep, "workspace.create", e))?;
    parse::find_id(&created, parse::ID_KEYS, 3)
        .map(|id| (id, true))
        .ok_or_else(|| SpawnError::WorkspaceFailed {
            machine: ep.label.clone(),
            reason: "workspace.create returned no workspace id".to_string(),
        })
}

/// Collect pane ids for a workspace, preserving response order.
fn pane_ids(client: &api_client::ApiClient, workspace_id: &str) -> Result<Vec<String>, SpawnError> {
    let list = client
        .pane_list(Some(workspace_id))
        .map_err(|e| SpawnError::PaneFailed {
            machine: "?".to_string(),
            reason: e.to_string(),
        })?;
    Ok(parse::items_array(&list)
        .iter()
        .filter_map(|p| parse::find_id(p, parse::ID_KEYS, 1))
        .collect())
}

/// Ensure a dedicated pane for the agent. Returns the pane id.
///
/// **Strategy (documented choice):**
/// - If we just created the workspace, its root pane *is* dedicated to this
///   agent — reuse it (no split needed, cleanest read/wait semantics).
/// - If the workspace pre-existed, `pane.split` a new pane (`down`
///   direction, unfocused, cwd = repo) so each agent gets its own pane —
///   PLAN.md §3.8 prefers one pane per agent.
/// - Fallback: if the split response carries no parseable id, re-list
///   panes and pick the first id not present before the split.
fn ensure_pane(
    client: &api_client::ApiClient,
    ep: &MachineEndpoint,
    workspace_id: &str,
    repo_path: &str,
    created_workspace: bool,
) -> Result<String, SpawnError> {
    let before = pane_ids(client, workspace_id).map_err(|mut e| {
        if let SpawnError::PaneFailed { machine, .. } = &mut e {
            *machine = ep.label.clone();
        }
        e
    })?;

    if created_workspace {
        if let Some(first) = before.first() {
            return Ok(first.clone());
        }
        // Fresh workspace with no visible pane — fall through to split.
    }

    let split = client
        .pane_split(&PaneSplitParams {
            direction: SplitDirection::Down,
            target_pane_id: before.first().cloned(),
            cwd: Some(repo_path.to_string()),
            env: None,
            focus: Some(false),
            ratio: None,
            workspace_id: Some(workspace_id.to_string()),
        })
        .map_err(|e| SpawnError::PaneFailed {
            machine: ep.label.clone(),
            reason: e.to_string(),
        })?;

    if let Some(id) = parse::find_id(&split, parse::ID_KEYS, 3) {
        return Ok(id);
    }

    // Fallback: diff the pane list.
    let after = pane_ids(client, workspace_id).map_err(|mut e| {
        if let SpawnError::PaneFailed { machine, .. } = &mut e {
            *machine = ep.label.clone();
        }
        e
    })?;
    let before_set: BTreeSet<&String> = before.iter().collect();
    after
        .into_iter()
        .find(|id| !before_set.contains(id))
        .ok_or_else(|| SpawnError::PaneFailed {
            machine: ep.label.clone(),
            reason: "pane.split succeeded but no new pane id could be determined".to_string(),
        })
}

/// Status strings that mean "the agent CLI is up and interactive".
const READY_STATUSES: &[&str] = &["idle", "interactive_ready", "ready"];

/// Extract a readiness signal from an `agent.get` response: the status
/// string under `status`/`state`, or an `interactive_ready: true` flag.
fn agent_ready(value: &Value) -> bool {
    let entity = parse::unwrap_entity(value, "agent");
    if entity.get("interactive_ready").and_then(Value::as_bool) == Some(true) {
        return true;
    }
    parse::str_field(entity, parse::STATUS_KEYS)
        .map(|s| READY_STATUSES.contains(&s))
        .unwrap_or(false)
}

/// Poll `agent.get` until the agent reports ready or `timeout` elapses.
fn wait_until_ready(
    client: &api_client::ApiClient,
    ep: &MachineEndpoint,
    name: &str,
    timeout: Duration,
) -> Result<(), SpawnError> {
    let deadline = Instant::now() + timeout;
    let target = serde_json::json!({ "target": name });
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(SpawnError::TimeoutWaitingReady {
                machine: ep.label.clone(),
                agent: name.to_string(),
                waited_secs: timeout.as_secs(),
            });
        }
        let req_timeout = remaining.min(READY_POLL_REQUEST_TIMEOUT);
        match client.request_with_timeout("agent.get", &target, req_timeout) {
            Ok(value) => {
                if agent_ready(&value) {
                    return Ok(());
                }
                // Status present but not ready yet — keep polling.
            }
            Err(ApiError::ServerNotRunning { .. }) | Err(ApiError::Io(_)) => {
                return Err(SpawnError::WorkerUnreachable {
                    machine: ep.label.clone(),
                    reason: "API socket went away while waiting for readiness".to_string(),
                });
            }
            Err(ApiError::IncompatibleProtocol { .. }) => {
                return Err(SpawnError::Api {
                    machine: Some(ep.label.clone()),
                    reason: "incompatible herdr API protocol".to_string(),
                });
            }
            // Timeout / server errors (incl. agent not yet registered):
            // retry until the deadline.
            Err(_) => {}
        }
        std::thread::sleep(READY_POLL_INTERVAL.min(remaining));
    }
}

/// Map an `ApiError` to a step-appropriate [`SpawnError`].
fn step_error(ep: &MachineEndpoint, step: &str, e: ApiError) -> SpawnError {
    let reason = e.to_string();
    match e {
        ApiError::ServerNotRunning { .. } | ApiError::Timeout | ApiError::Io(_) => {
            SpawnError::WorkerUnreachable {
                machine: ep.label.clone(),
                reason: format!("{step}: {reason}"),
            }
        }
        _ => SpawnError::Api {
            machine: Some(ep.label.clone()),
            reason: format!("{step}: {reason}"),
        },
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Compose and run a spawn: resolve machine → pre-flight → workspace →
/// pane → `agent.start` → wait ready. See module docs.
pub fn spawn(ctx: &FleetContext, req: &SpawnRequest) -> Result<SpawnResult, SpawnError> {
    let machine = resolve_machine(ctx, req)?;
    let ep = ctx
        .endpoint_for(&machine)
        .expect("resolve_machine guarantees a known endpoint")
        .clone();

    if ep.kind == MachineKind::Remote && ctx.is_offline(&ep.label) {
        return Err(SpawnError::WorkerUnreachable {
            machine: ep.label.clone(),
            reason: "worker offline (stale heartbeat)".to_string(),
        });
    }

    let client = ep.client().map_err(|e| SpawnError::Api {
        machine: Some(ep.label.clone()),
        reason: e.to_string(),
    })?;

    // Pre-flight: kind support (manifests ∪ reported kinds).
    check_kind_supported(ctx, &ep, &client, &req.kind)?;

    // Workspace: find by repo path or create.
    let (workspace_id, created) = ensure_workspace(&client, &ep, &req.repo_path, &req.name)?;

    // Dedicated pane.
    let pane_id = ensure_pane(&client, &ep, &workspace_id, &req.repo_path, created)?;

    // Launch the agent.
    let args = if req.extra_agent_args.is_empty() {
        None
    } else {
        Some(req.extra_agent_args.clone())
    };
    client
        .agent_start(&AgentStartParams {
            pane_id: pane_id.clone(),
            kind: req.kind.clone(),
            name: req.name.clone(),
            args,
            timeout_ms: Some(AGENT_START_TIMEOUT_MS),
        })
        .map_err(|e| SpawnError::AgentStartFailed {
            machine: ep.label.clone(),
            reason: e.to_string(),
        })?;

    // Wait for interactive readiness.
    let ready_timeout = req.ready_timeout.unwrap_or(DEFAULT_READY_TIMEOUT);
    wait_until_ready(&client, &ep, &req.name, ready_timeout)?;

    Ok(SpawnResult {
        global_id: format_global_id(&ep.label, &req.name),
        machine: ep.label.clone(),
        agent: req.name.clone(),
        kind: req.kind.clone(),
        pane_id,
        workspace_id,
        branch: req.branch.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::FleetContext;
    use cluster_core::protocol::Resources;
    use cluster_core::registry::{ClusterInfo, WorkerEntry, WorkerStatus};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn cluster() -> ClusterInfo {
        ClusterInfo {
            cluster_id: "c".into(),
            name: "n".into(),
            port: 1,
            protocol_version: 1,
            created_at: chrono::Utc::now(),
            machines: BTreeMap::new(),
        }
    }

    fn worker(label: &str, status: WorkerStatus, kinds: Vec<String>) -> WorkerEntry {
        WorkerEntry {
            worker_id: format!("id-{label}"),
            label: label.into(),
            pubkey: "ab".repeat(32),
            paired_at: chrono::Utc::now(),
            last_seen: None,
            status,
            resources: Some(Resources {
                cpu_cores: 8,
                mem_total_mb: 16384,
                mem_available_mb: 8192,
                agent_count: 0,
                max_agents: 4,
                load_avg: vec![],
                kinds,
            }),
        }
    }

    fn req(machine: MachineTarget) -> SpawnRequest {
        SpawnRequest {
            name: "agent-1".into(),
            kind: "claude".into(),
            machine,
            repo_path: "/repo".into(),
            branch: None,
            extra_agent_args: vec![],
            ready_timeout: Some(Duration::from_millis(10)),
        }
    }

    #[test]
    fn manifest_kinds_extracts_from_shapes() {
        // {"manifests":[{"kind":"claude"},{"agent_kind":"codex"}]}
        let v = json!({"manifests": [
            {"kind": "claude", "name": "Claude"},
            {"agent_kind": "codex"},
            {"id": "gemini", "name": "Gemini"}
        ]});
        let kinds = manifest_kinds(&v);
        assert!(kinds.contains(&"claude".to_string()));
        assert!(kinds.contains(&"codex".to_string()));
        assert!(kinds.contains(&"gemini".to_string()));

        // Bare kinds array.
        let v2 = json!({"kinds": ["a", "b"]});
        assert_eq!(manifest_kinds(&v2), vec!["a".to_string(), "b".to_string()]);

        // Garbage → empty.
        assert!(manifest_kinds(&json!({"x": 1})).is_empty());
    }

    #[test]
    fn find_workspace_by_path_matching() {
        let list = json!({"workspaces": [
            {"id": "w1", "cwd": "/repo"},
            {"workspace_id": "w2", "root": "/other"},
            {"id": "w3", "path": "/repo/sub/dir"}
        ]});
        // Exact match wins (first in order that matches).
        assert_eq!(find_workspace_by_path(&list, "/repo"), Some("w1".into()));
        // Workspace inside repo matches.
        assert_eq!(
            find_workspace_by_path(&list, "/repo/sub"),
            Some("w3".into())
        );
        // Trailing slash tolerated.
        assert_eq!(find_workspace_by_path(&list, "/repo/"), Some("w1".into()));
        // No match.
        assert_eq!(find_workspace_by_path(&list, "/nowhere"), None);
    }

    #[test]
    fn agent_ready_recognizes_states() {
        assert!(agent_ready(&json!({"agent": {"status": "idle"}})));
        assert!(agent_ready(&json!({"status": "interactive_ready"})));
        assert!(agent_ready(&json!({"state": "ready"})));
        assert!(agent_ready(&json!({"interactive_ready": true})));
        assert!(!agent_ready(&json!({"agent": {"status": "working"}})));
        assert!(!agent_ready(&json!({"agent": {}})));
    }

    #[test]
    fn explicit_unknown_machine_is_no_placement() {
        let ctx = FleetContext::build(PathBuf::from("/tmp/hc"), cluster(), vec![]).unwrap();
        let mut r = req(MachineTarget::Label("ghost".into()));
        r.ready_timeout = None;
        match spawn(&ctx, &r) {
            Err(SpawnError::NoPlacement { machine, .. }) => {
                assert_eq!(machine.as_deref(), Some("ghost"))
            }
            other => panic!("expected NoPlacement, got {other:?}"),
        }
    }

    #[test]
    fn auto_placement_rejects_all_offline() {
        // One worker, offline, wrong kind anyway → NoPlacement with reasons.
        let ctx = FleetContext::build(
            PathBuf::from("/tmp/hc"),
            cluster(),
            vec![worker("w1", WorkerStatus::Offline, vec!["other".into()])],
        )
        .unwrap();
        // Local probe will fail (no herdr) → local offline too.
        let r = req(MachineTarget::Auto);
        match spawn(&ctx, &r) {
            Err(SpawnError::NoPlacement { rejections, .. }) => {
                assert!(!rejections.is_empty());
            }
            // If a dev machine runs herdr, local may be online and proceed
            // to a later step failing on socket/workspace — still a typed
            // SpawnError, which is what this test guards.
            Err(e) => assert!(!e.code().is_empty()),
            Ok(_) => panic!("spawn unexpectedly succeeded without herdr"),
        }
    }

    #[test]
    fn worker_caps_reflect_registry_and_overrides() {
        let mut c = cluster();
        c.machines.insert(
            "w1".into(),
            cluster_core::registry::MachineOverride {
                max_agents: Some(2),
                kinds_override: Some(vec!["custom".into()]),
            },
        );
        let ctx = FleetContext::build(
            PathBuf::from("/tmp/hc"),
            c,
            vec![worker("w1", WorkerStatus::Online, vec!["claude".into()])],
        )
        .unwrap();
        let caps = build_worker_caps(&ctx);
        assert_eq!(caps.len(), 2); // local + w1
        let w1 = caps.iter().find(|c| c.label == "w1").unwrap();
        assert!(w1.online);
        assert_eq!(w1.kinds, vec!["custom".to_string()]); // override wins
        assert_eq!(w1.max_agents, 2); // override wins
        assert_eq!(w1.agent_count, 0);
        assert_eq!(w1.mem_available_mb, 8192);

        let local = caps.iter().find(|c| c.label == "local").unwrap();
        assert_eq!(local.max_agents, scheduler::DEFAULT_MAX_AGENTS);
        assert_eq!(local.mem_available_mb, LOCAL_MEM_AVAILABLE_FALLBACK_MB);
        // Local online-ness depends on whether herdr runs in the test env;
        // just assert the field exists logically (bool either way).
        let _ = local.online;
    }

    #[test]
    fn spawn_error_codes_stable() {
        assert_eq!(
            SpawnError::KindUnsupported {
                machine: "m".into(),
                kind: "k".into(),
                available: vec![]
            }
            .code(),
            "kind_unsupported"
        );
        assert_eq!(
            SpawnError::TimeoutWaitingReady {
                machine: "m".into(),
                agent: "a".into(),
                waited_secs: 1
            }
            .code(),
            "timeout_waiting_ready"
        );
        let e = SpawnError::NoPlacement {
            machine: None,
            rejections: vec![Rejection {
                machine: "w".into(),
                reason: scheduler::RejectionReason::Offline,
            }],
        };
        let j = e.to_json();
        assert_eq!(j["error"]["code"], "no_placement");
        assert_eq!(j["error"]["rejections"][0]["machine"], "w");
    }

    #[test]
    fn offline_explicit_label_short_circuits() {
        let ctx = FleetContext::build(
            PathBuf::from("/tmp/hc"),
            cluster(),
            vec![worker("w1", WorkerStatus::Offline, vec!["claude".into()])],
        )
        .unwrap();
        let r = req(MachineTarget::Label("w1".into()));
        match spawn(&ctx, &r) {
            Err(SpawnError::WorkerUnreachable { machine, .. }) => {
                assert_eq!(machine, "w1")
            }
            other => panic!("expected WorkerUnreachable, got {other:?}"),
        }
    }
}
