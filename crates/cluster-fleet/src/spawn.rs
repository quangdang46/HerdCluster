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
//! 3. **Ensure workspace** — `workspace.list` searched by repo path (or,
//!    when `--repo` is omitted, the machine's single existing workspace),
//!    else `workspace.create`. See [`ensure_workspace`].
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
//! `repo_path` is optional (`None` when `--repo` is omitted):
//! - Omitted → reuse the target machine's existing workspace when exactly
//!   one exists; otherwise `workspace.create` **without `cwd`** so the
//!   herdr server picks its default working directory (the server user's
//!   home directory in practice — `cwd` is optional in the 0.9.5 schema).
//! - `"~"` / `"~/…"` → expanded against the target's home directory. For
//!   the local machine that is `$HOME`; a remote worker's home is not
//!   discoverable through the API, so `~` there degrades to the same
//!   default-placement behavior as omitting `--repo`.
//! - Any other value → find-or-create a workspace for that target-local
//!   path, exactly as before.
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

use std::collections::{BTreeSet, HashMap};
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
    /// `None` = `--repo` omitted: reuse the machine's single workspace or
    /// create one with the server-default cwd.
    pub repo_path: Option<String>,
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
/// `agent_kind`, `agent`, `id`, or `slug` keys (objects only), plus any string
/// array under a `kinds` key. Upstream shape drift is expected to be
/// covered by the live e2e bead; when nothing parses we return empty and
/// callers fall back to other signals.
fn manifest_kinds(value: &Value) -> Vec<String> {
    fn walk(v: &Value, out: &mut BTreeSet<String>) {
        match v {
            Value::Object(map) => {
                for key in ["kind", "agent_kind", "agent", "id", "slug"] {
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

/// Path field aliases on workspace list items. herdr 0.9.5 `WorkspaceInfo`
/// has NO top-level cwd — kept for older/alternate shapes.
const WORKSPACE_PATH_KEYS: &[&str] = &["cwd", "root", "path", "repo_path", "worktree_path"];
/// Path field aliases nested under `worktree` on a `WorkspaceInfo`
/// (`WorkspaceWorktreeInfo`: `checkout_path`, `repo_root`). Present only
/// for workspaces that belong to a Herdr worktree group.
const WORKTREE_PATH_KEYS: &[&str] = &["checkout_path", "repo_root"];
/// Pane cwd aliases used for repo matching (`PaneInfo.cwd` /
/// `foreground_cwd`).
const PANE_CWD_KEYS: &[&str] = &["cwd", "foreground_cwd"];

/// Extract the workspace id from a workspace-ish response.
///
/// herdr 0.9.5 `workspace.create` returns
/// `{"type":"workspace_created","workspace":{…WorkspaceInfo…},"tab":{…},
/// "root_pane":{…PaneInfo…}}`. The id MUST come from the `workspace`
/// object (`workspace_id`, falling back to `id`). A generic id-key search
/// over the whole response can land on `root_pane.pane_id` ("w2:p1") —
/// pane and workspace ids share the `w<N>:…` namespace shape only in
/// appearance; passing a pane id where a workspace id is expected fails
/// downstream with `workspace_not_found` (the spawn blocker this guards
/// against). There is deliberately NO fallback to `pane_id`/`agent_id`.
fn workspace_id_of(value: &Value) -> Option<String> {
    let ws = parse::unwrap_entity(value, "workspace");
    parse::find_id(ws, &["workspace_id", "id"], 2)
        .or_else(|| parse::find_id(value, &["workspace_id"], 3))
}

/// Extract a pane id from a pane-ish response (`pane.split` →
/// `{"type":"pane_info","pane":{…}}`, possibly `tab_created` with a
/// `root_pane`). Prefers `pane_id` inside the `pane` wrapper; the
/// top-level `pane_id`-only fallback can still find `root_pane.pane_id`,
/// which IS a pane id — safe here, unlike the workspace case.
fn pane_id_of(value: &Value) -> Option<String> {
    let pane = parse::unwrap_entity(value, "pane");
    parse::find_id(pane, &["pane_id", "id"], 2).or_else(|| parse::find_id(value, &["pane_id"], 3))
}

/// Path candidates that identify which repo a workspace belongs to:
/// top-level aliases (older shapes), `worktree.checkout_path` /
/// `worktree.repo_root` (herdr 0.9.5), plus every pane `cwd` /
/// `foreground_cwd` reported for the workspace by `pane.list`.
fn workspace_path_candidates<'a>(
    item: &'a Value,
    pane_cwds: &'a HashMap<String, Vec<String>>,
) -> Vec<&'a str> {
    let mut out: Vec<&str> = Vec::new();
    if let Some(p) = parse::str_field(item, WORKSPACE_PATH_KEYS) {
        out.push(p);
    }
    if let Some(wt) = item.get("worktree") {
        // Collect EVERY worktree path alias — `checkout_path` (the linked
        // worktree dir, outside the repo) and `repo_root` (the main
        // checkout, == the repo) carry different matching value, so the
        // first-match `str_field` helper is not sufficient here.
        if let Some(wt_obj) = wt.as_object() {
            for key in WORKTREE_PATH_KEYS {
                if let Some(p) = wt_obj.get(*key).and_then(Value::as_str) {
                    if !p.is_empty() {
                        out.push(p);
                    }
                }
            }
        }
    }
    if let Some(ws_id) = parse::str_field(item, &["workspace_id", "id"]) {
        if let Some(cwds) = pane_cwds.get(ws_id) {
            out.extend(cwds.iter().map(String::as_str));
        }
    }
    out
}

/// `repo` is a prefix of `candidate` at a path boundary: equal, or
/// `candidate` lies inside `repo` (`<repo>/…`). Both inputs must already
/// be trimmed of trailing slashes.
fn path_within(repo: &str, candidate: &str) -> bool {
    candidate == repo || candidate.starts_with(&format!("{repo}/"))
}

/// Filesystem-canonical form of `path` for workspace matching, falling
/// back to the trimmed input when it cannot be canonicalized.
///
/// herdr reports pane cwds in canonical form — on macOS `/tmp` resolves
/// to `/private/tmp` — while `--repo` arrives verbatim. Comparing
/// canonical forms on both sides prevents duplicate workspaces for the
/// same directory. Paths on remote workers typically do not exist on the
/// machine running herdcluster, so they keep the raw string and the
/// previous comparison behavior.
fn canonicalize_for_match(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    let probe = if trimmed.is_empty() { "/" } else { trimmed };
    match std::fs::canonicalize(probe) {
        Ok(p) => p.to_string_lossy().into_owned(),
        Err(_) => trimmed.to_string(),
    }
}

/// `candidate` identifies `repo` when it equals the repo path or lies
/// inside it (`<repo>/…`) — same rule the original path matcher used,
/// checked both verbatim and in filesystem-canonical form (see
/// [`canonicalize_for_match`]).
fn path_matches_repo(candidate: &str, repo: &str) -> bool {
    if path_within(repo.trim_end_matches('/'), candidate.trim_end_matches('/')) {
        return true;
    }
    path_within(
        canonicalize_for_match(repo).trim_end_matches('/'),
        canonicalize_for_match(candidate).trim_end_matches('/'),
    )
}

/// Pure: workspace id to reuse when `--repo` is omitted — exactly one
/// workspace on the machine, per the spawn contract.
fn sole_workspace_id(list: &Value) -> Option<String> {
    let items = parse::items_array(list);
    if items.len() == 1 {
        workspace_id_of(&items[0])
    } else {
        None
    }
}

/// Pure: find the workspace serving `repo_path`.
///
/// `list` is a `workspace.list` result; `panes` is an (optional, possibly
/// empty) `pane.list` result used to recover cwds that `WorkspaceInfo`
/// does not expose. Matches when any path candidate equals `repo_path`
/// or lies inside it.
fn find_workspace_for_repo(list: &Value, panes: &Value, repo_path: &str) -> Option<String> {
    // workspace_id → pane cwds, from `pane.list` (PaneInfo.cwd /
    // foreground_cwd). Best-effort: callers pass an empty value on error.
    let mut pane_cwds: HashMap<String, Vec<String>> = HashMap::new();
    for p in parse::items_array(panes) {
        let Some(ws_id) = parse::str_field(p, &["workspace_id"]) else {
            continue;
        };
        let entry = pane_cwds.entry(ws_id.to_string()).or_default();
        for key in PANE_CWD_KEYS {
            if let Some(cwd) = p.get(*key).and_then(Value::as_str) {
                if !cwd.is_empty() && !entry.iter().any(|c| c == cwd) {
                    entry.push(cwd.to_string());
                }
            }
        }
    }

    for item in parse::items_array(list) {
        if workspace_path_candidates(item, &pane_cwds)
            .iter()
            .any(|c| path_matches_repo(c, repo_path))
        {
            if let Some(id) = workspace_id_of(item) {
                return Some(id);
            }
        }
    }
    None
}

/// `workspace.create` wrapper shared by both placement modes. `cwd=None`
/// omits the field so the herdr server picks its default working
/// directory (its user's home in practice).
fn create_workspace(
    client: &api_client::ApiClient,
    ep: &MachineEndpoint,
    cwd: Option<&str>,
    label: &str,
) -> Result<(String, bool), SpawnError> {
    let created = client
        .workspace_create(&WorkspaceCreateParams {
            cwd: cwd.map(str::to_string),
            label: Some(label.to_string()),
            focus: Some(false),
            env: None,
            source_workspace_id: None,
        })
        .map_err(|e| step_error(ep, "workspace.create", e))?;
    workspace_id_of(&created)
        .map(|id| (id, true))
        .ok_or_else(|| SpawnError::WorkspaceFailed {
            machine: ep.label.clone(),
            reason: "workspace.create returned no workspace id".to_string(),
        })
}

/// Ensure a workspace exists for the requested placement; returns
/// `(workspace_id, created)`.
///
/// `repo` is the effective target-local path (`None` = `--repo` omitted
/// or unresolvable `~`):
/// - `None` → reuse the machine's workspace when exactly one exists,
///   else create with `cwd` omitted (server default).
/// - `Some(path)` → find a workspace serving that path (workspace list +
///   pane cwds + worktree provenance), else `workspace.create {cwd}`.
fn ensure_workspace(
    client: &api_client::ApiClient,
    ep: &MachineEndpoint,
    repo: Option<&str>,
    label: &str,
) -> Result<(String, bool), SpawnError> {
    let list = client
        .workspace_list()
        .map_err(|e| step_error(ep, "workspace.list", e))?;

    match repo {
        None => {
            if let Some(id) = sole_workspace_id(&list) {
                return Ok((id, false));
            }
            create_workspace(client, ep, None, label)
        }
        Some(path) => {
            // `pane.list` is best-effort: WorkspaceInfo alone cannot prove
            // a path match for plain (non-worktree) workspaces, but a
            // failed pane list must not block the create fallback.
            let panes = client
                .pane_list(None)
                .unwrap_or_else(|_| serde_json::json!({"panes": []}));
            if let Some(id) = find_workspace_for_repo(&list, &panes, path) {
                return Ok((id, false));
            }
            create_workspace(client, ep, Some(path), label)
        }
    }
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
        .filter_map(pane_id_of)
        .collect())
}

/// Pane operations used by [`ensure_pane`], abstracted so tests can fake
/// the herdr API without a socket. Implemented for
/// [`api_client::ApiClient`].
trait PaneOps {
    /// Pane ids currently in `workspace_id`, in response order.
    fn list_pane_ids(&self, workspace_id: &str) -> Result<Vec<String>, SpawnError>;
    /// `pane.split` — returns the raw response for id extraction.
    fn split_pane(&self, params: &PaneSplitParams) -> Result<Value, SpawnError>;
}

impl PaneOps for api_client::ApiClient {
    fn list_pane_ids(&self, workspace_id: &str) -> Result<Vec<String>, SpawnError> {
        pane_ids(self, workspace_id)
    }

    fn split_pane(&self, params: &PaneSplitParams) -> Result<Value, SpawnError> {
        self.pane_split(params).map_err(|e| SpawnError::PaneFailed {
            machine: "?".to_string(),
            reason: e.to_string(),
        })
    }
}

/// Ensure a dedicated pane for the agent. Returns the pane id.
///
/// **Strategy (documented choice):** always `pane.split` a new pane
/// (`down` direction, unfocused, cwd = repo) — PLAN.md §3.8 prefers one
/// pane per agent. The root pane of a just-created workspace is
/// deliberately NOT reused: `workspace.create` returns while that pane's
/// shell is still spawning, and `pane.list` already sees it, so
/// `agent.start` raced it and failed with `agent_pane_busy` ("not an
/// available shell"). A freshly split pane is shell-ready by the time
/// `pane.split` returns, which the QA retry path proved reliable.
///
/// - Fallback: if the split response carries no parseable id, re-list
///   panes and pick the first id not present before the split.
fn ensure_pane(
    client: &dyn PaneOps,
    ep: &MachineEndpoint,
    workspace_id: &str,
    repo_path: Option<&str>,
) -> Result<String, SpawnError> {
    let before = client.list_pane_ids(workspace_id).map_err(|mut e| {
        if let SpawnError::PaneFailed { machine, .. } = &mut e {
            *machine = ep.label.clone();
        }
        e
    })?;

    let split = client
        .split_pane(&PaneSplitParams {
            direction: SplitDirection::Down,
            target_pane_id: before.first().cloned(),
            cwd: repo_path.map(str::to_string),
            env: None,
            focus: Some(false),
            ratio: None,
            workspace_id: Some(workspace_id.to_string()),
        })
        .map_err(|mut e| {
            if let SpawnError::PaneFailed { machine, .. } = &mut e {
                *machine = ep.label.clone();
            }
            e
        })?;

    if let Some(id) = pane_id_of(&split) {
        return Ok(id);
    }

    // Fallback: diff the pane list.
    let after = client.list_pane_ids(workspace_id).map_err(|mut e| {
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
/// `blocked` = waiting for input (login/permission prompt) and `done` =
/// finished its task — both are stable interactive states herdr reports
/// for a live agent, so spawn should not keep polling through them.
const READY_STATUSES: &[&str] = &["idle", "interactive_ready", "ready", "blocked", "done"];

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

/// Expand a leading `~` / `~/…` in a `--repo` value against the target
/// machine's home directory.
///
/// Returns `Some(path)` for ordinary absolute/relative target-local paths
/// and for `~` on the local machine (`$HOME`). Returns `None` — the
/// default-placement mode — when `--repo` was omitted, was blank, or was
/// `~`-rooted on a **remote** worker: a remote home directory is not
/// discoverable through the herdr API, so `~` degrades to the same
/// behavior as omitting `--repo` (reuse the single workspace, else
/// `workspace.create` with server-default cwd, which is the remote
/// server user's home in practice).
fn effective_repo_path(req_repo: &Option<String>, is_local: bool) -> Option<String> {
    let raw = req_repo.as_deref()?.trim();
    if raw.is_empty() {
        return None;
    }
    if raw == "~" || raw.starts_with("~/") {
        if !is_local {
            return None;
        }
        let home = std::env::home_dir()
            .map(|p| p.to_string_lossy().into_owned())
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| "/".to_string());
        return Some(if raw == "~" {
            home
        } else {
            format!("{home}/{}", &raw[2..])
        });
    }
    Some(raw.to_string())
}

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

    // Effective repo placement: `--repo` path, `~` expansion, or default
    // placement (`None`) when omitted.
    let repo = effective_repo_path(&req.repo_path, ep.kind == MachineKind::Local);

    // Workspace: find by repo path / sole workspace, or create.
    let (workspace_id, _created) = ensure_workspace(&client, &ep, repo.as_deref(), &req.name)?;

    // Dedicated pane — always a fresh split (see ensure_pane for why the
    // root pane of a new workspace is not reused).
    let pane_id = ensure_pane(&client, &ep, &workspace_id, repo.as_deref())?;

    // Launch the agent.
    let args = if req.extra_agent_args.is_empty() {
        None
    } else {
        Some(req.extra_agent_args.clone())
    };
    // `pane.split` returns before the new pane's shell is an available
    // target, so `agent.start` can race it with `agent_pane_busy`. Retry
    // briefly — the shell is ready within a second or two in practice.
    let pane_busy_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match client.agent_start(&AgentStartParams {
            pane_id: pane_id.clone(),
            kind: req.kind.clone(),
            name: req.name.clone(),
            args: args.clone(),
            timeout_ms: Some(AGENT_START_TIMEOUT_MS),
        }) {
            Ok(_) => break,
            Err(e) => {
                let reason = e.to_string();
                if reason.contains("agent_pane_busy") && Instant::now() < pane_busy_deadline {
                    std::thread::sleep(Duration::from_millis(500));
                    continue;
                }
                return Err(SpawnError::AgentStartFailed {
                    machine: ep.label.clone(),
                    reason,
                });
            }
        }
    }

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
            keepawake: true,
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
            repo_path: Some("/repo".into()),
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
    fn manifest_kinds_reads_herdr_095_agent_field() {
        // Real `server.agent_manifests` shape from herdr 0.9.5: kind names
        // live under `agent`, not `kind` — regression for the loopback
        // e2e that found spawn rejecting every kind as unsupported.
        let v = json!({
            "type": "agent_manifest_status",
            "last_result": "checked",
            "manifests": [
                {"agent": "claude", "source": "remote:.../claude.toml", "source_kind": "remote", "active_version": "2026.10.11.1"},
                {"agent": "codex", "source": "remote:.../codex.toml", "source_kind": "remote", "active_version": "2026.10.11.1"},
                {"agent": "gemini", "source": "remote:.../gemini.toml", "source_kind": "remote", "active_version": "2026.06.10.1"}
            ]
        });
        let kinds = manifest_kinds(&v);
        assert!(kinds.contains(&"claude".to_string()));
        assert!(kinds.contains(&"codex".to_string()));
        assert!(kinds.contains(&"gemini".to_string()));
        // Non-kind strings must not leak in.
        assert!(!kinds.contains(&"remote".to_string()));
        assert!(!kinds.contains(&"checked".to_string()));
    }

    /// Real herdr 0.9.5 `workspace.create` result shape (from the API
    /// schema `workspace_created`): `{type, workspace, tab, root_pane}`
    /// where `root_pane` is a full `PaneInfo` carrying BOTH `pane_id`
    /// ("w2E:p1") and `workspace_id` ("w2E").
    fn workspace_created_fixture() -> Value {
        json!({
            "type": "workspace_created",
            "workspace": {
                "workspace_id": "w2E", "number": 17, "label": "agent-1",
                "focused": false, "pane_count": 1, "tab_count": 1,
                "active_tab_id": "w2E:t1", "agent_status": "unknown"
            },
            "tab": {
                "tab_id": "w2E:t1", "workspace_id": "w2E", "number": 1,
                "label": "1", "focused": true, "pane_count": 1,
                "agent_status": "unknown"
            },
            "root_pane": {
                "pane_id": "w2E:p1", "terminal_id": "term_abc",
                "workspace_id": "w2E", "tab_id": "w2E:t1", "focused": true,
                "agent_status": "unknown", "cwd": "/repo", "revision": 0
            }
        })
    }

    #[test]
    fn workspace_id_of_never_returns_pane_id() {
        // Regression for the spawn blocker: a generic id search over the
        // workspace.create response lands on root_pane.pane_id ("w2E:p1")
        // and the pane step then fails with `workspace w2E:p1 not found`.
        let created = workspace_created_fixture();
        assert_eq!(workspace_id_of(&created).as_deref(), Some("w2E"));
        // The buggy generic search demonstrably finds the pane id first —
        // proving the dedicated extractor is load-bearing.
        assert_eq!(
            parse::find_id(&created, &["pane_id", "workspace_id", "id"], 3).as_deref(),
            Some("w2E:p1")
        );
    }

    #[test]
    fn workspace_id_of_reads_workspace_info_and_list_items() {
        // `workspace.get`-style single-entity response.
        let info = json!({"type": "workspace_info", "workspace": {
            "workspace_id": "w2", "number": 1, "label": "x", "focused": true,
            "pane_count": 1, "tab_count": 1, "active_tab_id": "w2:t1",
            "agent_status": "unknown"
        }});
        assert_eq!(workspace_id_of(&info).as_deref(), Some("w2"));
        // Bare WorkspaceInfo item (workspace.list element).
        let item = json!({"workspace_id": "w3", "label": "y"});
        assert_eq!(workspace_id_of(&item).as_deref(), Some("w3"));
        // Garbage → None, never a pane/agent id.
        assert_eq!(workspace_id_of(&json!({"pane_id": "w9:p1"})), None);
    }

    #[test]
    fn sole_workspace_id_requires_exactly_one() {
        let one = json!({"type": "workspace_list", "workspaces": [
            {"workspace_id": "w2", "label": "only"}
        ]});
        assert_eq!(sole_workspace_id(&one).as_deref(), Some("w2"));
        let two = json!({"workspaces": [
            {"workspace_id": "w2"}, {"workspace_id": "w3"}
        ]});
        assert_eq!(sole_workspace_id(&two), None);
        let zero = json!({"workspaces": []});
        assert_eq!(sole_workspace_id(&zero), None);
    }

    #[test]
    fn find_workspace_for_repo_matches_real_095_shapes() {
        // Real herdr 0.9.5 workspace.list: WorkspaceInfo has NO top-level
        // cwd; path info lives under `worktree` (worktree groups only) or
        // is recoverable from pane.list cwds.
        let list = json!({"type": "workspace_list", "workspaces": [
            // Plain workspace, no worktree provenance — matched via pane cwd.
            {"workspace_id": "w2", "label": "proj", "number": 1,
             "focused": true, "pane_count": 1, "tab_count": 1,
             "active_tab_id": "w2:t1", "agent_status": "unknown"},
            // Worktree-group workspace — matched via worktree.repo_root.
            {"workspace_id": "w3", "label": "wt", "number": 2,
             "focused": false, "pane_count": 1, "tab_count": 1,
             "active_tab_id": "w3:t1", "agent_status": "unknown",
             "worktree": {
                 "repo_key": "/repo/.git", "repo_name": "repo",
                 "repo_root": "/repo",
                 "checkout_path": "/home/u/.herdr/worktrees/repo/wt",
                 "is_linked_worktree": true
             }},
            // Unrelated workspace.
            {"workspace_id": "w4", "label": "other", "number": 3,
             "focused": false, "pane_count": 1, "tab_count": 1,
             "active_tab_id": "w4:t1", "agent_status": "unknown"}
        ]});
        let panes = json!({"type": "pane_list", "panes": [
            {"pane_id": "w2:p1", "workspace_id": "w2", "tab_id": "w2:t1",
             "cwd": "/repo", "focused": true, "agent_status": "unknown",
             "revision": 0, "terminal_id": "term_1"},
            {"pane_id": "w4:p1", "workspace_id": "w4", "tab_id": "w4:t1",
             "cwd": "/elsewhere", "focused": false, "agent_status": "unknown",
             "revision": 0, "terminal_id": "term_2"}
        ]});

        // Pane cwd equality matches the plain workspace (first in order).
        assert_eq!(
            find_workspace_for_repo(&list, &panes, "/repo"),
            Some("w2".into())
        );
        // worktree.repo_root equality also matches (w2 wins on order, so
        // use a repo only w3 serves).
        let list2 = json!({"workspaces": [
            {"workspace_id": "w3", "worktree": {"repo_root": "/repo2",
                "checkout_path": "/wt/x", "repo_key": "/repo2/.git",
                "repo_name": "r", "is_linked_worktree": true}},
            {"workspace_id": "w9"}
        ]});
        assert_eq!(
            find_workspace_for_repo(&list2, &json!({"panes": []}), "/repo2"),
            Some("w3".into())
        );
        // Inside-repo pane cwd matches; trailing slash tolerated.
        let panes_sub = json!({"panes": [
            {"pane_id": "w5:p1", "workspace_id": "w5",
             "cwd": "/repo/sub/dir"}
        ]});
        let list3 = json!({"workspaces": [{"workspace_id": "w5"}]});
        assert_eq!(
            find_workspace_for_repo(&list3, &panes_sub, "/repo/"),
            Some("w5".into())
        );
        // No candidates → None.
        assert_eq!(
            find_workspace_for_repo(&list3, &json!({"panes": []}), "/nowhere"),
            None
        );
    }

    #[test]
    fn pane_id_of_reads_pane_info_wrapper() {
        // pane.split → {"type":"pane_info","pane":{PaneInfo}}.
        let split = json!({"type": "pane_info", "pane": {
            "pane_id": "w2:p2", "workspace_id": "w2", "tab_id": "w2:t1",
            "terminal_id": "term_9", "focused": false,
            "agent_status": "unknown", "revision": 0
        }});
        assert_eq!(pane_id_of(&split).as_deref(), Some("w2:p2"));
        // tab_created shape: id lives under root_pane.
        let created = json!({"type": "tab_created",
            "tab": {"tab_id": "w2:t2", "workspace_id": "w2"},
            "root_pane": {"pane_id": "w2:p3", "workspace_id": "w2"}});
        assert_eq!(pane_id_of(&created).as_deref(), Some("w2:p3"));
        // Bare PaneInfo item.
        assert_eq!(
            pane_id_of(&json!({"pane_id": "w1:p1"})).as_deref(),
            Some("w1:p1")
        );
    }

    #[test]
    fn effective_repo_path_handles_tilde_and_blank() {
        // Omitted / blank → default placement.
        assert_eq!(effective_repo_path(&None, true), None);
        assert_eq!(effective_repo_path(&Some("  ".into()), true), None);
        // Ordinary path passes through.
        assert_eq!(
            effective_repo_path(&Some("/repo".into()), false),
            Some("/repo".into())
        );
        // `~` on a remote worker → default placement (remote home is not
        // discoverable via the API).
        assert_eq!(effective_repo_path(&Some("~".into()), false), None);
        assert_eq!(effective_repo_path(&Some("~/proj".into()), false), None);
        // `~` on local expands via $HOME (present in test envs; if absent
        // the fallback is "/").
        let home = std::env::home_dir().unwrap_or_else(|| "/".into());
        let home = home.to_string_lossy().into_owned();
        assert_eq!(
            effective_repo_path(&Some("~".into()), true),
            Some(home.clone())
        );
        assert_eq!(
            effective_repo_path(&Some("~/proj".into()), true),
            Some(format!("{home}/proj"))
        );
    }

    #[test]
    fn agent_ready_recognizes_states() {
        assert!(agent_ready(&json!({"agent": {"status": "idle"}})));
        assert!(agent_ready(&json!({"status": "interactive_ready"})));
        assert!(agent_ready(&json!({"state": "ready"})));
        // herdr 0.9.5 field name + post-launch interactive states.
        assert!(agent_ready(&json!({"agent": {"agent_status": "idle"}})));
        assert!(agent_ready(&json!({"agent": {"agent_status": "blocked"}})));
        assert!(agent_ready(&json!({"agent": {"agent_status": "done"}})));
        assert!(agent_ready(&json!({"interactive_ready": true})));
        assert!(!agent_ready(&json!({"agent": {"status": "working"}})));
        assert!(!agent_ready(&json!({"agent": {"agent_status": "working"}})));
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

    // ------------------------------------------------------------------
    // B2 regression: ensure_pane must never reuse a fresh workspace's
    // root pane (agent_pane_busy race).
    // ------------------------------------------------------------------

    /// Fake [`PaneOps`]: serves queued `pane.list` id sets and records
    /// `pane.split` calls.
    struct FakePaneOps {
        /// Id sets returned by successive `list_pane_ids` calls.
        list_results: std::cell::RefCell<std::collections::VecDeque<Vec<String>>>,
        /// Response returned by `split_pane`.
        split_response: Value,
        /// Number of `split_pane` invocations.
        split_calls: std::cell::Cell<usize>,
        /// Params of the last `split_pane` call.
        last_params: std::cell::RefCell<Option<PaneSplitParams>>,
    }

    impl FakePaneOps {
        fn new(lists: Vec<Vec<String>>, split_response: Value) -> Self {
            Self {
                list_results: std::cell::RefCell::new(lists.into()),
                split_response,
                split_calls: std::cell::Cell::new(0),
                last_params: std::cell::RefCell::new(None),
            }
        }
    }

    impl PaneOps for FakePaneOps {
        fn list_pane_ids(&self, _workspace_id: &str) -> Result<Vec<String>, SpawnError> {
            Ok(self
                .list_results
                .borrow_mut()
                .pop_front()
                .unwrap_or_default())
        }

        fn split_pane(&self, params: &PaneSplitParams) -> Result<Value, SpawnError> {
            self.split_calls.set(self.split_calls.get() + 1);
            *self.last_params.borrow_mut() = Some(params.clone());
            Ok(self.split_response.clone())
        }
    }

    fn test_endpoint() -> MachineEndpoint {
        MachineEndpoint {
            label: "local".into(),
            socket_path: PathBuf::from("/nonexistent.sock"),
            kind: MachineKind::Local,
        }
    }

    #[test]
    fn ensure_pane_splits_even_when_only_root_pane_exists() {
        // B2: a brand-new workspace's pane.list already shows the root
        // pane ("w2:p1") while its shell is still spawning — reusing it
        // made agent.start fail with agent_pane_busy. ensure_pane must
        // split a fresh pane instead of returning the root pane id.
        let fake = FakePaneOps::new(
            vec![vec!["w2:p1".to_string()]],
            json!({"type": "pane_info", "pane": {
                "pane_id": "w2:p2", "workspace_id": "w2", "tab_id": "w2:t1",
                "terminal_id": "term_2", "focused": false,
                "agent_status": "unknown", "revision": 0
            }}),
        );
        let pane = ensure_pane(&fake, &test_endpoint(), "w2", Some("/repo"))
            .expect("ensure_pane should succeed");
        assert_eq!(pane, "w2:p2");
        assert_eq!(fake.split_calls.get(), 1);
        let params = fake.last_params.borrow();
        let params = params.as_ref().unwrap();
        // Split targets the existing root pane, in the right workspace,
        // with the repo as cwd.
        assert_eq!(params.target_pane_id.as_deref(), Some("w2:p1"));
        assert_eq!(params.workspace_id.as_deref(), Some("w2"));
        assert_eq!(params.cwd.as_deref(), Some("/repo"));
        assert_eq!(params.direction, SplitDirection::Down);
    }

    #[test]
    fn ensure_pane_splits_when_workspace_has_no_listed_pane() {
        // Fresh workspace whose root pane is not yet visible in
        // pane.list: still split (target None → workspace-scoped split).
        let fake = FakePaneOps::new(
            vec![vec![]],
            json!({"type": "pane_info", "pane": {"pane_id": "w3:p1"}}),
        );
        let pane = ensure_pane(&fake, &test_endpoint(), "w3", None).unwrap();
        assert_eq!(pane, "w3:p1");
        assert_eq!(fake.split_calls.get(), 1);
        assert_eq!(
            fake.last_params.borrow().as_ref().unwrap().target_pane_id,
            None
        );
    }

    #[test]
    fn ensure_pane_falls_back_to_pane_list_diff() {
        // Split response carries no parseable id → diff successive
        // pane.list results for the new pane.
        let fake = FakePaneOps::new(
            vec![
                vec!["w4:p1".to_string()],
                vec!["w4:p1".to_string(), "w4:p2".to_string()],
            ],
            json!({"type": "ok"}),
        );
        let pane = ensure_pane(&fake, &test_endpoint(), "w4", None).unwrap();
        assert_eq!(pane, "w4:p2");
    }

    #[test]
    fn ensure_pane_errors_when_no_new_pane_appears() {
        let fake = FakePaneOps::new(
            vec![vec!["w5:p1".to_string()], vec!["w5:p1".to_string()]],
            json!({"type": "ok"}),
        );
        let err = ensure_pane(&fake, &test_endpoint(), "w5", None).unwrap_err();
        assert_eq!(err.code(), "pane_failed");
    }

    // ------------------------------------------------------------------
    // B3 regression: macOS /tmp vs /private/tmp canonicalization.
    // ------------------------------------------------------------------

    #[cfg(unix)]
    #[test]
    fn find_workspace_for_repo_matches_canonicalized_paths() {
        use std::os::unix::fs::symlink;

        // A symlinked alias stands in for macOS's /tmp → /private/tmp:
        // herdr reports the pane cwd canonicalized, --repo stays verbatim.
        let base = std::env::temp_dir();
        let tag = format!("hc-b3-{}", std::process::id());
        let real = base.join(format!("{tag}-real"));
        let alias = base.join(format!("{tag}-alias"));
        std::fs::create_dir_all(&real).unwrap();
        let _ = std::fs::remove_file(&alias);
        symlink(&real, &alias).unwrap();
        let canon = std::fs::canonicalize(&real).unwrap();
        let canon_str = canon.to_string_lossy().into_owned();
        let alias_str = alias.to_string_lossy().into_owned();

        // Sanity: alias and canonical path differ as strings.
        assert_ne!(
            alias_str.trim_end_matches('/'),
            canon_str.trim_end_matches('/')
        );

        let list = json!({"workspaces": [{"workspace_id": "w2"}]});
        // Pane cwd reported in canonical form; --repo given via alias.
        let panes = json!({"panes": [
            {"pane_id": "w2:p1", "workspace_id": "w2", "cwd": canon_str}
        ]});
        assert_eq!(
            find_workspace_for_repo(&list, &panes, &alias_str),
            Some("w2".into())
        );
        // And the reverse direction: canonical --repo, aliased cwd.
        let panes_alias = json!({"panes": [
            {"pane_id": "w2:p1", "workspace_id": "w2", "cwd": alias_str}
        ]});
        assert_eq!(
            find_workspace_for_repo(&list, &panes_alias, &canon_str),
            Some("w2".into())
        );
        // Subdirectory of the repo still matches through canonicalization.
        let sub = canon.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        let panes_sub = json!({"panes": [
            {"pane_id": "w2:p1", "workspace_id": "w2",
             "cwd": sub.to_string_lossy()}
        ]});
        assert_eq!(
            find_workspace_for_repo(&list, &panes_sub, &alias_str),
            Some("w2".into())
        );
        // Unrelated path still does not match.
        assert_eq!(
            find_workspace_for_repo(&list, &panes, "/definitely/not/the/repo"),
            None
        );

        let _ = std::fs::remove_dir_all(&real);
        let _ = std::fs::remove_file(&alias);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn find_workspace_for_repo_matches_macos_tmp_alias() {
        // Exact B3 repro: --repo /tmp/<dir>, herdr pane cwd
        // /private/tmp/<dir> — same directory, different strings.
        // temp_dir on macOS is /var/folders/...; force a /tmp path so the
        // /tmp → /private/tmp canonicalization is exercised.
        let tmp_dir = PathBuf::from(format!("/tmp/hc-b3-macos-{}", std::process::id()));
        std::fs::create_dir_all(&tmp_dir).unwrap();
        let canon = std::fs::canonicalize(&tmp_dir).unwrap();
        let canon_str = canon.to_string_lossy().into_owned();
        assert!(canon_str.starts_with("/private/tmp/"));

        let list = json!({"workspaces": [{"workspace_id": "w2"}]});
        let panes = json!({"panes": [
            {"pane_id": "w2:p1", "workspace_id": "w2", "cwd": canon_str}
        ]});
        assert_eq!(
            find_workspace_for_repo(&list, &panes, tmp_dir.to_str().unwrap()),
            Some("w2".into())
        );
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn path_matches_repo_canonical_fallback_for_missing_paths() {
        // Nonexistent paths (e.g. remote-worker cwds) keep verbatim
        // comparison — canonicalize fails and falls back to the raw string.
        assert!(path_matches_repo("/no/such/repo/sub", "/no/such/repo"));
        assert!(!path_matches_repo("/no/such/other", "/no/such/repo"));
        assert!(path_matches_repo("/no/such/repo", "/no/such/repo/"));
    }
}
