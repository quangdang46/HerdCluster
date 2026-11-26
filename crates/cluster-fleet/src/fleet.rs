//! `agent list --all` fan-out merge and the `machines` view (PLAN.md §3.6).
//!
//! Both functions fan out to every endpoint in the [`FleetContext`] using
//! `std::thread::scope` (api-client is synchronous — one request per
//! connection, cheap to parallelize). Offline workers (stale heartbeat in
//! `workers.json`) are **not** called; they produce placeholder rows so
//! scripts see the whole fleet, not just the reachable part.
//!
//! ## `--json` schema (stable)
//!
//! `agent list --all --json` emits a JSON array of rows, each either:
//!
//! ```json
//! {"machine": "local", "id": "local/claude-1", "agent": "claude-1",
//!  "name": "claude-1", "kind": "claude", "status": "idle", ...}
//! ```
//!
//! or an unreachable-machine placeholder:
//!
//! ```json
//! {"machine": "desktop-win", "status": "unreachable", "reason": "..."}
//! ```
//!
//! Agent rows keep every raw `agent.list` field not consumed above under
//! the top level (flattened) — new upstream fields pass through.
//!
//! `machines --json` emits an array of [`MachineInfo`] sorted by label.

use std::collections::BTreeSet;
use std::time::Duration;

use api_client::ApiClient;
use cluster_core::registry::WorkerEntry;
use serde::Serialize;
use serde_json::Value;

use crate::endpoint::{FleetContext, MachineEndpoint, MachineKind};
use crate::ids::{format_global_id, valid_id_part};
use crate::parse;
use crate::spawn::effective_max_agents;

/// Total per-call budget for the `agent list --all` fan-out. All machine
/// queries run in parallel scoped threads, so wall-clock ≈ this value.
pub const FANOUT_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-machine probe timeout for the `machines` view. Best-effort: one
/// slow worker never fails the whole view.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// One agent row in the merged `agent list --all` output.
#[derive(Debug, Clone, Serialize)]
pub struct FleetAgent {
    /// Machine label this agent lives on.
    pub machine: String,
    /// Global agent ID (`{machine}/{agent}`).
    pub id: String,
    /// Machine-local identifier — the `target` for `agent.*` calls on that
    /// machine (agent name preferred, terminal id as fallback).
    pub agent: String,
    /// Agent display name, if reported.
    pub name: Option<String>,
    /// Agent kind (e.g. `"claude"`), if reported.
    pub kind: Option<String>,
    /// Agent status string as reported by herdr, if present.
    pub status: Option<String>,
    /// Remaining raw fields from the machine's `agent.list` row.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

/// Placeholder row emitted for a machine whose agents could not be listed
/// (worker offline, bridge down, timeout).
#[derive(Debug, Clone, Serialize)]
pub struct MachinePlaceholder {
    /// Machine label.
    pub machine: String,
    /// Always `"unreachable"`.
    pub status: String,
    /// Why the machine could not be queried.
    pub reason: String,
}

/// A row of `agent list --all` output: either a real agent or a machine
/// placeholder. Untagged — the two shapes are distinguishable by fields.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum AgentListRow {
    /// A live agent on a reachable machine.
    Agent(FleetAgent),
    /// A machine that could not be queried.
    Unreachable(MachinePlaceholder),
}

/// One machine in the `machines` view.
#[derive(Debug, Clone, Serialize)]
pub struct MachineInfo {
    /// Machine label (`"local"` or worker label).
    pub label: String,
    /// `"local"` or `"remote"`.
    pub kind: MachineKind,
    /// `"online"` / `"degraded"` / `"offline"` from the registry, or
    /// `"unreachable"` when a live probe failed. The local machine reports
    /// `"online"`/`"unreachable"` only (it has no heartbeat).
    pub status: String,
    /// Whether the live `agent.list` probe succeeded this call.
    pub reachable: bool,
    /// Number of addressable agents seen by the live `agent.list` probe —
    /// the same rows `agent list --all` emits for this machine (absent when
    /// unreachable). This is *not* the heartbeat `agent_count` snapshot.
    pub agents: Option<u64>,
    /// Last heartbeat-reported running agent count (workers only).
    /// `null` when the machine is unreachable — the snapshot would be
    /// stale data, not a live metric (issue #11).
    pub agent_count: Option<u32>,
    /// Effective agent cap the spawn scheduler enforces for this machine
    /// (issue #12): the `cluster.json` `machines.<label>.max_agents`
    /// override wins over the last heartbeat-reported cap, then the
    /// scheduler default — same precedence as `spawn`/`scheduler_wire`.
    /// The raw worker-reported value remains under `resources.max_agents`.
    /// `null` when the machine is unreachable and no `cluster.json`
    /// override exists — without a live probe or an override the effective
    /// cap would just be the scheduler default, not a fact about the
    /// machine (issue #11).
    pub max_agents: Option<u32>,
    /// Last heartbeat-reported available memory in MiB (workers only);
    /// `null` when unreachable, same as [`MachineInfo::agent_count`].
    pub mem_available_mb: Option<u64>,
    /// Agent kinds the worker advertised (workers only; may be overridden
    /// by `cluster.json` — see spawn for the effective set). Empty when
    /// unreachable — the advertisement is part of the stale snapshot.
    pub kinds: Vec<String>,
    /// Raw last resource snapshot (workers only). `null` when unreachable:
    /// the snapshot (including `load_avg`) is withheld rather than
    /// serialized as if it were live (issue #11). [`MachineInfo::last_seen`]
    /// still records when the worker was last heard from.
    pub resources: Option<cluster_core::protocol::Resources>,
    /// `last_seen` from the registry, RFC 3339 (workers only). This is a
    /// timestamp, not a live metric — it is kept even when unreachable so
    /// consumers can tell *when* the worker was last seen.
    pub last_seen: Option<String>,
}

/// Outcome of querying one machine for `agent list --all`.
#[derive(Debug)]
enum MachineAgents {
    /// Parsed agent rows (machine field already injected).
    Agents(Vec<FleetAgent>),
    /// The machine could not be queried; `reason` is human-readable.
    Unreachable { reason: String },
}

/// Parse one machine's `agent.list` result into [`FleetAgent`] rows.
///
/// Defensive about response shape (bare array or `{"agents": [...]}`) and
/// field names — see [`crate::parse`]. Agents without any usable
/// identifier are skipped (they cannot be addressed by global ID anyway).
pub fn parse_agent_list(machine: &str, value: &Value) -> Vec<FleetAgent> {
    parse::items_array(value)
        .iter()
        .filter_map(|item| agent_row(machine, item))
        .collect()
}

/// Raw field aliases tried, in order, for the machine-local agent id.
///
/// `pane_id` outranks `terminal_id`: herdr `agent.*` targets resolve
/// agent name or pane id, but NOT terminal id — an unnamed agent listed
/// by `terminal_id` answered every op with `agent_not_found` (issue #9).
const AGENT_ID_KEYS: &[&str] = &["name", "pane_id", "terminal_id", "id", "agent_id"];
/// Raw field aliases for agent kind.
///
/// herdr 0.9.5 `agent.list` rows (`AgentInfo`) carry the kind under
/// `agent` (e.g. `"devin"`); `kind`/`agent_kind` are kept for older or
/// alternate shapes. Reading `agent` as the kind is safe because the id
/// keys above never include it — the machine-local id comes from `name`
/// (falling back to `terminal_id`/`id`/`agent_id`), and `agent` is
/// already a consumed key so the raw value never leaks into `extra`.
const KIND_KEYS: &[&str] = &["kind", "agent_kind", "agent"];
/// Top-level keys consumed into [`FleetAgent`] fields — removed from `extra`
/// so flattened output never duplicates a key.
const CONSUMED_KEYS: &[&str] = &["machine", "id", "agent", "name", "kind", "status"];

fn agent_row(machine: &str, item: &Value) -> Option<FleetAgent> {
    // Bare-string rows are treated as the agent id itself.
    if let Some(s) = item.as_str() {
        if !valid_id_part(s) {
            return None;
        }
        return Some(FleetAgent {
            machine: machine.to_string(),
            id: format_global_id(machine, s),
            agent: s.to_string(),
            name: None,
            kind: None,
            status: None,
            extra: serde_json::Map::new(),
        });
    }
    // First id alias that can form the agent half of a valid global ID.
    // A herdr-native name like "foo/bar" would stamp an unresolvable
    // `{machine}/{id}`, so invalid candidates fall through to the next
    // alias (pane_id/terminal_id); the row is skipped only when nothing
    // addressable remains (issue #53).
    let local_id = AGENT_ID_KEYS
        .iter()
        .filter_map(|key| item.get(*key).and_then(Value::as_str))
        .find(|s| valid_id_part(s))
        .map(str::to_string)?;
    let mut extra = item.as_object().cloned().unwrap_or_default();
    for key in CONSUMED_KEYS {
        extra.remove(*key);
    }
    Some(FleetAgent {
        machine: machine.to_string(),
        id: format_global_id(machine, &local_id),
        agent: local_id,
        name: parse::str_field(item, &["name"]).map(str::to_string),
        kind: parse::str_field(item, KIND_KEYS).map(str::to_string),
        status: parse::str_field(item, parse::STATUS_KEYS).map(str::to_string),
        extra,
    })
}

/// Normalize an `agent.get` (`agent_info`) response so `agent`/`kind`
/// carry the same semantics as `agent list` rows (issue #14).
///
/// herdr reports the agent *kind* under `agent` inside the agent record
/// (`{"agent": {"agent": "claude", "pane_id": "w2:pHF", ...}}`), while
/// `agent list` rows put the kind under `kind` and the machine-local
/// target id under `agent`. This rewrites the record in place:
///
/// * `kind` ← first string among [`KIND_KEYS`] (explicit `kind` wins,
///   then `agent_kind`, then the colliding `agent` value);
/// * `agent` ← the machine-local target id, resolved with the same
///   [`AGENT_ID_KEYS`] priority as [`parse_agent_list`], falling back to
///   `fallback_target` (the id the caller asked for) when the record has
///   none of those fields.
///
/// All other herdr fields pass through untouched and the
/// `{"agent": {...}}` envelope shape is preserved. Idempotent: an
/// already-normalized record round-trips unchanged.
pub fn normalize_agent_get(value: &Value, fallback_target: &str) -> Value {
    // Envelope form: {"type": "agent_info", "agent": {...}}.
    if value.get("agent").is_some_and(Value::is_object) {
        let mut out = value.clone();
        if let Some(inner) = out.get_mut("agent").and_then(Value::as_object_mut) {
            normalize_agent_record(inner, fallback_target);
        }
        return out;
    }
    // Flat form: the record itself is the top-level object.
    match value.as_object() {
        Some(obj) => {
            let mut map = obj.clone();
            normalize_agent_record(&mut map, fallback_target);
            Value::Object(map)
        }
        None => value.clone(),
    }
}

/// Rewrite the `agent`/`kind` fields of one agent record object; see
/// [`normalize_agent_get`].
fn normalize_agent_record(obj: &mut serde_json::Map<String, Value>, fallback_target: &str) {
    // Snapshot for the defensive key lookups — same helpers and key
    // priority `agent list` parsing uses, so both commands agree.
    let snapshot = Value::Object(obj.clone());
    if let Some(kind) = parse::str_field(&snapshot, KIND_KEYS) {
        obj.insert("kind".to_string(), Value::String(kind.to_string()));
    }
    // Same candidate walk as [`agent_row`]: an agent *name* like
    // "foo/bar" cannot form the agent half of a valid global ID, so skip
    // invalid candidates and fall through to pane_id/terminal_id/etc.
    let local_id = AGENT_ID_KEYS
        .iter()
        .filter_map(|key| snapshot.get(*key).and_then(Value::as_str))
        .find(|s| valid_id_part(s))
        .unwrap_or(fallback_target)
        .to_string();
    obj.insert("agent".to_string(), Value::String(local_id));
}

/// Merge per-machine query outcomes into sorted [`AgentListRow`]s.
///
/// Ordering is deterministic: machine label ascending; within a machine,
/// agent local id ascending (placeholders sort by machine only — a machine
/// yields either agents or one placeholder, never both).
fn merge_agent_rows(inputs: Vec<(String, MachineAgents)>) -> Vec<AgentListRow> {
    let mut rows: Vec<(String, String, AgentListRow)> = Vec::new();
    for (machine, outcome) in inputs {
        match outcome {
            MachineAgents::Agents(agents) => {
                for a in agents {
                    rows.push((machine.clone(), a.agent.clone(), AgentListRow::Agent(a)));
                }
            }
            MachineAgents::Unreachable { reason } => {
                rows.push((
                    machine.clone(),
                    String::new(),
                    AgentListRow::Unreachable(MachinePlaceholder {
                        machine,
                        status: "unreachable".to_string(),
                        reason,
                    }),
                ));
            }
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    rows.into_iter().map(|(_, _, row)| row).collect()
}

/// Fetch the raw `agent.list` result for one endpoint.
///
/// `Err(reason)` covers connect failures and request errors — the caller
/// turns it into a placeholder/`unreachable` marker.
fn fetch_agent_list(ep: &MachineEndpoint, timeout: Duration) -> Result<Value, String> {
    let client: ApiClient = ep.client().map_err(|e| e.to_string())?;
    client
        .request_with_timeout("agent.list", &serde_json::json!({}), timeout)
        .map_err(|e| e.to_string())
}

/// `agent list --all`: query local + every non-offline worker in parallel
/// and merge the results. Never fails as a whole — per-machine failures
/// become [`AgentListRow::Unreachable`] placeholders.
pub fn list_all_agents(ctx: &FleetContext) -> Vec<AgentListRow> {
    list_all_agents_with_timeout(ctx, FANOUT_TIMEOUT)
}

/// [`list_all_agents`] with an explicit per-machine timeout (mainly for
/// tests).
pub fn list_all_agents_with_timeout(ctx: &FleetContext, timeout: Duration) -> Vec<AgentListRow> {
    // Offline workers are not called at all — straight to placeholders.
    let skipped: BTreeSet<String> = ctx
        .endpoints
        .iter()
        .filter(|e| e.kind == MachineKind::Remote && ctx.is_offline(&e.label))
        .map(|e| e.label.clone())
        .collect();

    let live: Vec<&MachineEndpoint> = ctx
        .endpoints
        .iter()
        .filter(|e| !skipped.contains(&e.label))
        .collect();

    // One scoped thread per live endpoint; join in endpoint order so merge
    // input order is deterministic before sorting anyway.
    let outcomes: Vec<(String, MachineAgents)> = std::thread::scope(|s| {
        let handles: Vec<_> = live
            .iter()
            .map(|ep| {
                let label = ep.label.clone();
                let handle = s.spawn(move || query_machine(ep, timeout));
                (label, handle)
            })
            .collect();
        handles
            .into_iter()
            .map(|(label, h)| join_outcome(label, h.join()))
            .collect()
    });

    // Rebuild in endpoint order (outcomes are already in `live` order which
    // preserves endpoint order minus skipped).
    let mut inputs: Vec<(String, MachineAgents)> = Vec::with_capacity(ctx.endpoints.len());
    let mut outcome_map: std::collections::BTreeMap<String, MachineAgents> =
        outcomes.into_iter().collect();
    for ep in &ctx.endpoints {
        if skipped.contains(&ep.label) {
            inputs.push((
                ep.label.clone(),
                MachineAgents::Unreachable {
                    reason: "worker offline (stale heartbeat)".to_string(),
                },
            ));
        } else if let Some(outcome) = outcome_map.remove(&ep.label) {
            inputs.push((ep.label.clone(), outcome));
        }
    }
    merge_agent_rows(inputs)
}

/// Map one query thread's join result to its `(label, outcome)` pair.
/// A panicked query thread degrades to `MachineAgents::Unreachable`
/// like any other per-machine failure — the fan-out never aborts on
/// one machine's bug (issue #54).
fn join_outcome(
    label: String,
    result: std::thread::Result<MachineAgents>,
) -> (String, MachineAgents) {
    match result {
        Ok(outcome) => (label, outcome),
        Err(payload) => (
            label,
            MachineAgents::Unreachable {
                reason: panic_reason(payload),
            },
        ),
    }
}

/// Map one `machines` probe thread's join result to its `(label, probe)`
/// pair — the same panic handling [`join_outcome`] gives `agent list
/// --all` (issue #54). A panicked probe becomes an explicit `Err`
/// probe failure rather than a missing map entry: the old
/// `("", None)` substitution was filtered out, leaving no probe for a
/// non-skipped endpoint, so `assemble_machine_info` reported
/// `reachable: false` while `status` fell back to the registry value
/// (e.g. `"online"`) — a contradictory row (issue #80).
fn probe_join_outcome(
    label: String,
    result: std::thread::Result<Result<usize, String>>,
) -> (String, Result<usize, String>) {
    match result {
        Ok(probe) => (label, probe),
        Err(payload) => (label, Err(panic_reason(payload))),
    }
}

/// Human-readable reason for a panicked query thread, using the panic
/// payload when it carries a message.
fn panic_reason(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        return format!("internal query panic: {s}");
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return format!("internal query panic: {s}");
    }
    "internal query panic".to_string()
}

fn query_machine(ep: &MachineEndpoint, timeout: Duration) -> MachineAgents {
    match fetch_agent_list(ep, timeout) {
        Ok(value) => MachineAgents::Agents(parse_agent_list(&ep.label, &value)),
        Err(reason) => MachineAgents::Unreachable { reason },
    }
}

/// Assemble one [`MachineInfo`] from registry data + an optional live probe.
/// Pure — separated from IO for testing.
///
/// `override_max` is the `cluster.json` `machines.<label>.max_agents`
/// override for this machine (issue #12): it is folded into the reported
/// `max_agents` field so `machines --json` shows the same effective cap
/// the scheduler enforces.
fn assemble_machine_info(
    endpoint: &MachineEndpoint,
    worker: Option<&WorkerEntry>,
    probe: Option<Result<usize, String>>,
    override_max: Option<u32>,
) -> MachineInfo {
    let (reachable, agents, live_status) = match probe {
        Some(Ok(n)) => (true, Some(n as u64), None),
        Some(Err(_)) => (false, None, Some("unreachable".to_string())),
        None => (false, None, None), // offline worker: not probed
    };

    let registry_status = worker.and_then(|w| {
        serde_json::to_value(w.status)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
    });

    let status = if endpoint.kind == MachineKind::Local {
        if reachable {
            "online".to_string()
        } else {
            "unreachable".to_string()
        }
    } else {
        live_status
            .or(registry_status)
            .unwrap_or_else(|| "offline".to_string())
    };

    // Heartbeat-derived metrics are only meaningful while the machine is
    // reachable: an unreachable worker's last resource snapshot is stale
    // data, so withhold it entirely (issue #11) rather than serialize dead
    // numbers that scripts would read as live. `last_seen` below still
    // records when the worker was last heard from.
    let resources = if reachable {
        worker.and_then(|w| w.resources.clone())
    } else {
        None
    };
    MachineInfo {
        label: endpoint.label.clone(),
        kind: endpoint.kind,
        status,
        reachable,
        agents,
        agent_count: resources.as_ref().map(|r| r.agent_count),
        // Effective cap, not the raw reported one: the scheduler
        // (spawn.rs `build_worker_caps`/`configured_max_agents`,
        // scheduler_wire.rs `collect_worker_caps`) applies the same
        // override → reported → default precedence, so `machines --json`
        // and spawn enforcement can no longer disagree (issue #12).
        max_agents: if reachable || override_max.is_some() {
            Some(effective_max_agents(
                override_max,
                resources.as_ref().map(|r| r.max_agents),
            ))
        } else {
            None
        },
        mem_available_mb: resources.as_ref().map(|r| r.mem_available_mb),
        kinds: resources
            .as_ref()
            .map(|r| r.kinds.clone())
            .unwrap_or_default(),
        resources,
        last_seen: worker.and_then(|w| w.last_seen).map(|t| t.to_rfc3339()),
    }
}

/// `machines` view: registry workers + local machine + best-effort live
/// agent counts. One slow/unreachable worker never fails the view.
///
/// Offline workers are not probed (`reachable: false`, status from the
/// registry). Workers that are degraded/online get a short
/// [`PROBE_TIMEOUT`] `agent.list` call; failures mark them
/// `"unreachable"`.
pub fn machines(ctx: &FleetContext) -> Vec<MachineInfo> {
    machines_with_timeout(ctx, PROBE_TIMEOUT)
}

/// [`machines`] with an explicit probe timeout (mainly for tests).
pub fn machines_with_timeout(ctx: &FleetContext, timeout: Duration) -> Vec<MachineInfo> {
    let skipped: BTreeSet<String> = ctx
        .endpoints
        .iter()
        .filter(|e| e.kind == MachineKind::Remote && ctx.is_offline(&e.label))
        .map(|e| e.label.clone())
        .collect();

    let live: Vec<&MachineEndpoint> = ctx
        .endpoints
        .iter()
        .filter(|e| !skipped.contains(&e.label))
        .collect();

    let probes: Vec<(String, Result<usize, String>)> = std::thread::scope(|s| {
        let handles: Vec<_> = live
            .iter()
            .map(|ep| {
                let label = ep.label.clone();
                let handle = s.spawn(move || {
                    // Count the same addressable rows `agent list --all`
                    // emits so the two commands cannot disagree (issue #8).
                    fetch_agent_list(ep, timeout).map(|v| parse_agent_list(&ep.label, &v).len())
                });
                (label, handle)
            })
            .collect();
        handles
            .into_iter()
            .map(|(label, h)| probe_join_outcome(label, h.join()))
            .collect()
    });
    let mut probe_map: std::collections::BTreeMap<String, Result<usize, String>> =
        probes.into_iter().collect();

    let mut out: Vec<MachineInfo> = ctx
        .endpoints
        .iter()
        .map(|ep| {
            let worker = ctx.worker_entry(&ep.label);
            let probe = if skipped.contains(&ep.label) {
                None
            } else {
                probe_map.remove(&ep.label)
            };
            let override_max = ctx
                .machines_overrides
                .get(&ep.label)
                .and_then(|o| o.max_agents);
            assemble_machine_info(ep, worker, probe, override_max)
        })
        .collect();
    out.sort_by(|a, b| a.label.cmp(&b.label));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::FleetContext;
    use cluster_core::registry::ClusterInfo;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn agents_value() -> Value {
        json!({"agents": [
            {"name": "claude-1", "kind": "claude", "status": "idle", "pane_id": "p1"},
            {"terminal_id": "term-9", "kind": "codex", "state": "working"},
            {"pane_id": "w9:p3"}, // pane_id fallback: valid agent target
            {"cwd": "/nowhere"} // skipped: no usable identifier
        ]})
    }

    #[test]
    fn parse_agent_list_extracts_ids_and_fields() {
        let rows = parse_agent_list("local", &agents_value());
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].machine, "local");
        assert_eq!(rows[0].id, "local/claude-1");
        assert_eq!(rows[0].agent, "claude-1");
        assert_eq!(rows[0].name.as_deref(), Some("claude-1"));
        assert_eq!(rows[0].kind.as_deref(), Some("claude"));
        assert_eq!(rows[0].status.as_deref(), Some("idle"));
        // Raw fields pass through flattened.
        assert_eq!(rows[0].extra.get("pane_id").unwrap(), "p1");
        // Consumed keys are not duplicated in extra.
        assert!(rows[0].extra.get("name").is_none());
        assert!(rows[0].extra.get("status").is_none());

        // terminal_id fallback + "state" alias for status.
        assert_eq!(rows[1].agent, "term-9");
        assert_eq!(rows[1].id, "local/term-9");
        assert_eq!(rows[1].status.as_deref(), Some("working"));

        // pane_id outranks terminal_id when `name` is absent: it is a
        // valid `agent.*` target where terminal_id is not.
        assert_eq!(rows[2].agent, "w9:p3");
        assert_eq!(rows[2].id, "local/w9:p3");
    }

    #[test]
    fn parse_agent_list_accepts_bare_array() {
        let rows = parse_agent_list("w", &json!([{"name": "a"}, "bare-id"]));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].agent, "bare-id");
        assert_eq!(rows[1].id, "w/bare-id");
    }

    #[test]
    fn parse_agent_list_skips_unresolvable_ids() {
        // Issue #53: a herdr-native agent named "foo/bar" would stamp an
        // id `parse_global_id` rejects — fall back to a valid alias
        // (pane_id) when one exists, skip the row when none does.
        let rows = parse_agent_list(
            "local",
            &json!({"agents": [
                {"name": "foo/bar", "pane_id": "w2:p1", "kind": "claude"},
                {"name": "bad name", "terminal_id": "term_7"},
                {"name": "bad/name", "kind": "codex"},
                "also/bad"
            ]}),
        );
        assert_eq!(rows.len(), 2);
        // Invalid `name` falls through to `pane_id` — still addressable.
        assert_eq!(rows[0].agent, "w2:p1");
        assert_eq!(rows[0].id, "local/w2:p1");
        assert_eq!(rows[0].name.as_deref(), Some("foo/bar"));
        // Invalid `name` falls through to `terminal_id`.
        assert_eq!(rows[1].agent, "term_7");
        assert_eq!(rows[1].id, "local/term_7");
    }

    #[test]
    fn parse_agent_list_reads_herdr_095_agent_field_as_kind() {
        // Real herdr 0.9.5 `agent.list` row (`AgentInfo` from the API
        // schema): the kind lives under `agent` (e.g. `"devin"`), the
        // display name under `name`, status under `agent_status`. This is
        // the shape that made every `kind` serialize as `null`.
        let v = json!({"type": "agent_list", "agents": [
            {
                "terminal_id": "term_1", "agent": "devin", "name": "devin-mac",
                "agent_status": "idle", "pane_id": "w2:p1", "workspace_id": "w2",
                "tab_id": "w2:t1", "focused": true, "revision": 0,
                "interactive_ready": true, "cwd": "/repo", "foreground_cwd": "/repo",
                "display_agent": "devin", "agent_session": null,
                "launch_pending": false, "screen_detection_skipped": false,
                "state_change_seq": 0, "state_labels": {}, "tokens": {},
                "title": null, "terminal_title": null,
                "terminal_title_stripped": null, "completion_seq": null
            }
        ]});
        let rows = parse_agent_list("local", &v);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        // Local id still comes from `name`; `agent` is NOT used as id.
        assert_eq!(r.agent, "devin-mac");
        assert_eq!(r.id, "local/devin-mac");
        assert_eq!(r.name.as_deref(), Some("devin-mac"));
        assert_eq!(r.kind.as_deref(), Some("devin"));
        assert_eq!(r.status.as_deref(), Some("idle"));
        // `agent` is consumed into `kind` — never duplicated in `extra`;
        // unrelated raw fields still pass through flattened.
        assert!(r.extra.get("agent").is_none());
        assert_eq!(r.extra.get("pane_id").unwrap(), "w2:p1");
        assert_eq!(r.extra.get("interactive_ready").unwrap(), true);
        // --json schema stays stable: `kind` now populates, `agent`
        // remains the machine-local id.
        let j = serde_json::to_value(AgentListRow::Agent(r.clone())).unwrap();
        assert_eq!(j["kind"], "devin");
        assert_eq!(j["agent"], "devin-mac");
        assert_eq!(j["id"], "local/devin-mac");
        assert_eq!(j["status"], "idle");
    }

    #[test]
    fn parse_agent_list_kind_alias_priority() {
        // Explicit `kind` wins over the 0.9.5 `agent` alias.
        let rows = parse_agent_list(
            "m",
            &json!({"agents": [{"name": "a", "kind": "claude", "agent": "devin"}]}),
        );
        assert_eq!(rows[0].kind.as_deref(), Some("claude"));
        // `agent: null` (AgentInfo allows null) → kind stays None.
        let rows = parse_agent_list("m", &json!({"agents": [{"name": "b", "agent": null}]}));
        assert_eq!(rows[0].kind, None);
    }

    fn ctx_with_workers() -> FleetContext {
        FleetContext::build(
            PathBuf::from("/tmp/hc"),
            ClusterInfo {
                cluster_id: "c".into(),
                name: "n".into(),
                port: 1,
                protocol_version: 1,
                created_at: chrono::Utc::now(),
                machines: BTreeMap::new(),
                keepawake: true,
            },
            vec![],
        )
        .unwrap()
    }

    #[test]
    fn merge_orders_by_machine_then_agent() {
        let a1 = parse_agent_list("alpha", &agents_value());
        let z1 = parse_agent_list("zeta", &json!({"agents": [{"name": "z-agent"}]}));
        let rows = merge_agent_rows(vec![
            ("zeta".into(), MachineAgents::Agents(z1)),
            ("alpha".into(), MachineAgents::Agents(a1)),
            (
                "mid".into(),
                MachineAgents::Unreachable {
                    reason: "timeout".into(),
                },
            ),
        ]);
        let summary: Vec<String> = rows
            .iter()
            .map(|r| match r {
                AgentListRow::Agent(a) => format!("{}:{}", a.machine, a.agent),
                AgentListRow::Unreachable(p) => format!("{}:UNREACHABLE", p.machine),
            })
            .collect();
        assert_eq!(
            summary,
            vec![
                "alpha:claude-1",
                "alpha:term-9",
                "alpha:w9:p3",
                "mid:UNREACHABLE",
                "zeta:z-agent"
            ]
        );
    }

    #[test]
    fn unreachable_placeholder_shape() {
        let rows = merge_agent_rows(vec![(
            "w".into(),
            MachineAgents::Unreachable {
                reason: "socket missing".into(),
            },
        )]);
        assert_eq!(rows.len(), 1);
        let json = serde_json::to_value(&rows[0]).unwrap();
        assert_eq!(json["machine"], "w");
        assert_eq!(json["status"], "unreachable");
        assert_eq!(json["reason"], "socket missing");
        assert!(json.get("id").is_none());
    }

    #[test]
    fn panicked_query_thread_degrades_to_unreachable() {
        // Issue #54: a panic inside one machine's query thread must not
        // abort the whole `agent list --all` fan-out — it becomes the
        // same Unreachable placeholder any other per-machine failure
        // produces.
        let handle = std::thread::spawn(|| -> MachineAgents { panic!("boom") });
        let (label, outcome) = join_outcome("w".into(), handle.join());
        assert_eq!(label, "w");
        match outcome {
            MachineAgents::Unreachable { reason } => {
                assert!(reason.contains("boom"), "unexpected reason: {reason}");
            }
            MachineAgents::Agents(_) => panic!("expected Unreachable"),
        }
        // A non-string payload still maps to a reasoned placeholder.
        let (label, outcome) = join_outcome(
            "x".into(),
            Err(Box::new(42u32) as Box<dyn std::any::Any + Send>),
        );
        assert_eq!(label, "x");
        match outcome {
            MachineAgents::Unreachable { reason } => {
                assert_eq!(reason, "internal query panic");
            }
            MachineAgents::Agents(_) => panic!("expected Unreachable"),
        }
        // Successful joins pass through untouched.
        let (label, outcome) = join_outcome(
            "ok".into(),
            Ok(MachineAgents::Unreachable {
                reason: "timeout".into(),
            }),
        );
        assert_eq!(label, "ok");
        assert!(matches!(outcome, MachineAgents::Unreachable { .. }));
    }

    #[test]
    fn panicked_probe_thread_is_explicit_probe_failure() {
        // Issue #80: a panicked `machines` probe thread must yield an
        // explicit Err probe — not a missing entry — so the row reports
        // reachable:false AND status:"unreachable" instead of falling
        // back to the registry status.
        let handle = std::thread::spawn(|| -> Result<usize, String> { panic!("boom") });
        let (label, probe) = probe_join_outcome("w".into(), handle.join());
        assert_eq!(label, "w");
        let Err(reason) = probe else {
            panic!("expected Err probe, got {probe:?}");
        };
        assert!(reason.contains("boom"), "unexpected reason: {reason}");

        // A non-string payload still maps to a reasoned failure.
        let (label, probe) = probe_join_outcome(
            "x".into(),
            Err(Box::new(42u32) as Box<dyn std::any::Any + Send>),
        );
        assert_eq!(label, "x");
        assert_eq!(probe.unwrap_err(), "internal query panic");

        // Successful joins pass through untouched — Ok stays Ok, Err
        // stays Err.
        let (label, probe) = probe_join_outcome("ok".into(), Ok(Ok(3)));
        assert_eq!(label, "ok");
        assert_eq!(probe.unwrap(), 3);
        let (_, probe) = probe_join_outcome("bad".into(), Ok(Err("timeout".into())));
        assert_eq!(probe.unwrap_err(), "timeout");
    }

    #[test]
    fn assemble_machine_info_cases() {
        let ep = MachineEndpoint {
            label: "w".into(),
            socket_path: PathBuf::from("/x.sock"),
            kind: MachineKind::Remote,
        };
        // Offline worker: no probe → registry status, unreachable=false.
        // Heartbeat metrics are withheld — they would be stale (issue #11).
        let mut w = cluster_core::registry::WorkerEntry {
            worker_id: "i".into(),
            label: "w".into(),
            pubkey: "p".into(),
            paired_at: chrono::Utc::now(),
            last_seen: Some(chrono::Utc::now()),
            status: cluster_core::registry::WorkerStatus::Offline,
            resources: Some(cluster_core::protocol::Resources {
                cpu_cores: 8,
                mem_total_mb: 16384,
                mem_available_mb: 5120,
                agent_count: 3,
                max_agents: 4,
                load_avg: vec![0.1, 0.2, 0.3],
                kinds: vec!["claude".into()],
            }),
        };
        let info = assemble_machine_info(&ep, Some(&w), None, None);
        assert_eq!(info.status, "offline");
        assert!(!info.reachable);
        assert_eq!(info.agents, None);
        assert_stale_metrics_withheld(&info);
        // `last_seen` is a timestamp, not a live metric — kept.
        assert!(info.last_seen.is_some());

        // Degraded worker, probe ok → status stays degraded, count set,
        // and the heartbeat snapshot is still fresh enough to report.
        w.status = cluster_core::registry::WorkerStatus::Degraded;
        let info = assemble_machine_info(&ep, Some(&w), Some(Ok(3)), None);
        assert_eq!(info.status, "degraded");
        assert!(info.reachable);
        assert_eq!(info.agents, Some(3));
        assert_eq!(info.agent_count, Some(3));
        assert_eq!(info.max_agents, Some(4));
        assert_eq!(info.mem_available_mb, Some(5120));
        assert_eq!(info.kinds, vec!["claude".to_string()]);
        assert!(info.resources.is_some());

        // Probe fails → "unreachable" overrides registry status and the
        // heartbeat snapshot is withheld as stale.
        let info = assemble_machine_info(&ep, Some(&w), Some(Err("boom".into())), None);
        assert_eq!(info.status, "unreachable");
        assert!(!info.reachable);
        assert_stale_metrics_withheld(&info);

        // Local machine: no registry entry; probe decides.
        let local = MachineEndpoint {
            label: "local".into(),
            socket_path: PathBuf::from("/h.sock"),
            kind: MachineKind::Local,
        };
        let info = assemble_machine_info(&local, None, Some(Ok(2)), None);
        assert_eq!(info.status, "online");
        assert!(info.reachable);
        let info = assemble_machine_info(&local, None, Some(Err("down".into())), None);
        assert_eq!(info.status, "unreachable");
    }

    /// Every heartbeat-derived live metric must be absent on an
    /// unreachable machine so `--json` consumers cannot read stale data
    /// as live (issue #11).
    fn assert_stale_metrics_withheld(info: &MachineInfo) {
        assert_eq!(info.agent_count, None);
        assert_eq!(info.max_agents, None);
        assert_eq!(info.mem_available_mb, None);
        assert!(info.kinds.is_empty());
        assert!(info.resources.is_none());
    }

    #[test]
    fn machines_json_nulls_stale_metrics_when_unreachable() {
        // Serialize the exact `machines --json` row shape for an offline
        // worker that still has a heartbeat snapshot in the registry.
        let ep = MachineEndpoint {
            label: "w".into(),
            socket_path: PathBuf::from("/x.sock"),
            kind: MachineKind::Remote,
        };
        let w = cluster_core::registry::WorkerEntry {
            worker_id: "i".into(),
            label: "w".into(),
            pubkey: "p".into(),
            paired_at: chrono::Utc::now(),
            last_seen: Some(chrono::Utc::now()),
            status: cluster_core::registry::WorkerStatus::Offline,
            resources: Some(cluster_core::protocol::Resources {
                cpu_cores: 8,
                mem_total_mb: 16384,
                mem_available_mb: 5120,
                agent_count: 3,
                max_agents: 4,
                load_avg: vec![0.1],
                kinds: vec!["claude".into()],
            }),
        };
        let info = assemble_machine_info(&ep, Some(&w), None, None);
        let v = serde_json::to_value(&info).unwrap();
        assert_eq!(v["status"], "offline");
        assert_eq!(v["reachable"], false);
        for key in [
            "agents",
            "agent_count",
            "max_agents",
            "mem_available_mb",
            "resources",
        ] {
            assert!(v[key].is_null(), "{key} should be null: {v}");
        }
        assert_eq!(v["kinds"], json!([]));
        // The staleness marker consumers need: when the worker was last
        // seen, plus reachable=false / status.
        assert!(v["last_seen"].is_string());
    }

    #[test]
    fn assemble_machine_info_max_agents_is_effective_cap() {
        use cluster_core::protocol::Resources;
        use cluster_core::registry::WorkerStatus;
        use cluster_core::scheduler::DEFAULT_MAX_AGENTS;

        let ep = MachineEndpoint {
            label: "w".into(),
            socket_path: PathBuf::from("/x.sock"),
            kind: MachineKind::Remote,
        };
        let mut w = cluster_core::registry::WorkerEntry {
            worker_id: "i".into(),
            label: "w".into(),
            pubkey: "p".into(),
            paired_at: chrono::Utc::now(),
            last_seen: None,
            status: WorkerStatus::Online,
            resources: Some(Resources {
                cpu_cores: 8,
                mem_total_mb: 16384,
                mem_available_mb: 8192,
                agent_count: 1,
                max_agents: 4, // worker-reported cap (issue #12 scenario)
                load_avg: vec![],
                kinds: vec!["claude".into()],
            }),
        };

        // No override → reported cap passes through (reachable probe).
        let info = assemble_machine_info(&ep, Some(&w), Some(Ok(1)), None);
        assert_eq!(info.max_agents, Some(4));
        // Raw reported value stays visible under `resources`.
        assert_eq!(info.resources.as_ref().unwrap().max_agents, 4);

        // cluster.json override wins — matches the scheduler's
        // `configured_max_agents`/`build_worker_caps` precedence.
        let info = assemble_machine_info(&ep, Some(&w), Some(Ok(1)), Some(8));
        assert_eq!(info.max_agents, Some(8));
        assert_eq!(info.resources.as_ref().unwrap().max_agents, 4);

        // No reported resources at all → default cap (or override).
        w.resources = None;
        let info = assemble_machine_info(&ep, Some(&w), Some(Ok(1)), None);
        assert_eq!(info.max_agents, Some(DEFAULT_MAX_AGENTS));
        let info = assemble_machine_info(&ep, Some(&w), Some(Ok(1)), Some(6));
        assert_eq!(info.max_agents, Some(6));

        // Unreachable with no override → `null` (issue #11: nothing live
        // to report); unreachable WITH an override still shows the
        // configured cap, which is not stale data.
        w.resources = Some(Resources {
            cpu_cores: 8,
            mem_total_mb: 16384,
            mem_available_mb: 8192,
            agent_count: 1,
            max_agents: 4,
            load_avg: vec![],
            kinds: vec!["claude".into()],
        });
        let info = assemble_machine_info(&ep, Some(&w), None, None);
        assert_eq!(info.max_agents, None);
        let info = assemble_machine_info(&ep, Some(&w), None, Some(8));
        assert_eq!(info.max_agents, Some(8));

        // Local machine: no worker entry, so the override/default path
        // is the only source — same as the scheduler's local caps.
        let local = MachineEndpoint {
            label: "local".into(),
            socket_path: PathBuf::from("/h.sock"),
            kind: MachineKind::Local,
        };
        let info = assemble_machine_info(&local, None, Some(Ok(0)), None);
        assert_eq!(info.max_agents, Some(DEFAULT_MAX_AGENTS));
        let info = assemble_machine_info(&local, None, Some(Ok(0)), Some(7));
        assert_eq!(info.max_agents, Some(7));
    }

    #[test]
    fn machines_view_reports_effective_max_agents_from_cluster_json() {
        // Issue #12 end-to-end at the fleet layer: worker DESKTOP reports
        // max_agents 4 via heartbeat, cluster.json overrides the label to
        // 8 — `machines` must show 8, the cap the scheduler enforces.
        use cluster_core::protocol::Resources;
        use cluster_core::registry::{MachineOverride, WorkerStatus};

        let mut machines = BTreeMap::new();
        machines.insert(
            "desktop".to_string(),
            MachineOverride {
                max_agents: Some(8),
                kinds_override: None,
            },
        );
        machines.insert(
            "local".to_string(),
            MachineOverride {
                max_agents: Some(6),
                kinds_override: None,
            },
        );
        let ctx = FleetContext::build(
            PathBuf::from("/tmp/hc"),
            ClusterInfo {
                cluster_id: "c".into(),
                name: "n".into(),
                port: 1,
                protocol_version: 1,
                created_at: chrono::Utc::now(),
                machines,
                keepawake: true,
            },
            vec![cluster_core::registry::WorkerEntry {
                worker_id: "id-desktop".into(),
                label: "desktop".into(),
                pubkey: "ab".repeat(32),
                paired_at: chrono::Utc::now(),
                last_seen: None,
                status: WorkerStatus::Offline, // skipped probe; caps still apply
                resources: Some(Resources {
                    cpu_cores: 8,
                    mem_total_mb: 16384,
                    mem_available_mb: 8192,
                    agent_count: 2,
                    max_agents: 4,
                    load_avg: vec![],
                    kinds: vec!["claude".into()],
                }),
            }],
        )
        .unwrap();

        let infos = machines_with_timeout(&ctx, Duration::from_millis(10));
        let desktop = infos.iter().find(|m| m.label == "desktop").unwrap();
        // The worker is unreachable in this test (no socket), so its
        // heartbeat snapshot — including the reported cap of 4 — is
        // withheld as stale (issue #11). The cluster.json override is
        // config, not telemetry, so the effective cap still shows: 8.
        assert_eq!(desktop.max_agents, Some(8));
        assert!(desktop.resources.is_none());
        // Local machine: the override is config, not telemetry, so it is
        // the effective cap whether or not the live probe succeeds.
        let local = infos.iter().find(|m| m.label == "local").unwrap();
        assert_eq!(local.max_agents, Some(6));
    }

    #[test]
    fn normalize_agent_get_matches_list_row_semantics() {
        // Real herdr 0.9.5 `agent.get` shape: kind lives under `agent`,
        // pane id under `pane_id`, status under `agent_status` (issue #14).
        let v = json!({
            "type": "agent_info",
            "agent": {
                "terminal_id": "term_1", "agent": "claude", "pane_id": "w2:pHF",
                "agent_status": "idle", "workspace_id": "w2", "cwd": "/repo",
                "interactive_ready": true, "state_change_seq": 3
            }
        });
        let out = normalize_agent_get(&v, "w2:pHF");
        // Envelope preserved; inner record now matches list-row semantics.
        assert_eq!(out["type"], "agent_info");
        let inner = &out["agent"];
        assert_eq!(inner["kind"], "claude"); // kind under `kind`
        assert_eq!(inner["agent"], "w2:pHF"); // pane id under `agent`
        assert_eq!(inner["pane_id"], "w2:pHF"); // raw fields intact
        assert_eq!(inner["agent_status"], "idle");
        assert_eq!(inner["state_change_seq"], 3);
        assert_eq!(inner["interactive_ready"], true);
    }

    #[test]
    fn normalize_agent_get_prefers_name_like_list_rows() {
        // `agent list` resolves the local id as name → pane_id → ...;
        // `agent get` must agree so scripts see the same `agent` value.
        let v = json!({"agent": {"agent": "devin", "name": "devin-mac", "pane_id": "w2:p1"}});
        let out = normalize_agent_get(&v, "fallback");
        assert_eq!(out["agent"]["agent"], "devin-mac");
        assert_eq!(out["agent"]["kind"], "devin");
        assert_eq!(out["agent"]["name"], "devin-mac");
    }

    #[test]
    fn normalize_agent_get_skips_invalid_id_candidates() {
        // Issue #112: a herdr-native name like "foo/bar" cannot round-trip
        // as the agent half of a global ID, so `agent get` must skip it
        // and resolve `agent` to the same pane_id `agent list` reports.
        let v = json!({"agent": {"agent": "claude", "name": "foo/bar", "pane_id": "w2:p1"}});
        let out = normalize_agent_get(&v, "w2:p1");
        assert_eq!(out["agent"]["agent"], "w2:p1");
        // The raw `name` field still passes through untouched.
        assert_eq!(out["agent"]["name"], "foo/bar");
    }

    #[test]
    fn normalize_agent_get_explicit_kind_wins_and_fallback_target() {
        // An explicit `kind` beats the `agent` alias; a record with no id
        // fields falls back to the resolved target.
        let v = json!({"agent": {"agent": "claude", "kind": "codex"}});
        let out = normalize_agent_get(&v, "w9:p9");
        assert_eq!(out["agent"]["kind"], "codex");
        assert_eq!(out["agent"]["agent"], "w9:p9");
    }

    #[test]
    fn normalize_agent_get_flat_and_idempotent() {
        // Flat record (no envelope) is normalized in place.
        let flat = json!({"agent": "claude", "pane_id": "w1:p2", "agent_status": "working"});
        let out = normalize_agent_get(&flat, "t");
        assert_eq!(out["kind"], "claude");
        assert_eq!(out["agent"], "w1:p2");
        // Normalizing the result again changes nothing.
        let twice = normalize_agent_get(&out, "t");
        assert_eq!(twice, out);
        // Non-object payloads pass through untouched.
        assert_eq!(normalize_agent_get(&json!("x"), "t"), json!("x"));
    }

    #[test]
    fn machines_view_empty_fleet_local_only() {
        // No workers: only local endpoint; probe will fail (no socket) →
        // unreachable row, but the view still returns one entry.
        let ctx = ctx_with_workers();
        let infos = machines_with_timeout(&ctx, Duration::from_millis(50));
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].label, "local");
        assert_eq!(infos[0].kind, MachineKind::Local);
        // No herdr socket in test env → unreachable (or online if a dev
        // machine happens to run herdr — accept either, just check shape).
        assert!(infos[0].status == "online" || infos[0].status == "unreachable");
    }
}
