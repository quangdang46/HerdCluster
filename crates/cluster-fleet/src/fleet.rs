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
use crate::ids::format_global_id;
use crate::parse;

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
    /// Number of agents seen by the probe (absent when unreachable).
    pub agents: Option<u64>,
    /// Last heartbeat-reported running agent count (workers only).
    pub agent_count: Option<u32>,
    /// Last heartbeat-reported agent cap (workers only).
    pub max_agents: Option<u32>,
    /// Last heartbeat-reported available memory in MiB (workers only).
    pub mem_available_mb: Option<u64>,
    /// Agent kinds the worker advertised (workers only; may be overridden
    /// by `cluster.json` — see spawn for the effective set).
    pub kinds: Vec<String>,
    /// Raw last resource snapshot (workers only).
    pub resources: Option<cluster_core::protocol::Resources>,
    /// `last_seen` from the registry, RFC 3339 (workers only).
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
const AGENT_ID_KEYS: &[&str] = &["name", "terminal_id", "id", "agent_id"];
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
        if s.is_empty() {
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
    let local_id = parse::str_field(item, AGENT_ID_KEYS).map(str::to_string)?;
    if local_id.is_empty() {
        return None;
    }
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
            .map(|ep| s.spawn(move || (ep.label.clone(), query_machine(ep, timeout))))
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or_else(|_| unreachable!("query thread panicked"))
            })
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

fn query_machine(ep: &MachineEndpoint, timeout: Duration) -> MachineAgents {
    match fetch_agent_list(ep, timeout) {
        Ok(value) => MachineAgents::Agents(parse_agent_list(&ep.label, &value)),
        Err(reason) => MachineAgents::Unreachable { reason },
    }
}

/// Assemble one [`MachineInfo`] from registry data + an optional live probe.
/// Pure — separated from IO for testing.
fn assemble_machine_info(
    endpoint: &MachineEndpoint,
    worker: Option<&WorkerEntry>,
    probe: Option<Result<usize, String>>,
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

    let resources = worker.and_then(|w| w.resources.clone());
    MachineInfo {
        label: endpoint.label.clone(),
        kind: endpoint.kind,
        status,
        reachable,
        agents,
        agent_count: resources.as_ref().map(|r| r.agent_count),
        max_agents: resources.as_ref().map(|r| r.max_agents),
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

    let probes: Vec<(String, Option<Result<usize, String>>)> = std::thread::scope(|s| {
        let handles: Vec<_> = live
            .iter()
            .map(|ep| {
                s.spawn(move || {
                    let result =
                        fetch_agent_list(ep, timeout).map(|v| parse::items_array(&v).len());
                    (ep.label.clone(), Some(result))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| ("".to_string(), None)))
            .filter(|(l, _)| !l.is_empty())
            .collect()
    });
    let mut probe_map: std::collections::BTreeMap<String, Option<Result<usize, String>>> =
        probes.into_iter().collect();

    let mut out: Vec<MachineInfo> = ctx
        .endpoints
        .iter()
        .map(|ep| {
            let worker = ctx.worker_entry(&ep.label);
            let probe = if skipped.contains(&ep.label) {
                None
            } else {
                probe_map.remove(&ep.label).flatten()
            };
            assemble_machine_info(ep, worker, probe)
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
            {"pane_id": "no-id-here"} // skipped: no usable identifier
        ]})
    }

    #[test]
    fn parse_agent_list_extracts_ids_and_fields() {
        let rows = parse_agent_list("local", &agents_value());
        assert_eq!(rows.len(), 2);
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
    }

    #[test]
    fn parse_agent_list_accepts_bare_array() {
        let rows = parse_agent_list("w", &json!([{"name": "a"}, "bare-id"]));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].agent, "bare-id");
        assert_eq!(rows[1].id, "w/bare-id");
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
    fn assemble_machine_info_cases() {
        let ep = MachineEndpoint {
            label: "w".into(),
            socket_path: PathBuf::from("/x.sock"),
            kind: MachineKind::Remote,
        };
        // Offline worker: no probe → registry status, unreachable=false.
        let mut w = cluster_core::registry::WorkerEntry {
            worker_id: "i".into(),
            label: "w".into(),
            pubkey: "p".into(),
            paired_at: chrono::Utc::now(),
            last_seen: None,
            status: cluster_core::registry::WorkerStatus::Offline,
            resources: None,
        };
        let info = assemble_machine_info(&ep, Some(&w), None);
        assert_eq!(info.status, "offline");
        assert!(!info.reachable);
        assert_eq!(info.agents, None);

        // Degraded worker, probe ok → status stays degraded, count set.
        w.status = cluster_core::registry::WorkerStatus::Degraded;
        let info = assemble_machine_info(&ep, Some(&w), Some(Ok(3)));
        assert_eq!(info.status, "degraded");
        assert!(info.reachable);
        assert_eq!(info.agents, Some(3));

        // Probe fails → "unreachable" overrides registry status.
        let info = assemble_machine_info(&ep, Some(&w), Some(Err("boom".into())));
        assert_eq!(info.status, "unreachable");
        assert!(!info.reachable);

        // Local machine: no registry entry; probe decides.
        let local = MachineEndpoint {
            label: "local".into(),
            socket_path: PathBuf::from("/h.sock"),
            kind: MachineKind::Local,
        };
        let info = assemble_machine_info(&local, None, Some(Ok(2)));
        assert_eq!(info.status, "online");
        assert!(info.reachable);
        let info = assemble_machine_info(&local, None, Some(Err("down".into())));
        assert_eq!(info.status, "unreachable");
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
