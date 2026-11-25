//! Agent operations that accept global agent IDs (PLAN.md §3.6).
//!
//! Each function resolves `{machine}/{agent}` via [`resolve_global_id`],
//! then forwards to the corresponding `api-client` method on that
//! machine's endpoint. `wait` is **per-machine only** — there is no
//! cross-machine wait in MVP; the wait happens on the machine hosting the
//! agent.
//!
//! Offline workers short-circuit to [`FleetError::WorkerUnreachable`]
//! before any socket work. Server-side "agent not found" errors are mapped
//! to [`FleetError::AgentNotFound`].

use std::time::Duration;

use api_client::methods::{ReadFormat, ReadSource, WaitOptions};
use api_client::ApiError;
use serde_json::Value;

use crate::endpoint::FleetContext;
use crate::error::FleetError;
use crate::ids::{agent_not_found, map_api_error, resolve_global_id};

/// Shared preamble: parse + resolve the global ID, short-circuit offline
/// workers, and connect the endpoint. `pub(crate)` so [`crate::watch`]
/// can reuse the same routing for its polling loops.
pub(crate) fn resolve_and_connect(
    ctx: &FleetContext,
    global_id: &str,
) -> Result<(crate::ids::ResolvedAgent, api_client::ApiClient), FleetError> {
    let resolved = resolve_global_id(ctx, global_id)?;
    if resolved.endpoint.kind == crate::endpoint::MachineKind::Remote
        && ctx.is_offline(&resolved.machine)
    {
        return Err(FleetError::WorkerUnreachable {
            machine: resolved.machine,
            reason: "worker offline (stale heartbeat)".to_string(),
        });
    }
    let client = resolved.endpoint.client()?;
    Ok((resolved, client))
}

/// Map an `ApiError` from an agent op: not-found server codes become
/// [`FleetError::AgentNotFound`]; the rest go through [`map_api_error`].
/// `pub(crate)` for [`crate::watch`].
pub(crate) fn op_error(resolved: &crate::ids::ResolvedAgent, e: ApiError) -> FleetError {
    if let ApiError::Server { code, .. } = &e {
        if let Some(err) = agent_not_found(&resolved.machine, &resolved.agent, code) {
            return err;
        }
    }
    map_api_error(&resolved.machine, e)
}

/// `agent.get <global-id>` — fetch one agent's details.
///
/// The response is normalized via [`crate::fleet::normalize_agent_get`]
/// so `agent`/`kind` match `agent list` row semantics (issue #14); all
/// other herdr `agent_info` fields pass through.
pub fn agent_get(ctx: &FleetContext, global_id: &str) -> Result<Value, FleetError> {
    let (resolved, client) = resolve_and_connect(ctx, global_id)?;
    client
        .agent_get(&resolved.agent)
        .map(|v| crate::fleet::normalize_agent_get(&v, &resolved.agent))
        .map_err(|e| op_error(&resolved, e))
}

/// `agent.read <global-id>` — read the agent pane's terminal content.
#[allow(clippy::too_many_arguments)]
pub fn agent_read(
    ctx: &FleetContext,
    global_id: &str,
    source: ReadSource,
    format: Option<ReadFormat>,
    lines: Option<u32>,
    strip_ansi: Option<bool>,
) -> Result<Value, FleetError> {
    let (resolved, client) = resolve_and_connect(ctx, global_id)?;
    client
        .agent_read(&resolved.agent, source, format, lines, strip_ansi)
        .map_err(|e| op_error(&resolved, e))
}

/// `agent.wait <global-id>` — block until the agent reaches a status.
///
/// **Per-machine only**: the wait runs on the machine hosting the agent;
/// there is no cross-machine wait in MVP (PLAN.md §3.6).
pub fn agent_wait(
    ctx: &FleetContext,
    global_id: &str,
    wait: Option<WaitOptions>,
) -> Result<Value, FleetError> {
    agent_wait_with_timeout(ctx, global_id, wait, api_client::DEFAULT_TIMEOUT)
}

/// [`agent_wait`] with an explicit client-side timeout; must exceed
/// `wait.timeout_ms` or the client gives up first.
pub fn agent_wait_with_timeout(
    ctx: &FleetContext,
    global_id: &str,
    wait: Option<WaitOptions>,
    client_timeout: Duration,
) -> Result<Value, FleetError> {
    let (resolved, client) = resolve_and_connect(ctx, global_id)?;
    client
        .agent_wait_with_timeout(&resolved.agent, wait, client_timeout)
        .map_err(|e| op_error(&resolved, e))
}

/// How long [`agent_prompt`] watches for a turn to start before pressing
/// `Enter` as a submit retry, and again before declaring the prompt
/// stalled (issue #9).
const PROMPT_VERIFY_TIMEOUT: Duration = Duration::from_secs(8);
const PROMPT_VERIFY_POLL: Duration = Duration::from_millis(400);
/// Scrollback re-read cadence inside the verify loop — `agent.read` is
/// heavier than `agent.get`, so it runs less often.
const PROMPT_VERIFY_READ_INTERVAL: Duration = Duration::from_millis(1200);
/// Lines of recent scrollback sampled for the content-change signal.
const PROMPT_VERIFY_READ_LINES: u32 = 60;
/// `agent.get` statuses that prove the prompt started (or is running) a
/// turn. `idle`/`done`/`unknown` are deliberately excluded: an idle
/// agent that never leaves idle is exactly the stall this guards.
const TURN_ACTIVE_STATUSES: &[&str] = &["working", "blocked"];
/// Settle delay after `agent.prompt` returns before sampling the
/// scrollback baseline, so parked (typed-but-unsubmitted) text is already
/// in the baseline and cannot fake a "turn started" diff.
const PROMPT_BASELINE_SETTLE: Duration = Duration::from_millis(1000);

/// `agent.prompt <global-id> <text>` — send a prompt, optionally waiting
/// for a resulting status via `wait`.
///
/// With `wait: None` the prompt is still *verified* before reporting
/// success: herdr's `agent.prompt` types the text but some agent TUIs
/// never observe the submit, leaving the text parked in the input box
/// while the call returns `agent_prompted` (issue #9). A turn counts as
/// started when `agent.get` shows working/blocked, `state_change_seq`
/// moves, or the recent scrollback changes — the last signal covers
/// agents whose panes are too small for herdr's status detection (the
/// `agent.explain` region collapses to the TUI footer). If nothing
/// moves we press `Enter` once — the parked text then submits — and
/// watch again. Still nothing → [`FleetError::PromptStalled`]. An
/// explicit `wait` is passed through untouched: upstream already
/// verifies in that mode.
pub fn agent_prompt(
    ctx: &FleetContext,
    global_id: &str,
    text: &str,
    wait: Option<WaitOptions>,
) -> Result<Value, FleetError> {
    let (resolved, client) = resolve_and_connect(ctx, global_id)?;
    let target = resolved.agent.clone();
    let seq_before = agent_state_seq(&client, &target);

    let prompt_result = client.agent_prompt(&target, text, wait.clone());
    if wait.is_some() {
        // Upstream `wait` already turns a non-starting prompt into
        // `agent_prompt_stalled`/`timeout` — pass the verdict through.
        return prompt_result.map_err(|e| op_error(&resolved, e));
    }

    let ok_value = match prompt_result {
        Ok(v) => v,
        Err(e) if is_prompt_stalled(&e) => {
            // Upstream already detected the stall; the text is typed but
            // unsubmitted — skip straight to the Enter retry below.
            serde_json::json!({ "type": "agent_prompted" })
        }
        Err(e) => return Err(op_error(&resolved, e)),
    };

    std::thread::sleep(PROMPT_BASELINE_SETTLE);
    let baseline = recent_tail(&client, &target);
    if prompt_turn_started(&client, &target, seq_before, baseline.as_deref()) {
        return Ok(ok_value);
    }
    // One submit retry: `agent.send_keys Enter` flushes text parked in
    // the agent's input box (verified against devin TUIs, issue #9).
    let _ = client.agent_send_keys(&target, &["Enter".to_string()]);
    std::thread::sleep(PROMPT_BASELINE_SETTLE);
    let baseline = recent_tail(&client, &target);
    if prompt_turn_started(&client, &target, seq_before, baseline.as_deref()) {
        return Ok(serde_json::json!({
            "type": "agent_prompted",
            "submitted_via": "enter",
        }));
    }
    Err(FleetError::PromptStalled {
        machine: resolved.machine.clone(),
        agent: resolved.agent.clone(),
    })
}

/// True when the error is upstream's `agent_prompt_stalled` (prompt
/// accepted but no working/blocked state observed within its window).
fn is_prompt_stalled(e: &ApiError) -> bool {
    matches!(e, ApiError::Server { code, .. } if code == "agent_prompt_stalled")
}

/// `state_change_seq` from `agent.get`, if readable. `None` when the
/// agent is gone or the field is absent — verification then relies on
/// status alone.
fn agent_state_seq(client: &api_client::ApiClient, target: &str) -> Option<u64> {
    let v = client.agent_get(target).ok()?;
    v.pointer("/agent/state_change_seq")
        .or_else(|| v.get("state_change_seq"))
        .and_then(Value::as_u64)
}

/// `agent_status` from `agent.get`, if readable.
fn agent_status_of(v: &Value) -> Option<&str> {
    v.pointer("/agent/agent_status")
        .or_else(|| v.pointer("/agent/status"))
        .or_else(|| v.get("agent_status"))
        .and_then(Value::as_str)
}

/// Tail of `agent.read source=recent` for content-diff verification.
/// `None` when the read fails — callers then rely on status/seq only.
fn recent_tail(client: &api_client::ApiClient, target: &str) -> Option<String> {
    let v = client
        .agent_read(
            target,
            ReadSource::Recent,
            None,
            Some(PROMPT_VERIFY_READ_LINES),
            None,
        )
        .ok()?;
    v.pointer("/read/text")
        .or_else(|| v.get("text"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Poll until the agent shows a working/blocked status, its
/// `state_change_seq` moves off `seq_before`, or the recent scrollback
/// differs from `baseline`. The content signal covers agents whose panes
/// are too small for herdr's status detection — `agent.explain` then
/// sees only the TUI footer and `agent_status` never leaves `idle`
/// (issue #9). A fast turn that returns to `idle` between polls still
/// bumps the seq or the scrollback, so it counts.
fn prompt_turn_started(
    client: &api_client::ApiClient,
    target: &str,
    seq_before: Option<u64>,
    baseline: Option<&str>,
) -> bool {
    let deadline = std::time::Instant::now() + PROMPT_VERIFY_TIMEOUT;
    let mut last_read = std::time::Instant::now() - PROMPT_VERIFY_READ_INTERVAL;
    loop {
        if let Ok(v) = client.agent_get(target) {
            if agent_status_of(&v).is_some_and(|s| TURN_ACTIVE_STATUSES.contains(&s)) {
                return true;
            }
            if let (Some(before), Some(now)) = (
                seq_before,
                v.pointer("/agent/state_change_seq")
                    .or_else(|| v.get("state_change_seq"))
                    .and_then(Value::as_u64),
            ) {
                if now != before {
                    return true;
                }
            }
        }
        if let Some(base) = baseline {
            if last_read.elapsed() >= PROMPT_VERIFY_READ_INTERVAL {
                last_read = std::time::Instant::now();
                if let Some(cur) = recent_tail(client, target) {
                    if cur != base {
                        return true;
                    }
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(PROMPT_VERIFY_POLL);
    }
}

/// `agent.send_keys <global-id> <keys...>` — send key names (e.g.
/// `["Enter"]`, `["C-c"]`) to the agent's pane.
pub fn agent_send_keys(
    ctx: &FleetContext,
    global_id: &str,
    keys: &[String],
) -> Result<Value, FleetError> {
    let (resolved, client) = resolve_and_connect(ctx, global_id)?;
    client
        .agent_send_keys(&resolved.agent, keys)
        .map_err(|e| op_error(&resolved, e))
}

/// `agent.rename <global-id> [name]` — rename an agent; `None` clears the
/// name (upstream `name: null`).
pub fn agent_rename(
    ctx: &FleetContext,
    global_id: &str,
    name: Option<&str>,
) -> Result<Value, FleetError> {
    let (resolved, client) = resolve_and_connect(ctx, global_id)?;
    client
        .agent_rename(&resolved.agent, name)
        .map_err(|e| op_error(&resolved, e))
}

/// Shared preamble for machine-scoped ops that are *not* addressed by a
/// global agent id: resolve the endpoint by label, short-circuit offline
/// workers, and connect.
fn connect_machine(
    ctx: &FleetContext,
    machine: &str,
) -> Result<(crate::endpoint::MachineEndpoint, api_client::ApiClient), FleetError> {
    let ep = ctx
        .endpoint_for(machine)
        .cloned()
        .ok_or_else(|| FleetError::UnknownMachine {
            label: machine.to_string(),
        })?;
    if ep.kind == crate::endpoint::MachineKind::Remote && ctx.is_offline(machine) {
        return Err(FleetError::WorkerUnreachable {
            machine: machine.to_string(),
            reason: "worker offline (stale heartbeat)".to_string(),
        });
    }
    let client = ep.client()?;
    Ok((ep, client))
}

/// `pane.close` on `machine` — tears down the pane (and any agent in it).
/// Used by `herdcluster clean` for agents recorded in `.herdcluster/`.
pub fn machine_pane_close(
    ctx: &FleetContext,
    machine: &str,
    pane_id: &str,
) -> Result<Value, FleetError> {
    let (ep, client) = connect_machine(ctx, machine)?;
    client
        .pane_close(pane_id)
        .map_err(|e| map_api_error(&ep.label, e))
}

/// `workspace.close` on `machine` — closes Herdr workspace state only
/// (never deletes files on disk).
pub fn machine_workspace_close(
    ctx: &FleetContext,
    machine: &str,
    workspace_id: &str,
    close_group: Option<bool>,
) -> Result<Value, FleetError> {
    let (ep, client) = connect_machine(ctx, machine)?;
    client
        .workspace_close(workspace_id, close_group)
        .map_err(|e| map_api_error(&ep.label, e))
}

/// `agent.focus <global-id>` — focus the pane hosting the agent. Note this
/// only makes sense for the local machine's UI; on a worker it focuses the
/// pane in that worker's herdr session.
pub fn agent_focus(ctx: &FleetContext, global_id: &str) -> Result<Value, FleetError> {
    let (resolved, client) = resolve_and_connect(ctx, global_id)?;
    client
        .agent_focus(&resolved.agent)
        .map_err(|e| op_error(&resolved, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::FleetContext;
    use cluster_core::registry::ClusterInfo;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn ctx() -> FleetContext {
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
    fn malformed_id_errors_before_any_io() {
        let e = agent_get(&ctx(), "no-slash").unwrap_err();
        assert_eq!(e.code(), "malformed_id");
        let e = agent_get(&ctx(), "ghost/a").unwrap_err();
        assert_eq!(e.code(), "unknown_machine");
    }

    #[test]
    fn local_machine_without_herdr_is_api_error_not_panic() {
        // "local/xxx" resolves; with no herdr socket the op fails cleanly.
        let e = agent_get(&ctx(), "local/xxx").unwrap_err();
        // A dev machine may actually run herdr — then the server answers
        // and the missing agent maps to AgentNotFound. Either way it is a
        // typed FleetError, never a panic.
        assert!(matches!(
            e,
            FleetError::Api { .. }
                | FleetError::WorkerUnreachable { .. }
                | FleetError::AgentNotFound { .. }
        ));
    }

    #[test]
    fn offline_worker_short_circuits() {
        use cluster_core::registry::{WorkerEntry, WorkerStatus};
        let mut w = WorkerEntry {
            worker_id: "i".into(),
            label: "w1".into(),
            pubkey: "p".into(),
            paired_at: chrono::Utc::now(),
            last_seen: None,
            status: WorkerStatus::Offline,
            resources: None,
        };
        let ctx = FleetContext::build(
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
            vec![w.clone()],
        )
        .unwrap();
        w.status = WorkerStatus::Offline;
        let e = agent_get(&ctx, "w1/agent").unwrap_err();
        assert_eq!(e.code(), "worker_unreachable");
    }

    #[test]
    fn error_json_envelope() {
        let e = FleetError::UnknownMachine { label: "x".into() };
        let v = e.to_json();
        assert_eq!(v["error"]["code"], "unknown_machine");
        assert!(v["error"]["message"].is_string());
    }

    #[test]
    fn wait_options_serialize_through() {
        // Compile-level check that WaitOptions is usable by callers.
        let w = WaitOptions {
            timeout_ms: Some(1000),
            until: Some(vec![api_client::methods::AgentStatus::Idle]),
        };
        assert_eq!(w.timeout_ms, Some(1000));
    }
}
