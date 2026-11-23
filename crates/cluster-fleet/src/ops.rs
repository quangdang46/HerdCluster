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
/// workers, and connect the endpoint.
fn resolve_and_connect(
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
fn op_error(resolved: &crate::ids::ResolvedAgent, e: ApiError) -> FleetError {
    if let ApiError::Server { code, .. } = &e {
        if let Some(err) = agent_not_found(&resolved.machine, &resolved.agent, code) {
            return err;
        }
    }
    map_api_error(&resolved.machine, e)
}

/// `agent.get <global-id>` — fetch one agent's details.
pub fn agent_get(ctx: &FleetContext, global_id: &str) -> Result<Value, FleetError> {
    let (resolved, client) = resolve_and_connect(ctx, global_id)?;
    client
        .agent_get(&resolved.agent)
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

/// `agent.prompt <global-id> <text>` — send a prompt, optionally waiting
/// for a resulting status via `wait`.
pub fn agent_prompt(
    ctx: &FleetContext,
    global_id: &str,
    text: &str,
    wait: Option<WaitOptions>,
) -> Result<Value, FleetError> {
    let (resolved, client) = resolve_and_connect(ctx, global_id)?;
    client
        .agent_prompt(&resolved.agent, text, wait)
        .map_err(|e| op_error(&resolved, e))
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
