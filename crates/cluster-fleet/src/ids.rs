//! Global agent ID scheme and resolution (PLAN.md §3.6).
//!
//! Format: `{machine-label}/{agent-name-or-terminal-id}`.
//!
//! * The machine label is either [`LOCAL_LABEL`] (`"local"`) or a worker
//!   label from `workers.json`.
//! * The agent part is the machine-local identifier accepted by herdr's
//!   `agent.*` methods (agent name or terminal id).
//!
//! Resolution is direct: the machine half selects the endpoint; the agent
//! half is passed through to that machine's herdr API. A bare name without
//! a machine prefix is **not** a valid global ID (rejected as
//! [`FleetError::MalformedId`]) — cross-machine bare-name search is a
//! possible future feature, which is what
//! [`FleetError::AmbiguousAgent`] is reserved for.

use crate::endpoint::{FleetContext, MachineEndpoint, LOCAL_LABEL};
use crate::error::FleetError;

/// Parsed global agent ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalId {
    /// Machine label (`"local"` or a worker label).
    pub machine: String,
    /// Machine-local agent identifier (name or terminal id).
    pub agent: String,
}

/// A resolved global ID: the endpoint to call plus the machine-local
/// agent target to pass to `agent.*` methods.
#[derive(Debug, Clone)]
pub struct ResolvedAgent {
    /// Machine label (echo of [`GlobalId::machine`]).
    pub machine: String,
    /// Machine-local agent identifier to use as the `target` param.
    pub agent: String,
    /// The machine's API endpoint.
    pub endpoint: MachineEndpoint,
}

/// Format a global agent ID. Does not validate — use
/// [`parse_global_id`] to check round-trips.
pub fn format_global_id(machine: &str, agent: &str) -> String {
    format!("{machine}/{agent}")
}

/// A path segment is valid iff non-empty and free of `/`, whitespace, and
/// control characters. Machine labels and agent names/terminal-ids do not
/// legitimately contain those.
///
/// This is the canonical rule for both halves of a global ID; callers
/// validating an agent name *before* a global ID exists (e.g. `spawn`,
/// `agent rename`) use this directly so stamped IDs always round-trip
/// through [`parse_global_id`].
pub fn valid_id_part(part: &str) -> bool {
    !part.is_empty()
        && !part
            .chars()
            .any(|c| c == '/' || c.is_whitespace() || c.is_control())
}

/// Parse and validate a global agent ID.
///
/// Rejects: missing `/`, empty machine or agent part, extra `/` inside the
/// agent part, whitespace/control characters, and surrounding whitespace
/// on the whole input.
pub fn parse_global_id(input: &str) -> Result<GlobalId, FleetError> {
    let malformed = |reason: &str| FleetError::MalformedId {
        input: input.to_string(),
        reason: reason.to_string(),
    };
    if input.trim() != input {
        return Err(malformed("leading/trailing whitespace"));
    }
    let (machine, agent) = input
        .split_once('/')
        .ok_or_else(|| malformed("expected `{machine}/{agent}`"))?;
    if !valid_id_part(machine) {
        return Err(malformed("empty or invalid machine label"));
    }
    if agent.contains('/') {
        return Err(malformed("agent part must not contain '/'"));
    }
    if !valid_id_part(agent) {
        return Err(malformed("empty or invalid agent identifier"));
    }
    Ok(GlobalId {
        machine: machine.to_string(),
        agent: agent.to_string(),
    })
}

/// Resolve a global ID to a machine endpoint + local agent target.
///
/// Errors: [`FleetError::MalformedId`] on bad syntax,
/// [`FleetError::UnknownMachine`] when the label is neither `"local"` nor a
/// registered worker. Does **not** verify the agent exists on the machine
/// — that surfaces as [`FleetError::AgentNotFound`] (or a server error
/// mapped to it) when the `agent.*` call runs. Use
/// [`resolve_existing_agent`] for an explicit existence check.
pub fn resolve_global_id(ctx: &FleetContext, input: &str) -> Result<ResolvedAgent, FleetError> {
    let gid = parse_global_id(input)?;
    let endpoint = ctx
        .endpoint_for(&gid.machine)
        .ok_or_else(|| FleetError::UnknownMachine {
            label: gid.machine.clone(),
        })?
        .clone();
    Ok(ResolvedAgent {
        machine: gid.machine,
        agent: gid.agent,
        endpoint,
    })
}

/// Like [`resolve_global_id`] but additionally verifies the agent exists on
/// the target machine via `agent.get`. Missing agents produce
/// [`FleetError::AgentNotFound`]; unreachable machines produce
/// [`FleetError::WorkerUnreachable`].
///
/// Ops that forward to `agent.*` do not need this pre-check — the server
/// returns a not-found error which ops map to [`FleetError::AgentNotFound`]
/// anyway. This exists for callers that want a single resolution step.
pub fn resolve_existing_agent(
    ctx: &FleetContext,
    input: &str,
) -> Result<ResolvedAgent, FleetError> {
    let resolved = resolve_global_id(ctx, input)?;
    let client = resolved.endpoint.client()?;
    match client.agent_get(&resolved.agent) {
        Ok(_) => Ok(resolved),
        Err(e) => {
            if let api_client::ApiError::Server { code, .. } = &e {
                if let Some(err) = agent_not_found(&resolved.machine, &resolved.agent, code) {
                    return Err(err);
                }
            }
            Err(map_api_error(&resolved.endpoint.label, e))
        }
    }
}

/// Map an `api-client` failure to a [`FleetError`], used by both ids and
/// ops. `ServerNotRunning`/IO/timeout → [`FleetError::WorkerUnreachable`];
/// other server/protocol errors → [`FleetError::Api`].
pub(crate) fn map_api_error(machine: &str, e: api_client::ApiError) -> FleetError {
    use api_client::ApiError;
    match e {
        ApiError::ServerNotRunning { .. } | ApiError::Timeout | ApiError::Io(_) => {
            FleetError::WorkerUnreachable {
                machine: machine.to_string(),
                reason: e.to_string(),
            }
        }
        other => FleetError::Api {
            message: other.to_string(),
        },
    }
}

/// herdr 0.9.5 server error codes meaning "the agent target does not
/// exist" — emitted by `agent.*`/`pane.*` handlers when the target name,
/// agent, or backing pane is gone (verified against the installed
/// `herdr` 0.9.5 binary's `api/agents.rs` error table).
///
/// This is an **explicit allowlist**, not a substring match: codes like
/// `unknown_method`, `workspace_not_found`, or `internal_unknown_state`
/// merely *contain* `unknown`/`not_found` and must surface as real API
/// errors, not `agent_not_found` (issue #42).
const AGENT_NOT_FOUND_CODES: &[&str] = &[
    "agent_not_found",
    "agent_name_not_found",
    "agent_pane_not_found",
    "pane_not_found",
    "target_pane_not_found",
    "source_pane_not_found",
    "not_found",
];

/// Map an `api-client` *server* error code to [`FleetError::AgentNotFound`]
/// when the code is a known herdr agent-target-missing code; `None`
/// means "not an agent-not-found".
pub(crate) fn agent_not_found(machine: &str, agent: &str, code: &str) -> Option<FleetError> {
    if AGENT_NOT_FOUND_CODES.contains(&code) {
        Some(FleetError::AgentNotFound {
            machine: machine.to_string(),
            agent: agent.to_string(),
        })
    } else {
        None
    }
}

/// Convenience: is `label` the local machine?
pub fn is_local_label(label: &str) -> bool {
    label == LOCAL_LABEL
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_ids() {
        let gid = parse_global_id("local/claude-1").unwrap();
        assert_eq!(gid.machine, "local");
        assert_eq!(gid.agent, "claude-1");

        let gid = parse_global_id("desktop-win/term_42").unwrap();
        assert_eq!(gid.machine, "desktop-win");
        assert_eq!(gid.agent, "term_42");

        // Agent part may contain dots/dashes/underscores.
        assert!(parse_global_id("wsl2/my.agent-v2").is_ok());
        // format/parse round-trip.
        let s = format_global_id("m", "a.b-c_d");
        assert_eq!(parse_global_id(&s).unwrap().agent, "a.b-c_d");
    }

    #[test]
    fn parse_rejects_malformed() {
        for bad in [
            "",
            "claude",         // no machine prefix
            "/agent",         // empty machine
            "local/",         // empty agent
            "local//x",       // empty middle → agent contains '/'
            "a/b/c",          // extra slash
            " local/x",       // leading space
            "local/x ",       // trailing space
            "local/my agent", // whitespace inside
            "local/agent\n",  // control char
            "loc al/x",       // whitespace in machine
        ] {
            assert!(
                matches!(parse_global_id(bad), Err(FleetError::MalformedId { .. })),
                "expected MalformedId for {bad:?}"
            );
        }
    }

    #[test]
    fn valid_id_part_matches_parse_rules() {
        for ok in ["claude-1", "my.agent_v2", "term_42", "a.b-c_d"] {
            assert!(valid_id_part(ok), "expected accept for {ok:?}");
        }
        for bad in [
            "",
            "foo/bar",
            "my agent",
            " lead",
            "trail ",
            "new\nline",
            "tab\there",
            "nul\0byte",
        ] {
            assert!(!valid_id_part(bad), "expected reject for {bad:?}");
        }
    }

    #[test]
    fn agent_not_found_uses_explicit_code_allowlist() {
        // Known herdr agent-target-missing codes → AgentNotFound.
        for code in [
            "agent_not_found",
            "agent_name_not_found",
            "agent_pane_not_found",
            "pane_not_found",
            "target_pane_not_found",
            "source_pane_not_found",
            "not_found",
        ] {
            let err = agent_not_found("m", "a", code).expect("should be agent-not-found");
            assert!(matches!(err, FleetError::AgentNotFound { .. }), "{code}");
        }
        // Codes that merely *contain* not_found/unknown/no_such must NOT
        // be misreported as agent_not_found (issue #42).
        for code in [
            "unknown_method",
            "method_not_found",
            "workspace_not_found",
            "internal_unknown_state",
            "tab_not_found",
            "worktree_not_found",
            "plugin_not_found",
            "plugin_action_not_found",
            "plugin_pane_not_found",
            "command_not_found",
            "layout_not_found",
            "split_not_found",
            "repo_not_found",
            "agent_target_ambiguous",
            "agent_not_ready",
            "invalid_target",
            "timeout",
            "internal_error",
        ] {
            assert!(
                agent_not_found("m", "a", code).is_none(),
                "{code} must not map to AgentNotFound"
            );
        }
    }

    #[test]
    fn error_codes_are_stable() {
        assert_eq!(
            FleetError::UnknownMachine { label: "x".into() }.code(),
            "unknown_machine"
        );
        assert_eq!(
            FleetError::WorkerUnreachable {
                machine: "x".into(),
                reason: "r".into()
            }
            .code(),
            "worker_unreachable"
        );
        assert_eq!(
            FleetError::MalformedId {
                input: "x".into(),
                reason: "r".into()
            }
            .code(),
            "malformed_id"
        );
        assert_eq!(
            FleetError::AgentNotFound {
                machine: "m".into(),
                agent: "a".into()
            }
            .code(),
            "agent_not_found"
        );
    }

    #[test]
    fn resolve_unknown_machine() {
        use crate::endpoint::FleetContext;
        use cluster_core::registry::ClusterInfo;
        use std::collections::BTreeMap;
        use std::path::PathBuf;

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
            vec![],
        )
        .unwrap();

        match resolve_global_id(&ctx, "ghost/agent") {
            Err(FleetError::UnknownMachine { label }) => assert_eq!(label, "ghost"),
            other => panic!("expected UnknownMachine, got {other:?}"),
        }
        // Local label resolves (endpoint exists even if herdr isn't running —
        // reachability is a call-time concern).
        let r = resolve_global_id(&ctx, "local/claude").unwrap();
        assert_eq!(r.machine, "local");
        assert_eq!(r.agent, "claude");
        assert_eq!(r.endpoint.kind, crate::endpoint::MachineKind::Local);
    }
}
