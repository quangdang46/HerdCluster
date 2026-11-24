//! Typed errors for cluster-fleet, serializable for `--json` CLI output.
//!
//! Every variant maps to a stable snake_case `code` string
//! (`unknown_machine`, `worker_unreachable`, …) so the CLI layer can emit
//! `{"error": {"code": ..., "message": ...}}` without re-deriving meaning.
//! The codes are a published contract — do not rename them.

use serde::Serialize;

/// Errors produced by ID parsing/resolution, fan-out queries, agent ops,
/// and endpoint resolution.
#[derive(Debug)]
pub enum FleetError {
    /// The global ID string could not be parsed (`{machine}/{agent}`).
    MalformedId {
        /// The offending input.
        input: String,
        /// Why it was rejected.
        reason: String,
    },
    /// No machine with this label is known (not local, not in `workers.json`).
    UnknownMachine {
        /// The unrecognized machine label.
        label: String,
    },
    /// Reserved for future bare-name (machine-less) resolution where one
    /// agent name matches on several machines. Global IDs always carry a
    /// machine label today, so this is currently unreachable.
    AmbiguousAgent {
        /// Agent name that matched multiple machines.
        agent: String,
        /// Candidate global IDs.
        candidates: Vec<String>,
    },
    /// The machine is known but has no agent with this name/id.
    AgentNotFound {
        /// Machine label the ID pointed at.
        machine: String,
        /// Machine-local agent identifier.
        agent: String,
    },
    /// The machine's API endpoint could not be reached: worker offline,
    /// bridge socket absent, connection refused, or request timed out.
    WorkerUnreachable {
        /// Machine label.
        machine: String,
        /// Human-readable cause.
        reason: String,
    },
    /// An underlying `api-client` failure that is not a reachability issue
    /// (protocol incompatibility, malformed response, server-side error).
    Api {
        /// Human-readable cause (includes the `ApiError` display text).
        message: String,
    },
    /// herdcluster state/config could not be read (state dir, cluster.json,
    /// workers.json).
    Config {
        /// Human-readable cause.
        message: String,
    },
    /// A CLI argument failed validation (bad duration, empty `--until`
    /// set, ...). Distinct from [`FleetError::MalformedId`], which is
    /// specifically about the `{machine}/{agent}` syntax.
    InvalidArgument {
        /// Human-readable cause.
        message: String,
    },
}

impl FleetError {
    /// Stable snake_case code for `--json` error output.
    pub fn code(&self) -> &'static str {
        match self {
            FleetError::MalformedId { .. } => "malformed_id",
            FleetError::UnknownMachine { .. } => "unknown_machine",
            FleetError::AmbiguousAgent { .. } => "ambiguous_agent",
            FleetError::AgentNotFound { .. } => "agent_not_found",
            FleetError::WorkerUnreachable { .. } => "worker_unreachable",
            FleetError::Api { .. } => "api_error",
            FleetError::Config { .. } => "config_error",
            FleetError::InvalidArgument { .. } => "invalid_argument",
        }
    }

    /// Build the canonical `{"error": {"code", "message", ...}}` JSON value.
    pub fn to_json(&self) -> serde_json::Value {
        let mut error = serde_json::json!({
            "code": self.code(),
            "message": self.to_string(),
        });
        if let FleetError::AmbiguousAgent { candidates, .. } = self {
            error["candidates"] = serde_json::json!(candidates);
        }
        serde_json::json!({ "error": error })
    }
}

impl std::fmt::Display for FleetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FleetError::MalformedId { input, reason } => {
                write!(f, "malformed agent id {input:?}: {reason}")
            }
            FleetError::UnknownMachine { label } => {
                write!(
                    f,
                    "unknown machine {label:?}: not the local machine \
                            and not present in workers.json"
                )
            }
            FleetError::AmbiguousAgent { agent, candidates } => write!(
                f,
                "agent name {agent:?} is ambiguous across machines: {}",
                candidates.join(", ")
            ),
            FleetError::AgentNotFound { machine, agent } => {
                write!(f, "no agent {agent:?} on machine {machine:?}")
            }
            FleetError::WorkerUnreachable { machine, reason } => {
                write!(f, "machine {machine:?} is unreachable: {reason}")
            }
            FleetError::Api { message } => write!(f, "herdr API error: {message}"),
            FleetError::Config { message } => write!(f, "herdcluster config error: {message}"),
            FleetError::InvalidArgument { message } => write!(f, "invalid argument: {message}"),
        }
    }
}

impl std::error::Error for FleetError {}

impl Serialize for FleetError {
    /// Serializes as `{"code": ..., "message": ...}` (plus `candidates`
    /// for `ambiguous_agent`) — embed under `"error"` for the full
    /// envelope via [`FleetError::to_json`].
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut error = serde_json::json!({
            "code": self.code(),
            "message": self.to_string(),
        });
        if let FleetError::AmbiguousAgent { candidates, .. } = self {
            error["candidates"] = serde_json::json!(candidates);
        }
        error.serialize(s)
    }
}
