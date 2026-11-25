//! `cluster-fleet` — fleet-level agent operations for HerdCluster.
//!
//! This crate implements PLAN.md §3.6 (fleet aggregation & global IDs) and
//! §3.8 (spawn helper). It contains **no network/TLS code** (that is
//! `cluster-net`) and **no daemon code** (that is `cluster-node`).
//!
//! # Machine model
//!
//! Every machine — the local controller or a paired worker — is addressed
//! the same way: an [`api_client::ApiClient`] connected to a unix-socket
//! path. For the local machine that is herdr's own API socket (via
//! [`api_client::platform::socket_path`]); for a remote worker it is the
//! **cluster bridge socket** exposed by `cluster-bridge`:
//!
//! ```text
//! <state_dir>/bridge/<sanitized-worker-label>.sock
//! ```
//!
//! See [`endpoint::bridge_socket_path`] for the exact convention
//! `cluster-bridge` must implement. Until the bridge socket exists, calls
//! against a worker fail with [`FleetError::WorkerUnreachable`] — that is
//! the intended behavior, not a bug.
//!
//! # Global agent IDs
//!
//! Agents are addressed as `{machine-label}/{agent-name-or-terminal-id}`
//! (PLAN.md §3.6). The local machine's label is the reserved constant
//! [`endpoint::LOCAL_LABEL`] (`"local"`). See [`ids`] for parsing and
//! resolution.
//!
//! # Sync, not async
//!
//! `api-client` is synchronous (one `std::os::unix::net::UnixStream` per
//! request). Fan-out uses `std::thread::scope` — no async runtime
//! dependency.

pub mod endpoint;
pub mod error;
pub mod fleet;
pub mod ids;
pub mod ops;
pub(crate) mod parse;
pub mod spawn;
pub mod watch;

pub use endpoint::{
    bridge_socket_path, local_endpoint, remote_endpoint, FleetContext, MachineEndpoint,
    MachineKind, LOCAL_LABEL,
};
pub use error::FleetError;
pub use fleet::{
    list_all_agents, machines, normalize_agent_get, AgentListRow, FleetAgent, MachineInfo,
    FANOUT_TIMEOUT, PROBE_TIMEOUT,
};
pub use ids::{
    format_global_id, parse_global_id, resolve_global_id, valid_id_part, GlobalId, ResolvedAgent,
};
pub use ops::{
    agent_focus, agent_get, agent_prompt, agent_read, agent_rename, agent_send_keys, agent_wait,
    machine_pane_close, machine_workspace_close,
};
pub use spawn::{spawn, MachineTarget, SpawnError, SpawnRequest, SpawnResult};
pub use watch::{
    agent_pane_id, agent_poll, agent_status, agent_wait_live, agent_watch_live, parse_duration,
    parse_until, poll_until, until_agent_statuses, PollEvent, PollResult, WatchTuning,
    DEFAULT_STUCK_AFTER, POLL_INTERVAL, WATCH_TERMINAL_STATUSES,
};
