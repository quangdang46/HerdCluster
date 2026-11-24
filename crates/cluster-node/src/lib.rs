//! cluster-node — HerdCluster daemons and services.
//!
//! Owns the long-running roles: the controller service (`init`), the worker
//! daemon (`join`/`worker`), the herdr runtime supervisor (release-binary
//! install + server lifecycle), the resource monitor, and `doctor`.
//!
//! Module ownership is split across implementation beads; `lib.rs` declares
//! the full module surface up front so parallel work stays conflict-free.

pub mod autostart;
pub mod cluster_config;
pub mod controller;
pub mod doctor;
pub mod join_ux;
pub mod keepawake;
pub mod project_state;
pub mod provisioning;
pub mod resources;
pub mod revoke;
pub mod runtime;
pub mod scheduler_wire;
pub mod skills;
pub mod worker;
