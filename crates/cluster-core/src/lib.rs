//! cluster-core — pure cluster logic for HerdCluster.
//!
//! No networking, no tokio, no async. Modules:
//!
//! - [`identity`] — Ed25519 keypairs, fingerprints, challenge signing,
//!   `0600` atomic key-file persistence.
//! - [`pairing`] — one-time-code pairing FSM (controller + worker sides)
//!   with injected clock and RNG.
//! - [`protocol`] — length-prefixed JSON wire codec (`version`-enveloped,
//!   16 MiB frame cap).
//! - [`registry`] — `cluster.json` / `workers.json` schemas, atomic
//!   persistence, corrupt-file quarantine, worker status FSM.
//! - [`config`] — state dir resolution and path accessors (the single place
//!   other crates get paths from).
//! - [`scheduler`] — deterministic `--machine auto` placement scoring.
//!
//! See `docs/herdcluster/PLAN.md` §3.3–3.7 for the architecture this
//! implements.

mod fsutil;

pub mod config;
pub mod identity;
pub mod pairing;
pub mod protocol;
pub mod registry;
pub mod scheduler;
