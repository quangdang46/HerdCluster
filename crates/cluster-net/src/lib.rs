//! cluster-net — TLS transport, stream multiplexing, connection
//! management, and LAN discovery for HerdCluster.
//!
//! See `docs/herdcluster/PLAN.md` §3.3–3.5. This crate contains **no**
//! herdr-specific logic: it carries opaque byte streams plus
//! `cluster_core::protocol::Message` control frames. Bridging those
//! streams to the local herdr NDJSON API socket lives in `cluster-bridge`.
//!
//! - [`tls`] — cluster CA + server cert lifecycle, TLS 1.3 configs,
//!   post-handshake Ed25519 mutual-auth challenge.
//! - [`mux`] — length-prefixed stream multiplexer (stream 0 = control,
//!   1..=63 = API byte streams; 64-stream cap, 1 MiB/stream cap,
//!   FIN/RST semantics, backpressure).
//! - [`conn`] — [`conn::Session`] abstraction, worker dial-out with
//!   heartbeat/degraded/offline tracking and jittered reconnect,
//!   controller accept side with per-worker sessions.
//! - [`discovery`] — mDNS `_herdcluster._tcp.local.` advertise/browse
//!   plus UDP broadcast fallback on port 47820, manual `--controller`
//!   override resolution.

pub mod conn;
pub mod discovery;
pub mod mux;
pub mod tls;
