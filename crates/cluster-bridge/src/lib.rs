//! cluster-bridge — byte-pipe bridge between local herdr API unix sockets
//! and `cluster-net` mux streams.
//!
//! See `docs/herdcluster/PLAN.md` §3.3. This crate contains **no** herdr
//! protocol logic: it moves raw bytes between two endpoints that both speak
//! the herdr NDJSON API framing (one JSON request line → one JSON response
//! line → close, verified against herdr 0.9.5).
//!
//! # Architecture
//!
//! ```text
//! controller machine                     worker machine
//! ┌──────────────────────┐   cluster    ┌──────────────────────┐
//! │ ApiClient ──► bridge │══ TLS/mux ══►│ bridge ──► herdr API │
//! │   unix socket        │   stream     │   unix socket        │
//! └──────────────────────┘              └──────────────────────┘
//! ```
//!
//! - [`controller::ControllerBridge`] runs on the controller, once per
//!   paired worker. It binds a private unix listener; every accepted
//!   connection opens one mux stream on the worker's [`Session`] and is
//!   piped bidirectionally.
//! - [`worker::WorkerBridge`] runs on the worker. It accepts peer-opened
//!   mux streams and pipes each to the *local* herdr API socket discovered
//!   via [`api_client::platform::socket_path`].
//!
//! Because the herdr API is one-request-per-connection, every
//! `ApiClient::request()` produces: new local connection → new accepted
//! socket → new mux stream → new herdr socket connection. No connection
//! reuse is assumed anywhere in the pipe.
//!
//! # Bridge socket convention
//!
//! For a worker with registry label `L`, the controller-side bridge listens
//! on
//!
//! ```text
//! <state_dir>/bridge/<sanitize(L)>.sock
//! ```
//!
//! with the bridge directory mode `0700` and socket mode `0600`. This
//! mirrors `cluster-fleet`'s endpoint convention
//! (`crates/cluster-fleet/src/endpoint.rs`); [`paths`] carries a
//! deliberate copy of `sanitize_label`/`bridge_socket_path` so this crate
//! does not depend on the fleet layer. `api_client::ApiClient::connect(path)`
//! works against the socket unchanged.
//!
//! # Failure propagation
//!
//! Two failure classes are distinguished:
//!
//! - **Stream cannot be opened** (worker offline / session dead): the
//!   controller bridge accepts the local connection, fails
//!   `Session::open_stream()`, and answers with a well-formed NDJSON error
//!   envelope — `{"id":"cluster-bridge","error":{"code":"worker_unreachable",...}}` —
//!   before closing. `ApiClient` surfaces this as
//!   [`api_client::ApiError::Server`] with `code == "worker_unreachable"`,
//!   which is strictly more informative than a bare connection drop (the
//!   client would otherwise see `connection_closed` or an I/O error).
//! - **Mid-stream failure or missing local herdr socket**: the failing side
//!   aborts/closes its end; the surviving side sees EOF or a mux error and
//!   shuts its socket write half down, so the client always observes a clean
//!   EOF rather than a hang. On the worker, an unreachable herdr API socket
//!   additionally emits an NDJSON error envelope
//!   (`code == "herdr_unavailable"`) into the stream before closing, giving
//!   the caller a typed reason.
//!
//! Abandoned streams are reaped by an idle timeout (no bytes in either
//! direction for [`controller::DEFAULT_IDLE_TIMEOUT`] / the configured
//! worker timeout → mux `RST` + socket close). Stale bridge socket files
//! left by a crashed process are swept at bind time by a connect probe:
//! a live listener is never stolen (`BridgeError::SocketInUse`), a refused
//! connect means the file is stale and is unlinked.
//!
//! # Session lifetime
//!
//! A bridge is bound to **one** [`cluster_net::conn::Session`]. On worker
//! reconnect the old session (and every stream on it) is dead; callers must
//! drop the bridge and construct a new one from the replacement session.

#[cfg(unix)]
mod paths;
#[cfg(unix)]
mod pipe;

#[cfg(unix)]
pub mod controller;
#[cfg(unix)]
pub mod worker;

#[cfg(unix)]
pub use paths::{bridge_socket_path, sanitize_label, BRIDGE_DIR_NAME};

use std::path::PathBuf;

/// Errors from bridge setup and socket management.
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    /// Underlying I/O error (bind, accept, chmod, stale-socket probe).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Cluster session error while resolving the herdr socket or during
    /// bridge construction.
    #[error("cluster session error: {0}")]
    Session(#[from] cluster_net::conn::ConnError),
    /// A live process already listens on the bridge socket path — binding
    /// would steal its endpoint, so this is refused.
    #[error("bridge socket already in use: {0}")]
    SocketInUse(PathBuf),
    /// The local herdr API socket path could not be discovered.
    #[error("herdr API socket discovery failed: {0}")]
    Discovery(String),
}

/// Build the NDJSON error envelope the bridge writes when it must fail a
/// request *before* any byte piping starts. The `id` is a fixed sentinel —
/// `api-client` does not enforce id matching (one connection per request
/// already correlates), so any well-formed envelope works.
#[cfg(unix)]
pub(crate) fn ndjson_error_line(code: &str, message: &str) -> Vec<u8> {
    let envelope = serde_json::json!({
        "id": "cluster-bridge",
        "error": { "code": code, "message": message },
    });
    let mut line = serde_json::to_vec(&envelope).unwrap_or_else(|_| {
        b"{\"id\":\"cluster-bridge\",\"error\":{\"code\":\"bridge_error\",\"message\":\"bridge failure\"}}"
            .to_vec()
    });
    line.push(b'\n');
    line
}
