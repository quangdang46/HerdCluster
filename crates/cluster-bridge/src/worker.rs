//! Worker side of the bridge: cluster stream ↔ local herdr API socket.
//!
//! [`WorkerBridge`] runs on the worker host. It accepts peer-opened mux
//! streams on the worker's [`Session`] (`Session::accept_stream`) and pipes
//! each to the local herdr API socket — the same NDJSON endpoint a local
//! `ApiClient` would use, discovered via
//! [`api_client::platform::socket_path`] (honors `HERDR_SOCKET_PATH` and
//! `HERDR_SESSION`).
//!
//! If the herdr API socket is missing or refuses the connection, the
//! stream is answered with an NDJSON error envelope
//! (`code: "herdr_unavailable"`) and closed, so the controller-side client
//! sees a typed failure instead of a silent hang. Mid-stream failures rely
//! on the shared pipe's EOF propagation.
//!
//! Like the controller side, the bridge is bound to one [`Session`]; on
//! reconnect, rebuild it from the new session.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cluster_net::conn::Session;
use cluster_net::mux::StreamHandle;
use tokio::net::UnixStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::pipe::pipe_unix_stream;
use crate::{ndjson_error_line, BridgeError};

/// Default idle timeout for worker-side streams (same rationale as the
/// controller side — see [`crate::controller::DEFAULT_IDLE_TIMEOUT`]).
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
/// Deadline for connecting to the local herdr API socket per stream.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Worker-side bridge: accepted cluster streams → local herdr API socket.
pub struct WorkerBridge {
    herdr_socket: PathBuf,
    shutdown_tx: watch::Sender<bool>,
    accept_task: JoinHandle<()>,
}

impl WorkerBridge {
    /// Resolve the local herdr API socket path via
    /// [`api_client::platform::socket_path`] and start the accept loop on
    /// `session`.
    pub fn new(session: Arc<Session>) -> Result<Self, BridgeError> {
        let path = api_client::platform::socket_path()
            .map_err(|e| BridgeError::Discovery(e.to_string()))?;
        Ok(Self::with_herdr_socket(session, path))
    }

    /// Like [`WorkerBridge::new`] but with an explicit herdr socket path —
    /// used by tests and by callers that already resolved discovery.
    pub fn with_herdr_socket(session: Arc<Session>, herdr_socket: PathBuf) -> Self {
        Self::with_options(
            session,
            herdr_socket,
            DEFAULT_IDLE_TIMEOUT,
            DEFAULT_CONNECT_TIMEOUT,
        )
    }

    /// Full knob set (tests): custom idle + connect timeouts.
    pub fn with_options(
        session: Arc<Session>,
        herdr_socket: PathBuf,
        idle_timeout: Duration,
        connect_timeout: Duration,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let accept_task = tokio::spawn(accept_loop(
            session,
            herdr_socket.clone(),
            idle_timeout,
            connect_timeout,
            shutdown_rx,
        ));
        Self {
            herdr_socket,
            shutdown_tx,
            accept_task,
        }
    }

    /// The local herdr API socket this bridge proxies to.
    pub fn herdr_socket(&self) -> &std::path::Path {
        &self.herdr_socket
    }

    /// Stop accepting new streams. In-flight stream pipes are left to
    /// finish or hit their idle timeout — they are independent tasks.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        self.accept_task.abort();
    }
}

impl Drop for WorkerBridge {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Accept loop: one pipe task per peer-opened stream. Exits on shutdown or
/// when the session dies (`accept_stream` → `WorkerUnreachable`).
async fn accept_loop(
    session: Arc<Session>,
    herdr_socket: PathBuf,
    idle_timeout: Duration,
    connect_timeout: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        let stream = tokio::select! {
            _ = shutdown_rx.changed() => break,
            res = session.accept_stream() => match res {
                Ok(s) => s,
                // Session dead — nothing more can arrive; exit so the
                // caller can rebuild the bridge on the new session.
                Err(_) => break,
            },
        };

        let sock_path = herdr_socket.clone();
        tokio::spawn(async move {
            handle_stream(stream, sock_path, idle_timeout, connect_timeout).await;
        });
    }
}

/// Connect one accepted stream to the local herdr API socket and pipe.
async fn handle_stream(
    stream: StreamHandle,
    herdr_socket: PathBuf,
    idle_timeout: Duration,
    connect_timeout: Duration,
) {
    let sock = match timeout(connect_timeout, UnixStream::connect(&herdr_socket)).await {
        Ok(Ok(sock)) => sock,
        Ok(Err(e)) => {
            fail_stream(
                &stream,
                "herdr_unavailable",
                &format!(
                    "cannot connect to herdr API socket {}: {e}",
                    herdr_socket.display()
                ),
            )
            .await;
            return;
        }
        Err(_) => {
            fail_stream(
                &stream,
                "herdr_unavailable",
                &format!(
                    "timed out connecting to herdr API socket {}",
                    herdr_socket.display()
                ),
            )
            .await;
            return;
        }
    };
    pipe_unix_stream(sock, stream, idle_timeout).await;
}

/// Send a typed NDJSON error envelope into the stream, then FIN. The
/// controller-side pipe forwards the bytes verbatim to the waiting
/// `ApiClient`, which parses them as `ApiError::Server{code}`.
async fn fail_stream(stream: &StreamHandle, code: &str, message: &str) {
    let line = ndjson_error_line(code, message);
    if stream.send(Bytes::from(line)).await.is_ok() {
        // close() queues FIN *after* the data frame — ordering is FIFO, so
        // the client always reads the error line before EOF.
        let _ = stream.close();
    } else {
        stream.abort();
    }
}
