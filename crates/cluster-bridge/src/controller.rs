//! Controller side of the bridge: one private unix listener per worker.
//!
//! [`ControllerBridge`] binds `<state_dir>/bridge/<sanitize(label)>.sock`
//! (see [`crate::paths`]) and accepts local [`api_client::ApiClient`]
//! connections. Each accepted connection opens one mux stream on the
//! worker's [`Session`] via `Session::open_stream()` and is piped
//! bidirectionally by [`crate::pipe::pipe_unix_stream`].
//!
//! When the worker is unreachable (`open_stream` fails), the connection is
//! answered with an NDJSON error envelope (`code: "worker_unreachable"`)
//! instead of a bare drop — see the crate docs for the rationale.
//!
//! The bridge is bound to a single [`Session`]. On worker reconnect, drop
//! this bridge and construct a new one from the replacement session; the
//! stale-socket sweep at bind time makes rebinding the same label safe.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cluster_net::conn::Session;
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::paths;
use crate::pipe::pipe_unix_stream;
use crate::{ndjson_error_line, BridgeError};

/// Default per-stream idle timeout: no bytes in either direction for this
/// long → the stream is aborted and the socket closed. Matches the "idle
/// timeout for abandoned streams" requirement; herdr API calls are
/// request/response so 5 minutes of silence means the caller went away.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Controller-side bridge for one worker: unix listener ↔ cluster streams.
pub struct ControllerBridge {
    socket_path: PathBuf,
    idle_timeout: Duration,
    shutdown_tx: watch::Sender<bool>,
    accept_task: JoinHandle<()>,
}

impl ControllerBridge {
    /// Bind the bridge socket for `worker_label` under `state_dir` and
    /// start the accept loop on `session`.
    ///
    /// Stale socket sweep: if the path exists, a connect probe decides —
    /// refused ⇒ stale file, unlink and rebind; accepted ⇒ a live bridge
    /// already owns it, [`BridgeError::SocketInUse`].
    pub async fn new(
        state_dir: &Path,
        worker_label: &str,
        session: Arc<Session>,
    ) -> Result<Self, BridgeError> {
        Self::with_idle_timeout(state_dir, worker_label, session, DEFAULT_IDLE_TIMEOUT).await
    }

    /// Like [`ControllerBridge::new`] with a custom idle timeout (tests).
    pub async fn with_idle_timeout(
        state_dir: &Path,
        worker_label: &str,
        session: Arc<Session>,
        idle_timeout: Duration,
    ) -> Result<Self, BridgeError> {
        let (listener, socket_path) = bind_bridge_socket(state_dir, worker_label).await?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let accept_task = tokio::spawn(accept_loop(listener, session, idle_timeout, shutdown_rx));
        Ok(Self {
            socket_path,
            idle_timeout,
            shutdown_tx,
            accept_task,
        })
    }

    /// The unix socket path local clients connect to — the fleet convention
    /// `<state_dir>/bridge/<sanitize(label)>.sock`.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// The configured per-stream idle timeout.
    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// Stop accepting, close the listener, and remove the socket file.
    /// Per-connection pipe tasks are *not* awaited — they observe socket
    /// closure / stream death and exit on their own (bounded by the idle
    /// timeout at worst).
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        // Aborting the accept task drops the listener inside it, closing
        // the socket; removing the filesystem entry makes
        // `ApiClient::connect`'s path-exists probe fail fast with
        // ServerNotRunning.
        self.accept_task.abort();
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

impl Drop for ControllerBridge {
    fn drop(&mut self) {
        // Best-effort cleanup if the caller never called shutdown(): abort
        // the accept task (dropping the listener inside it closes the
        // socket) and unlink the path so no stale file survives.
        self.accept_task.abort();
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

/// Bound on the stale-socket connect probe. A connect to a healthy unix
/// listener completes (or is refused) essentially instantly; hitting this
/// timeout means the listener's accept backlog is saturated — which only
/// happens when a *live* bridge owns the path — so a probe timeout maps to
/// [`BridgeError::SocketInUse`], never "stale".
const SOCKET_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Create `<state_dir>/bridge/` (0700), sweep a stale socket at `path`,
/// bind the listener, and chmod the socket file to 0600.
///
/// The connect probe is async + timeout-bounded: a blocking `connect(2)`
/// on a live-but-backlogged unix listener could otherwise stall a tokio
/// worker thread indefinitely (#102). The remaining fs ops (`create_dir_all`,
/// `bind`, `chmod`, `remove_file`) are O(µs) local syscalls and stay
/// synchronous.
async fn bind_bridge_socket(
    state_dir: &Path,
    worker_label: &str,
) -> Result<(UnixListener, PathBuf), BridgeError> {
    let dir = state_dir.join(paths::BRIDGE_DIR_NAME);
    std::fs::create_dir_all(&dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    }

    let socket_path = paths::bridge_socket_path(state_dir, worker_label);

    // Stale-socket sweep: a connect probe distinguishes "live listener"
    // (refuse to steal) from "dead file" (unlink and rebind).
    if socket_path.exists() || socket_path.symlink_metadata().is_ok() {
        match timeout(SOCKET_PROBE_TIMEOUT, UnixStream::connect(&socket_path)).await {
            // Connected — or the connect stalled past the timeout because
            // the owner's accept backlog is full: either way a live bridge
            // owns this path, refuse to steal it.
            Ok(Ok(_)) | Err(_) => return Err(BridgeError::SocketInUse(socket_path)),
            // Refused (or other connect error): stale file, unlink and rebind.
            Ok(Err(_)) => {
                std::fs::remove_file(&socket_path)?;
            }
        }
    }

    let listener = UnixListener::bind(&socket_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // unix socket bind honors umask; force 0600 regardless.
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok((listener, socket_path))
}

/// First retry delay after a transient `accept()` failure; doubled per
/// consecutive failure up to [`ACCEPT_RETRY_BACKOFF_MAX`].
const ACCEPT_RETRY_BACKOFF: Duration = Duration::from_millis(50);

/// Cap for the accept-retry backoff.
const ACCEPT_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// Whether an `accept()` error is transient and the loop should retry
/// rather than exit. `accept(2)` fails spuriously under fd pressure
/// (EMFILE/ENFILE), kernel buffer exhaustion (ENOBUFS/ENOMEM), or when a
/// client connects then resets before the accept completes
/// (ECONNABORTED); none of these mean the listener is dead.
fn is_retryable_accept_error(e: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    match e.kind() {
        ErrorKind::ConnectionAborted | ErrorKind::Interrupted | ErrorKind::WouldBlock => true,
        // std has no stable ErrorKind for fd/buffer exhaustion, so match
        // the raw errno (unix-only crate).
        _ => matches!(
            e.raw_os_error(),
            Some(libc::EMFILE) | Some(libc::ENFILE) | Some(libc::ENOBUFS) | Some(libc::ENOMEM)
        ),
    }
}

/// Accept loop: one spawned pipe task per local connection. Exits on
/// shutdown signal or a *fatal* listener error; transient `accept()`
/// failures (fd pressure, aborted connects) are retried with backoff so
/// the bridge does not die silently while its socket file remains.
async fn accept_loop(
    listener: UnixListener,
    session: Arc<Session>,
    idle_timeout: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut consecutive_errors: u32 = 0;
    loop {
        let accepted = tokio::select! {
            _ = shutdown_rx.changed() => break,
            res = listener.accept() => res,
        };
        let (sock, _addr) = match accepted {
            Ok(v) => {
                consecutive_errors = 0;
                v
            }
            Err(e) if is_retryable_accept_error(&e) => {
                consecutive_errors += 1;
                // Log the first failure and then every 100th — a persistently
                // exhausted fd table must not spam the log every 50ms.
                if consecutive_errors == 1 || consecutive_errors.is_multiple_of(100) {
                    tracing::warn!(
                        "bridge accept transient error #{consecutive_errors}: {e}; retrying"
                    );
                }
                // Exponential backoff, capped: 50ms → 1s.
                let backoff = ACCEPT_RETRY_BACKOFF
                    .saturating_mul(1u32 << consecutive_errors.min(5))
                    .min(ACCEPT_RETRY_BACKOFF_MAX);
                tokio::time::sleep(backoff).await;
                continue;
            }
            Err(e) => {
                tracing::error!("bridge accept loop exiting on fatal listener error: {e}");
                break;
            }
        };

        let session = session.clone();
        tokio::spawn(async move {
            // One mux stream per local connection — matches the herdr
            // one-request-per-connection model.
            let stream = match session.open_stream() {
                Ok(s) => s,
                Err(e) => {
                    // Worker unreachable / session dead: answer with a typed
                    // NDJSON error instead of a bare close so the caller gets
                    // ApiError::Server{code:"worker_unreachable"} rather than
                    // an opaque connection_closed.
                    let line = ndjson_error_line(
                        "worker_unreachable",
                        &format!("cannot open API stream to worker: {e}"),
                    );
                    let mut sock = sock;
                    let _ = sock.write_all(&line).await;
                    let _ = sock.shutdown().await;
                    return;
                }
            };
            pipe_unix_stream(sock, stream, idle_timeout).await;
        });
    }
    // Listener drops here → socket closed; shutdown() removes the file.
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #68: transient accept() failures must be retried, fatal ones fatal.
    #[test]
    fn accept_error_classification() {
        use std::io::ErrorKind;

        for kind in [
            ErrorKind::ConnectionAborted,
            ErrorKind::Interrupted,
            ErrorKind::WouldBlock,
        ] {
            assert!(
                is_retryable_accept_error(&std::io::Error::new(kind, "x")),
                "{kind:?} should be retryable"
            );
        }
        for errno in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM] {
            assert!(
                is_retryable_accept_error(&std::io::Error::from_raw_os_error(errno)),
                "errno {errno} should be retryable"
            );
        }
        for kind in [
            ErrorKind::PermissionDenied,
            ErrorKind::AddrInUse,
            ErrorKind::InvalidInput,
            ErrorKind::ConnectionRefused,
        ] {
            let e = std::io::Error::new(kind, "x");
            assert!(!is_retryable_accept_error(&e), "{kind:?} should be fatal");
        }
    }
}
