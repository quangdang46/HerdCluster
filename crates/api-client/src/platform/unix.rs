//! Unix socket transport (macOS / Linux).
//!
//! Verified against a real herdr 0.9.5 install on macOS:
//!
//! * The API socket is a stream unix socket, one request per connection.
//!   The server closes the connection after writing exactly one JSON
//!   response line, so every request opens a fresh connection.
//! * Socket path resolution, in priority order:
//!   1. `HERDR_SOCKET_PATH` env override — explicit, always wins, even
//!      when the configured path does not exist (a missing socket is
//!      reported by `connect` as `ServerNotRunning`; it must never fall
//!      through to a different socket and silently talk to the wrong
//!      herdr).
//!   2. `<herdr config dir>/sessions/<name>/herdr.sock` when
//!      `HERDR_SESSION` names a non-default session — also returned
//!      unconditionally, for the same reason. Observed layout: the
//!      `default` session uses `~/.config/herdr/herdr.sock` directly;
//!      non-default sessions are believed to live under `sessions/<name>/`
//!      inside the config dir — this is best-effort and unverified.
//!   3. Default: `$XDG_CONFIG_HOME/herdr/herdr.sock`, falling back to
//!      `~/.config/herdr/herdr.sock`. Confirmed on macOS 0.9.5:
//!      `~/.config/herdr/herdr.sock` (NOT `~/Library/Application Support`).
//! * `~/.config/herdr/herdr-client.sock` is the TUI client socket and does
//!   NOT speak the public NDJSON API — do not use it.

use std::env;
use std::fs;
use std::io;
use std::os::unix::fs::FileTypeExt;
use std::path::PathBuf;
use std::time::Duration;

use crate::ApiError;

/// A bidirectional byte stream returned by [`connect`].
///
/// Implemented for [`std::os::unix::net::UnixStream`]; the trait exists so
/// callers can adjust the per-read timeout while enforcing an overall
/// request deadline (see [`crate::ApiClient::request_with_timeout`]).
pub trait IoStream: std::io::Read + std::io::Write + Send {
    /// Adjust the per-read timeout on the underlying stream.
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()>;
}

impl IoStream for std::os::unix::net::UnixStream {
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        std::os::unix::net::UnixStream::set_read_timeout(self, timeout)
    }
}

/// Resolve the herdr API socket path.
///
/// An explicit override — `HERDR_SOCKET_PATH`, or a non-default
/// `HERDR_SESSION` — always wins, even when the configured socket does
/// not exist: probing for a fallback could silently select a *different*
/// herdr (e.g. a stale `HERDR_SOCKET_PATH` falling through to the
/// default-session socket). A missing configured socket is instead
/// reported by `connect` as `ServerNotRunning` naming that path.
///
/// With no explicit env config, the first candidate that exists on disk
/// is returned; if none exist, the default (non-session) path is
/// returned so callers get a stable, meaningful `ServerNotRunning`
/// error on connect.
pub fn socket_path() -> Result<PathBuf, ApiError> {
    let candidates = socket_path_candidates();
    // `socket_path_candidates` prepends an explicit env-configured path
    // (HERDR_SOCKET_PATH, or the session-specific path for a non-default
    // HERDR_SESSION) ahead of the default; more than one candidate means
    // such an override is in effect.
    if candidates.len() > 1 {
        return Ok(candidates.into_iter().next().unwrap());
    }
    for path in &candidates {
        if path.exists() {
            return Ok(path.clone());
        }
    }
    // Nothing exists yet — return the default candidate so the error
    // message names a stable socket path.
    Ok(candidates
        .into_iter()
        .next()
        .unwrap_or_else(default_socket_path))
}

/// Ordered candidate socket paths, highest priority first.
pub fn socket_path_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(explicit) = env::var("HERDR_SOCKET_PATH") {
        if !explicit.is_empty() {
            candidates.push(PathBuf::from(explicit));
        }
    }

    let config_dir = herdr_config_dir();
    if let Ok(session) = env::var("HERDR_SESSION") {
        if !session.is_empty() && session != "default" {
            // Best-effort guess for non-default session layout; verified
            // only for the `default` session on herdr 0.9.5.
            candidates.push(
                config_dir
                    .join("sessions")
                    .join(&session)
                    .join("herdr.sock"),
            );
        }
    }

    candidates.push(config_dir.join("herdr.sock"));
    candidates
}

/// herdr config/state directory.
///
/// `$XDG_CONFIG_HOME/herdr` when set, else `~/.config/herdr`. Verified on
/// macOS: herdr uses the XDG-style dir even though it is not under
/// `~/Library/Application Support`.
fn herdr_config_dir() -> PathBuf {
    if let Ok(xdg) = env::var("XDG_CONFIG_HOME") {
        if !xdg.is_empty() {
            return PathBuf::from(xdg).join("herdr");
        }
    }
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config")
        .join("herdr")
}

fn default_socket_path() -> PathBuf {
    herdr_config_dir().join("herdr.sock")
}

/// Connect to the herdr API socket at `path`.
///
/// `timeout` is applied to both reads and writes on the returned stream so
/// a hung server cannot block a request forever.
pub fn connect(path: &std::path::Path, timeout: Duration) -> Result<Box<dyn IoStream>, ApiError> {
    let stream = std::os::unix::net::UnixStream::connect(path).map_err(|e| match e.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused => {
            ApiError::server_not_running(path)
        }
        _ => ApiError::Io(e),
    })?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(ApiError::Io)?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(ApiError::Io)?;
    Ok(Box::new(stream))
}

/// Check whether a socket file exists (used by tests / callers that want a
/// cheap liveness probe without connecting).
pub fn socket_exists(path: &std::path::Path) -> bool {
    fs::metadata(path)
        .map(|m| m.file_type().is_socket())
        .unwrap_or(false)
        || path.exists()
}
