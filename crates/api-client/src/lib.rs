//! `api-client` — minimal NDJSON client for the herdr socket API.
//!
//! Transport (verified empirically against herdr 0.9.5 on macOS):
//!
//! * Newline-delimited JSON over a local stream socket (unix socket on
//!   macOS/Linux; Windows named pipe — stubbed, see [`platform`]).
//! * Request: `{"id": "<string>", "method": "<method>", "params": {...}}`
//!   followed by `\n`. The `id` field MUST be a JSON string — integer ids
//!   are rejected with `invalid_request`.
//! * Response: exactly one JSON line per connection —
//!   `{"id": "...", "result": <value>}` on success or
//!   `{"id": "...", "error": {"code": "...", "message": "..."}}` on failure.
//! * **One request per connection.** The server closes the socket after
//!   writing the single response line (verified: a second write on the same
//!   connection fails with `EPIPE`). [`ApiClient::request`] therefore opens
//!   a fresh connection per call.
//!
//! See `docs/herdcluster/PLAN.md` §2.1 for the upstream research this is
//! based on. Typed method wrappers live in [`methods`]; the protocol
//! version guard lives in [`compat`].

pub mod compat;
pub mod methods;
pub mod platform;

use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Default per-request timeout. `agent.wait`-style calls need longer —
/// use [`ApiClient::request_with_timeout`] or the `*_with_timeout` method
/// wrappers.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum accepted size of a single herdr response line, in bytes.
///
/// NDJSON API responses are small — agent/pane metadata plus terminal
/// reads bounded by the caller's `lines` parameter — so 1 MiB is far
/// above any legitimate response while still bounding memory against a
/// hostile or broken endpoint that streams an endless "line" without
/// ever sending `\n`.
pub const MAX_RESPONSE_LINE_BYTES: usize = 1024 * 1024;

/// Errors returned by [`ApiClient`].
#[derive(Debug)]
pub enum ApiError {
    /// Underlying socket/IO failure that is not a "server not running" case.
    Io(std::io::Error),
    /// The response line could not be parsed as JSON, or had an unexpected
    /// envelope shape.
    Json(serde_json::Error),
    /// The server returned an error object: `{"error":{"code","message"}}`.
    Server { code: String, message: String },
    /// No response arrived within the per-request timeout.
    Timeout,
    /// The socket does not exist or refused the connection — the herdr
    /// server is almost certainly not running.
    ServerNotRunning { path: std::path::PathBuf },
    /// The negotiated herdr API protocol version is outside the range this
    /// build of herdcluster was validated against.
    IncompatibleProtocol { protocol: u64 },
    /// This platform/transport is not supported yet (e.g. Windows named
    /// pipe).
    Unsupported(String),
}

impl ApiError {
    /// Construct a [`ApiError::ServerNotRunning`] for a socket path.
    pub(crate) fn server_not_running(path: &std::path::Path) -> Self {
        ApiError::ServerNotRunning {
            path: path.to_path_buf(),
        }
    }

    /// True when the error means "no herdr server is listening".
    pub fn is_server_not_running(&self) -> bool {
        matches!(self, ApiError::ServerNotRunning { .. })
    }

    /// Structured error code from the server's
    /// `{"error":{"code","message"}}` envelope (e.g. `"agent_pane_busy"`),
    /// or `None` for non-server errors. Prefer this over matching on
    /// [`Display`](fmt::Display) output.
    pub fn code(&self) -> Option<&str> {
        match self {
            ApiError::Server { code, .. } => Some(code),
            _ => None,
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::Io(e) => write!(f, "herdr API I/O error: {e}"),
            ApiError::Json(e) => write!(f, "herdr API returned malformed JSON: {e}"),
            ApiError::Server { code, message } => {
                write!(f, "herdr API error [{code}]: {message}")
            }
            ApiError::Timeout => write!(f, "herdr API request timed out"),
            ApiError::ServerNotRunning { path } => write!(
                f,
                "herdr server is not running (no socket at {}). Start herdr first.",
                path.display()
            ),
            ApiError::IncompatibleProtocol { protocol } => write!(
                f,
                "herdr API protocol version {protocol} is not supported by this \
                 herdcluster build; update herdcluster"
            ),
            ApiError::Unsupported(msg) => write!(f, "unsupported: {msg}"),
        }
    }
}

impl std::error::Error for ApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ApiError::Io(e) => Some(e),
            ApiError::Json(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self {
        ApiError::Io(e)
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        ApiError::Json(e)
    }
}

/// A client for the herdr NDJSON socket API.
///
/// The client is cheap to clone-share (`&self` methods, atomic id counter)
/// and stateless between calls: each [`request`](Self::request) opens a new
/// connection because the herdr socket is one-request-per-connection.
///
/// ```no_run
/// use api_client::ApiClient;
/// let client = ApiClient::connect_default()?;
/// let agents = client.agent_list()?;
/// # Ok::<(), api_client::ApiError>(())
/// ```
#[derive(Debug)]
pub struct ApiClient {
    socket_path: std::path::PathBuf,
    next_id: AtomicU64,
}

impl ApiClient {
    /// Connect using an explicit socket path.
    ///
    /// Does not perform any handshake; use [`check_compat`](Self::check_compat)
    /// (or [`connect_checked`](Self::connect_checked)) to verify the server
    /// protocol before issuing real calls.
    pub fn connect(socket_path: impl Into<std::path::PathBuf>) -> Result<Self, ApiError> {
        let path = socket_path.into();
        // Probe once so `connect` fails fast with ServerNotRunning rather
        // than on the first request. A missing socket is the common case.
        if !path.exists() {
            return Err(ApiError::server_not_running(&path));
        }
        Ok(ApiClient {
            socket_path: path,
            next_id: AtomicU64::new(1),
        })
    }

    /// Connect using automatic socket path discovery
    /// ([`platform::socket_path`]).
    pub fn connect_default() -> Result<Self, ApiError> {
        Self::connect(platform::socket_path()?)
    }

    /// Connect and immediately run the protocol compat check
    /// ([`compat::check_compat`]).
    pub fn connect_checked(socket_path: impl Into<std::path::PathBuf>) -> Result<Self, ApiError> {
        let client = Self::connect(socket_path)?;
        compat::check_compat(&client)?;
        Ok(client)
    }

    /// Connect with automatic discovery + compat check.
    pub fn connect_default_checked() -> Result<Self, ApiError> {
        let client = Self::connect_default()?;
        compat::check_compat(&client)?;
        Ok(client)
    }

    /// The socket path this client talks to.
    pub fn socket_path(&self) -> &std::path::Path {
        &self.socket_path
    }

    /// Send one request and read its single response line.
    ///
    /// Uses [`DEFAULT_TIMEOUT`]. For long-running calls (e.g. `agent.wait`)
    /// use [`request_with_timeout`](Self::request_with_timeout).
    pub fn request(
        &self,
        method: &str,
        params: &serde_json::Value,
    ) -> Result<serde_json::Value, ApiError> {
        self.request_with_timeout(method, params, DEFAULT_TIMEOUT)
    }

    /// Send one request with an explicit per-call timeout.
    ///
    /// `timeout` is an **overall** deadline covering the write and the
    /// response read — not a per-syscall idle timeout — so `agent.wait`-style
    /// calls still work by passing a client timeout larger than
    /// `wait.timeout_ms`. Response lines are capped at
    /// [`MAX_RESPONSE_LINE_BYTES`].
    ///
    /// `params` is serialized verbatim as the request `params` field — pass
    /// `&serde_json::json!({})` (or an empty params struct) for methods that
    /// take none.
    pub fn request_with_timeout(
        &self,
        method: &str,
        params: &serde_json::Value,
        timeout: Duration,
    ) -> Result<serde_json::Value, ApiError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = serde_json::json!({
            "id": format!("hc-{id}"),
            "method": method,
            "params": params,
        });
        let mut line = serde_json::to_vec(&request)?;
        line.push(b'\n');

        // One connection per request — the server closes after one response.
        let mut stream = platform::connect(&self.socket_path, timeout)?;
        let deadline = Instant::now().checked_add(timeout);
        stream.write_all(&line)?;
        stream.flush()?;

        let response = read_response_line(&mut *stream, deadline)?;

        // Trim trailing whitespace (the line's `\n` is excluded already,
        // but tolerate `\r\n` and stray padding like `read_line` did).
        let end = response
            .iter()
            .rposition(|b| !b.is_ascii_whitespace())
            .map(|i| i + 1)
            .unwrap_or(0);
        let envelope: serde_json::Value = serde_json::from_slice(&response[..end])?;
        // Envelope: {"id": "...", "result": ...} or {"id": "...", "error": {...}}.
        // The echoed id is informational; we do not enforce matching because
        // one-conn-per-request already correlates request/response.
        if let Some(error) = envelope.get("error") {
            let code = error
                .get("code")
                .and_then(|c| c.as_str())
                .unwrap_or("unknown")
                .to_string();
            let message = error
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("herdr API error")
                .to_string();
            return Err(ApiError::Server { code, message });
        }
        envelope
            .get("result")
            .cloned()
            .ok_or_else(|| ApiError::Server {
                code: "invalid_response".to_string(),
                message: "herdr response had neither 'result' nor 'error'".to_string(),
            })
    }
}

/// Read a single NDJSON response line from `stream`.
///
/// `deadline` is an **overall** request budget, not a per-syscall idle
/// timeout: before every `read()` the stream's read timeout is shrunk to
/// the remaining time, so a peer that dribbles one byte per interval
/// cannot keep the request alive past `deadline` (the raw
/// `set_read_timeout` on the socket only bounds each individual `read()`
/// syscall — see issue #44). `None` means "no overall deadline" and falls
/// back to the per-syscall timeout set at connect time.
///
/// The line is capped at [`MAX_RESPONSE_LINE_BYTES`]; exceeding it is a
/// [`ApiError::Server`] `response_too_large` error rather than unbounded
/// memory growth.
fn read_response_line(
    stream: &mut dyn platform::IoStream,
    deadline: Option<Instant>,
) -> Result<Vec<u8>, ApiError> {
    fn too_large() -> ApiError {
        ApiError::Server {
            code: "response_too_large".to_string(),
            message: format!("herdr response line exceeded {MAX_RESPONSE_LINE_BYTES} bytes"),
        }
    }

    let mut response = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ApiError::Timeout);
            }
            stream
                .set_read_timeout(Some(remaining))
                .map_err(ApiError::Io)?;
        }
        match stream.read(&mut chunk) {
            Ok(0) => {
                if response.is_empty() {
                    return Err(ApiError::Server {
                        code: "connection_closed".to_string(),
                        message: "herdr closed the connection without a response".to_string(),
                    });
                }
                // EOF mid-line: parse what arrived (same as `read_line`).
                return Ok(response);
            }
            Ok(n) => {
                let data = &chunk[..n];
                match data.iter().position(|&b| b == b'\n') {
                    // Stop at the first newline; bytes after it belong to
                    // a next message this one-request-per-connection
                    // protocol never sends.
                    Some(pos) => {
                        if response.len() + pos > MAX_RESPONSE_LINE_BYTES {
                            return Err(too_large());
                        }
                        response.extend_from_slice(&data[..pos]);
                        return Ok(response);
                    }
                    None => {
                        if response.len() + n > MAX_RESPONSE_LINE_BYTES {
                            return Err(too_large());
                        }
                        response.extend_from_slice(data);
                    }
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                return Err(ApiError::Timeout)
            }
            Err(e) => return Err(ApiError::Io(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_error_code_only_for_server_errors() {
        let server = ApiError::Server {
            code: "agent_pane_busy".to_string(),
            message: "pane is not an available shell".to_string(),
        };
        assert_eq!(server.code(), Some("agent_pane_busy"));
        assert_eq!(ApiError::Timeout.code(), None);
        assert_eq!(
            ApiError::ServerNotRunning {
                path: "/tmp/x.sock".into()
            }
            .code(),
            None
        );
    }
}
