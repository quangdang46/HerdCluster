//! Non-unix stub: Windows named-pipe transport is not implemented yet.
//!
//! herdr on Windows exposes the API over a named pipe rather than a unix
//! socket. MVP targets WSL2/Linux workers, so this stub returns a clear
//! `ApiError::Unsupported` until named-pipe support lands.

use std::path::Path;
use std::time::Duration;

use crate::ApiError;

/// A bidirectional byte stream returned by [`connect`].
pub trait IoStream: std::io::Read + std::io::Write + Send {
    /// Adjust the per-read timeout on the underlying stream.
    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> std::io::Result<()>;
}

/// Resolve the herdr API endpoint.
///
/// Not implemented on this platform yet.
pub fn socket_path() -> Result<std::path::PathBuf, ApiError> {
    Err(ApiError::Unsupported(
        "herdr API socket discovery is only implemented on unix (macOS/Linux); \
         Windows named-pipe support is pending"
            .to_string(),
    ))
}

/// Ordered candidate endpoint paths — empty on unsupported platforms.
pub fn socket_path_candidates() -> Vec<std::path::PathBuf> {
    Vec::new()
}

/// Connect to the herdr API endpoint — always unsupported on this platform.
pub fn connect(_path: &Path, _timeout: Duration) -> Result<Box<dyn IoStream>, ApiError> {
    Err(ApiError::Unsupported(
        "herdr API transport is only implemented on unix (macOS/Linux); \
         Windows named-pipe support is pending"
            .to_string(),
    ))
}

/// Always `false` on unsupported platforms.
pub fn socket_exists(_path: &Path) -> bool {
    false
}
