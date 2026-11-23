//! Platform-specific bits for `api-client`.
//!
//! All OS-specific behavior lives behind this module so the core NDJSON
//! client stays free of `#[cfg(target_os)]`.

#[cfg(not(unix))]
mod stub;
#[cfg(unix)]
mod unix;

#[cfg(not(unix))]
pub use stub::*;
#[cfg(unix)]
pub use unix::*;
