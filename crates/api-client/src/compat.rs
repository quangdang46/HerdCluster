//! Protocol compatibility guard.
//!
//! herdr's socket API is versioned by an integer `protocol` field returned
//! by the `ping` handshake method (verified on herdr 0.9.5: `ping` →
//! `{"type":"pong","version":"0.9.5","protocol":22,"capabilities":{...}}`).
//!
//! Note on naming drift: earlier herdr revisions exposed `status.server`
//! and `api.snapshot`; in 0.9.5 the handshake is `ping` and the snapshot is
//! `session.snapshot`. This crate tracks the live schema — see
//! [`methods::api_snapshot`] and [`methods::ping`].
//!
//! Policy: herdcluster pins the protocol range it was validated against.
//! A server outside the range produces
//! [`ApiError::IncompatibleProtocol`] — never silent wrong behavior.
//! Widen the range only after validating the new protocol (see
//! `docs/herdcluster/PLAN.md` §3, ADR-005).

use serde::Deserialize;

use crate::{ApiClient, ApiError};

/// Oldest herdr API protocol this build supports.
///
/// Tested against herdr **0.9.5 stable** (protocol 22, API `schema_version`
/// 1 per `herdr api schema`).
pub const MIN_SUPPORTED_PROTOCOL: u64 = 22;

/// Newest herdr API protocol this build supports.
pub const MAX_SUPPORTED_PROTOCOL: u64 = 22;

/// The herdr version this compat range was validated against.
pub const TESTED_HERDR_VERSION: &str = "0.9.5";

/// Server identity reported by the `ping` handshake.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerInfo {
    /// herdr server version string, e.g. `"0.9.5"`.
    #[serde(default)]
    pub version: Option<String>,
    /// Wire protocol version — the compat key.
    pub protocol: u64,
}

/// Call `ping` and verify the server protocol is in the supported range.
///
/// Returns the parsed [`ServerInfo`] on success.
pub fn check_compat(client: &ApiClient) -> Result<ServerInfo, ApiError> {
    let result = client.request("ping", &serde_json::json!({}))?;
    let info: ServerInfo = serde_json::from_value(result)?;
    if !(MIN_SUPPORTED_PROTOCOL..=MAX_SUPPORTED_PROTOCOL).contains(&info.protocol) {
        return Err(ApiError::IncompatibleProtocol {
            protocol: info.protocol,
        });
    }
    Ok(info)
}
