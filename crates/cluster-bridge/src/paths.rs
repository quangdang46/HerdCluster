//! Bridge socket path convention — mirrors `cluster-fleet`'s endpoint layer.
//!
//! The functions below are a deliberate copy of
//! `crates/cluster-fleet/src/endpoint.rs` (`sanitize_label`,
//! `bridge_socket_path`, `BRIDGE_DIR_NAME`, `BRIDGE_SOCKET_SUFFIX`). They
//! MUST stay byte-for-byte compatible: the fleet builds
//! `ApiClient::connect(<state_dir>/bridge/<label>.sock)` paths from its own
//! copy, and this crate binds the listener at the same path. If the
//! convention ever changes, change both crates together (or move the helper
//! into `cluster-core` and re-export).

use std::path::{Path, PathBuf};

/// Directory (under the herdcluster state dir) where the controller-side
/// bridge exposes one unix socket per paired worker.
pub const BRIDGE_DIR_NAME: &str = "bridge";
/// File extension for bridge sockets.
pub const BRIDGE_SOCKET_SUFFIX: &str = ".sock";

/// Sanitize a worker label into a filesystem-safe bridge socket basename.
///
/// Identical to `cluster_fleet::endpoint::sanitize_label`: keeps
/// `[A-Za-z0-9._-]`, maps everything else to `_`, trims leading/trailing
/// dots, dashes, and underscores, and falls back to `"worker"` if nothing
/// usable remains.
pub fn sanitize_label(label: &str) -> String {
    let mut out: String = label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    while out.starts_with('.') || out.starts_with('-') || out.starts_with('_') {
        out.remove(0);
    }
    while out.ends_with('.') || out.ends_with('-') || out.ends_with('_') {
        out.pop();
    }
    if out.is_empty() {
        "worker".to_string()
    } else {
        out
    }
}

/// The bridge socket path for a worker label:
/// `<state_dir>/bridge/<sanitize(label)>.sock`.
pub fn bridge_socket_path(state_dir: &Path, label: &str) -> PathBuf {
    state_dir
        .join(BRIDGE_DIR_NAME)
        .join(format!("{}{BRIDGE_SOCKET_SUFFIX}", sanitize_label(label)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keep this in lockstep with `cluster_fleet::endpoint` tests — same
    /// inputs, same expected outputs.
    #[test]
    fn sanitize_label_matches_fleet_convention() {
        assert_eq!(sanitize_label("desktop-win"), "desktop-win");
        assert_eq!(sanitize_label("wsl2 box!"), "wsl2_box");
        assert_eq!(sanitize_label("..weird.."), "weird");
        assert_eq!(sanitize_label("!!!"), "worker");
        assert_eq!(sanitize_label("a/b\\c"), "a_b_c");
    }

    #[test]
    fn bridge_socket_path_convention() {
        let dir = Path::new("/tmp/hc-state");
        assert_eq!(
            bridge_socket_path(dir, "desktop-win"),
            dir.join("bridge").join("desktop-win.sock")
        );
        assert_eq!(
            bridge_socket_path(dir, "wsl 2"),
            dir.join("bridge").join("wsl_2.sock")
        );
    }
}
