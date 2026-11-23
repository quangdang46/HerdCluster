//! `herdcluster autostart install|uninstall|status` — worker daemon
//! autostart management.
//!
//! Thin wrapper over [`cluster_node::autostart`]: the service owns the
//! platform mechanism (launchd on macOS, systemd user unit on Linux,
//! etc.); this module only renders the result.

use cluster_node::autostart::{self, AutostartStatus};

use crate::cli::AutostartCommands;

/// `herdcluster autostart <install|uninstall|status>`
pub fn run(command: AutostartCommands) -> i32 {
    match command {
        AutostartCommands::Install => match autostart::install_autostart() {
            Ok(path) => {
                println!("autostart installed");
                println!("  entry: {}", path.display());
                0
            }
            Err(e) => {
                eprintln!("error: autostart install failed: {e}");
                1
            }
        },
        AutostartCommands::Uninstall => match autostart::uninstall_autostart() {
            Ok(()) => {
                println!("autostart removed");
                0
            }
            Err(e) => {
                eprintln!("error: autostart uninstall failed: {e}");
                1
            }
        },
        AutostartCommands::Status => match autostart::autostart_status() {
            AutostartStatus::Installed { path } => {
                println!("autostart: installed");
                println!("  entry: {}", path.display());
                0
            }
            AutostartStatus::NotInstalled => {
                println!("autostart: not installed");
                0
            }
            AutostartStatus::Unsupported { reason } => {
                println!("autostart: unsupported on this platform");
                println!("  reason: {reason}");
                0
            }
        },
    }
}
