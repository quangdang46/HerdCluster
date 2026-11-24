//! `herdcluster init` — run this machine as the cluster controller.
//!
//! Wraps [`cluster_node::controller::ControllerHandle::run`]: binds the
//! TLS listener + discovery, then blocks until Ctrl-C. This command IS
//! the controller daemon — there is no separate `controller` subcommand.
//! Unless `--no-skills`, it offers to provision the bundled orchestration
//! skill into the current project (consent-gated via `join_ux::confirm`,
//! or automatic with `--yes`).

use std::path::Path;

use cluster_node::controller::{ControllerConfig, ControllerHandle};

use super::{block_on, skills, vlog};

/// `herdcluster init [--name <n>] [--no-skills] [--port <p>] [--yes]`
pub fn run(
    name: Option<String>,
    no_skills: bool,
    port: Option<u16>,
    yes: bool,
    verbose: bool,
) -> i32 {
    let cfg = ControllerConfig {
        name,
        no_skills,
        port,
        // --yes doubles as unattended mode: accept pairings without
        // prompting on the controller side.
        auto_accept_pairing: yes,
        state_dir: None,
    };

    let fut = async move {
        let handle = match ControllerHandle::run(cfg).await {
            Ok(h) => h,
            Err(e) => {
                eprintln!("error: failed to start controller: {e}");
                return 1;
            }
        };

        println!(
            "herdcluster controller '{}' listening on {}",
            handle.cluster_name(),
            handle.local_addr()
        );
        println!("pairing code: {}", handle.pairing_code());
        println!("press Ctrl-C to stop");

        // Copy-paste block for the other machine — non-technical users
        // should not have to work out the LAN IP themselves.
        let port = handle.local_addr().port();
        match cluster_core::netutil::lan_ipv4() {
            Some(ip) => {
                println!();
                println!("On the OTHER machine, run:");
                println!("  herdcluster join --controller {ip}:{port}");
                println!("  (it will ask for the pairing code shown above)");
            }
            None => {
                println!();
                println!("On the OTHER machine, run:");
                println!("  herdcluster join --controller <this-machine-LAN-IP>:{port}");
                println!("  (find the LAN IP with `ipconfig getifaddr en0` on macOS or `hostname -I` on Linux)");
            }
        }

        if !no_skills {
            if let Ok(cwd) = std::env::current_dir() {
                // Skill provisioning is best-effort: never take the
                // controller down over a project-local file copy.
                let _ = skills::offer(Path::new(&cwd), yes);
            }
        }

        // The controller's services run on tasks spawned inside this
        // runtime; park here until SIGINT, then shut down cleanly.
        let _ = tokio::signal::ctrl_c().await;
        handle.shutdown();
        0
    };

    vlog(verbose, "starting controller service");
    block_on(fut).unwrap_or(1)
}
