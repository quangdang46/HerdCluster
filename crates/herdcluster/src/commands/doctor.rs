//! `herdcluster doctor` — minimal real diagnostics for now.
//!
//! The full check suite (discovery, TLS, pairing, per-kind CLI presence,
//! repo paths, disk/mem — PLAN.md §3.9, milestone M7) lands later. What
//! runs today is genuinely useful: cluster config load, local herdr API
//! ping, and per-machine reachability from the fleet `machines` view.
//! `--fix` is accepted for forward compatibility and reports that
//! remediation is not implemented yet.

use api_client::ApiClient;
use cluster_fleet::{machines, FleetContext};
use serde::Serialize;

use super::{print_json, vlog};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckStatus {
    Ok,
    Warn,
    Fail,
}

#[derive(Debug, Serialize)]
struct Check {
    name: String,
    status: CheckStatus,
    detail: String,
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    ok: bool,
    checks: Vec<Check>,
}

/// `herdcluster doctor [--fix] [--machine <label>] [--json]`
pub fn run(fix: bool, machine: Option<String>, json: bool, verbose: bool) -> i32 {
    if fix {
        eprintln!(
            "note: --fix is not implemented yet (full doctor lands in milestone M7; \
             see docs/herdcluster/PLAN.md §3.9)"
        );
    }

    let mut checks: Vec<Check> = Vec::new();

    // 1. Cluster config / state.
    let ctx = match load_ctx_for_doctor(verbose, &mut checks) {
        Some(ctx) => Some(ctx),
        None => None,
    };

    // 2. Local herdr API ping (independent of herdcluster state).
    check_local_herdr(&mut checks, verbose);

    // 3. Per-machine reachability via the fleet machines view.
    if let Some(ctx) = &ctx {
        check_machines(ctx, machine.as_deref(), &mut checks, verbose);
    } else {
        checks.push(Check {
            name: "machines".into(),
            status: CheckStatus::Warn,
            detail: "skipped: herdcluster state failed to load".into(),
        });
    }

    let ok = !checks.iter().any(|c| c.status == CheckStatus::Fail);
    let report = DoctorReport { ok, checks };

    if json {
        print_json(&report);
    } else {
        print_human(&report);
    }
    if ok {
        0
    } else {
        1
    }
}

/// Load the fleet context for doctor, recording a check result instead
/// of aborting — doctor reports, it doesn't fail fast.
fn load_ctx_for_doctor(verbose: bool, checks: &mut Vec<Check>) -> Option<FleetContext> {
    vlog(verbose, "loading herdcluster state");
    // Create the state dir if needed so first-run machines don't report a
    // spurious config failure (same rationale as `load_ctx`).
    if let Err(e) = cluster_core::config::ensure_layout() {
        checks.push(Check {
            name: "cluster config".into(),
            status: CheckStatus::Fail,
            detail: format!("cannot create state dir: {e}"),
        });
        return None;
    }
    match FleetContext::load() {
        Ok(ctx) => {
            checks.push(Check {
                name: "cluster config".into(),
                status: CheckStatus::Ok,
                detail: format!(
                    "cluster {:?} at {}",
                    ctx.cluster_name,
                    ctx.state_dir.display()
                ),
            });
            Some(ctx)
        }
        Err(e) => {
            checks.push(Check {
                name: "cluster config".into(),
                status: CheckStatus::Fail,
                detail: e.to_string(),
            });
            None
        }
    }
}

/// Ping the local herdr API socket.
fn check_local_herdr(checks: &mut Vec<Check>, verbose: bool) {
    vlog(verbose, "pinging local herdr API");
    let client = match ApiClient::connect_default() {
        Ok(c) => c,
        Err(e) => {
            checks.push(Check {
                name: "local herdr api".into(),
                status: CheckStatus::Fail,
                detail: e.to_string(),
            });
            return;
        }
    };
    match client.ping() {
        Ok(pong) => {
            let version = pong.get("version").and_then(|v| v.as_str());
            let protocol = pong.get("protocol").and_then(|v| v.as_u64());
            checks.push(Check {
                name: "local herdr api".into(),
                status: CheckStatus::Ok,
                detail: format!(
                    "pong (version {}, protocol {})",
                    version.unwrap_or("?"),
                    protocol
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| "?".into())
                ),
            });
        }
        Err(e) => checks.push(Check {
            name: "local herdr api".into(),
            status: CheckStatus::Fail,
            detail: e.to_string(),
        }),
    }
}

/// Per-machine reachability rows from the fleet view.
fn check_machines(
    ctx: &FleetContext,
    machine_filter: Option<&str>,
    checks: &mut Vec<Check>,
    verbose: bool,
) {
    vlog(verbose, "probing machine reachability");
    let infos = machines(ctx);
    let mut reachable = 0usize;
    let mut total = 0usize;
    for m in &infos {
        if let Some(filter) = machine_filter {
            if m.label != filter {
                continue;
            }
        }
        total += 1;
        let (status, detail) = if m.reachable {
            reachable += 1;
            let agents = m
                .agents
                .map(|n| format!("{n} agents"))
                .unwrap_or_else(|| "agent count unknown".into());
            (CheckStatus::Ok, format!("reachable — {agents}"))
        } else {
            match m.status.as_str() {
                // Registry says offline/degraded — warn, not fail.
                "offline" | "degraded" => (
                    CheckStatus::Warn,
                    format!("{} (last registry status)", m.status),
                ),
                _ => (CheckStatus::Fail, "unreachable".into()),
            }
        };
        checks.push(Check {
            name: format!("machine {}", m.label),
            status,
            detail,
        });
    }
    if machine_filter.is_some() && total == 0 {
        checks.push(Check {
            name: "machine filter".into(),
            status: CheckStatus::Fail,
            detail: format!("no machine labeled {machine_filter:?} in the fleet"),
        });
    } else {
        checks.push(Check {
            name: "machines summary".into(),
            status: if reachable == total {
                CheckStatus::Ok
            } else {
                CheckStatus::Warn
            },
            detail: format!("{reachable}/{total} machines reachable"),
        });
    }
}

fn print_human(report: &DoctorReport) {
    for check in &report.checks {
        let mark = match check.status {
            CheckStatus::Ok => "✓",
            CheckStatus::Warn => "!",
            CheckStatus::Fail => "✗",
        };
        println!("{mark} {} — {}", check.name, check.detail);
    }
    println!(
        "{}",
        if report.ok {
            "doctor: all checks passed"
        } else {
            "doctor: failures detected (see ✗ above)"
        }
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_status_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(CheckStatus::Ok).unwrap(),
            serde_json::json!("ok")
        );
        assert_eq!(
            serde_json::to_value(CheckStatus::Warn).unwrap(),
            serde_json::json!("warn")
        );
        assert_eq!(
            serde_json::to_value(CheckStatus::Fail).unwrap(),
            serde_json::json!("fail")
        );
    }

    #[test]
    fn report_ok_flag_logic() {
        let checks = vec![
            Check {
                name: "a".into(),
                status: CheckStatus::Ok,
                detail: String::new(),
            },
            Check {
                name: "b".into(),
                status: CheckStatus::Warn,
                detail: String::new(),
            },
        ];
        assert!(!checks.iter().any(|c| c.status == CheckStatus::Fail));
        let checks2 = vec![Check {
            name: "c".into(),
            status: CheckStatus::Fail,
            detail: String::new(),
        }];
        assert!(checks2.iter().any(|c| c.status == CheckStatus::Fail));
    }
}
