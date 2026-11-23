//! `herdcluster machines [--json]` — cluster machine table.
//!
//! Thin wrapper over [`cluster_fleet::machines`]: the fleet crate does the
//! registry merge + best-effort live probes; this module only renders.

use cluster_fleet::{machines, FleetContext, MachineInfo, MachineKind};

use super::render::render_table;
use super::{load_ctx, print_json, vlog};

const HEADERS: &[&str] = &["NAME", "TYPE", "OS", "STATUS", "AGENTS", "CPU", "MEM"];

/// `herdcluster machines`
pub fn run(json: bool, verbose: bool) -> i32 {
    let ctx: FleetContext = match load_ctx(verbose, json) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };
    vlog(
        verbose,
        "probing machines (best-effort, per-machine timeout)",
    );
    let infos = machines(&ctx);

    if json {
        print_json(&infos);
        return 0;
    }

    let rows: Vec<Vec<String>> = infos.iter().map(machine_row).collect();
    println!("{}", render_table(HEADERS, &rows));
    0
}

/// One table row per [`MachineInfo`]. Columns the fleet view cannot fill
/// yet (OS, CPU, MEM until the resource reporter lands) render `"-"`.
fn machine_row(m: &MachineInfo) -> Vec<String> {
    let kind = match m.kind {
        MachineKind::Local => "local",
        MachineKind::Remote => "remote",
    };
    // Prefer the live probe count; fall back to the last heartbeat report.
    let agents = m
        .agents
        .map(|n| n.to_string())
        .or_else(|| m.agent_count.map(|n| n.to_string()))
        .unwrap_or_else(|| "-".to_string());
    let (cpu, mem) = match m.resources.as_ref() {
        Some(r) => (
            format!("{}c", r.cpu_cores),
            format!("{}MiB", r.mem_available_mb),
        ),
        None => ("-".to_string(), "-".to_string()),
    };
    vec![
        m.label.clone(),
        kind.to_string(),
        "-".to_string(), // OS — not reported yet
        m.status.clone(),
        agents,
        cpu,
        mem,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster_core::protocol::Resources;
    use cluster_fleet::MachineInfo;

    fn info(label: &str) -> MachineInfo {
        MachineInfo {
            label: label.into(),
            kind: MachineKind::Local,
            status: "online".into(),
            reachable: true,
            agents: Some(2),
            agent_count: None,
            max_agents: None,
            mem_available_mb: None,
            kinds: vec![],
            resources: None,
            last_seen: None,
        }
    }

    #[test]
    fn row_uses_live_agent_count_and_dashes_for_missing() {
        let row = machine_row(&info("local"));
        assert_eq!(row[0], "local");
        assert_eq!(row[1], "local");
        assert_eq!(row[2], "-"); // OS
        assert_eq!(row[3], "online");
        assert_eq!(row[4], "2"); // live probe count
        assert_eq!(row[5], "-"); // CPU
        assert_eq!(row[6], "-"); // MEM
    }

    #[test]
    fn row_falls_back_to_heartbeat_counts_and_resources() {
        let mut m = info("wsl");
        m.kind = MachineKind::Remote;
        m.agents = None;
        m.agent_count = Some(3);
        m.resources = Some(Resources {
            cpu_cores: 8,
            mem_total_mb: 16384,
            mem_available_mb: 5120,
            agent_count: 3,
            max_agents: 4,
            load_avg: vec![],
            kinds: vec!["claude".into()],
        });
        let row = machine_row(&m);
        assert_eq!(row[1], "remote");
        assert_eq!(row[4], "3");
        assert_eq!(row[5], "8c");
        assert_eq!(row[6], "5120MiB");
    }

    #[test]
    fn table_renders_fixture_rows() {
        let rows: Vec<Vec<String>> = [info("local"), {
            let mut m = info("wsl");
            m.kind = MachineKind::Remote;
            m.status = "offline".into();
            m.agents = None;
            m
        }]
        .iter()
        .map(machine_row)
        .collect();
        let out = render_table(HEADERS, &rows);
        assert!(out.contains("NAME"));
        assert!(out.contains("local"));
        assert!(out.contains("wsl"));
        assert!(out.contains("offline"));
    }
}
