//! Per-machine caps → `--machine auto` placement (beads `m6-policy`,
//! `m6-caps`; PLAN.md §3.7).
//!
//! [`collect_worker_caps`] builds a [`cluster_core::scheduler::WorkerCaps`]
//! for every known machine — the local machine (live
//! [`crate::resources::sample_resources`] + API probe) and every paired
//! worker (last-heartbeat resources from the registry) — with
//! `cluster.json` `machines.<label>` overrides (`max_agents`,
//! `kinds_override`) applied on top. [`resolve_auto`] is the canonical
//! entry point for `--machine auto`: it loads the [`FleetContext`], runs
//! [`cluster_core::scheduler::place`], and returns the winning machine
//! label or a [`SchedulerWireError::NoPlacement`] carrying every
//! per-machine rejection reason.
//!
//! # Fleet reconciliation
//!
//! `cluster-fleet`'s `spawn` already builds equivalent caps internally
//! (`spawn::build_worker_caps`, private) and honors the same
//! `machines.<label>` overrides — so config caps *are* honored on the
//! fleet auto path today. This module is the cluster-node-side canonical
//! implementation; the differences are deliberate:
//!
//! - Local `mem_available_mb` comes from a live
//!   [`crate::resources::sample_resources`] probe instead of fleet's
//!   `LOCAL_MEM_AVAILABLE_FALLBACK_MB` constant.
//! - Local `agent_count`/`kinds` come from the same live probe.
//!
//! `cluster-fleet` cannot call this module (dependency direction is
//! `cluster-node → cluster-fleet`, never the reverse). If fleet's private
//! builder is ever promoted, it should adopt the live-probe approach or
//! share a caps source in `cluster-core`.

use cluster_core::registry::WorkerStatus;
use cluster_core::scheduler::{self, PlacementRequest, Rejection, WorkerCaps};
use cluster_fleet::endpoint::{FleetContext, LOCAL_LABEL};
use cluster_fleet::FleetError;

/// Effective `max_agents` for a machine: `cluster.json` override wins,
/// then the worker-reported cap, then the scheduler default — clamped to
/// the hard ceiling. Mirrors `cluster-fleet`'s private helper.
pub(crate) fn effective_max_agents(override_max: Option<u32>, reported: Option<u32>) -> u32 {
    let v = override_max
        .or(reported)
        .unwrap_or(scheduler::DEFAULT_MAX_AGENTS);
    v.min(scheduler::MAX_AGENTS_CEILING)
}

/// Effective `kinds` for a machine: `kinds_override` replaces the
/// reported set entirely (PLAN.md §3.7).
fn effective_kinds(override_kinds: &Option<Vec<String>>, reported: &[String]) -> Vec<String> {
    override_kinds.clone().unwrap_or_else(|| reported.to_vec())
}

/// Build [`WorkerCaps`] for every machine in `ctx` (local included — it
/// competes equally, PLAN.md §3.7).
///
/// Synchronous and best-effort: the local probe and
/// [`crate::resources::sample_resources`] make short-timeout API calls;
/// workers use their last-reported registry snapshot (offline workers
/// keep their caps but are rejected as `Offline` by the scorer).
pub fn collect_worker_caps(ctx: &FleetContext) -> Vec<WorkerCaps> {
    let mut caps = Vec::with_capacity(ctx.endpoints.len());

    // --- local machine ---
    let local_override = ctx.machines_overrides.get(LOCAL_LABEL);
    let local_resources = crate::resources::sample_resources();
    let local_online = ctx
        .local()
        .client()
        .ok()
        .and_then(|c| c.ping().ok())
        .is_some();
    caps.push(WorkerCaps {
        label: LOCAL_LABEL.to_string(),
        online: local_online,
        // No per-machine "enabled" flag exists in the registry schema yet;
        // every known machine is enabled (scheduler supports the flag for
        // when config grows one).
        enabled: true,
        kinds: effective_kinds(
            &local_override.and_then(|o| o.kinds_override.clone()),
            &local_resources.kinds,
        ),
        agent_count: local_resources.agent_count,
        max_agents: effective_max_agents(
            local_override.and_then(|o| o.max_agents),
            Some(local_resources.max_agents),
        ),
        mem_available_mb: local_resources.mem_available_mb,
    });

    // --- remote workers ---
    for (label, entry) in &ctx.workers {
        let resources = entry.resources.as_ref();
        let reported_kinds: Vec<String> = resources.map(|r| r.kinds.clone()).unwrap_or_default();
        let override_ = ctx.machines_overrides.get(label);
        caps.push(WorkerCaps {
            label: label.clone(),
            online: entry.status != WorkerStatus::Offline,
            enabled: true,
            kinds: effective_kinds(
                &override_.and_then(|o| o.kinds_override.clone()),
                &reported_kinds,
            ),
            agent_count: resources.map(|r| r.agent_count).unwrap_or(0),
            max_agents: effective_max_agents(
                override_.and_then(|o| o.max_agents),
                resources.map(|r| r.max_agents),
            ),
            mem_available_mb: resources.map(|r| r.mem_available_mb).unwrap_or(0),
        });
    }
    caps
}

/// Errors from auto-placement resolution.
#[derive(Debug, thiserror::Error)]
pub enum SchedulerWireError {
    /// The fleet context (cluster.json / workers.json / socket discovery)
    /// could not be loaded.
    #[error("fleet context error: {0}")]
    Fleet(#[from] FleetError),
    /// No machine qualified for the requested kind; `rejections` carries
    /// every per-machine reason (PLAN.md §3.7 policy v1.5).
    #[error("no machine can run agent kind '{kind}': {detail}")]
    NoPlacement {
        /// The requested agent kind.
        kind: String,
        /// Human-readable per-machine rejection summary.
        detail: String,
        /// Structured per-machine rejections.
        rejections: Vec<Rejection>,
    },
}

impl SchedulerWireError {
    /// Per-machine rejections when this is [`SchedulerWireError::NoPlacement`].
    pub fn rejections(&self) -> &[Rejection] {
        match self {
            SchedulerWireError::NoPlacement { rejections, .. } => rejections,
            _ => &[],
        }
    }

    fn no_placement(kind: &str, rejections: Vec<Rejection>) -> Self {
        let detail = if rejections.is_empty() {
            "no machines are registered".to_string()
        } else {
            rejections
                .iter()
                .map(|r| format!("{}: {}", r.machine, reason_label(r.reason)))
                .collect::<Vec<_>>()
                .join("; ")
        };
        SchedulerWireError::NoPlacement {
            kind: kind.to_string(),
            detail,
            rejections,
        }
    }
}

fn reason_label(reason: cluster_core::scheduler::RejectionReason) -> &'static str {
    use cluster_core::scheduler::RejectionReason as R;
    match reason {
        R::UnknownMachine => "unknown_machine",
        R::Offline => "offline",
        R::Disabled => "disabled",
        R::KindUnsupported => "kind_unsupported",
        R::AtCapacity => "at_capacity",
    }
}

/// Resolve `--machine auto` for `kind` against live fleet state.
///
/// Loads the [`FleetContext`] from herdcluster state (creating
/// `cluster.json` on first run, same as other fleet commands), collects
/// caps, and runs the scheduler. Returns the winning machine label or
/// [`SchedulerWireError::NoPlacement`] with per-machine reasons.
pub fn resolve_auto(kind: &str) -> Result<String, SchedulerWireError> {
    let ctx = FleetContext::load()?;
    resolve_auto_in(kind, &ctx)
}

/// [`resolve_auto`] against an explicit [`FleetContext`] — the testable
/// core and the entry point for callers that already hold a context.
pub fn resolve_auto_in(kind: &str, ctx: &FleetContext) -> Result<String, SchedulerWireError> {
    let caps = collect_worker_caps(ctx);
    resolve_auto_caps(kind, &caps)
}

/// Pure scoring entry: run [`cluster_core::scheduler::place`] over
/// pre-collected caps. Useful for tests and for callers that cache caps.
pub fn resolve_auto_caps(kind: &str, caps: &[WorkerCaps]) -> Result<String, SchedulerWireError> {
    scheduler::place(
        caps,
        &PlacementRequest {
            kind: kind.to_string(),
            machine_hint: None,
        },
    )
    .map(|p| p.machine)
    .map_err(|rejections| SchedulerWireError::no_placement(kind, rejections))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster_core::registry::{ClusterInfo, WorkerEntry};
    use cluster_core::scheduler::RejectionReason;
    use std::path::PathBuf;

    /// Build a `WorkerEntry` via serde — avoids needing `chrono` as a
    /// dev-dependency just for `paired_at`.
    fn worker_entry(
        label: &str,
        status: &str,
        resources: Option<serde_json::Value>,
    ) -> WorkerEntry {
        serde_json::from_value(serde_json::json!({
            "worker_id": format!("id-{label}"),
            "label": label,
            "pubkey": "ab".repeat(32),
            "paired_at": "2024-01-01T00:00:00Z",
            "last_seen": null,
            "status": status,
            "resources": resources,
        }))
        .unwrap()
    }

    fn resources_json(
        agent_count: u32,
        max_agents: u32,
        mem_avail: u64,
        kinds: &[&str],
    ) -> serde_json::Value {
        serde_json::json!({
            "cpu_cores": 8,
            "mem_total_mb": 16384,
            "mem_available_mb": mem_avail,
            "agent_count": agent_count,
            "max_agents": max_agents,
            "load_avg": [0.1, 0.2, 0.3],
            "kinds": kinds,
        })
    }

    fn cluster_with_machines(machines: serde_json::Value) -> ClusterInfo {
        serde_json::from_value(serde_json::json!({
            "cluster_id": "test-cluster",
            "name": "test-cluster",
            "port": 47820,
            "protocol_version": 1,
            "created_at": "2024-01-01T00:00:00Z",
            "machines": machines,
        }))
        .unwrap()
    }

    fn caps(label: &str) -> WorkerCaps {
        WorkerCaps {
            label: label.into(),
            online: true,
            enabled: true,
            kinds: vec!["claude".into(), "codex".into()],
            agent_count: 0,
            max_agents: 4,
            mem_available_mb: 8192,
        }
    }

    #[test]
    fn collect_worker_caps_applies_registry_and_overrides() {
        // local override: max_agents 6 (deterministic — comes from ctx,
        // not disk). Worker w1: max_agents override 2 + kinds_override.
        // Worker w2: plain reported resources, offline.
        let cluster = cluster_with_machines(serde_json::json!({
            "local": { "max_agents": 6 },
            "w1": { "max_agents": 2, "kinds_override": ["codex"] },
        }));
        let workers = vec![
            worker_entry(
                "w1",
                "online",
                Some(resources_json(1, 4, 4096, &["claude", "codex"])),
            ),
            worker_entry(
                "w2",
                "offline",
                Some(resources_json(3, 4, 2048, &["claude"])),
            ),
        ];
        let ctx = FleetContext::build(PathBuf::from("/tmp/hc-wire"), cluster, workers).unwrap();
        let caps = collect_worker_caps(&ctx);

        // local + w1 + w2
        assert_eq!(caps.len(), 3);

        let local = caps.iter().find(|c| c.label == LOCAL_LABEL).unwrap();
        assert_eq!(local.max_agents, 6); // override beats reported default
        assert!(local.enabled);
        // online/kinds/agent_count/mem come from the live probe — don't
        // assert exact values (herdr may or may not be running).

        let w1 = caps.iter().find(|c| c.label == "w1").unwrap();
        assert!(w1.online);
        assert_eq!(w1.kinds, vec!["codex".to_string()]); // kinds_override wins
        assert_eq!(w1.agent_count, 1);
        assert_eq!(w1.max_agents, 2); // override beats reported 4
        assert_eq!(w1.mem_available_mb, 4096);

        let w2 = caps.iter().find(|c| c.label == "w2").unwrap();
        assert!(!w2.online); // registry status offline
        assert_eq!(w2.kinds, vec!["claude".to_string()]);
        assert_eq!(w2.max_agents, 4);
    }

    #[test]
    fn resolve_auto_caps_picks_winner() {
        let mut a = caps("alpha");
        a.mem_available_mb = 4096; // 1024/slot
        let mut b = caps("beta");
        b.mem_available_mb = 16384; // 4096/slot
        let machines = vec![a, b];
        assert_eq!(resolve_auto_caps("claude", &machines).unwrap(), "beta");
    }

    #[test]
    fn resolve_auto_caps_returns_rejection_reasons() {
        let mut off = caps("off");
        off.online = false;
        let mut dis = caps("dis");
        dis.enabled = false;
        let mut kind = caps("kind");
        kind.kinds = vec!["codex".into()];
        let mut full = caps("full");
        full.agent_count = full.max_agents;
        let machines = vec![off, dis, kind, full];

        let err = resolve_auto_caps("claude", &machines).unwrap_err();
        match err {
            SchedulerWireError::NoPlacement {
                kind,
                rejections,
                detail,
            } => {
                assert_eq!(kind, "claude");
                assert_eq!(rejections.len(), 4);
                assert!(rejections
                    .iter()
                    .any(|r| r.reason == RejectionReason::Offline));
                assert!(rejections
                    .iter()
                    .any(|r| r.reason == RejectionReason::Disabled));
                assert!(rejections
                    .iter()
                    .any(|r| r.reason == RejectionReason::KindUnsupported));
                assert!(rejections
                    .iter()
                    .any(|r| r.reason == RejectionReason::AtCapacity));
                // detail string names every machine + reason.
                assert!(detail.contains("off: offline"));
                assert!(detail.contains("dis: disabled"));
                assert!(detail.contains("kind: kind_unsupported"));
                assert!(detail.contains("full: at_capacity"));
            }
            other => panic!("expected NoPlacement, got {other}"),
        }
    }

    #[test]
    fn resolve_auto_caps_empty_caps_is_no_placement() {
        let err = resolve_auto_caps("claude", &[]).unwrap_err();
        assert!(matches!(err, SchedulerWireError::NoPlacement { .. }));
        assert!(err.rejections().is_empty());
    }

    #[test]
    fn resolve_auto_in_matches_direct_scoring() {
        // resolve_auto_in must agree with place() over the same caps —
        // guards against the ctx→caps wiring drifting from the scorer.
        let cluster = cluster_with_machines(serde_json::json!({}));
        let workers = vec![worker_entry(
            "wsl",
            "online",
            Some(resources_json(0, 4, 16384, &["claude"])),
        )];
        let ctx = FleetContext::build(PathBuf::from("/tmp/hc-wire2"), cluster, workers).unwrap();
        let caps = collect_worker_caps(&ctx);
        let direct = scheduler::place(
            &caps,
            &PlacementRequest {
                kind: "claude".into(),
                machine_hint: None,
            },
        );
        let wired = resolve_auto_in("claude", &ctx);
        match (direct, wired) {
            (Ok(p), Ok(label)) => assert_eq!(p.machine, label),
            (Err(_), Err(SchedulerWireError::NoPlacement { .. })) => {}
            (d, w) => panic!("mismatch: direct={d:?} wired={w:?}"),
        }
    }

    #[test]
    fn overrides_map_is_carried_into_caps() {
        // BTreeMap sanity: machines_overrides keyed by label feed both
        // local and worker override lookups.
        let cluster = cluster_with_machines(serde_json::json!({
            "w1": { "kinds_override": ["claude"] }
        }));
        assert!(cluster.machines.contains_key("w1"));
        let workers = vec![worker_entry("w1", "online", None)];
        let ctx = FleetContext::build(PathBuf::from("/tmp/hc-wire3"), cluster, workers).unwrap();
        let caps = collect_worker_caps(&ctx);
        let w1 = caps.iter().find(|c| c.label == "w1").unwrap();
        // No reported resources → kinds come solely from the override.
        assert_eq!(w1.kinds, vec!["claude".to_string()]);
        // No reported max_agents → default 4.
        assert_eq!(w1.max_agents, scheduler::DEFAULT_MAX_AGENTS);
    }
}
