//! Pure placement scoring for `--machine auto` (PLAN.md §3.7).
//!
//! Input: a snapshot of [`WorkerCaps`] for every known machine (the local
//! machine is represented the same way — it is "just another worker") plus
//! a [`PlacementRequest`]. Output: the winning machine's label, or every
//! per-machine [`Rejection`] reason when nothing qualifies.
//!
//! Policy v1:
//! 1. Filter: `online && enabled && kinds.contains(kind) &&
//!    agent_count < max_agents`.
//! 2. Score: memory headroom per configured agent slot
//!    (`mem_available_mb / max_agents`, higher is better).
//! 3. Tie-breaks (deterministic): lowest `agent_count`, then `label` ascending.
//! 4. `machine_hint` restricts candidates to the machine with that label;
//!    a hint matching no known machine is an error.
//!
//! Ambiguity resolved: the spec's "mem headroom ratio" uses
//! `mem_available_mb / max_agents` ("headroom per slot" per PLAN §3.7)
//! because `WorkerCaps` carries no `mem_total_mb`.

use serde::{Deserialize, Serialize};

/// Scheduler hard ceiling on `max_agents` per machine (PLAN.md §3.7: default
/// 4, ceiling 8 — enforced by callers; the scorer trusts the input).
pub const DEFAULT_MAX_AGENTS: u32 = 4;
/// Hard ceiling for `max_agents` overrides.
pub const MAX_AGENTS_CEILING: u32 = 8;

/// Capability/resource snapshot for one machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerCaps {
    /// Machine label — the placement key and hint target.
    pub label: String,
    /// Whether the machine is currently reachable.
    pub online: bool,
    /// Whether the machine is enabled for scheduling (admin toggle).
    pub enabled: bool,
    /// Agent kinds this machine can run.
    #[serde(default)]
    pub kinds: Vec<String>,
    /// Agents currently running.
    pub agent_count: u32,
    /// Configured cap on concurrent agents.
    pub max_agents: u32,
    /// Currently available memory in MiB.
    pub mem_available_mb: u64,
}

/// A placement request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementRequest {
    /// Required agent kind (e.g. `"claude"`).
    pub kind: String,
    /// Optional explicit machine label; `None` = auto placement.
    pub machine_hint: Option<String>,
}

/// Why a machine was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    /// `machine_hint` named a label that no known machine has. `machine`
    /// carries the unmatched hint text in this case.
    UnknownMachine,
    /// Machine is unreachable.
    Offline,
    /// Machine is disabled for scheduling.
    Disabled,
    /// Machine does not support the requested kind.
    KindUnsupported,
    /// Machine is at its agent cap.
    AtCapacity,
}

/// One machine's rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rejection {
    /// Machine label.
    pub machine: String,
    /// Why it was rejected.
    pub reason: RejectionReason,
}

/// The chosen placement.
#[derive(Debug, Clone, PartialEq)]
pub struct Placement {
    /// Winning machine label.
    pub machine: String,
    /// Headroom score: `mem_available_mb / max_agents` (MiB per slot).
    pub headroom_per_slot: f64,
    /// The machine's `agent_count` at decision time.
    pub agent_count: u32,
}

/// Choose a machine for `req` from `caps`.
///
/// - `Ok(Placement)` — best-scoring qualifying machine.
/// - `Err(Vec<Rejection>)` — every considered machine was rejected, or
///   `machine_hint` matched no known label (single `UnknownMachine`
///   rejection carrying the hint text). An empty vec means `caps` was
///   empty and no hint was given.
pub fn place(caps: &[WorkerCaps], req: &PlacementRequest) -> Result<Placement, Vec<Rejection>> {
    let candidates: Vec<&WorkerCaps> = match &req.machine_hint {
        Some(hint) => {
            let matched: Vec<&WorkerCaps> =
                caps.iter().filter(|c| &c.label == hint).collect();
            if matched.is_empty() {
                return Err(vec![Rejection {
                    machine: hint.clone(),
                    reason: RejectionReason::UnknownMachine,
                }]);
            }
            matched
        }
        None => caps.iter().collect(),
    };

    let mut best: Option<&WorkerCaps> = None;
    let mut rejections = Vec::new();
    for c in candidates {
        match rejection_reason(c, &req.kind) {
            Some(reason) => rejections.push(Rejection {
                machine: c.label.clone(),
                reason,
            }),
            None => {
                let better = match best {
                    None => true,
                    Some(b) => {
                        // Higher headroom wins; ties → fewer agents → label asc.
                        let (hc, hb) = (headroom(c), headroom(b));
                        hc > hb
                            || (hc == hb
                                && (c.agent_count < b.agent_count
                                    || (c.agent_count == b.agent_count
                                        && c.label < b.label)))
                    }
                };
                if better {
                    best = Some(c);
                }
            }
        }
    }

    match best {
        Some(c) => Ok(Placement {
            machine: c.label.clone(),
            headroom_per_slot: headroom(c),
            agent_count: c.agent_count,
        }),
        None => Err(rejections),
    }
}

/// Memory headroom per configured agent slot. `max_agents == 0` yields 0.0
/// (such machines fail the capacity filter anyway).
fn headroom(c: &WorkerCaps) -> f64 {
    if c.max_agents == 0 {
        0.0
    } else {
        c.mem_available_mb as f64 / c.max_agents as f64
    }
}

/// First failing filter reason for `c`, or `None` if it qualifies.
fn rejection_reason(c: &WorkerCaps, kind: &str) -> Option<RejectionReason> {
    if !c.online {
        Some(RejectionReason::Offline)
    } else if !c.enabled {
        Some(RejectionReason::Disabled)
    } else if !c.kinds.iter().any(|k| k == kind) {
        Some(RejectionReason::KindUnsupported)
    } else if c.agent_count >= c.max_agents {
        Some(RejectionReason::AtCapacity)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn req(kind: &str) -> PlacementRequest {
        PlacementRequest {
            kind: kind.into(),
            machine_hint: None,
        }
    }

    #[test]
    fn picks_highest_headroom() {
        let mut a = caps("alpha");
        a.mem_available_mb = 4096; // 1024/slot
        let mut b = caps("beta");
        b.mem_available_mb = 16384; // 4096/slot
        let machines = vec![a, b];
        let p = place(&machines, &req("claude")).unwrap();
        assert_eq!(p.machine, "beta");
        assert_eq!(p.headroom_per_slot, 4096.0);
    }

    #[test]
    fn tie_breaks_by_agent_count_then_label() {
        // Same headroom: alpha has fewer agents → wins over beta.
        let mut alpha = caps("alpha");
        alpha.agent_count = 1;
        alpha.mem_available_mb = 4096;
        let mut beta = caps("beta");
        beta.agent_count = 2;
        beta.mem_available_mb = 4096;
        let machines = vec![beta.clone(), alpha.clone()];
        let p = place(&machines, &req("claude")).unwrap();
        assert_eq!(p.machine, "alpha");

        // Same headroom AND same agent_count → label asc wins.
        let mut zeta = caps("zeta");
        zeta.agent_count = 1;
        zeta.mem_available_mb = 4096;
        let machines = vec![zeta, alpha];
        let p = place(&machines, &req("claude")).unwrap();
        assert_eq!(p.machine, "alpha");
    }

    #[test]
    fn filters_offline_disabled_kind_and_capacity() {
        let mut off = caps("off");
        off.online = false;
        let mut dis = caps("dis");
        dis.enabled = false;
        let mut kind = caps("kind");
        kind.kinds = vec!["codex".into()];
        let mut full = caps("full");
        full.agent_count = full.max_agents;
        let good = caps("good");
        let machines = vec![off, dis, kind, full, good];

        let p = place(&machines, &req("claude")).unwrap();
        assert_eq!(p.machine, "good");

        // All rejected → per-machine reasons.
        let machines = vec![
            caps_offline(),
            caps_disabled(),
            caps_wrong_kind(),
            caps_full(),
        ];
        let err = place(&machines, &req("claude")).unwrap_err();
        assert_eq!(err.len(), 4);
        assert!(err.iter().any(|r| r.reason == RejectionReason::Offline));
        assert!(err.iter().any(|r| r.reason == RejectionReason::Disabled));
        assert!(err.iter().any(|r| r.reason == RejectionReason::KindUnsupported));
        assert!(err.iter().any(|r| r.reason == RejectionReason::AtCapacity));
    }

    fn caps_offline() -> WorkerCaps {
        let mut c = caps("off");
        c.online = false;
        c
    }
    fn caps_disabled() -> WorkerCaps {
        let mut c = caps("dis");
        c.enabled = false;
        c
    }
    fn caps_wrong_kind() -> WorkerCaps {
        let mut c = caps("kind");
        c.kinds = vec!["codex".into()];
        c
    }
    fn caps_full() -> WorkerCaps {
        let mut c = caps("full");
        c.agent_count = c.max_agents;
        c
    }

    #[test]
    fn machine_hint_restricts_and_validates() {
        let a = caps("alpha");
        let b = caps("beta");
        let machines = vec![a, b];

        // Hint to a valid machine.
        let mut r = req("claude");
        r.machine_hint = Some("beta".into());
        assert_eq!(place(&machines, &r).unwrap().machine, "beta");

        // Hint to a machine that exists but can't run the kind.
        let mut bad = caps("gamma");
        bad.kinds = vec![];
        let machines2 = vec![caps("alpha"), bad];
        let mut r2 = req("claude");
        r2.machine_hint = Some("gamma".into());
        let err = place(&machines2, &r2).unwrap_err();
        assert_eq!(err.len(), 1);
        assert_eq!(err[0].machine, "gamma");
        assert_eq!(err[0].reason, RejectionReason::KindUnsupported);

        // Unknown hint → UnknownMachine rejection carrying the hint text.
        let mut r3 = req("claude");
        r3.machine_hint = Some("nope".into());
        let err = place(&machines, &r3).unwrap_err();
        assert_eq!(err.len(), 1);
        assert_eq!(err[0].machine, "nope");
        assert_eq!(err[0].reason, RejectionReason::UnknownMachine);
    }

    #[test]
    fn local_machine_competes_equally() {
        // "local" is just another WorkerCaps entry.
        let mut local = caps("local");
        local.mem_available_mb = 2048; // 512/slot — loses to worker
        let worker = caps("remote");
        let machines = vec![local, worker];
        let p = place(&machines, &req("claude")).unwrap();
        assert_eq!(p.machine, "remote");

        // Give local more headroom and it wins.
        let mut local2 = caps("local");
        local2.mem_available_mb = 65536;
        let machines = vec![local2, caps("remote")];
        assert_eq!(place(&machines, &req("claude")).unwrap().machine, "local");
    }

    #[test]
    fn empty_candidates_all_rejected() {
        assert_eq!(place(&[], &req("claude")).unwrap_err(), Vec::new());
        let mut r = req("claude");
        r.machine_hint = Some("ghost".into());
        let err = place(&[], &r).unwrap_err();
        assert_eq!(err.len(), 1);
        assert_eq!(err[0].reason, RejectionReason::UnknownMachine);
    }

    #[test]
    fn max_agents_zero_is_at_capacity() {
        let mut c = caps("zero");
        c.max_agents = 0;
        let err = place(&[c], &req("claude")).unwrap_err();
        assert_eq!(err[0].reason, RejectionReason::AtCapacity);
    }

    #[test]
    fn headroom_uses_available_over_max_agents() {
        let mut c = caps("m");
        c.mem_available_mb = 12288;
        c.max_agents = 4;
        assert_eq!(headroom(&c), 3072.0);
        c.max_agents = 0;
        assert_eq!(headroom(&c), 0.0);
    }

    #[test]
    fn exhaustive_fixture_matrix() {
        // Deterministic sweep: vary online/enabled/kind/agent_count/headroom
        // and confirm exactly one winner or full rejection coverage.
        let labels = ["a", "b", "c"];
        let mut machines = Vec::new();
        for (i, label) in labels.iter().enumerate() {
            let mut c = caps(label);
            c.online = i != 0; // a offline
            c.enabled = i != 1; // b disabled
            c.kinds = if i == 2 {
                vec!["other".into()]
            } else {
                vec!["claude".into()]
            };
            c.agent_count = i as u32;
            c.mem_available_mb = 4096 * (i as u64 + 1);
            machines.push(c);
        }
        // a: offline, b: disabled, c: wrong kind → all rejected.
        let err = place(&machines, &req("claude")).unwrap_err();
        assert_eq!(err.len(), 3);

        // Flip c to support claude with capacity → c wins (only qualifier).
        machines[2].kinds = vec!["claude".into()];
        machines[2].agent_count = 0;
        assert_eq!(place(&machines, &req("claude")).unwrap().machine, "c");

        // Bring a online → a has headroom 4096/4=1024, c has 12288/4=3072 → c.
        machines[0].online = true;
        assert_eq!(place(&machines, &req("claude")).unwrap().machine, "c");

        // Crank a's memory → a wins.
        machines[0].mem_available_mb = 1_000_000;
        assert_eq!(place(&machines, &req("claude")).unwrap().machine, "a");
    }
}
