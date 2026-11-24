//! Fleet-aware `agent wait` / `agent watch`: client-side polling of
//! `agent.get` through the resolved machine endpoint.
//!
//! [`ops::agent_wait`] forwards to the machine's own blocking
//! `agent.wait` method. This module instead polls `agent.get` on a fixed
//! interval ([`POLL_INTERVAL`]) so the orchestrator gets uniform behavior
//! across the local socket and remote bridge endpoints — and can detect
//! stuck agents, which a blocking server-side wait cannot.
//!
//! The poll loop ([`poll_until`]) is generic over the fetch closure so
//! unit tests drive it with a fake `agent.get` — no sockets involved.
//! [`agent_poll`] is the production wiring: it resolves the global ID
//! once via [`ops::resolve_and_connect`] (local socket or bridge, same
//! routing as `agent get`/`read`) and polls `agent.get` on that endpoint.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::Value;

use crate::endpoint::FleetContext;
use crate::error::FleetError;
use crate::ops;
use crate::parse;

/// Default poll cadence for wait/watch loops.
pub const POLL_INTERVAL: Duration = Duration::from_secs(3);
/// Default stuck threshold: warn when a status sits in the
/// working/blocked set unchanged for this long. Overridable per call
/// via `WatchTuning::stuck_after` (the CLI `--stuck-after` flag).
pub const DEFAULT_STUCK_AFTER: Duration = Duration::from_secs(10 * 60);
/// Statuses considered "busy" for stuck detection.
const BUSY_STATUSES: &[&str] = &["working", "blocked"];
/// Statuses that terminate `agent watch` even when they are not in the
/// caller's until-set. `done` is the agent-finished signal; `idle` is
/// deliberately excluded (an idle-at-prompt agent has not necessarily
/// finished anything).
pub const WATCH_TERMINAL_STATUSES: &[&str] = &["done"];

/// Per-loop tuning for [`poll_until`].
#[derive(Debug, Clone)]
pub struct WatchTuning {
    /// Give up after this long; `None` waits forever (still interruptible
    /// via Ctrl-C in the CLI).
    pub timeout: Option<Duration>,
    /// Delay between `agent.get` polls.
    pub interval: Duration,
    /// Warn once when the status stays in [`BUSY_STATUSES`] unchanged for
    /// at least this long; `None` disables stuck detection.
    pub stuck_after: Option<Duration>,
}

impl Default for WatchTuning {
    fn default() -> Self {
        WatchTuning {
            timeout: None,
            interval: POLL_INTERVAL,
            stuck_after: Some(DEFAULT_STUCK_AFTER),
        }
    }
}

/// A mid-poll observation worth reporting to the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum PollEvent {
    /// The agent's status changed (the first poll reports
    /// `from: null` — it is the initial observation, not a transition).
    Status {
        /// Previous status, `None` on the first observation.
        from: Option<String>,
        /// New status.
        to: String,
    },
    /// The status has been `working`/`blocked` unchanged for at least
    /// the configured stuck threshold.
    Stuck {
        /// The stuck status.
        status: String,
        /// Seconds the status has been unchanged.
        elapsed_secs: u64,
    },
}

/// How [`poll_until`] ended.
#[derive(Debug)]
pub enum PollResult {
    /// The status entered the until-set. `agent` is the last `agent.get`
    /// response (contains the full agent record).
    Matched { status: String, agent: Value },
    /// The agent disappeared mid-poll (`agent.get` → not-found). For
    /// `watch` this is a clean end state: the agent exited.
    Gone,
    /// The timeout elapsed before the status entered the until-set.
    TimedOut { last_status: Option<String> },
}

/// Extract the status string from an `agent.get` response, tolerating
/// the `{"agent": {...}}` envelope and the `agent_status`/`status`/`state`
/// field aliases (same keys as `agent list` parsing).
pub fn agent_status(value: &Value) -> Option<String> {
    let inner = parse::unwrap_entity(value, "agent");
    parse::str_field(inner, parse::STATUS_KEYS)
        .or_else(|| parse::str_field(value, parse::STATUS_KEYS))
        .map(|s| s.to_lowercase())
}

/// Parse a duration like `30s`, `5m`, `1h`, or compound `1h30m`.
///
/// Units: `s` (seconds), `m` (minutes), `h` (hours). Every number must
/// carry a unit; bare numbers and unknown units are rejected with
/// [`FleetError::InvalidArgument`].
pub fn parse_duration(input: &str) -> Result<Duration, FleetError> {
    let invalid = |reason: &str| FleetError::InvalidArgument {
        message: format!("invalid duration {input:?}: {reason}"),
    };
    let s = input.trim();
    if s.is_empty() {
        return Err(invalid("empty string"));
    }
    let mut total_secs: u64 = 0;
    let mut digits = String::new();
    let mut seen_unit = false;
    for c in s.chars() {
        if c.is_ascii_digit() {
            digits.push(c);
            continue;
        }
        let unit_secs = match c {
            's' => 1u64,
            'm' => 60,
            'h' => 3600,
            _ => {
                return Err(invalid(
                    "unknown unit (expected s, m, or h — e.g. 30s, 5m, 1h)",
                ))
            }
        };
        if digits.is_empty() {
            return Err(invalid("missing number before unit"));
        }
        let n: u64 = digits.parse().map_err(|_| invalid("number too large"))?;
        let part = n
            .checked_mul(unit_secs)
            .ok_or_else(|| invalid("duration overflows u64 seconds"))?;
        total_secs = total_secs
            .checked_add(part)
            .ok_or_else(|| invalid("duration overflows u64 seconds"))?;
        digits.clear();
        seen_unit = true;
    }
    if !digits.is_empty() {
        return Err(invalid("missing unit suffix (expected s, m, or h)"));
    }
    if !seen_unit {
        return Err(invalid("no duration found"));
    }
    Ok(Duration::from_secs(total_secs))
}

/// Parse a comma-separated `--until` list (`done,idle,blocked`) into a
/// normalized (lowercased) status set. Rejects empty tokens and anything
/// that is not a plain lowercase status token.
pub fn parse_until(input: &str) -> Result<BTreeSet<String>, FleetError> {
    let invalid = |reason: &str| FleetError::InvalidArgument {
        message: format!("invalid --until value {input:?}: {reason}"),
    };
    let mut set = BTreeSet::new();
    for token in input.split(',') {
        let t = token.trim().to_lowercase();
        if t.is_empty() {
            return Err(invalid("empty status in list"));
        }
        if !t
            .chars()
            .all(|c| c.is_ascii_lowercase() || c == '_' || c.is_ascii_digit())
        {
            return Err(invalid(&format!("bad status token {token:?}")));
        }
        set.insert(t);
    }
    if set.is_empty() {
        return Err(invalid("empty status set"));
    }
    Ok(set)
}

/// The shared polling loop. `fetch` performs one `agent.get`-equivalent
/// call and returns the raw response `Value`.
///
/// Each iteration:
/// 1. Returns [`PollResult::TimedOut`] if the deadline passed.
/// 2. Calls `fetch`. [`FleetError::AgentNotFound`] maps to
///    [`PollResult::Gone`]; every other error aborts the loop.
/// 3. Emits a [`PollEvent::Status`] on every status change (including the
///    first observation) and a [`PollEvent::Stuck`] once per busy streak
///    that exceeds `tuning.stuck_after`.
/// 4. Returns [`PollResult::Matched`] when the status enters `until`.
///
/// Sleeps `tuning.interval` between polls (capped at the deadline).
pub fn poll_until<F>(
    fetch: &mut F,
    until: &BTreeSet<String>,
    tuning: &WatchTuning,
    emit: &mut dyn FnMut(PollEvent),
) -> Result<PollResult, FleetError>
where
    F: FnMut() -> Result<Value, FleetError>,
{
    let start = Instant::now();
    let deadline = tuning.timeout.map(|t| start + t);
    let mut prev_status: Option<String> = None;
    let mut status_since = start;
    let mut stuck_warned = false;

    loop {
        if let Some(d) = deadline {
            if Instant::now() >= d {
                return Ok(PollResult::TimedOut {
                    last_status: prev_status,
                });
            }
        }

        let value = match fetch() {
            Ok(v) => v,
            Err(FleetError::AgentNotFound { .. }) => return Ok(PollResult::Gone),
            Err(e) => return Err(e),
        };
        let status = agent_status(&value).unwrap_or_else(|| "unknown".to_string());

        if prev_status.as_deref() != Some(status.as_str()) {
            emit(PollEvent::Status {
                from: prev_status.clone(),
                to: status.clone(),
            });
            prev_status = Some(status.clone());
            status_since = Instant::now();
            stuck_warned = false;
        }

        if let Some(threshold) = tuning.stuck_after {
            let busy = BUSY_STATUSES.contains(&status.as_str());
            if busy && !stuck_warned && status_since.elapsed() >= threshold {
                emit(PollEvent::Stuck {
                    status: status.clone(),
                    elapsed_secs: status_since.elapsed().as_secs(),
                });
                stuck_warned = true;
            }
        }

        if until.contains(&status) {
            return Ok(PollResult::Matched {
                status,
                agent: value,
            });
        }

        let sleep = match deadline {
            Some(d) => tuning
                .interval
                .min(d.saturating_duration_since(Instant::now())),
            None => tuning.interval,
        };
        if !sleep.is_zero() {
            std::thread::sleep(sleep);
        }
    }
}

/// Production entry point: resolve `global_id` (`{machine}/{agent}`) to
/// its endpoint — local socket or worker bridge, same routing as
/// [`ops::agent_get`] — then poll `agent.get` until the status enters
/// `until`, the agent disappears, or `tuning.timeout` elapses.
///
/// `emit` receives every mid-poll observation ([`PollEvent::Status`]
/// transitions and [`PollEvent::Stuck`] warnings); the caller decides how
/// to render them (text lines, JSONL, stderr warnings).
pub fn agent_poll(
    ctx: &FleetContext,
    global_id: &str,
    until: &BTreeSet<String>,
    tuning: &WatchTuning,
    emit: &mut dyn FnMut(PollEvent),
) -> Result<PollResult, FleetError> {
    let (resolved, client) = ops::resolve_and_connect(ctx, global_id)?;
    let target = resolved.agent.clone();
    let mut fetch = || -> Result<Value, FleetError> {
        client
            .agent_get(&target)
            .map_err(|e| ops::op_error(&resolved, e))
    };
    poll_until(&mut fetch, until, tuning, emit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fast() -> WatchTuning {
        WatchTuning {
            timeout: None,
            interval: Duration::from_millis(1),
            stuck_after: Some(Duration::from_millis(5)),
        }
    }

    /// Fake `agent.get`: pops one canned response per call, in order.
    /// `Err` variants exercise the Gone/abort paths.
    fn fake<'a>(
        responses: &'a mut std::collections::VecDeque<Result<Value, FleetError>>,
    ) -> impl FnMut() -> Result<Value, FleetError> + 'a {
        move || {
            responses
                .pop_front()
                .unwrap_or_else(|| panic!("fake agent.get called more times than scripted"))
        }
    }

    fn until(statuses: &[&str]) -> BTreeSet<String> {
        statuses.iter().map(|s| s.to_string()).collect()
    }

    fn get(status: &str) -> Value {
        json!({"agent": {"name": "a", "agent_status": status}})
    }

    fn not_found() -> FleetError {
        FleetError::AgentNotFound {
            machine: "local".into(),
            agent: "a".into(),
        }
    }

    #[test]
    fn parse_duration_cases() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration(" 2m ").unwrap(), Duration::from_secs(120));
    }

    #[test]
    fn parse_duration_rejects_bad_input() {
        for bad in ["", "   ", "30", "5x", "m", "1h30", "-5m", "1.5h"] {
            assert!(
                matches!(parse_duration(bad), Err(FleetError::InvalidArgument { .. })),
                "expected InvalidArgument for {bad:?}"
            );
        }
    }

    #[test]
    fn parse_until_cases() {
        let set = parse_until("done,idle,Blocked").unwrap();
        assert_eq!(
            set.iter().cloned().collect::<Vec<_>>(),
            vec![
                "blocked".to_string(),
                "done".to_string(),
                "idle".to_string()
            ]
        );
        assert!(parse_until("done").unwrap().contains("done"));
        for bad in ["", ",", "done,,idle", "do ne", "done!"] {
            assert!(
                matches!(parse_until(bad), Err(FleetError::InvalidArgument { .. })),
                "expected InvalidArgument for {bad:?}"
            );
        }
    }

    #[test]
    fn agent_status_reads_envelope_and_aliases() {
        assert_eq!(
            agent_status(&json!({"agent": {"agent_status": "Working"}})).as_deref(),
            Some("working")
        );
        assert_eq!(
            agent_status(&json!({"status": "idle"})).as_deref(),
            Some("idle")
        );
        assert_eq!(agent_status(&json!({"agent": {}})), None);
        assert_eq!(
            agent_status(&json!({"agent_status": "blocked"})).as_deref(),
            Some("blocked")
        );
    }

    #[test]
    fn poll_matches_eventually_and_emits_transitions() {
        // working, working, done → two status events (initial + change),
        // Matched on the third poll.
        let mut responses = std::collections::VecDeque::from(vec![
            Ok(get("working")),
            Ok(get("working")),
            Ok(get("done")),
        ]);
        let mut events = Vec::new();
        let result = poll_until(
            &mut fake(&mut responses),
            &until(&["done", "idle"]),
            &fast(),
            &mut |e| events.push(e),
        )
        .unwrap();
        match result {
            PollResult::Matched { status, .. } => assert_eq!(status, "done"),
            other => panic!("expected Matched, got {other:?}"),
        }
        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0],
            PollEvent::Status {
                from: None,
                to: "working".into()
            }
        );
        assert_eq!(
            events[1],
            PollEvent::Status {
                from: Some("working".into()),
                to: "done".into()
            }
        );
    }

    #[test]
    fn poll_returns_timeout_with_last_status() {
        let mut tuning = fast();
        tuning.timeout = Some(Duration::from_millis(3));
        let mut responses: std::collections::VecDeque<Result<Value, FleetError>> =
            (0..10).map(|_| Ok(get("working"))).collect();
        let result = poll_until(
            &mut fake(&mut responses),
            &until(&["done"]),
            &tuning,
            &mut |_| {},
        )
        .unwrap();
        match result {
            PollResult::TimedOut { last_status } => {
                assert_eq!(last_status.as_deref(), Some("working"))
            }
            other => panic!("expected TimedOut, got {other:?}"),
        }
    }

    #[test]
    fn poll_agent_not_found_is_gone_not_error() {
        let mut responses =
            std::collections::VecDeque::from(vec![Ok(get("working")), Err(not_found())]);
        let result = poll_until(
            &mut fake(&mut responses),
            &until(&["done"]),
            &fast(),
            &mut |_| {},
        )
        .unwrap();
        assert!(matches!(result, PollResult::Gone));
    }

    #[test]
    fn poll_other_errors_abort() {
        let mut responses =
            std::collections::VecDeque::from(vec![Err(FleetError::WorkerUnreachable {
                machine: "w".into(),
                reason: "bridge down".into(),
            })]);
        let e = poll_until(
            &mut fake(&mut responses),
            &until(&["done"]),
            &fast(),
            &mut |_| {},
        )
        .unwrap_err();
        assert_eq!(e.code(), "worker_unreachable");
    }

    #[test]
    fn poll_stuck_warns_once_per_busy_streak() {
        // working long enough to cross the 5ms threshold → one Stuck
        // event; the streak must not re-warn while status is unchanged.
        let mut tuning = fast();
        tuning.stuck_after = Some(Duration::from_millis(2));
        tuning.interval = Duration::from_millis(1);
        let mut responses: std::collections::VecDeque<Result<Value, FleetError>> =
            (0..30).map(|_| Ok(get("working"))).collect();
        let mut events = Vec::new();
        // Until-set never matches; bound with a timeout.
        tuning.timeout = Some(Duration::from_millis(15));
        let _ = poll_until(
            &mut fake(&mut responses),
            &until(&["done"]),
            &tuning,
            &mut |e| events.push(e),
        )
        .unwrap();
        let stucks = events
            .iter()
            .filter(|e| matches!(e, PollEvent::Stuck { .. }))
            .count();
        assert_eq!(
            stucks, 1,
            "expected exactly one stuck warning, got {events:?}"
        );
    }

    #[test]
    fn poll_stuck_resets_on_status_change() {
        // working (stuck) → idle (leaves busy set) → working again:
        // each busy streak gets its own warning.
        let mut tuning = fast();
        tuning.stuck_after = Some(Duration::from_millis(1));
        tuning.interval = Duration::from_millis(1);
        tuning.timeout = Some(Duration::from_millis(12));
        let statuses = [
            "working", "working", "working", "idle", "idle", "working", "working",
        ];
        // Pad with "working" so the fake never runs dry before the timeout.
        let mut scripted: Vec<&str> = statuses.to_vec();
        scripted.extend(std::iter::repeat_n("working", 30));
        let mut responses: std::collections::VecDeque<Result<Value, FleetError>> =
            scripted.iter().map(|s| Ok(get(s))).collect();
        let mut events = Vec::new();
        let _ = poll_until(
            &mut fake(&mut responses),
            &until(&["done"]),
            &tuning,
            &mut |e| events.push(e),
        )
        .unwrap();
        let stucks = events
            .iter()
            .filter(|e| matches!(e, PollEvent::Stuck { .. }))
            .count();
        assert!(
            stucks >= 1,
            "expected at least one stuck warning: {events:?}"
        );
    }

    #[test]
    fn poll_no_stuck_when_disabled() {
        let mut tuning = fast();
        tuning.stuck_after = None;
        tuning.timeout = Some(Duration::from_millis(6));
        let mut responses: std::collections::VecDeque<Result<Value, FleetError>> =
            (0..20).map(|_| Ok(get("working"))).collect();
        let mut events = Vec::new();
        let _ = poll_until(
            &mut fake(&mut responses),
            &until(&["done"]),
            &tuning,
            &mut |e| events.push(e),
        )
        .unwrap();
        assert!(!events.iter().any(|e| matches!(e, PollEvent::Stuck { .. })));
    }
}
