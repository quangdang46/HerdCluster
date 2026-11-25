//! Fleet-aware `agent wait` / `agent watch`.
//!
//! Both prefer herdr's real event/wait APIs routed through the resolved
//! machine endpoint (local socket or worker bridge — the bridge pipes
//! bytes verbatim, so streaming subscriptions work remotely too):
//!
//! * [`agent_wait_live`] uses the server-side blocking `agent.wait`
//!   method. When `--stuck-after` is enabled a lightweight `agent.get`
//!   monitor runs concurrently (on this thread) purely to emit
//!   [`PollEvent::Status`]/[`PollEvent::Stuck`]; the server call stays
//!   authoritative for the outcome. With stuck detection off it is a
//!   single blocking request — no polling at all.
//! * [`agent_watch_live`] streams `events.subscribe` filtered to the
//!   agent's pane, with `pane.exited`/`pane.closed` for gone-detection.
//!
//! Both fall back to the client-side poll loop ([`agent_poll`], fixed
//! [`POLL_INTERVAL`]) when the endpoint can't express the request —
//! older herdr without `agent.wait`/`events.subscribe`, an `--until`
//! status outside the upstream `AgentStatus` enum, or a missing
//! `pane_id`. The poll loop ([`poll_until`]) is generic over the fetch
//! closure so unit tests drive it with a fake `agent.get` — no sockets
//! involved. [`agent_poll`] is the production wiring: it resolves the
//! global ID once via [`ops::resolve_and_connect`] (local socket or
//! bridge, same routing as `agent get`/`read`) and polls `agent.get` on
//! that endpoint.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use api_client::methods::{AgentStatus, EventsSubscribeParams, Subscription, WaitOptions};
use api_client::ApiError;
use serde::Serialize;
use serde_json::Value;

use crate::endpoint::FleetContext;
use crate::error::FleetError;
use crate::ids::ResolvedAgent;
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
    /// The agent disappeared *after* being observed at least once
    /// (`agent.get` → not-found mid-poll). For `watch` this is a clean
    /// end state: the agent exited. A not-found on the very first poll
    /// is [`FleetError::AgentNotFound`] instead — the ID never existed
    /// (e.g. a typo), which must not look like success (issue #10).
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
/// normalized (lowercased) status set. Rejects empty tokens, malformed
/// tokens, and any status outside the upstream [`AgentStatus`] enum —
/// a typo'd status would otherwise burn the whole timeout and surface
/// as a misleading "timed out" (issue #20).
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
        if AgentStatus::parse(&t).is_none() {
            let valid = AgentStatus::ALL
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(invalid(&format!(
                "unknown status {t:?} (expected one of: {valid})"
            )));
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
///    [`PollResult::Gone`] once the agent has been observed; on the
///    first poll it propagates as an error (never-existed ID, issue
///    #10). Every other error aborts the loop.
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
            // `prev_status` is `Some` iff a fetch already succeeded, so
            // not-found here means the agent vanished mid-wait → Gone.
            // On the first poll it means the ID never existed → error.
            Err(FleetError::AgentNotFound { .. }) if prev_status.is_some() => {
                return Ok(PollResult::Gone)
            }
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
            .map(|v| crate::fleet::normalize_agent_get(&v, &target))
            .map_err(|e| ops::op_error(&resolved, e))
    };
    poll_until(&mut fetch, until, tuning, emit)
}

// ---------------------------------------------------------------------------
// Event-API fast paths (`agent.wait` / `events.subscribe`) with poll fallback
// ---------------------------------------------------------------------------

/// Read timeout for `events.subscribe` streams. Doubles as the
/// stuck-check tick: [`api_client::methods::EventStream::next_event`]
/// returns [`ApiError::Timeout`] whenever no event arrives within this
/// window.
const STREAM_TICK: Duration = Duration::from_secs(1);
/// Pause between resubscribe/retry attempts after a dropped stream or a
/// transient `agent.get` failure mid-watch.
const RESUBSCRIBE_DELAY: Duration = Duration::from_millis(500);
/// Consecutive subscribe/resolve failures tolerated mid-watch before
/// surfacing a real error (covers bridge reconnects, herdr restarts).
const MAX_STREAM_RETRIES: u32 = 5;
/// Client-side slack added on top of the server-side `agent.wait`
/// `timeout_ms`, so the server's own timeout error wins the race.
const WAIT_CLIENT_SLACK: Duration = Duration::from_secs(30);
/// Far-future client bound for waits with no `--timeout`: the server
/// waits indefinitely, so this only guards against a wedged connection.
const WAIT_CLIENT_MAX: Duration = Duration::from_secs(31_536_000); // ~1 year

/// Map a `--until` status set to upstream [`AgentStatus`] values.
///
/// Returns `None` when any token has no upstream equivalent — the poll
/// loop accepts arbitrary status strings, `agent.wait` cannot.
pub fn until_agent_statuses(until: &BTreeSet<String>) -> Option<Vec<AgentStatus>> {
    until.iter().map(|s| AgentStatus::parse(s)).collect()
}

/// Extract `pane_id` from an `agent.get` (`agent_info`) response,
/// tolerating the `{"agent": {...}}` envelope.
pub fn agent_pane_id(value: &Value) -> Option<String> {
    let inner = parse::unwrap_entity(value, "agent");
    parse::str_field(inner, &["pane_id"])
        .or_else(|| parse::str_field(value, &["pane_id"]))
        .map(str::to_string)
}

/// Server error codes meaning "this endpoint has no `agent.wait`" —
/// herdr rejects unknown method variants as `invalid_request`, and
/// older/alternate servers may use explicit unsupported markers. Any of
/// these → caller falls back to polling (which is always correct).
fn is_unsupported_wait_code(code: &str) -> bool {
    matches!(
        code,
        "invalid_request"
            | "unknown_method"
            | "method_not_found"
            | "unsupported_method"
            | "unimplemented"
    ) || code.contains("unsupported")
}

/// True when an [`ApiError`] is the *server-side* wait timeout
/// (`{"code":"timeout"}`), as opposed to a client-side read timeout
/// ([`ApiError::Timeout`]), which is a reachability failure.
fn is_server_timeout(e: &ApiError) -> bool {
    matches!(e, ApiError::Server { code, .. } if code == "timeout")
}

/// Map an `agent.wait` API error to a wait outcome:
///
/// * `Ok(None)` — endpoint can't do server-side waits; caller polls.
/// * `Ok(Some(PollResult::Gone))` — agent target vanished after the
///   upfront existence check in [`agent_wait_server`] passed (it
///   disappeared mid-wait). A not-found *before* any successful lookup
///   never reaches here — the pre-check returns `agent_not_found`.
/// * `Ok(Some(PollResult::TimedOut))` — server-side `timeout_ms` elapsed.
/// * `Err` — real failure (unreachable, protocol, ...).
///
/// `last_status` fills [`PollResult::TimedOut`]; callers pass the last
/// observed status (monitor loop) or fetch one best-effort.
fn classify_wait_error(
    resolved: &ResolvedAgent,
    e: ApiError,
    last_status: Option<String>,
) -> Result<Option<PollResult>, FleetError> {
    if let ApiError::Server { code, .. } = &e {
        // Order matters: unsupported checks run before the not-found
        // predicate because codes like `unknown_method` contain
        // "unknown", which the not-found family also matches.
        if is_unsupported_wait_code(code) {
            return Ok(None);
        }
        if crate::ids::agent_not_found(&resolved.machine, &resolved.agent, code).is_some() {
            return Ok(Some(PollResult::Gone));
        }
        if code == "timeout" {
            return Ok(Some(PollResult::TimedOut { last_status }));
        }
    }
    Err(ops::op_error(resolved, e))
}

/// Best-effort current status, used to fill `TimedOut.last_status`.
fn last_agent_status(client: &api_client::ApiClient, target: &str) -> Option<String> {
    client.agent_get(target).ok().and_then(|v| agent_status(&v))
}

/// Try herdr's server-side `agent.wait` for `global_id`.
///
/// `until` must already be mapped via [`until_agent_statuses`]. Returns
/// `Ok(Some(PollResult))` when the wait resolved server-side,
/// `Ok(None)` when the endpoint rejected `agent.wait` (caller falls back
/// to [`agent_poll`]), `Err` on real failures.
///
/// With `tuning.stuck_after` set, the blocking call runs on a helper
/// thread while this thread polls `agent.get` at `tuning.interval` —
/// solely to emit [`PollEvent::Status`]/[`PollEvent::Stuck`]; the server
/// call decides the outcome. Without it this is a single blocking
/// request — no polling at all.
fn agent_wait_server(
    ctx: &FleetContext,
    global_id: &str,
    until: &[AgentStatus],
    tuning: &WatchTuning,
    emit: &mut dyn FnMut(PollEvent),
) -> Result<Option<PollResult>, FleetError> {
    let (resolved, client) = ops::resolve_and_connect(ctx, global_id)?;
    let target = resolved.agent.clone();

    // First lookup: confirm the agent exists before blocking on
    // `agent.wait`. Without this, a typo'd ID surfaces as
    // `PollResult::Gone` ("agent_gone", exit 0) — indistinguishable
    // from a mid-wait disappearance (issue #10). A not-found from
    // `agent.wait` itself, after this check passed, still maps to Gone.
    client
        .agent_get(&target)
        .map_err(|e| ops::op_error(&resolved, e))?;

    let timeout_ms = tuning
        .timeout
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    let wait = WaitOptions {
        timeout_ms,
        until: Some(until.to_vec()),
    };
    // The client-side bound must exceed the server-side timeout_ms or
    // the client gives up first; with no --timeout the server waits
    // indefinitely, so use a far-future bound.
    let client_timeout = tuning
        .timeout
        .map(|t| t + WAIT_CLIENT_SLACK)
        .unwrap_or(WAIT_CLIENT_MAX);

    if tuning.stuck_after.is_none() {
        // Pure server-side wait — no polling at all.
        return match client.agent_wait_with_timeout(&target, Some(wait), client_timeout) {
            Ok(v) => Ok(Some(PollResult::Matched {
                status: agent_status(&v).unwrap_or_else(|| "unknown".to_string()),
                agent: v,
            })),
            Err(e) => {
                let last = if is_server_timeout(&e) {
                    last_agent_status(&client, &target)
                } else {
                    None
                };
                classify_wait_error(&resolved, e, last)
            }
        };
    }

    // Stuck detection enabled: block on agent.wait in a helper thread and
    // monitor agent.get here for Status/Stuck events. The spawned thread
    // is detached — if we return early it dies with the process and its
    // in-flight server-side wait is harmless.
    let wait_client = resolved.endpoint.client()?;
    let wait_target = target.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let res = wait_client.agent_wait_with_timeout(&wait_target, Some(wait), client_timeout);
        let _ = tx.send(res);
    });

    let start = Instant::now();
    let monitor_deadline = client_timeout;
    let mut prev_status: Option<String> = None;
    let mut status_since = start;
    let mut stuck_warned = false;
    let mut last_status: Option<String> = None;

    let server_result = loop {
        if start.elapsed() >= monitor_deadline {
            break None;
        }
        // One observation per interval. Fetch errors are ignored — the
        // server wait is authoritative and surfaces real failures.
        if let Ok(v) = client.agent_get(&target) {
            let status = agent_status(&v).unwrap_or_else(|| "unknown".to_string());
            last_status = Some(status.clone());
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
        }
        let remaining = monitor_deadline.saturating_sub(start.elapsed());
        let nap = tuning.interval.min(remaining);
        if nap.is_zero() {
            break None;
        }
        match rx.recv_timeout(nap) {
            Ok(res) => break Some(res),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                // Wait thread died without sending (panic) — keep
                // monitoring until the deadline.
                std::thread::sleep(nap);
            }
        }
    };

    match server_result {
        Some(Ok(v)) => Ok(Some(PollResult::Matched {
            status: agent_status(&v).unwrap_or_else(|| "unknown".to_string()),
            agent: v,
        })),
        Some(Err(e)) => classify_wait_error(&resolved, e, last_status),
        // Server never answered within the client bound — report as a
        // wait timeout with the last observed status.
        None => Ok(Some(PollResult::TimedOut { last_status })),
    }
}

/// `agent wait` entry point: prefer herdr's server-side `agent.wait`
/// ([`agent_wait_server`]); fall back to the client-side [`agent_poll`]
/// loop when the until-set can't be expressed upstream or the endpoint
/// rejects the method. Matched/gone/timeout semantics are identical
/// either way.
pub fn agent_wait_live(
    ctx: &FleetContext,
    global_id: &str,
    until: &BTreeSet<String>,
    tuning: &WatchTuning,
    emit: &mut dyn FnMut(PollEvent),
) -> Result<PollResult, FleetError> {
    let Some(statuses) = until_agent_statuses(until) else {
        return agent_poll(ctx, global_id, until, tuning, emit);
    };
    match agent_wait_server(ctx, global_id, &statuses, tuning, emit)? {
        Some(result) => Ok(result),
        None => agent_poll(ctx, global_id, until, tuning, emit),
    }
}

/// What one streamed `{"event","data"}` subscription envelope means for
/// `agent watch`.
enum StreamEvent {
    /// `pane_agent_status_changed` for our pane — carries the new status
    /// and the event's `data` payload (used as the `Matched.agent` value).
    Status { status: String, data: Value },
    /// `pane_exited`/`pane.closed` for our pane — the agent's pane is gone.
    Gone,
    /// Anything else (other panes' lifecycle events, unknown kinds).
    Ignore,
}

/// Classify one subscription envelope against the pane being watched.
/// `pane.agent_status_changed` arrives server-filtered to `pane_id`;
/// `pane.exited`/`pane.closed` are global subscriptions, so their
/// `data.pane_id` is checked here.
fn classify_stream_event(event: &Value, pane_id: &str) -> StreamEvent {
    let kind = event.get("event").and_then(Value::as_str);
    let data = event.get("data");
    let data_pane = data.and_then(|d| d.get("pane_id")).and_then(Value::as_str);
    match kind {
        Some("pane_agent_status_changed") => {
            if data_pane != Some(pane_id) {
                return StreamEvent::Ignore;
            }
            let status = data
                .and_then(|d| d.get("agent_status").or_else(|| d.get("status")))
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_lowercase();
            StreamEvent::Status {
                status,
                data: data.cloned().unwrap_or(Value::Null),
            }
        }
        Some("pane_exited") | Some("pane_closed") => {
            if data_pane == Some(pane_id) {
                StreamEvent::Gone
            } else {
                StreamEvent::Ignore
            }
        }
        _ => StreamEvent::Ignore,
    }
}

/// `agent watch` via `events.subscribe`: resolve the agent's pane, then
/// stream `pane.agent_status_changed` (pane-filtered server-side) plus
/// global `pane.exited`/`pane.closed` (filtered client-side) for
/// gone-detection.
///
/// Returns `Ok(Some(PollResult))` on terminal status or gone,
/// `Ok(None)` when the endpoint can't stream (caller falls back to
/// [`agent_poll`] — nothing has been emitted yet at that point, so the
/// fallback doesn't duplicate events), `Err` on real mid-stream
/// failures. A clean EOF (server/bridge closed the stream) triggers
/// resubscribe with a fresh `agent.get`, which also re-resolves the pane
/// if the agent moved.
///
/// Known limitation: while a stream is live the pane is not re-resolved;
/// an agent that moves panes mid-watch stops producing events until the
/// next reconnect.
fn agent_watch_stream(
    ctx: &FleetContext,
    global_id: &str,
    tuning: &WatchTuning,
    emit: &mut dyn FnMut(PollEvent),
) -> Result<Option<PollResult>, FleetError> {
    let (resolved, client) = ops::resolve_and_connect(ctx, global_id)?;
    let target = resolved.agent.clone();

    let mut prev_status: Option<String> = None;
    let mut status_since = Instant::now();
    let mut stuck_warned = false;
    let mut emitted_initial = false;
    let mut subscribe_failures = 0u32;
    let mut resolve_failures = 0u32;

    loop {
        // (Re)resolve the agent: fresh pane_id + baseline status. On
        // reconnects this also picks up pane moves.
        let info = match client.agent_get(&target) {
            Ok(v) => {
                resolve_failures = 0;
                crate::fleet::normalize_agent_get(&v, &target)
            }
            Err(e) => {
                let gone = matches!(&e, ApiError::Server { code, .. }
                    if crate::ids::agent_not_found(&resolved.machine, &resolved.agent, code)
                        .is_some());
                // Mid-watch disappearance is a clean end state. On the
                // first lookup not-found means the ID never existed —
                // fall through so the poll loop surfaces the typed
                // `agent_not_found` error (issue #10).
                if gone && emitted_initial {
                    return Ok(Some(PollResult::Gone));
                }
                if !emitted_initial {
                    // Nothing emitted yet — let the poll loop produce
                    // the proper typed error.
                    return Ok(None);
                }
                // Mid-watch fetch failure — retry through transient
                // bridge/socket drops rather than erroring out.
                resolve_failures += 1;
                if resolve_failures > MAX_STREAM_RETRIES {
                    return Err(ops::op_error(&resolved, e));
                }
                std::thread::sleep(RESUBSCRIBE_DELAY);
                continue;
            }
        };
        let Some(pane_id) = agent_pane_id(&info) else {
            if !emitted_initial {
                return Ok(None);
            }
            // Mid-watch: a response without pane_id is malformed —
            // count it like a resolve failure.
            resolve_failures += 1;
            if resolve_failures > MAX_STREAM_RETRIES {
                return Err(FleetError::Api {
                    message: format!("agent {target:?} response has no pane_id"),
                });
            }
            std::thread::sleep(RESUBSCRIBE_DELAY);
            continue;
        };
        let baseline = agent_status(&info).unwrap_or_else(|| "unknown".to_string());

        let params = EventsSubscribeParams {
            subscriptions: vec![
                Subscription::PaneAgentStatusChanged {
                    pane_id: pane_id.clone(),
                    agent_status: None,
                },
                Subscription::PaneExited,
                Subscription::PaneClosed,
            ],
        };

        let mut stream = loop {
            match client.events_subscribe(&params, STREAM_TICK) {
                Ok(s) => {
                    subscribe_failures = 0;
                    break s;
                }
                Err(_) if !emitted_initial => return Ok(None),
                Err(e) => {
                    subscribe_failures += 1;
                    if subscribe_failures > MAX_STREAM_RETRIES {
                        return Err(ops::op_error(&resolved, e));
                    }
                    std::thread::sleep(RESUBSCRIBE_DELAY);
                }
            }
        };

        // Baseline observation — same shape the poll loop emits on its
        // first fetch (from: null). On reconnects, a status that drifted
        // while the stream was down surfaces as a normal transition.
        if !emitted_initial {
            emit(PollEvent::Status {
                from: None,
                to: baseline.clone(),
            });
            prev_status = Some(baseline.clone());
            status_since = Instant::now();
            emitted_initial = true;
        } else if prev_status.as_deref() != Some(baseline.as_str()) {
            emit(PollEvent::Status {
                from: prev_status.clone(),
                to: baseline.clone(),
            });
            prev_status = Some(baseline.clone());
            status_since = Instant::now();
            stuck_warned = false;
        }
        // The agent may already be terminal (fresh agent.get or drift
        // during a reconnect) — don't wait for a transition that will
        // never come.
        if WATCH_TERMINAL_STATUSES.contains(&baseline.as_str()) {
            return Ok(Some(PollResult::Matched {
                status: baseline,
                agent: info,
            }));
        }

        // Read events until EOF (resubscribe), a fatal error, or a
        // terminal status / gone.
        loop {
            let event = match stream.next_event() {
                Ok(Some(v)) => Some(v),
                Ok(None) => break, // EOF → outer loop resubscribes
                // Read tick: no event within STREAM_TICK.
                Err(ApiError::Timeout) => None,
                Err(e) => return Err(ops::op_error(&resolved, e)),
            };
            if let Some(v) = event {
                match classify_stream_event(&v, &pane_id) {
                    StreamEvent::Status { status, data } => {
                        let changed = prev_status.as_deref() != Some(status.as_str());
                        if changed {
                            emit(PollEvent::Status {
                                from: prev_status.clone(),
                                to: status.clone(),
                            });
                            prev_status = Some(status.clone());
                            status_since = Instant::now();
                            stuck_warned = false;
                        }
                        if WATCH_TERMINAL_STATUSES.contains(&status.as_str()) {
                            return Ok(Some(PollResult::Matched {
                                status,
                                agent: data,
                            }));
                        }
                    }
                    StreamEvent::Gone => return Ok(Some(PollResult::Gone)),
                    StreamEvent::Ignore => {}
                }
            }
            // Stuck check runs on every wake — event or tick.
            if let Some(threshold) = tuning.stuck_after {
                let busy = prev_status
                    .as_deref()
                    .map(|s| BUSY_STATUSES.contains(&s))
                    .unwrap_or(false);
                if busy && !stuck_warned && status_since.elapsed() >= threshold {
                    if let Some(status) = prev_status.clone() {
                        emit(PollEvent::Stuck {
                            status,
                            elapsed_secs: status_since.elapsed().as_secs(),
                        });
                        stuck_warned = true;
                    }
                }
            }
        }
        std::thread::sleep(RESUBSCRIBE_DELAY);
    }
}

/// `agent watch` entry point: stream pane agent-status events via
/// `events.subscribe` ([`agent_watch_stream`]); fall back to the
/// client-side [`agent_poll`] loop when the endpoint can't stream.
/// Terminates on [`WATCH_TERMINAL_STATUSES`], agent gone, or error —
/// same outcomes as the poll loop.
pub fn agent_watch_live(
    ctx: &FleetContext,
    global_id: &str,
    tuning: &WatchTuning,
    emit: &mut dyn FnMut(PollEvent),
) -> Result<PollResult, FleetError> {
    let until: BTreeSet<String> = WATCH_TERMINAL_STATUSES
        .iter()
        .map(|s| s.to_string())
        .collect();
    match agent_watch_stream(ctx, global_id, tuning, emit)? {
        Some(result) => Ok(result),
        None => agent_poll(ctx, global_id, &until, tuning, emit),
    }
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
        // Every upstream AgentStatus value parses, case-insensitively.
        let all = parse_until("Idle,WORKING,blocked,Done,Unknown").unwrap();
        assert_eq!(all.len(), 5);
        assert!(parse_until("done").unwrap().contains("done"));
        for bad in ["", ",", "done,,idle", "do ne", "done!"] {
            assert!(
                matches!(parse_until(bad), Err(FleetError::InvalidArgument { .. })),
                "expected InvalidArgument for {bad:?}"
            );
        }
    }

    #[test]
    fn parse_until_rejects_unknown_status_with_valid_list() {
        // A typo'd status must fail fast at parse time, not burn the
        // whole timeout (issue #20). The error names the valid values.
        let e = parse_until("bogus_status").unwrap_err();
        match e {
            FleetError::InvalidArgument { message } => {
                assert!(message.contains("bogus_status"), "{message}");
                for valid in ["idle", "working", "blocked", "done", "unknown"] {
                    assert!(message.contains(valid), "{message}");
                }
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
        // Unknown tokens are rejected even in a mixed valid list.
        assert!(matches!(
            parse_until("done,pause"),
            Err(FleetError::InvalidArgument { .. })
        ));
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
        // Observed once, then disappears → mid-wait gone, still success.
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
    fn poll_agent_not_found_on_first_poll_is_error() {
        // Never existed (typo'd id) → typed agent_not_found error, not a
        // success-looking Gone (issue #10).
        let mut responses =
            std::collections::VecDeque::from(vec![Err(not_found())]);
        let e = poll_until(
            &mut fake(&mut responses),
            &until(&["done"]),
            &fast(),
            &mut |_| {},
        )
        .unwrap_err();
        assert_eq!(e.code(), "agent_not_found");
        assert_eq!(e.to_json()["error"]["code"], "agent_not_found");
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

    // ---- event-API helpers -------------------------------------------------

    fn resolved() -> ResolvedAgent {
        ResolvedAgent {
            machine: "local".into(),
            agent: "a".into(),
            endpoint: crate::endpoint::MachineEndpoint {
                label: "local".into(),
                socket_path: "/tmp/nonexistent.sock".into(),
                kind: crate::endpoint::MachineKind::Local,
            },
        }
    }

    fn server_err(code: &str) -> ApiError {
        ApiError::Server {
            code: code.into(),
            message: "boom".into(),
        }
    }

    #[test]
    fn until_agent_statuses_maps_known_and_rejects_unknown() {
        let set = until(&["done", "idle", "blocked", "working", "unknown"]);
        let mapped = until_agent_statuses(&set).unwrap();
        assert_eq!(mapped.len(), 5);
        assert!(until_agent_statuses(&until(&["done", "paused"])).is_none());
        assert!(until_agent_statuses(&until(&["Done-Case"])).is_none());
    }

    #[test]
    fn agent_pane_id_reads_agent_info_envelope() {
        let v =
            json!({"type": "agent_info", "agent": {"pane_id": "w2:p1", "agent_status": "idle"}});
        assert_eq!(agent_pane_id(&v).as_deref(), Some("w2:p1"));
        assert_eq!(
            agent_pane_id(&json!({"pane_id": "w1:p9"})).as_deref(),
            Some("w1:p9")
        );
        assert_eq!(agent_pane_id(&json!({"agent": {}})), None);
    }

    #[test]
    fn classify_wait_error_unsupported_codes_fall_back() {
        for code in [
            "invalid_request",
            "unknown_method",
            "method_not_found",
            "unsupported_method",
            "unsupported_thing",
        ] {
            let out = classify_wait_error(&resolved(), server_err(code), None).unwrap();
            assert!(out.is_none(), "expected fallback for {code}");
        }
    }

    #[test]
    fn classify_wait_error_timeout_and_gone() {
        // Server-side timeout → TimedOut carrying last_status.
        let out = classify_wait_error(&resolved(), server_err("timeout"), Some("working".into()))
            .unwrap()
            .unwrap();
        assert!(matches!(
            out,
            PollResult::TimedOut { ref last_status } if last_status.as_deref() == Some("working")
        ));
        // Agent not found → Gone (same as poll loop).
        let out = classify_wait_error(&resolved(), server_err("agent_not_found"), None)
            .unwrap()
            .unwrap();
        assert!(matches!(out, PollResult::Gone));
        // Unknown codes → real error, not a silent fallback.
        let e = classify_wait_error(&resolved(), server_err("internal"), None).unwrap_err();
        assert_eq!(e.code(), "api_error");
        // Client-side timeout is a reachability error, not a wait timeout.
        let e = classify_wait_error(&resolved(), ApiError::Timeout, None).unwrap_err();
        assert_eq!(e.code(), "worker_unreachable");
    }

    #[test]
    fn classify_stream_event_status_gone_ignore() {
        let pane = "w2:p1";
        // Status change for our pane.
        let ev = json!({"event": "pane_agent_status_changed", "data": {
            "pane_id": pane, "agent_status": "working", "agent": "devin"}});
        match classify_stream_event(&ev, pane) {
            StreamEvent::Status { status, data } => {
                assert_eq!(status, "working");
                assert_eq!(data["agent_status"], "working");
            }
            _ => panic!("expected Status"),
        }
        // pane.exited for our pane → Gone; for another pane → Ignore.
        let ev = json!({"event": "pane_exited", "data": {"pane_id": pane}});
        assert!(matches!(
            classify_stream_event(&ev, pane),
            StreamEvent::Gone
        ));
        let ev = json!({"event": "pane_exited", "data": {"pane_id": "w2:p2"}});
        assert!(matches!(
            classify_stream_event(&ev, pane),
            StreamEvent::Ignore
        ));
        // Status event for a different pane is ignored (defensive — the
        // subscription is pane-filtered server-side).
        let ev = json!({"event": "pane_agent_status_changed", "data": {
            "pane_id": "w2:p2", "agent_status": "done"}});
        assert!(matches!(
            classify_stream_event(&ev, pane),
            StreamEvent::Ignore
        ));
        // Unrelated event kinds are ignored.
        let ev = json!({"event": "layout_updated", "data": {}});
        assert!(matches!(
            classify_stream_event(&ev, pane),
            StreamEvent::Ignore
        ));
    }
}
