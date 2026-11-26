//! Keep-awake: inhibit system sleep while local herdr agents are working.
//!
//! Each herdcluster daemon — the controller (`init`) and the worker
//! (`worker`) — runs a supervisor that polls the **local** herdr
//! `agent.list` API every [`POLL_INTERVAL`] and holds a platform
//! sleep-inhibition assertion while at least one agent reports
//! `working`. Once no agent has been `working` for [`RELEASE_GRACE`]
//! the assertion is released, so an idle machine can sleep normally.
//!
//! # Status policy
//!
//! Only `working` counts as active. `blocked` agents wait on a human —
//! the machine may sleep; the human will wake it when they return.
//! `idle`/`done`/`unknown` need nothing. Probe failures (herdr API
//! down, socket missing) are **neutral**: the current assertion state is
//! kept so a transient hiccup cannot flap sleep inhibition, and the
//! error is logged at debug level.
//!
//! # Platform backends ([`platform_asserter`])
//!
//! - **macOS**: `caffeinate -dims -w <pid>` — stock macOS tool; `-w`
//!   makes caffeinate exit when the daemon pid exits, so even a
//!   SIGKILLed daemon cannot leak the assertion.
//! - **Linux**: `systemd-inhibit --what=sleep:idle --why=... --mode=block`
//!   wrapping a tiny `sh` watchdog loop that exits when the daemon pid
//!   dies — an orphaned inhibitor cannot hold the sleep lock forever.
//!   Requires `systemd-inhibit` on `PATH` (systemd distros; works inside
//!   WSL2 where it inhibits the distro's sleep/idle — note the WSL VM's
//!   sleep is ultimately governed by the Windows host).
//! - **other**: unsupported — warns once and the feature is a no-op.
//!
//! The config gate is `cluster.json`'s `keepawake` flag
//! ([`cluster_core::config::keepawake_enabled`], default `true`;
//! editable via `herdcluster config set keepawake <bool>`).

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// How often the supervisor polls `agent.list`.
pub const POLL_INTERVAL: Duration = Duration::from_secs(20);
/// How long with zero working agents before the assertion is released.
pub const RELEASE_GRACE: Duration = Duration::from_secs(60);
/// Per-call timeout for the `agent.list` probe.
pub const AGENT_LIST_TIMEOUT: Duration = Duration::from_secs(5);

/// Config gate: `cluster.json` `keepawake` (absent/unreadable → `true`).
pub fn enabled() -> bool {
    cluster_core::config::keepawake_enabled()
}

// ---------------------------------------------------------------------------
// agent.list probing
// ---------------------------------------------------------------------------

/// Source of "how many local agents are working". Abstracted so tests
/// (and future non-herdr sources) can substitute.
pub trait AgentProbe: Send + Sync {
    /// Number of local agents currently in an active (`working`) status.
    /// `Err` means the probe itself failed — callers treat it as neutral.
    fn working_agent_count(&self) -> Result<usize, String>;
}

/// Probe the local herdr API socket via `agent.list`.
pub struct LocalHerdrProbe {
    timeout: Duration,
}

impl LocalHerdrProbe {
    /// Probe with [`AGENT_LIST_TIMEOUT`].
    pub fn new() -> Self {
        Self {
            timeout: AGENT_LIST_TIMEOUT,
        }
    }

    /// Probe with an explicit per-call timeout (tests).
    pub fn with_timeout(timeout: Duration) -> Self {
        Self { timeout }
    }
}

impl Default for LocalHerdrProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentProbe for LocalHerdrProbe {
    fn working_agent_count(&self) -> Result<usize, String> {
        let client = api_client::ApiClient::connect_default().map_err(|e| e.to_string())?;
        let value = client
            .request_with_timeout("agent.list", &serde_json::json!({}), self.timeout)
            .map_err(|e| e.to_string())?;
        Ok(count_active_agents(&value))
    }
}

/// Field aliases tried, in order, for an agent's status. Mirrors
/// `cluster-fleet`'s defensive parsing (`agent_status` is the herdr
/// 0.9.5 name; `status`/`state` cover older shapes).
const STATUS_KEYS: &[&str] = &["agent_status", "status", "state"];
/// Collection keys tried when the response is not a bare array.
const COLLECTION_KEYS: &[&str] = &["agents", "items", "results"];

/// Whether an agent status string counts as "needs the machine awake".
/// Only `working` — see module docs for the `blocked` rationale.
pub fn status_is_active(status: &str) -> bool {
    status.eq_ignore_ascii_case("working")
}

fn agent_items(value: &serde_json::Value) -> &[serde_json::Value] {
    if let Some(a) = value.as_array() {
        return a;
    }
    for key in COLLECTION_KEYS {
        if let Some(a) = value.get(*key).and_then(serde_json::Value::as_array) {
            return a;
        }
    }
    &[]
}

fn status_of(item: &serde_json::Value) -> Option<&str> {
    let obj = item.as_object()?;
    for key in STATUS_KEYS {
        if let Some(s) = obj.get(*key).and_then(serde_json::Value::as_str) {
            if !s.is_empty() {
                return Some(s);
            }
        }
    }
    None
}

/// Count agents in an `agent.list` result reporting an active status.
/// Defensive about envelope shape (bare array or `{"agents": [...]}`);
/// non-object rows (bare-string ids) carry no status and never count.
pub fn count_active_agents(value: &serde_json::Value) -> usize {
    agent_items(value)
        .iter()
        .filter(|item| status_of(item).map(status_is_active).unwrap_or(false))
        .count()
}

// ---------------------------------------------------------------------------
// Sleep asserters (platform backends)
// ---------------------------------------------------------------------------

/// Platform sleep-inhibition assertion. `acquire` is idempotent;
/// `release` is idempotent and must leave the machine free to sleep.
pub trait SleepAsserter: Send {
    /// Ensure the assertion is held. `Err` = backend unavailable/failed;
    /// the manager disables itself after the first failure.
    fn acquire(&mut self) -> Result<(), String>;
    /// Release the assertion (idempotent).
    fn release(&mut self);
    /// Whether an assertion is currently held (reaps exited children).
    fn is_held(&mut self) -> bool;
    /// Backend name for logs/status (`"caffeinate"`, `"systemd-inhibit"`,
    /// `"unsupported"`).
    fn backend(&self) -> &'static str;
    /// Whether the backend binary/platform is usable at all.
    fn supported(&self) -> bool;
}

/// Asserter that spawns a helper process and kills it on release/drop.
///
/// The spawned argv is constructed so the helper also exits when the
/// daemon pid exits — a crashed/killed daemon cannot leak the
/// inhibition (see module docs).
pub struct ProcessAsserter {
    program: String,
    args: Vec<String>,
    child: Option<Child>,
    backend: &'static str,
    supported: bool,
}

impl ProcessAsserter {
    /// Build an asserter for `program args...`. `supported=false` marks a
    /// missing binary: `acquire` fails fast with a clear error instead of
    /// attempting doomed spawns.
    pub fn new(program: &str, args: Vec<String>, backend: &'static str, supported: bool) -> Self {
        Self {
            program: program.to_string(),
            args,
            child: None,
            backend,
            supported,
        }
    }
}

impl SleepAsserter for ProcessAsserter {
    fn acquire(&mut self) -> Result<(), String> {
        if !self.supported {
            return Err(format!("{} not found on PATH", self.program));
        }
        if self.is_held() {
            return Ok(());
        }
        let child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("failed to spawn {}: {e}", self.program))?;
        self.child = Some(child);
        Ok(())
    }

    fn release(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn is_held(&mut self) -> bool {
        match self.child.as_mut() {
            None => false,
            Some(child) => match child.try_wait() {
                Ok(None) => true,
                // Exited (or failed to poll — treat as gone; acquire
                // will respawn or surface the real error).
                _ => {
                    self.child = None;
                    false
                }
            },
        }
    }

    fn backend(&self) -> &'static str {
        self.backend
    }

    fn supported(&self) -> bool {
        self.supported
    }
}

impl Drop for ProcessAsserter {
    /// Never leak the inhibition helper: kill + reap on drop so daemon
    /// shutdown (including task aborts) releases the assertion.
    fn drop(&mut self) {
        self.release();
    }
}

/// Asserter for platforms without a supported backend: every `acquire`
/// fails, so the manager warns once and stays a no-op.
pub struct UnsupportedAsserter {
    reason: &'static str,
}

impl UnsupportedAsserter {
    pub fn new(reason: &'static str) -> Self {
        Self { reason }
    }
}

impl SleepAsserter for UnsupportedAsserter {
    fn acquire(&mut self) -> Result<(), String> {
        Err(format!("keep-awake unsupported: {}", self.reason))
    }
    fn release(&mut self) {}
    fn is_held(&mut self) -> bool {
        false
    }
    fn backend(&self) -> &'static str {
        "unsupported"
    }
    fn supported(&self) -> bool {
        false
    }
}

/// `true` when `exe` resolves to an executable file on `PATH`.
fn on_path(exe: &str) -> bool {
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&paths).any(|dir| {
        let candidate = dir.join(exe);
        candidate.is_file()
    })
}

#[cfg(target_os = "macos")]
fn caffeinate_args(pid: u32) -> Vec<String> {
    // -d display, -i idle, -m disk, -s system sleep; -w <pid> exits
    // caffeinate when the daemon exits (crash safety).
    vec!["-dims".to_string(), "-w".to_string(), pid.to_string()]
}

#[cfg(target_os = "linux")]
fn systemd_inhibit_args(pid: u32) -> Vec<String> {
    vec![
        "--what=sleep:idle".to_string(),
        "--why=herdcluster agents working".to_string(),
        "--mode=block".to_string(),
        "sh".to_string(),
        "-c".to_string(),
        // Watchdog: exit when the daemon pid dies so an orphaned
        // systemd-inhibit cannot hold the sleep lock forever.
        format!("while kill -0 {pid} 2>/dev/null; do sleep 30; done"),
    ]
}

/// The platform-appropriate sleep asserter for this machine.
pub fn platform_asserter() -> Box<dyn SleepAsserter> {
    #[cfg(target_os = "macos")]
    {
        Box::new(ProcessAsserter::new(
            "caffeinate",
            caffeinate_args(std::process::id()),
            "caffeinate",
            on_path("caffeinate"),
        ))
    }
    #[cfg(target_os = "linux")]
    {
        if on_path("systemd-inhibit") {
            Box::new(ProcessAsserter::new(
                "systemd-inhibit",
                systemd_inhibit_args(std::process::id()),
                "systemd-inhibit",
                true,
            ))
        } else {
            Box::new(UnsupportedAsserter::new(
                "systemd-inhibit not found (non-systemd distro?)",
            ))
        }
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Box::new(UnsupportedAsserter::new(
            "no keep-awake backend for this platform",
        ))
    }
}

// ---------------------------------------------------------------------------
// Assertion manager (decision logic — pure, unit-testable)
// ---------------------------------------------------------------------------

/// One-shot status report for `herdcluster keepawake status`.
///
/// A separate CLI process cannot see inside the running daemon, so
/// `asserting` (the live assertion state) is not reported here — the
/// daemon logs acquire/release transitions.
#[derive(Debug, Clone, Serialize)]
pub struct StatusReport {
    /// `cluster.json` `keepawake` flag.
    pub enabled: bool,
    /// Backend name (`caffeinate`, `systemd-inhibit`, `unsupported`).
    pub backend: String,
    /// Whether the backend binary/platform is usable.
    pub backend_supported: bool,
    /// Local agents currently `working`, or `null` when the probe failed.
    pub working_agents: Option<usize>,
    /// Probe failure detail, when `working_agents` is null.
    pub probe_error: Option<String>,
}

/// One-shot keep-awake status: config flag + backend availability + a
/// single `agent.list` probe. Never fails — probe errors land in
/// [`StatusReport::probe_error`].
pub fn report_status() -> StatusReport {
    let asserter = platform_asserter();
    let backend = asserter.backend().to_string();
    let backend_supported = asserter.supported();
    let (working_agents, probe_error) = match LocalHerdrProbe::new().working_agent_count() {
        Ok(n) => (Some(n), None),
        Err(e) => (None, Some(e)),
    };
    StatusReport {
        enabled: enabled(),
        backend,
        backend_supported,
        working_agents,
        probe_error,
    }
}

/// The keep-awake decision engine: probe results in, assertion state
/// transitions out. All timing is explicit (`Instant` parameters) so the
/// grace-period logic is deterministic under test.
pub struct KeepAwake {
    probe: Arc<dyn AgentProbe>,
    asserter: Box<dyn SleepAsserter>,
    /// First instant with no working agents; `None` while active.
    idle_since: Option<Instant>,
    asserting: bool,
    /// Set after the first `acquire` failure — the backend is treated as
    /// unavailable for the daemon's lifetime (warned once).
    disabled: bool,
    release_grace: Duration,
}

impl KeepAwake {
    /// Manager with default [`POLL_INTERVAL`]/[`RELEASE_GRACE`] timing.
    /// (The poll cadence lives in [`spawn`]; the manager only needs the
    /// grace period.)
    pub fn new(probe: Arc<dyn AgentProbe>, asserter: Box<dyn SleepAsserter>) -> Self {
        Self::with_timing(probe, asserter, RELEASE_GRACE)
    }

    /// Manager with an explicit release grace (tests).
    pub fn with_timing(
        probe: Arc<dyn AgentProbe>,
        asserter: Box<dyn SleepAsserter>,
        release_grace: Duration,
    ) -> Self {
        Self {
            probe,
            asserter,
            idle_since: None,
            asserting: false,
            disabled: false,
            release_grace,
        }
    }

    /// Feed one probe outcome and apply the decision. `Err` is neutral:
    /// the assertion state is left untouched (a herdr API hiccup must
    /// not flap sleep inhibition).
    pub fn observe(&mut self, result: Result<usize, String>, now: Instant) {
        match result {
            Err(e) => {
                tracing::debug!(error = %e, "keep-awake probe failed; assertion state unchanged");
            }
            Ok(working) => {
                let should_hold = if working > 0 {
                    self.idle_since = None;
                    true
                } else {
                    // The release grace only applies while an assertion
                    // is already held (i.e. activity recently stopped) —
                    // a daemon that never saw work must not acquire the
                    // sleep assertion just because the first probe was
                    // idle (issue #111).
                    self.asserting && {
                        let since = *self.idle_since.get_or_insert(now);
                        now.saturating_duration_since(since) < self.release_grace
                    }
                };
                self.apply(should_hold);
            }
        }
    }

    /// Run one probe cycle synchronously (tests / non-async callers).
    pub fn tick(&mut self, now: Instant) {
        let result = self.probe.working_agent_count();
        self.observe(result, now);
    }

    /// Force-release the assertion (idempotent). Also invoked on drop of
    /// the asserter, so daemon shutdown paths are covered even when the
    /// supervisor task is aborted.
    pub fn release(&mut self) {
        if self.asserting {
            self.asserter.release();
            self.asserting = false;
            tracing::info!("keep-awake assertion released");
        }
    }

    /// Whether the assertion is currently held.
    pub fn asserting(&self) -> bool {
        self.asserting
    }

    /// Backend name (`caffeinate`, `systemd-inhibit`, `unsupported`).
    pub fn backend(&self) -> &'static str {
        self.asserter.backend()
    }

    /// `true` once `acquire` has failed — further holds are skipped.
    pub fn disabled(&self) -> bool {
        self.disabled
    }

    /// Clone of the probe (the async runner needs it for
    /// `spawn_blocking`).
    fn probe_arc(&self) -> Arc<dyn AgentProbe> {
        self.probe.clone()
    }

    fn apply(&mut self, should_hold: bool) {
        // Self-heal: the helper may have died underneath us.
        if should_hold && self.asserting && !self.disabled && !self.asserter.is_held() {
            self.asserting = false;
        }
        if should_hold == self.asserting || (should_hold && self.disabled) {
            return;
        }
        if should_hold {
            match self.asserter.acquire() {
                Ok(()) => {
                    self.asserting = true;
                    tracing::info!(
                        backend = self.asserter.backend(),
                        "keep-awake assertion held"
                    );
                }
                Err(e) => {
                    self.disabled = true;
                    tracing::warn!(
                        backend = self.asserter.backend(),
                        error = %e,
                        "keep-awake unavailable; agents working will not inhibit sleep"
                    );
                }
            }
        } else {
            self.release();
        }
    }
}

/// Spawn the keep-awake supervisor loop as a tokio task.
///
/// Polls every `poll_interval` (the first tick fires immediately, so an
/// already-working agent inhibits sleep at daemon start). The probe runs
/// on `spawn_blocking` — `ApiClient` is synchronous socket IO.
///
/// The loop exits when `stop` changes (or its sender drops), releasing
/// the assertion before returning. Aborting the returned
/// [`JoinHandle`] also releases it: the asserter's `Drop` kills the
/// helper process.
pub fn spawn(
    probe: Arc<dyn AgentProbe>,
    asserter: Box<dyn SleepAsserter>,
    poll_interval: Duration,
    release_grace: Duration,
    mut stop: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ka = KeepAwake::with_timing(probe, asserter, release_grace);
        let mut ticker = tokio::time::interval(poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let probe = ka.probe_arc();
                    let result = match tokio::task::spawn_blocking(move || {
                        probe.working_agent_count()
                    })
                    .await
                    {
                        Ok(r) => r,
                        Err(e) => Err(format!("probe task failed: {e}")),
                    };
                    ka.observe(result, Instant::now());
                }
                _ = stop.changed() => break,
            }
        }
        ka.release();
        tracing::debug!("keep-awake supervisor stopped");
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    // ---- agent.list parsing ----

    #[test]
    fn status_is_active_only_working() {
        assert!(status_is_active("working"));
        assert!(status_is_active("Working"));
        assert!(status_is_active("WORKING"));
        for s in ["idle", "blocked", "done", "unknown", ""] {
            assert!(!status_is_active(s), "{s} should not be active");
        }
    }

    #[test]
    fn count_active_agents_parses_shapes() {
        // Bare array.
        let v = json!([
            {"name": "a", "agent_status": "working"},
            {"name": "b", "status": "idle"},
            {"name": "c", "state": "blocked"},
            {"name": "d", "agent_status": "Working"},
        ]);
        assert_eq!(count_active_agents(&v), 2);

        // {"agents": [...]} envelope.
        let v = json!({"type": "agent_list", "agents": [
            {"name": "a", "agent_status": "working"},
            {"name": "b", "agent_status": "done"},
        ]});
        assert_eq!(count_active_agents(&v), 1);

        // Status key priority + bare-string rows carry no status.
        let v = json!({"agents": ["term-1", {"name": "x", "state": "working", "status": "idle"}]});
        assert_eq!(
            count_active_agents(&v),
            0,
            "first non-empty key wins: status=idle"
        );

        // Unknown/garbage shapes → 0, never panic.
        assert_eq!(count_active_agents(&json!({})), 0);
        assert_eq!(count_active_agents(&json!(null)), 0);
        assert_eq!(count_active_agents(&json!({"agents": "nope"})), 0);
    }

    // ---- decision logic ----

    #[derive(Default)]
    struct FakeAsserterState {
        acquires: usize,
        releases: usize,
        held: bool,
        fail_acquire: bool,
        die_on_acquire: bool,
    }

    #[derive(Clone, Default)]
    struct FakeAsserter {
        state: Arc<Mutex<FakeAsserterState>>,
    }

    impl SleepAsserter for FakeAsserter {
        fn acquire(&mut self) -> Result<(), String> {
            let mut s = self.state.lock().unwrap();
            if s.fail_acquire {
                return Err("boom".to_string());
            }
            s.acquires += 1;
            s.held = !s.die_on_acquire;
            if s.die_on_acquire {
                Err("spawn failed".to_string())
            } else {
                Ok(())
            }
        }
        fn release(&mut self) {
            let mut s = self.state.lock().unwrap();
            if s.held {
                s.releases += 1;
            }
            s.held = false;
        }
        fn is_held(&mut self) -> bool {
            self.state.lock().unwrap().held
        }
        fn backend(&self) -> &'static str {
            "fake"
        }
        fn supported(&self) -> bool {
            true
        }
    }

    struct StaticProbe(Result<usize, String>);
    impl AgentProbe for StaticProbe {
        fn working_agent_count(&self) -> Result<usize, String> {
            self.0.clone()
        }
    }

    fn manager(
        result: Result<usize, String>,
        grace: Duration,
        asserter: FakeAsserter,
    ) -> (KeepAwake, FakeAsserter) {
        let probe: Arc<dyn AgentProbe> = Arc::new(StaticProbe(result));
        let ka = KeepAwake::with_timing(probe, Box::new(asserter.clone()), grace);
        (ka, asserter)
    }

    fn working(n: usize) -> Result<usize, String> {
        Ok(n)
    }

    #[test]
    fn holds_while_agents_work() {
        let grace = Duration::from_secs(60);
        let asserter = FakeAsserter::default();
        let (mut ka, fake) = manager(working(2), grace, asserter);
        let t0 = Instant::now();

        ka.observe(working(2), t0);
        assert!(ka.asserting());
        assert_eq!(fake.state.lock().unwrap().acquires, 1);

        // Still working later → stays held, no re-acquire.
        ka.observe(working(1), t0 + Duration::from_secs(30));
        assert!(ka.asserting());
        assert_eq!(fake.state.lock().unwrap().acquires, 1);
    }

    #[test]
    fn releases_after_grace_with_no_working_agents() {
        let grace = Duration::from_secs(60);
        let asserter = FakeAsserter::default();
        let (mut ka, fake) = manager(working(0), grace, asserter);
        let t0 = Instant::now();

        // First acquire needs an active observation.
        ka.observe(working(1), t0);
        assert!(ka.asserting());

        // First idle observation at t0+30 starts the grace clock.
        ka.observe(working(0), t0 + Duration::from_secs(30));
        assert!(ka.asserting());
        assert_eq!(fake.state.lock().unwrap().releases, 0);

        // 45s into grace → still held.
        ka.observe(working(0), t0 + Duration::from_secs(75));
        assert!(ka.asserting());

        // 65s idle (> 60s grace) → released.
        ka.observe(working(0), t0 + Duration::from_secs(95));
        assert!(!ka.asserting());
        assert_eq!(fake.state.lock().unwrap().releases, 1);

        // Remains released while still idle.
        ka.observe(working(0), t0 + Duration::from_secs(160));
        assert!(!ka.asserting());
        assert_eq!(fake.state.lock().unwrap().releases, 1);
    }

    #[test]
    fn never_acquires_when_idle_from_the_start() {
        // Issue #111: grace exists to smooth the active→idle transition,
        // not to hold sleep inhibition before any agent ever worked.
        let grace = Duration::from_secs(60);
        let asserter = FakeAsserter::default();
        let (mut ka, fake) = manager(working(0), grace, asserter);
        let t0 = Instant::now();

        for secs in [0, 30, 90, 200] {
            ka.observe(working(0), t0 + Duration::from_secs(secs));
            assert!(!ka.asserting());
        }
        let s = fake.state.lock().unwrap();
        assert_eq!(s.acquires, 0);
        assert_eq!(s.releases, 0);
    }

    #[test]
    fn activity_resets_grace() {
        let grace = Duration::from_secs(60);
        let asserter = FakeAsserter::default();
        let (mut ka, _fake) = manager(working(0), grace, asserter);
        let t0 = Instant::now();

        ka.observe(working(1), t0);
        ka.observe(working(0), t0 + Duration::from_secs(50));
        // Blip of activity at t=55 restarts the idle clock.
        ka.observe(working(1), t0 + Duration::from_secs(55));
        ka.observe(working(0), t0 + Duration::from_secs(100)); // idle clock restarts here
        assert!(ka.asserting());
        ka.observe(working(0), t0 + Duration::from_secs(150)); // 50s idle < 60s grace
        assert!(ka.asserting());
        ka.observe(working(0), t0 + Duration::from_secs(170)); // 70s idle > grace
        assert!(!ka.asserting());
    }

    #[test]
    fn probe_error_is_neutral() {
        let grace = Duration::from_secs(60);
        let asserter = FakeAsserter::default();
        let (mut ka, fake) = manager(working(0), grace, asserter);
        let t0 = Instant::now();

        ka.observe(working(1), t0);
        assert!(ka.asserting());

        // Errors must not advance the idle clock or release.
        ka.observe(Err("api down".to_string()), t0 + Duration::from_secs(30));
        ka.observe(Err("api down".to_string()), t0 + Duration::from_secs(90));
        assert!(ka.asserting());
        assert_eq!(fake.state.lock().unwrap().releases, 0);

        // And must not acquire either when released.
        ka.release();
        ka.observe(Err("api down".to_string()), t0 + Duration::from_secs(120));
        assert!(!ka.asserting());
        assert_eq!(fake.state.lock().unwrap().acquires, 1);
    }

    #[test]
    fn acquire_failure_disables_keepawake() {
        let grace = Duration::from_secs(60);
        let asserter = FakeAsserter::default();
        asserter.state.lock().unwrap().fail_acquire = true;
        let (mut ka, fake) = manager(working(1), grace, asserter);

        ka.observe(working(1), Instant::now());
        assert!(!ka.asserting());
        assert!(ka.disabled());

        // Never retries, even while agents keep working.
        ka.observe(working(1), Instant::now() + Duration::from_secs(60));
        assert!(!ka.asserting());
        assert_eq!(fake.state.lock().unwrap().acquires, 0);
    }

    #[test]
    fn dead_helper_is_reacquired() {
        let grace = Duration::from_secs(60);
        let asserter = FakeAsserter::default();
        let (mut ka, fake) = manager(working(1), grace, asserter);
        let t0 = Instant::now();

        ka.observe(working(1), t0);
        assert!(ka.asserting());

        // Helper dies underneath us; next active observation re-acquires.
        fake.state.lock().unwrap().held = false;
        ka.observe(working(1), t0 + Duration::from_secs(20));
        assert!(ka.asserting());
        assert_eq!(fake.state.lock().unwrap().acquires, 2);
    }

    // ---- async supervisor ----

    struct CountingProbe(AtomicUsize);
    impl AgentProbe for CountingProbe {
        fn working_agent_count(&self) -> Result<usize, String> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    #[tokio::test]
    async fn spawn_supervisor_acquires_and_releases() {
        let probe = Arc::new(CountingProbe(AtomicUsize::new(1)));
        let asserter = FakeAsserter::default();
        let fake = asserter.clone();
        let (stop_tx, stop_rx) = watch::channel(false);
        let handle = spawn(
            probe.clone(),
            Box::new(asserter),
            Duration::from_millis(10),
            Duration::from_millis(40),
            stop_rx,
        );

        // First tick is immediate → assertion held quickly.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(fake.state.lock().unwrap().held);

        // Agents go idle → released after the 40ms grace.
        probe.0.store(0, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!fake.state.lock().unwrap().held);

        // Graceful stop → task exits cleanly.
        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }
}
