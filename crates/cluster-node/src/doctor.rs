//! `doctor` — layered cluster self-diagnostics + safe remediations
//! (beads `m7-checks` + `m7-fix`, PLAN.md §3.9).
//!
//! [`run_doctor`] runs the check suite in PLAN order and returns a
//! [`DoctorReport`]; `report.ok` is false iff any check is
//! [`CheckStatus::Fail`]. With `opts.fix`, a remediation pass runs
//! *after* the checks and may flip individual check results — every
//! remediation is recorded in `report.fixed` and nothing is ever fixed
//! silently:
//!
//! - **Safe, auto-applied under `--fix`** (still consent-gated by
//!   `--yes`/prompt where noted): create missing state dirs, tighten
//!   `0600` on sensitive files, install the missing orchestration skill
//!   (fresh installs only — a *diverged* skill copy is never overwritten
//!   without an explicit interactive confirm, `--yes` does not count).
//! - **Never auto-fixed**: a stopped herdr server, offline workers,
//!   auth failures, missing agent CLIs. These get plain-language
//!   `fix_hint`s (and, for auth-ish failures in interactive mode, the
//!   re-pair steps are printed).
//!
//! `--machine <label>` scopes the worker/endpoint checks to that label
//! only; cluster-wide checks (config, local herdr, discovery, resources,
//! skill) always run. `"local"` means the controller itself: remote
//! workers are excluded from the check list entirely. No `--machine`
//! checks the whole cluster.

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;
use serde::Serialize;
use thiserror::Error;

use cluster_core::registry::{self, WorkerEntry, WorkerStatus};
use cluster_fleet::endpoint::{bridge_socket_path, LOCAL_LABEL};

/// How long the LAN discovery probe listens before giving up.
const DISCOVERY_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// Per-request timeout for bridge `ping` probes.
const BRIDGE_PROBE_TIMEOUT: Duration = Duration::from_secs(4);
/// Per-request timeout for `agent.list` capacity probes.
const CAPACITY_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Free disk below this is a hard fail; below [`DISK_WARN_BYTES`] warns.
const DISK_FAIL_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB
const DISK_WARN_BYTES: u64 = 8 * 1024 * 1024 * 1024; // 8 GiB
/// Available memory below this warns (agents need headroom).
const MEM_WARN_MB: u64 = 512;

/// Options for [`run_doctor`].
#[derive(Debug, Clone, Default)]
pub struct DoctorOptions {
    /// Scope worker/endpoint checks to one machine label; `None` =
    /// whole cluster. `"local"` = controller only (remote workers are
    /// excluded); any other label scopes to that single worker.
    pub machine: Option<String>,
    /// Attempt safe remediations after the check pass.
    pub fix: bool,
    /// Non-interactive `--yes`: apply safe fixes without prompting.
    pub yes: bool,
    /// JSON output mode (suppresses fix narration prompts' stdout use).
    pub json: bool,
    /// Optional repo path to verify exists — checked on the machine
    /// running doctor (PLAN.md §3.9 "repo path exists (if given)").
    pub repo: Option<String>,
}

/// Outcome of one doctor check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    /// Check passed.
    Pass,
    /// Worth attention but not broken.
    Warn,
    /// Broken — makes `report.ok` false.
    Fail,
}

/// One named check result.
#[derive(Debug, Clone, Serialize)]
pub struct CheckResult {
    /// Stable check name (e.g. `"local herdr api"`).
    pub name: String,
    /// Pass / warn / fail.
    pub status: CheckStatus,
    /// Plain-language what-was-found.
    pub detail: String,
    /// What the user can do about it, when not passing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix_hint: Option<String>,
}

/// The full doctor report.
#[derive(Debug, Clone, Serialize)]
pub struct DoctorReport {
    /// Every check that ran, in suite order.
    pub checks: Vec<CheckResult>,
    /// `true` when no check failed (computed after `--fix` remediations).
    pub ok: bool,
    /// Human-readable descriptions of remediations `--fix` applied.
    pub fixed: Vec<String>,
}

impl DoctorReport {
    /// JSON rendering used by `herdcluster doctor --json`.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_else(|_| serde_json::json!({ "ok": self.ok }))
    }
}

/// Errors from [`run_doctor`] — reserved for failures that prevent the
/// suite from running at all; individual check failures live in the
/// report, not here.
#[derive(Debug, Error)]
pub enum DoctorError {
    /// Filesystem error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Herdcluster config/state-dir resolution failed.
    #[error("config error: {0}")]
    Config(String),
    /// Fleet-layer failure (registry/context load).
    #[error("fleet error: {0}")]
    Fleet(String),
}

/// Run the full doctor suite against the default state dir.
///
/// Blocking probes (herdr API pings, `df`) run inline — the CLI drives
/// this on a dedicated runtime, and every call has a bounded timeout.
pub async fn run_doctor(opts: DoctorOptions) -> Result<DoctorReport, DoctorError> {
    let state_dir =
        cluster_core::config::state_dir().map_err(|e| DoctorError::Config(e.to_string()))?;

    let mut checks: Vec<CheckResult> = Vec::new();
    let mut fixed: Vec<String> = Vec::new();

    // ---- phase 1: checks ----
    let cluster = check_cluster_config(&state_dir, &mut checks);
    check_machine_caps(cluster.as_ref(), &state_dir, &mut checks);
    check_state_layout(&state_dir, &mut checks);
    check_sensitive_perms(&state_dir, &mut checks);
    check_local_herdr(&mut checks);
    check_lan_discovery(&mut checks).await;
    let workers = check_workers_registered(&state_dir, opts.machine.as_deref(), &mut checks);
    check_machine_endpoints(&state_dir, &workers, &mut checks);
    check_machine_capacity(
        cluster.as_ref(),
        &workers,
        &state_dir,
        opts.machine.as_deref(),
        &mut checks,
    );
    check_agent_clis(&mut checks);
    check_repo_path(opts.repo.as_deref(), &mut checks);
    check_disk_space(&state_dir, &mut checks);
    check_memory(&mut checks);
    check_worker_pairing(&state_dir, &mut checks);
    check_orchestration_skill(&mut checks);

    // Auth-failure guidance is printed (not auto-fixed) in interactive
    // fix mode — see PLAN §3.9 "guided re-pair".
    if opts.fix && !opts.json {
        print_repair_guidance(&checks);
    }

    // ---- phase 2: --fix remediations ----
    if opts.fix {
        apply_fixes(&opts, &state_dir, &mut checks, &mut fixed)?;
    }

    let ok = !checks.iter().any(|c| c.status == CheckStatus::Fail);
    Ok(DoctorReport { checks, ok, fixed })
}

// ---------------------------------------------------------------------------
// check helpers
// ---------------------------------------------------------------------------

fn push(checks: &mut Vec<CheckResult>, name: &str, status: CheckStatus, detail: impl Into<String>) {
    checks.push(CheckResult {
        name: name.to_string(),
        status,
        detail: detail.into(),
        fix_hint: None,
    });
}

fn push_hint(
    checks: &mut Vec<CheckResult>,
    name: &str,
    status: CheckStatus,
    detail: impl Into<String>,
    fix_hint: impl Into<String>,
) {
    checks.push(CheckResult {
        name: name.to_string(),
        status,
        detail: detail.into(),
        fix_hint: Some(fix_hint.into()),
    });
}

/// Replace the first check named `name` (used by `--fix` to flip a
/// result after remediation).
fn update_check(
    checks: &mut [CheckResult],
    name: &str,
    status: CheckStatus,
    detail: impl Into<String>,
) {
    if let Some(c) = checks.iter_mut().find(|c| c.name == name) {
        c.status = status;
        c.detail = detail.into();
        c.fix_hint = None;
    }
}

// ---- 1. cluster config ----

/// Returns the loaded [`registry::ClusterInfo`] when `cluster.json`
/// exists and parses; the check result is always recorded.
fn check_cluster_config(
    state_dir: &Path,
    checks: &mut Vec<CheckResult>,
) -> Option<registry::ClusterInfo> {
    let path = state_dir.join(registry::CLUSTER_FILE_NAME);
    if !path.exists() {
        push_hint(
            checks,
            "cluster config",
            CheckStatus::Warn,
            format!(
                "no cluster.json at {} — this machine hasn't run `herdcluster init`",
                path.display()
            ),
            "run `herdcluster init` on the controller machine (workers can ignore this)",
        );
        return None;
    }
    match registry::load_or_init_cluster(&path, "herdcluster") {
        Ok(outcome) => {
            let info = outcome.value;
            if let Some(w) = &outcome.warning {
                push_hint(
                    checks,
                    "cluster config",
                    CheckStatus::Warn,
                    format!("cluster '{}' loaded with warning: {w}", info.name),
                    "inspect the quarantined *.bad file next to cluster.json",
                );
            } else {
                push(
                    checks,
                    "cluster config",
                    CheckStatus::Pass,
                    format!(
                        "cluster '{}' (id {}, port {}, protocol v{})",
                        info.name, info.cluster_id, info.port, info.protocol_version
                    ),
                );
            }
            Some(info)
        }
        Err(e) => {
            push_hint(
                checks,
                "cluster config",
                CheckStatus::Fail,
                format!("cannot load cluster.json: {e}"),
                format!(
                    "restore or delete {} and re-run `herdcluster init`",
                    path.display()
                ),
            );
            None
        }
    }
}

// ---- 1b. machine caps (cluster.json machines.<label> overrides) ----

/// Validate `machines.<label>` scheduler overrides (beads `m6-caps`):
/// out-of-range `max_agents` or malformed `kinds_override` would be
/// silently clamped/ignored by the scheduler, so doctor fails loudly.
///
/// Overrides whose label is neither `local` nor a `workers.json` entry
/// are dead config — a separate warning class that does not affect the
/// value-validation verdict (issue #21).
fn check_machine_caps(
    cluster: Option<&registry::ClusterInfo>,
    state_dir: &Path,
    checks: &mut Vec<CheckResult>,
) {
    let Some(info) = cluster else {
        return; // the "cluster config" check already reported why
    };
    if info.machines.is_empty() {
        push(
            checks,
            "machine caps",
            CheckStatus::Pass,
            "no per-machine overrides configured",
        );
        return;
    }
    let problems = info.machine_override_problems();
    if !problems.is_empty() {
        let detail = problems
            .iter()
            .map(|(label, p)| format!("{label}: {p}"))
            .collect::<Vec<_>>()
            .join("; ");
        push_hint(
            checks,
            "machine caps",
            CheckStatus::Fail,
            format!("invalid machines.<label> overrides — {detail}"),
            "edit cluster.json: max_agents must be 0..=8, kinds_override a non-empty list of kind names",
        );
        return;
    }
    let unknown = info.unknown_machine_overrides(&known_machine_labels(state_dir));
    if unknown.is_empty() {
        push(
            checks,
            "machine caps",
            CheckStatus::Pass,
            format!(
                "{} machine override(s) valid (max_agents ≤ {})",
                info.machines.len(),
                cluster_core::scheduler::MAX_AGENTS_CEILING
            ),
        );
    } else {
        let labels = unknown
            .iter()
            .map(|l| format!("machines.{l}"))
            .collect::<Vec<_>>()
            .join(", ");
        push_hint(
            checks,
            "machine caps",
            CheckStatus::Warn,
            format!(
                "{labels} override(s) target unknown machine label(s) — the scheduler ignores them"
            ),
            "remove dead config: `herdcluster config unset machines.<label>`",
        );
    }
}

/// Labels a `machines.<label>` override may legally target: `local`
/// plus every `workers.json` entry. Read-only parse — deliberately not
/// [`registry::load_workers`], which would quarantine a corrupt file
/// as a side effect before the "workers registered" check reports it.
fn known_machine_labels(state_dir: &Path) -> std::collections::BTreeSet<String> {
    let mut known = std::collections::BTreeSet::from([LOCAL_LABEL.to_string()]);
    let path = state_dir.join(registry::WORKERS_FILE_NAME);
    if let Ok(raw) = std::fs::read_to_string(&path) {
        if let Ok(file) = serde_json::from_str::<registry::WorkersFile>(&raw) {
            known.extend(file.workers.iter().map(|w| w.label.clone()));
        }
    }
    known
}

// ---- 2. state dir layout ----

fn check_state_layout(state_dir: &Path, checks: &mut Vec<CheckResult>) {
    let mut missing: Vec<String> = Vec::new();
    if !state_dir.is_dir() {
        missing.push(state_dir.display().to_string());
    }
    for sub in ["credentials", "runtime"] {
        let p = state_dir.join(sub);
        if !p.is_dir() {
            missing.push(p.display().to_string());
        }
    }
    if missing.is_empty() {
        push(
            checks,
            "state dir layout",
            CheckStatus::Pass,
            format!(
                "{} exists with credentials/ + runtime/",
                state_dir.display()
            ),
        );
    } else {
        push_hint(
            checks,
            "state dir layout",
            CheckStatus::Warn,
            format!("missing: {}", missing.join(", ")),
            "run `herdcluster doctor --fix` to create them",
        );
    }
}

// ---- 3. sensitive file permissions (unix) ----

#[cfg(unix)]
fn sensitive_files(state_dir: &Path) -> Vec<PathBuf> {
    let mut files = vec![
        state_dir.join("identity.key"),
        state_dir.join(registry::CLUSTER_FILE_NAME),
        state_dir.join(registry::WORKERS_FILE_NAME),
        state_dir.join("worker_credential.json"),
        state_dir.join("worker.json"),
    ];
    if let Ok(rd) = std::fs::read_dir(state_dir.join("credentials")) {
        files.extend(rd.flatten().map(|e| e.path()));
    }
    files.retain(|p| p.is_file());
    files
}

#[cfg(unix)]
fn world_readable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|m| m.permissions().mode() & 0o077 != 0)
        .unwrap_or(false)
}

#[cfg(unix)]
fn check_sensitive_perms(state_dir: &Path, checks: &mut Vec<CheckResult>) {
    let loose: Vec<PathBuf> = sensitive_files(state_dir)
        .into_iter()
        .filter(|p| world_readable(p))
        .collect();
    if loose.is_empty() {
        push(
            checks,
            "file permissions",
            CheckStatus::Pass,
            "sensitive files are 0600",
        );
    } else {
        let names = loose
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        push_hint(
            checks,
            "file permissions",
            CheckStatus::Warn,
            format!("group/other-readable: {names}"),
            "run `herdcluster doctor --fix` to chmod 0600",
        );
    }
}

#[cfg(not(unix))]
fn check_sensitive_perms(_state_dir: &Path, checks: &mut Vec<CheckResult>) {
    push(
        checks,
        "file permissions",
        CheckStatus::Pass,
        "not applicable on this platform",
    );
}

// ---- 4. local herdr API ----

fn check_local_herdr(checks: &mut Vec<CheckResult>) {
    let client = match api_client::ApiClient::connect_default() {
        Ok(c) => c,
        Err(e) => {
            let (status, hint) = if e.is_server_not_running() {
                (
                    CheckStatus::Fail,
                    "start herdr on this machine (`herdr`), or run `herdcluster worker` which supervises it",
                )
            } else {
                (
                    CheckStatus::Fail,
                    "check the herdr socket path (HERDR_SOCKET_PATH)",
                )
            };
            push_hint(checks, "local herdr api", status, e.to_string(), hint);
            return;
        }
    };
    match api_client::compat::check_compat(&client) {
        Ok(info) => push(
            checks,
            "local herdr api",
            CheckStatus::Pass,
            format!(
                "pong — herdr {} protocol {}",
                info.version.as_deref().unwrap_or("?"),
                info.protocol
            ),
        ),
        Err(api_client::ApiError::IncompatibleProtocol { protocol }) => {
            push_hint(
                checks,
                "local herdr api",
                CheckStatus::Fail,
                format!("herdr API protocol {protocol} is outside the supported range"),
                "update herdcluster to a version validated against this herdr",
            );
        }
        Err(e) => {
            push_hint(
                checks,
                "local herdr api",
                CheckStatus::Fail,
                format!("ping failed: {e}"),
                "restart herdr and re-run doctor",
            );
        }
    }
}

// ---- 5. LAN discovery ----

async fn check_lan_discovery(checks: &mut Vec<CheckResult>) {
    let found = cluster_net::discovery::discover_controllers(DISCOVERY_PROBE_TIMEOUT).await;
    if found.is_empty() {
        push_hint(
            checks,
            "lan discovery",
            CheckStatus::Warn,
            "no herdcluster controllers discovered via mDNS/UDP broadcast",
            "ensure both machines share a LAN; on WSL2 check mirrored networking or use `join --controller <addr>`",
        );
    } else {
        let names: Vec<String> = found
            .iter()
            .map(|c| c.cluster_name.clone().unwrap_or_else(|| c.hostname.clone()))
            .collect();
        push(
            checks,
            "lan discovery",
            CheckStatus::Pass,
            format!(
                "{} controller(s) visible: {}",
                found.len(),
                names.join(", ")
            ),
        );
    }
}

// ---- 6. workers registered ----

/// Load `workers.json` and return the entries in scope for this run
/// (all workers, or just `opts.machine`). `None` entries → the check
/// already recorded why.
fn check_workers_registered(
    state_dir: &Path,
    machine: Option<&str>,
    checks: &mut Vec<CheckResult>,
) -> Vec<WorkerEntry> {
    let path = state_dir.join(registry::WORKERS_FILE_NAME);
    let outcome = match registry::load_workers(&path) {
        Ok(o) => o,
        Err(e) => {
            push_hint(
                checks,
                "workers registered",
                CheckStatus::Fail,
                format!("cannot load workers.json: {e}"),
                format!("restore or delete {} and re-pair workers", path.display()),
            );
            return Vec::new();
        }
    };

    let mut file = outcome.value;
    file.workers.retain(|w| w.label != LOCAL_LABEL);
    let now = Utc::now();
    for w in &mut file.workers {
        w.status = registry::derive_status(w.last_seen, now);
    }

    if let Some(warning) = &outcome.warning {
        push_hint(
            checks,
            "workers registered",
            CheckStatus::Warn,
            format!("workers.json needed repair: {warning}"),
            "inspect the quarantined workers.json.bad file",
        );
        return Vec::new();
    }

    // Scope filter: `--machine <label>` limits worker checks to that
    // label only. `"local"` is the controller itself — remote workers
    // are out of scope entirely, so no `machine <label>` endpoint
    // checks are emitted. `None` keeps the whole cluster.
    let scoped: Vec<WorkerEntry> = match machine {
        Some(LOCAL_LABEL) => {
            push(
                checks,
                "workers registered",
                CheckStatus::Pass,
                "scoped to 'local' — remote worker checks skipped",
            );
            return Vec::new();
        }
        Some(label) => {
            let hits: Vec<WorkerEntry> = file
                .workers
                .iter()
                .filter(|w| w.label == label)
                .cloned()
                .collect();
            if hits.is_empty() {
                push_hint(
                    checks,
                    "workers registered",
                    CheckStatus::Fail,
                    format!("no worker labeled '{label}' in workers.json"),
                    "check `herdcluster machines` for valid labels",
                );
                return Vec::new();
            }
            hits
        }
        None => file.workers.clone(),
    };

    if scoped.is_empty() {
        push_hint(
            checks,
            "workers registered",
            CheckStatus::Warn,
            "no workers paired yet",
            "run `herdcluster join` on the worker machine",
        );
        return Vec::new();
    }

    let online = scoped
        .iter()
        .filter(|w| w.status == WorkerStatus::Online)
        .count();
    let degraded = scoped
        .iter()
        .filter(|w| w.status == WorkerStatus::Degraded)
        .count();
    let offline = scoped.len() - online - degraded;
    let detail = format!(
        "{} worker(s): {online} online, {degraded} degraded, {offline} offline",
        scoped.len()
    );
    let (status, hint) = if online > 0 {
        (CheckStatus::Pass, None)
    } else {
        (
            CheckStatus::Warn,
            Some("workers exist but none are heartbeating — check `herdcluster worker` on each machine"),
        )
    };
    match hint {
        Some(h) => push_hint(checks, "workers registered", status, detail, h),
        None => push(checks, "workers registered", status, detail),
    }
    scoped
}

// ---- 7. per-machine bridge endpoint probes ----

fn check_machine_endpoints(
    state_dir: &Path,
    workers: &[WorkerEntry],
    checks: &mut Vec<CheckResult>,
) {
    for w in workers {
        let name = format!("machine {}", w.label);
        let sock = bridge_socket_path(state_dir, &w.label);

        if w.status == WorkerStatus::Offline {
            let last = w
                .last_seen
                .map(|t| t.to_rfc3339())
                .unwrap_or_else(|| "never".into());
            push_hint(
                checks,
                &name,
                CheckStatus::Warn,
                format!("worker offline (last seen {last}); bridge not probed"),
                "start `herdcluster worker` on that machine and check LAN connectivity",
            );
            continue;
        }

        let client = match api_client::ApiClient::connect(&sock) {
            Ok(c) => c,
            Err(e) => {
                push_hint(
                    checks,
                    &name,
                    CheckStatus::Warn,
                    format!(
                        "worker {} but no bridge socket at {} ({e})",
                        status_word(w.status),
                        sock.display()
                    ),
                    "restart `herdcluster init` so the controller rebinds the bridge",
                );
                continue;
            }
        };

        match client.request_with_timeout("ping", &serde_json::json!({}), BRIDGE_PROBE_TIMEOUT) {
            Ok(pong) => {
                let protocol = pong.get("protocol").and_then(|v| v.as_u64());
                let mut detail = format!(
                    "reachable via bridge — herdr protocol {}",
                    protocol
                        .map(|p| p.to_string())
                        .unwrap_or_else(|| "?".into())
                );
                if let Some(kinds) = w
                    .resources
                    .as_ref()
                    .map(|r| r.kinds.join(", "))
                    .filter(|k| !k.is_empty())
                {
                    detail.push_str(&format!("; agent kinds: {kinds}"));
                }
                push(checks, &name, CheckStatus::Pass, detail);
            }
            Err(e) => {
                push_hint(
                    checks,
                    &name,
                    CheckStatus::Warn,
                    format!("bridge socket present but API ping failed: {e}"),
                    "if this is an auth failure, re-pair the worker: run `herdcluster init` for a fresh code, then `herdcluster join` on the worker",
                );
            }
        }
    }
}

fn status_word(s: WorkerStatus) -> &'static str {
    match s {
        WorkerStatus::Online => "online",
        WorkerStatus::Degraded => "degraded",
        WorkerStatus::Offline => "offline",
    }
}

// ---- 7b. machine capacity vs live agent counts ----

/// Count items in a list-style API response (`{"agents": [...]}`, bare
/// array, …). Mirrors `cluster-fleet`'s defensive `items_array` (private
/// there); kept local so doctor does not depend on fleet internals.
fn agent_list_len(value: &serde_json::Value) -> usize {
    if let Some(a) = value.as_array() {
        return a.len();
    }
    const COLLECTION_KEYS: &[&str] = &["agents", "items", "results", "manifests"];
    for key in COLLECTION_KEYS {
        if let Some(a) = value.get(*key).and_then(|v| v.as_array()) {
            return a.len();
        }
    }
    0
}

/// Best-effort live `agent.list` count for one connected endpoint.
fn probe_agent_count(client: &api_client::ApiClient) -> Option<usize> {
    client
        .request_with_timeout("agent.list", &serde_json::json!({}), CAPACITY_PROBE_TIMEOUT)
        .ok()
        .map(|v| agent_list_len(&v))
}

/// Compare each in-scope machine's effective `max_agents` against its live
/// `agent.list` count (issue #7).
///
/// `machine caps` above only proves the *configured value* is well-formed;
/// this check is the one that would have caught a cap being exceeded 4x.
/// Unreachable machines are skipped — the endpoint checks already report
/// why — and the verdict is `warn` (not `fail`) because over-cap agents
/// are still running correctly, just outside policy.
fn check_machine_capacity(
    cluster: Option<&registry::ClusterInfo>,
    workers: &[WorkerEntry],
    state_dir: &Path,
    machine: Option<&str>,
    checks: &mut Vec<CheckResult>,
) {
    let Some(info) = cluster else {
        return; // the "cluster config" check already reported why
    };

    let include_local = machine.map(|m| m == LOCAL_LABEL).unwrap_or(true);
    let mut evaluated = 0usize;
    let mut violations: Vec<String> = Vec::new();

    if include_local {
        if let Ok(client) = api_client::ApiClient::connect_default() {
            if let Some(count) = probe_agent_count(&client) {
                evaluated += 1;
                let max = crate::scheduler_wire::effective_max_agents(
                    info.machines.get(LOCAL_LABEL).and_then(|o| o.max_agents),
                    None,
                );
                if count as u32 > max {
                    violations.push(format!(
                        "{LOCAL_LABEL} holds {count} agents, max_agents={max}"
                    ));
                }
            }
        }
    }

    for w in workers {
        if w.status == WorkerStatus::Offline {
            continue; // endpoint check already warned; no live count available
        }
        let sock = bridge_socket_path(state_dir, &w.label);
        let Ok(client) = api_client::ApiClient::connect(&sock) else {
            continue;
        };
        let Some(count) = probe_agent_count(&client) else {
            continue;
        };
        evaluated += 1;
        let max = crate::scheduler_wire::effective_max_agents(
            info.machines.get(&w.label).and_then(|o| o.max_agents),
            w.resources.as_ref().map(|r| r.max_agents),
        );
        if count as u32 > max {
            violations.push(format!(
                "{} holds {} agents, max_agents={}",
                w.label, count, max
            ));
        }
    }

    if !violations.is_empty() {
        push_hint(
            checks,
            "machine capacity",
            CheckStatus::Warn,
            violations.join("; "),
            "stop agents or raise machines.<label>.max_agents in cluster.json",
        );
    } else if evaluated > 0 {
        push(
            checks,
            "machine capacity",
            CheckStatus::Pass,
            format!("{evaluated} machine(s) within agent caps"),
        );
    } else {
        push_hint(
            checks,
            "machine capacity",
            CheckStatus::Warn,
            "no reachable machine agent counts; caps not verified",
            "start herdr/the worker bridges, then re-run doctor",
        );
    }
}

// ---- 8. agent CLIs ----

fn check_agent_clis(checks: &mut Vec<CheckResult>) {
    let probes = crate::provisioning::probe_agent_kinds();
    if probes.is_empty() {
        push_hint(
            checks,
            "agent clis",
            CheckStatus::Warn,
            "could not determine supported agent kinds (herdr API unreachable and no fallback)",
            "start herdr, then re-run doctor",
        );
        return;
    }
    let installed: Vec<&str> = probes
        .iter()
        .filter(|p| p.installed)
        .map(|p| p.kind.as_str())
        .collect();
    let missing: Vec<&str> = probes
        .iter()
        .filter(|p| !p.installed)
        .map(|p| p.kind.as_str())
        .collect();

    if installed.is_empty() {
        let hint = missing
            .iter()
            .find_map(|k| crate::provisioning::suggested_install_command(k))
            .map(|cmd| format!("install one, e.g. `{cmd}`"))
            .unwrap_or_else(|| {
                "install an agent CLI (claude, codex, …) and ensure it is on PATH".into()
            });
        push_hint(
            checks,
            "agent clis",
            CheckStatus::Warn,
            format!("no agent CLIs on PATH ({} kind(s) supported)", probes.len()),
            hint,
        );
        return;
    }

    let mut detail = format!(
        "{}/{} kinds have a CLI on PATH: {}",
        installed.len(),
        probes.len(),
        installed.join(", ")
    );
    // Surface auth hints for installed kinds — the most common
    // "CLI present but unusable" cause.
    let hints: Vec<String> = probes
        .iter()
        .filter(|p| p.installed)
        .filter_map(|p| p.auth_hint.clone())
        .collect();
    if !hints.is_empty() && !missing.is_empty() {
        detail.push_str(&format!("; missing: {}", missing.join(", ")));
    }
    let status = if missing.is_empty() {
        CheckStatus::Pass
    } else {
        CheckStatus::Warn
    };
    if status == CheckStatus::Warn {
        push_hint(
            checks,
            "agent clis",
            status,
            detail,
            hints.first().cloned().unwrap_or_else(|| {
                "missing kinds can be installed with the commands in `herdcluster` provisioning"
                    .into()
            }),
        );
    } else {
        push(checks, "agent clis", status, detail);
    }
}

// ---- 8b. repo path (only when --repo is given) ----

/// Verify `--repo` exists as a directory on the machine running doctor.
/// Remote-machine paths cannot be checked from here — the detail says so.
fn check_repo_path(repo: Option<&str>, checks: &mut Vec<CheckResult>) {
    let Some(repo) = repo else { return };
    let path = Path::new(repo);
    if path.is_dir() {
        push(
            checks,
            "repo path",
            CheckStatus::Pass,
            format!("{repo} exists on this machine"),
        );
    } else if path.exists() {
        push_hint(
            checks,
            "repo path",
            CheckStatus::Fail,
            format!("{repo} exists but is not a directory"),
            "pass the repo root directory, not a file inside it",
        );
    } else {
        push_hint(
            checks,
            "repo path",
            CheckStatus::Fail,
            format!("{repo} does not exist on this machine"),
            "check the path — for a remote machine, run doctor there or verify the path is correct for that machine's filesystem",
        );
    }
}

// ---- 9/10. disk + memory ----

fn check_disk_space(state_dir: &Path, checks: &mut Vec<CheckResult>) {
    match disk_free_bytes(state_dir) {
        Some(free) => {
            let gib = free / (1024 * 1024 * 1024);
            let frac = free as f64 / (1024.0 * 1024.0 * 1024.0);
            let detail = format!("{frac:.1} GiB free on the state-dir filesystem");
            if free < DISK_FAIL_BYTES {
                push_hint(
                    checks,
                    "disk space",
                    CheckStatus::Fail,
                    detail,
                    "free disk space — agents need room for builds and deps",
                );
            } else if free < DISK_WARN_BYTES {
                push_hint(
                    checks,
                    "disk space",
                    CheckStatus::Warn,
                    detail,
                    "consider freeing space; under 8 GiB is tight for agent workloads",
                );
            } else {
                push(
                    checks,
                    "disk space",
                    CheckStatus::Pass,
                    format!("{gib} GiB free"),
                );
            }
        }
        None => push(
            checks,
            "disk space",
            CheckStatus::Warn,
            "could not determine free disk space on this platform",
        ),
    }
}

/// Free bytes available on the filesystem holding `path`.
#[cfg(unix)]
fn disk_free_bytes(path: &Path) -> Option<u64> {
    let out = std::process::Command::new("df")
        .arg("-kP")
        .arg(path)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    // `df -kP`: header + one line per filesystem; avail is field 4
    // (1024-blocks units).
    let line = text.lines().rfind(|l| !l.trim().is_empty())?;
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() < 4 {
        return None;
    }
    fields.get(3)?.parse::<u64>().ok().map(|kb| kb * 1024)
}

#[cfg(not(unix))]
fn disk_free_bytes(_path: &Path) -> Option<u64> {
    None
}

fn check_memory(checks: &mut Vec<CheckResult>) {
    let r = crate::resources::sample_resources();
    if r.mem_total_mb == 0 {
        push(
            checks,
            "memory",
            CheckStatus::Warn,
            "memory info unavailable on this platform",
        );
        return;
    }
    let avail_gib = r.mem_available_mb as f64 / 1024.0;
    let total_gib = r.mem_total_mb as f64 / 1024.0;
    let detail = format!("{avail_gib:.1} GiB available of {total_gib:.1} GiB");
    if r.mem_available_mb < MEM_WARN_MB {
        push_hint(
            checks,
            "memory",
            CheckStatus::Warn,
            detail,
            "close memory-heavy applications before spawning agents",
        );
    } else {
        push(checks, "memory", CheckStatus::Pass, detail);
    }
}

// ---- 11. worker daemon health (this machine as a worker) ----

fn check_worker_pairing(state_dir: &Path, checks: &mut Vec<CheckResult>) {
    match crate::worker::load_worker_info(state_dir) {
        Ok(info) => {
            let cred = cluster_net::conn::worker_credential_path(state_dir);
            if cred.exists() {
                push(
                    checks,
                    "worker pairing",
                    CheckStatus::Pass,
                    format!(
                        "paired as worker '{}' → {}",
                        info.worker_label, info.controller_addr
                    ),
                );
            } else {
                push_hint(
                    checks,
                    "worker pairing",
                    CheckStatus::Fail,
                    format!(
                        "worker.json says paired as '{}' but credential {} is missing",
                        info.worker_label,
                        cred.display()
                    ),
                    "re-run `herdcluster join` to re-pair",
                );
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            push_hint(
                checks,
                "worker pairing",
                CheckStatus::Warn,
                "this machine is not paired as a worker",
                "run `herdcluster join` here if it should be a worker (controllers can ignore)",
            );
        }
        Err(e) => {
            push_hint(
                checks,
                "worker pairing",
                CheckStatus::Warn,
                format!("worker.json unreadable: {e}"),
                "re-run `herdcluster join` to rewrite pairing metadata",
            );
        }
    }
}

// ---- 12. orchestration skill ----

fn check_orchestration_skill(checks: &mut Vec<CheckResult>) {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    match crate::skills::skills_status(&cwd) {
        crate::skills::SkillStatus::Installed => push(
            checks,
            "orchestration skill",
            CheckStatus::Pass,
            format!("bundled skill present in {}", cwd.display()),
        ),
        crate::skills::SkillStatus::Missing => {
            push_hint(
                checks,
                "orchestration skill",
                CheckStatus::Warn,
                format!("not installed in {}", cwd.display()),
                "run `herdcluster skills install` (or `herdcluster doctor --fix`)",
            );
        }
        crate::skills::SkillStatus::Diverged => {
            push_hint(
                checks,
                "orchestration skill",
                CheckStatus::Warn,
                format!(
                    "local copy in {} differs from the bundled version",
                    cwd.display()
                ),
                "review local edits, then `herdcluster skills install --overwrite` to replace",
            );
        }
    }
}

// ---------------------------------------------------------------------------
// --fix remediations
// ---------------------------------------------------------------------------

/// Apply safe remediations for the checks that are still Warn/Fail.
/// Each applied fix updates its check and appends to `fixed`.
fn apply_fixes(
    opts: &DoctorOptions,
    state_dir: &Path,
    checks: &mut [CheckResult],
    fixed: &mut Vec<String>,
) -> Result<(), DoctorError> {
    // 1. State dir layout — always safe to create.
    let needs_layout = checks
        .iter()
        .any(|c| c.name == "state dir layout" && c.status != CheckStatus::Pass);
    if needs_layout {
        let proceed = opts.yes
            || crate::join_ux::confirm("Create missing herdcluster state directories?", true);
        if proceed {
            cluster_core::config::ensure_layout()
                .map_err(|e| DoctorError::Config(e.to_string()))?;
            update_check(
                checks,
                "state dir layout",
                CheckStatus::Pass,
                format!("created layout under {}", state_dir.display()),
            );
            fixed.push(format!("created state dirs under {}", state_dir.display()));
        }
    }

    // 2. File permissions — chmod 0600 is always safe.
    #[cfg(unix)]
    {
        let needs_perms = checks
            .iter()
            .any(|c| c.name == "file permissions" && c.status != CheckStatus::Pass);
        if needs_perms {
            let proceed = opts.yes
                || crate::join_ux::confirm(
                    "Tighten permissions on sensitive herdcluster files to 0600?",
                    true,
                );
            if proceed {
                use std::os::unix::fs::PermissionsExt;
                let mut n = 0usize;
                for f in sensitive_files(state_dir) {
                    if world_readable(&f)
                        && std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600))
                            .is_ok()
                    {
                        n += 1;
                    }
                }
                if n > 0 {
                    update_check(
                        checks,
                        "file permissions",
                        CheckStatus::Pass,
                        format!("chmod 0600 applied to {n} file(s)"),
                    );
                    fixed.push(format!("fixed permissions on {n} file(s)"));
                }
            }
        }
    }

    // 3. Orchestration skill — install when missing (fresh install is
    //    safe); a diverged copy requires explicit interactive consent,
    //    which --yes deliberately does NOT grant.
    let skill_check = checks
        .iter()
        .find(|c| c.name == "orchestration skill")
        .map(|c| (c.status, c.detail.clone()));
    if let Some((status, _)) = skill_check {
        if status != CheckStatus::Pass {
            let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            match crate::skills::skills_status(&cwd) {
                crate::skills::SkillStatus::Missing => {
                    // Writes files into the user's project — unlike the
                    // state-dir/chmod fixes above, unattended (non-TTY)
                    // runs must not auto-install; --yes is the consent.
                    let proceed = opts.yes
                        || crate::join_ux::confirm(
                            "Install the herdcluster skills into this project?",
                            false,
                        );
                    if proceed {
                        // Install every bundled skill that is still missing.
                        let mut installed = Vec::new();
                        let mut failed = false;
                        for name in crate::skills::SKILL_NAMES {
                            if crate::skills::skill_status(&cwd, name)
                                != crate::skills::SkillStatus::Missing
                            {
                                continue;
                            }
                            match crate::skills::install_skill(&cwd, name, false) {
                                Ok(outcome) if outcome.skipped_reason.is_none() => {
                                    installed.push(name);
                                }
                                Ok(outcome) => {
                                    update_check(
                                        checks,
                                        "orchestration skill",
                                        CheckStatus::Warn,
                                        format!(
                                            "{name} skill install skipped: {}",
                                            outcome.skipped_reason.unwrap_or_default()
                                        ),
                                    );
                                }
                                Err(e) => {
                                    failed = true;
                                    update_check(
                                        checks,
                                        "orchestration skill",
                                        CheckStatus::Warn,
                                        format!("{name} skill install failed: {e}"),
                                    );
                                }
                            }
                        }
                        if !installed.is_empty() && !failed {
                            update_check(
                                checks,
                                "orchestration skill",
                                CheckStatus::Pass,
                                format!(
                                    "installed bundled skills at {}: {}",
                                    cwd.display(),
                                    installed.join(", ")
                                ),
                            );
                            fixed.push(format!(
                                "installed orchestration skills: {}",
                                installed.join(", ")
                            ));
                        }
                    }
                }
                crate::skills::SkillStatus::Diverged => {
                    // Never clobber user edits unattended.
                    let proceed = !opts.json
                        && !opts.yes
                        && crate::join_ux::confirm(
                            "Diverged skill copy/copies exist. Overwrite with the bundled version?",
                            false,
                        );
                    if proceed {
                        let mut replaced = Vec::new();
                        for name in crate::skills::SKILL_NAMES {
                            if crate::skills::skill_status(&cwd, name)
                                != crate::skills::SkillStatus::Diverged
                            {
                                continue;
                            }
                            if let Ok(outcome) = crate::skills::install_skill(&cwd, name, true) {
                                if outcome.skipped_reason.is_none() {
                                    replaced.push(name);
                                }
                            }
                        }
                        if !replaced.is_empty() {
                            update_check(
                                checks,
                                "orchestration skill",
                                CheckStatus::Pass,
                                format!(
                                    "replaced diverged skills at {}: {}",
                                    cwd.display(),
                                    replaced.join(", ")
                                ),
                            );
                            fixed.push(format!(
                                "replaced diverged orchestration skills: {}",
                                replaced.join(", ")
                            ));
                        }
                    }
                }
                crate::skills::SkillStatus::Installed => {}
            }
        }
    }

    Ok(())
}

/// Print re-pair / restart guidance for failures `--fix` cannot repair.
/// Informational only — never mutates state.
fn print_repair_guidance(checks: &[CheckResult]) {
    let authish = checks.iter().any(|c| {
        c.status == CheckStatus::Fail
            && (c.name.starts_with("machine ") || c.name == "worker pairing")
    });
    if authish {
        println!("guidance: worker unreachable/auth failing — to re-pair:");
        println!("  1. on the controller: `herdcluster init` shows a fresh pairing code");
        println!("  2. on the worker: `herdcluster join` (enter the new code)");
        println!("  3. `herdcluster doctor` again to confirm");
    }
    let herdr_down = checks
        .iter()
        .any(|c| c.name == "local herdr api" && c.status == CheckStatus::Fail);
    if herdr_down {
        println!("guidance: local herdr API is down — start `herdr`, or let");
        println!("  `herdcluster worker`'s runtime supervisor bring it up, then re-run doctor.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::STATE_DIR_ENV_LOCK;

    /// Serializes env-mutating doctor tests (also guards against other
    /// modules' HERDCLUSTER_STATE_DIR tests running concurrently).
    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn tmpdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hc-doctor-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn report_to_json_shape() {
        let report = DoctorReport {
            checks: vec![CheckResult {
                name: "x".into(),
                status: CheckStatus::Pass,
                detail: "fine".into(),
                fix_hint: None,
            }],
            ok: true,
            fixed: vec![],
        };
        let v = report.to_json();
        assert_eq!(v["ok"], true);
        assert_eq!(v["checks"][0]["status"], "pass");
        assert_eq!(v["checks"][0]["name"], "x");
    }

    #[test]
    fn agent_list_len_counts_list_shapes() {
        assert_eq!(agent_list_len(&serde_json::json!([])), 0);
        assert_eq!(
            agent_list_len(&serde_json::json!({"agents": [{}, {}, {}]})),
            3
        );
        assert_eq!(agent_list_len(&serde_json::json!([{}, {}])), 2);
        assert_eq!(agent_list_len(&serde_json::json!({"ok": true})), 0);
    }

    #[test]
    fn check_status_serializes_snake_case() {
        assert_eq!(
            serde_json::to_value(CheckStatus::Pass).unwrap(),
            serde_json::json!("pass")
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

    #[tokio::test]
    async fn doctor_on_empty_state_dir_does_not_panic() {
        let _g1 = STATE_DIR_ENV_LOCK.lock().await;
        let _g2 = ENV_LOCK.lock().await;
        let dir = tmpdir("empty");
        std::env::set_var(cluster_core::config::ENV_STATE_DIR, &dir);

        let report = run_doctor(DoctorOptions::default()).await.unwrap();
        // No herdr, no cluster, no workers → warnings/failures expected,
        // but the suite must complete and produce a definite verdict.
        assert!(!report.checks.is_empty());
        assert!(report.checks.iter().any(|c| c.name == "local herdr api"));
        assert!(report.checks.iter().any(|c| c.name == "workers registered"));
        // ok flag consistent with Fail presence.
        let has_fail = report.checks.iter().any(|c| c.status == CheckStatus::Fail);
        assert_eq!(report.ok, !has_fail);

        std::env::remove_var(cluster_core::config::ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn repo_path_check_only_runs_when_given() {
        let mut checks = Vec::new();
        check_repo_path(None, &mut checks);
        assert!(checks.is_empty());

        let dir = tmpdir("repo");
        let file = dir.join("file.txt");
        std::fs::write(&file, "x").unwrap();

        let mut checks = Vec::new();
        check_repo_path(Some(dir.to_str().unwrap()), &mut checks);
        assert_eq!(checks[0].status, CheckStatus::Pass);

        let mut checks = Vec::new();
        check_repo_path(Some(file.to_str().unwrap()), &mut checks);
        assert_eq!(checks[0].status, CheckStatus::Fail);
        assert!(checks[0].detail.contains("not a directory"));

        let mut checks = Vec::new();
        check_repo_path(Some(dir.join("nope").to_str().unwrap()), &mut checks);
        assert_eq!(checks[0].status, CheckStatus::Fail);
        assert!(checks[0].detail.contains("does not exist"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn doctor_machine_caps_flags_invalid_overrides() {
        let _g1 = STATE_DIR_ENV_LOCK.lock().await;
        let _g2 = ENV_LOCK.lock().await;
        let dir = tmpdir("caps");
        std::env::set_var(cluster_core::config::ENV_STATE_DIR, &dir);
        // "wsl" must be a known label (workers.json entry) or the
        // override counts as dead config and warns instead of passing.
        std::fs::write(
            dir.join(registry::WORKERS_FILE_NAME),
            serde_json::json!({
                "version": registry::WORKERS_FILE_VERSION,
                "workers": [{
                    "worker_id": "w1",
                    "label": "wsl",
                    "pubkey": "aa",
                    "paired_at": "2024-01-01T00:00:00Z",
                    "last_seen": null,
                }]
            })
            .to_string(),
        )
        .unwrap();

        // Valid override first: machine caps must pass.
        std::fs::write(
            dir.join("cluster.json"),
            serde_json::json!({
                "cluster_id": "c1",
                "name": "c1",
                "port": 47820,
                "protocol_version": 1,
                "created_at": "2024-01-01T00:00:00Z",
                "machines": { "wsl": { "max_agents": 4, "kinds_override": ["claude"] } }
            })
            .to_string(),
        )
        .unwrap();
        let report = run_doctor(DoctorOptions::default()).await.unwrap();
        let caps = report
            .checks
            .iter()
            .find(|c| c.name == "machine caps")
            .unwrap();
        assert_eq!(caps.status, CheckStatus::Pass, "{}", caps.detail);

        // Out-of-range max_agents → Fail with the label + reason.
        std::fs::write(
            dir.join("cluster.json"),
            serde_json::json!({
                "cluster_id": "c1",
                "name": "c1",
                "port": 47820,
                "protocol_version": 1,
                "created_at": "2024-01-01T00:00:00Z",
                "machines": { "wsl": { "max_agents": 99 } }
            })
            .to_string(),
        )
        .unwrap();
        let report = run_doctor(DoctorOptions::default()).await.unwrap();
        let caps = report
            .checks
            .iter()
            .find(|c| c.name == "machine caps")
            .unwrap();
        assert_eq!(caps.status, CheckStatus::Fail, "{}", caps.detail);
        assert!(caps.detail.contains("wsl"));
        assert!(caps.detail.contains("99"));
        assert!(!report.ok);

        std::env::remove_var(cluster_core::config::ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Build a [`registry::ClusterInfo`] with the given `machines` map.
    fn cluster_info_with_machines(
        machines: std::collections::BTreeMap<String, registry::MachineOverride>,
    ) -> registry::ClusterInfo {
        registry::ClusterInfo {
            cluster_id: "c1".into(),
            name: "c1".into(),
            port: 47820,
            protocol_version: 1,
            created_at: chrono::Utc::now(),
            machines,
            keepawake: true,
        }
    }

    /// Write a `workers.json` registering the given labels (offline,
    /// never-seen workers — no sockets are probed by this check).
    fn write_worker_labels(state_dir: &Path, labels: &[&str]) {
        let workers: Vec<serde_json::Value> = labels
            .iter()
            .enumerate()
            .map(|(i, label)| {
                serde_json::json!({
                    "worker_id": format!("w{i}"),
                    "label": label,
                    "pubkey": "aa",
                    "paired_at": "2024-01-01T00:00:00Z",
                    "last_seen": null,
                })
            })
            .collect();
        std::fs::write(
            state_dir.join(registry::WORKERS_FILE_NAME),
            serde_json::json!({
                "version": registry::WORKERS_FILE_VERSION,
                "workers": workers,
            })
            .to_string(),
        )
        .unwrap();
    }

    #[test]
    fn doctor_machine_caps_warns_on_dead_overrides() {
        let dir = tmpdir("deadcaps");
        write_worker_labels(&dir, &["wsl"]);

        let mut machines = std::collections::BTreeMap::new();
        machines.insert("wsl".to_string(), registry::MachineOverride::default());
        machines.insert("local".to_string(), registry::MachineOverride::default());
        machines.insert("ghost".to_string(), registry::MachineOverride::default());
        let info = cluster_info_with_machines(machines);

        let mut checks = Vec::new();
        check_machine_caps(Some(&info), &dir, &mut checks);
        assert_eq!(checks.len(), 1);
        let caps = &checks[0];
        assert_eq!(caps.status, CheckStatus::Warn, "{}", caps.detail);
        assert!(caps.detail.contains("machines.ghost"), "{}", caps.detail);
        assert!(!caps.detail.contains("machines.wsl"), "{}", caps.detail);
        let hint = caps.fix_hint.as_deref().unwrap_or("");
        assert!(hint.contains("config unset machines.<label>"), "{hint}");

        // Dead label alone (no workers.json at all) still warns — only
        // `local` is known then.
        let dir2 = tmpdir("deadcaps-noworkers");
        let mut checks = Vec::new();
        check_machine_caps(Some(&info), &dir2, &mut checks);
        assert_eq!(checks[0].status, CheckStatus::Warn);
        assert!(checks[0].detail.contains("machines.wsl"));
        assert!(checks[0].detail.contains("machines.ghost"));

        // Invalid values still Fail — the dead-label warning is a
        // separate class and never masks a value problem.
        let mut machines = std::collections::BTreeMap::new();
        machines.insert(
            "ghost".to_string(),
            registry::MachineOverride {
                max_agents: Some(99),
                kinds_override: None,
            },
        );
        let info = cluster_info_with_machines(machines);
        let mut checks = Vec::new();
        check_machine_caps(Some(&info), &dir, &mut checks);
        assert_eq!(checks[0].status, CheckStatus::Fail);
        assert!(checks[0].detail.contains("99"));

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    #[tokio::test]
    async fn doctor_fix_creates_layout_and_installs_skill() {
        let _g1 = STATE_DIR_ENV_LOCK.lock().await;
        let _g2 = ENV_LOCK.lock().await;
        let dir = tmpdir("fix");
        let project = tmpdir("fixproj");
        std::env::set_var(cluster_core::config::ENV_STATE_DIR, &dir);

        // The skill check probes the process cwd — chdir into a temp
        // project so the fix path installs there instead of polluting
        // the crate dir. cwd is process-global; doctor tests are
        // serialized by ENV_LOCK and no other test reads cwd.
        let prev = std::env::current_dir().unwrap();
        std::env::set_current_dir(&project).unwrap();
        let report = run_doctor(DoctorOptions {
            machine: None,
            fix: true,
            yes: true,
            json: true,
            repo: None,
        })
        .await
        .unwrap();
        std::env::set_current_dir(&prev).unwrap();

        // ensure_layout ran → credentials/ + runtime/ exist now.
        assert!(dir.join("credentials").is_dir());
        assert!(dir.join("runtime").is_dir());
        assert!(report.fixed.iter().any(|f| f.contains("state dirs")));
        // --yes auto-installed the missing skill into the temp project.
        assert!(
            crate::skills::agents_skill_dir(&project, crate::skills::SKILL_NAME)
                .join("SKILL.md")
                .exists()
        );
        assert!(report
            .fixed
            .iter()
            .any(|f| f.contains("orchestration skill")));

        std::env::remove_var(cluster_core::config::ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&project);
    }

    #[tokio::test]
    async fn doctor_machine_scope_unknown_label_fails() {
        let _g1 = STATE_DIR_ENV_LOCK.lock().await;
        let _g2 = ENV_LOCK.lock().await;
        let dir = tmpdir("scope");
        std::env::set_var(cluster_core::config::ENV_STATE_DIR, &dir);

        let report = run_doctor(DoctorOptions {
            machine: Some("ghost".into()),
            fix: false,
            yes: false,
            json: false,
            repo: None,
        })
        .await
        .unwrap();
        let scoped = report
            .checks
            .iter()
            .find(|c| c.name == "workers registered")
            .unwrap();
        // Either "no workers" warn or "no worker labeled ghost" fail —
        // both are definite, non-panic outcomes.
        assert!(matches!(
            scoped.status,
            CheckStatus::Warn | CheckStatus::Fail
        ));

        std::env::remove_var(cluster_core::config::ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disk_free_bytes_parses_df() {
        // On unix with df present this returns Some; elsewhere None.
        // Either way it must not panic.
        let _ = disk_free_bytes(Path::new("/"));
    }

    /// Write a `workers.json` with two never-seen (offline) workers so
    /// endpoint probes never touch a real socket.
    fn write_two_workers(state_dir: &Path) {
        let worker = |id: &str, label: &str| {
            serde_json::json!({
                "worker_id": id,
                "label": label,
                "pubkey": "aa",
                "paired_at": "2024-01-01T00:00:00Z",
                "last_seen": null,
            })
        };
        std::fs::write(
            state_dir.join(registry::WORKERS_FILE_NAME),
            serde_json::json!({
                "version": registry::WORKERS_FILE_VERSION,
                "workers": [worker("w1", "desktop-a"), worker("w2", "desktop-b")],
            })
            .to_string(),
        )
        .unwrap();
    }

    fn machine_check_names(checks: &[CheckResult]) -> Vec<&str> {
        checks
            .iter()
            .map(|c| c.name.as_str())
            .filter(|n| n.starts_with("machine "))
            .collect()
    }

    #[test]
    fn doctor_machine_local_excludes_remote_workers() {
        // B4 regression: `--machine local` must scope worker/endpoint
        // checks to the controller only — remote workers are excluded.
        let dir = tmpdir("scope-local");
        write_two_workers(&dir);

        let mut checks = Vec::new();
        let workers = check_workers_registered(&dir, Some(LOCAL_LABEL), &mut checks);
        assert!(workers.is_empty(), "local scope must return no workers");
        let reg = checks
            .iter()
            .find(|c| c.name == "workers registered")
            .unwrap();
        assert_eq!(reg.status, CheckStatus::Pass, "{}", reg.detail);
        assert!(reg.detail.contains("local"));

        // Endpoint pass must emit no `machine <label>` checks at all.
        check_machine_endpoints(&dir, &workers, &mut checks);
        assert!(
            machine_check_names(&checks).is_empty(),
            "unexpected machine checks: {:?}",
            machine_check_names(&checks)
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn doctor_machine_label_scopes_to_single_worker() {
        let dir = tmpdir("scope-label");
        write_two_workers(&dir);

        let mut checks = Vec::new();
        let workers = check_workers_registered(&dir, Some("desktop-a"), &mut checks);
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].label, "desktop-a");

        // Both workers are offline → endpoint check is a warn, and only
        // the in-scope label appears.
        check_machine_endpoints(&dir, &workers, &mut checks);
        assert_eq!(machine_check_names(&checks), vec!["machine desktop-a"]);

        // Unknown label still fails loudly.
        let mut checks = Vec::new();
        let workers = check_workers_registered(&dir, Some("ghost"), &mut checks);
        assert!(workers.is_empty());
        let reg = checks
            .iter()
            .find(|c| c.name == "workers registered")
            .unwrap();
        assert_eq!(reg.status, CheckStatus::Fail);
        assert!(reg.detail.contains("ghost"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn doctor_no_machine_scope_returns_all_workers() {
        let dir = tmpdir("scope-all");
        write_two_workers(&dir);

        let mut checks = Vec::new();
        let workers = check_workers_registered(&dir, None, &mut checks);
        assert_eq!(workers.len(), 2);
        let names = machine_check_names(&checks);
        assert!(names.is_empty()); // endpoint pass not run yet
        check_machine_endpoints(&dir, &workers, &mut checks);
        assert_eq!(
            machine_check_names(&checks),
            vec!["machine desktop-a", "machine desktop-b"]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
