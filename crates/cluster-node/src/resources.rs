//! Worker resource reporting (bead `m6-resources`, PLAN.md §3.7).
//!
//! [`sample_resources`] produces a one-shot [`cluster_core::protocol::Resources`]
//! snapshot of the local machine: CPU cores, memory totals, load average,
//! live agent count, supported agent kinds, and the configured agent cap.
//! [`ResourceMonitor`] wraps it in a periodic sampler that invokes a
//! callback on every report interval *and* promptly when the values change
//! materially — the worker daemon feeds those snapshots into `Resources`
//! control messages (PLAN.md: "every 15 s + on-change").
//!
//! Best-effort contract: every probe failure degrades to `0`/empty rather
//! than panicking or erroring — a worker with no herdr API socket simply
//! reports `agent_count: 0, kinds: []`.
//!
//! Platform notes:
//! - Memory: `/proc/meminfo` on Linux; `sysctlbyname` on macOS
//!   (`hw.memsize` total, `kern.memorystatus_level` percentage for
//!   available — an approximation, documented). Other platforms report `0`
//!   ("unknown") — the scheduler treats `mem_available_mb: 0` as no
//!   headroom, which is the safe direction.
//! - Load average: `/proc/loadavg` on Linux; `getloadavg(3)` on other
//!   unixes (incl. macOS). Windows/unknown → empty vec.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use cluster_core::protocol::Resources;
use cluster_core::scheduler::{DEFAULT_MAX_AGENTS, MAX_AGENTS_CEILING};
use serde_json::Value;

/// `mem_available_mb` delta (MiB) that counts as a material change for
/// [`materially_changed`]. Available memory fluctuates constantly; 64 MiB
/// is well below what one agent consumes but above sampler noise.
pub const MEM_CHANGE_THRESHOLD_MB: u64 = 64;

/// Fallback report interval used by callers that don't tune one
/// (PLAN.md §3.7: resources every ~15 s).
pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(15);

/// Serializes tests that mutate `HERDCLUSTER_STATE_DIR` (process-global
/// env). Shared with `scheduler_wire` tests via `pub(crate)`.
#[doc(hidden)]
pub static STATE_DIR_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---------------------------------------------------------------------------
// Sampling
// ---------------------------------------------------------------------------

/// One-shot snapshot of local machine resources in the wire shape.
///
/// Never panics and never returns an error: every subsystem probe is
/// best-effort (see module docs).
pub fn sample_resources() -> Resources {
    let (agent_count, kinds) = probe_agents();
    Resources {
        cpu_cores: cpu_cores(),
        mem_total_mb: mem_total_mb(),
        mem_available_mb: mem_available_mb(),
        agent_count,
        max_agents: configured_max_agents(),
        load_avg: load_avg(),
        kinds,
    }
}

/// CPU core count visible to this process.
fn cpu_cores() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

/// Total physical memory in MiB (`0` = unknown platform).
fn mem_total_mb() -> u64 {
    mem_info().map(|m| m.total_mb).unwrap_or(0)
}

/// Currently available memory in MiB (`0` = unknown platform).
fn mem_available_mb() -> u64 {
    mem_info().map(|m| m.available_mb).unwrap_or(0)
}

struct MemInfo {
    total_mb: u64,
    available_mb: u64,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn mem_info() -> Option<MemInfo> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let mut total_kb = None;
    let mut avail_kb = None;
    for line in text.lines() {
        let mut it = line.split_whitespace();
        match (it.next(), it.next()) {
            (Some("MemTotal:"), Some(v)) => total_kb = v.parse::<u64>().ok(),
            (Some("MemAvailable:"), Some(v)) => avail_kb = v.parse::<u64>().ok(),
            // Older kernels lack MemAvailable; fall back to MemFree.
            (Some("MemFree:"), Some(v)) if avail_kb.is_none() => avail_kb = v.parse::<u64>().ok(),
            _ => {}
        }
    }
    Some(MemInfo {
        total_mb: total_kb? / 1024,
        available_mb: avail_kb? / 1024,
    })
}

#[cfg(target_os = "macos")]
fn mem_info() -> Option<MemInfo> {
    // `hw.memsize` is reported in bytes; `kern.memorystatus_level` is a
    // percentage (0-100) of memory currently available to apps — an
    // approximation of "available", good enough for placement scoring.
    let total_bytes = sysctl_u64("hw.memsize")?;
    let level_pct = sysctl_u64("kern.memorystatus_level").unwrap_or(100);
    let total_mb = total_bytes / (1024 * 1024);
    let available_mb = total_mb.saturating_mul(level_pct.min(100)) / 100;
    Some(MemInfo {
        total_mb,
        available_mb,
    })
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_os = "macos"))
))]
fn mem_info() -> Option<MemInfo> {
    // Other unixes (FreeBSD etc.): best-effort via sysctlbyname too.
    let total_bytes = sysctl_u64("hw.physmem")?;
    Some(MemInfo {
        total_mb: total_bytes / (1024 * 1024),
        available_mb: 0,
    })
}

#[cfg(not(unix))]
fn mem_info() -> Option<MemInfo> {
    None
}

/// `sysctlbyname` wrapper returning an unsigned 64-bit (or 32-bit)
/// integer-valued sysctl. `None` on any failure.
#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
fn sysctl_u64(name: &str) -> Option<u64> {
    use std::ffi::CString;
    use std::os::raw::{c_char, c_int, c_void};

    extern "C" {
        fn sysctlbyname(
            name: *const c_char,
            oldp: *mut c_void,
            oldlenp: *mut usize,
            newp: *mut c_void,
            newlen: usize,
        ) -> c_int;
    }

    let cname = CString::new(name).ok()?;
    let mut buf = [0u8; 8];
    let mut len = buf.len();
    // SAFETY: `buf`/`len` describe a valid writable region; sysctlbyname
    // only writes up to `len` bytes and reports the real size in `len`.
    let rc = unsafe {
        sysctlbyname(
            cname.as_ptr(),
            buf.as_mut_ptr() as *mut c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return None;
    }
    match len {
        4 => Some(u32::from_ne_bytes(buf[..4].try_into().ok()?) as u64),
        8 => Some(u64::from_ne_bytes(buf)),
        _ => None,
    }
}

/// Load averages `[1min, 5min, 15min]`; empty when unknown.
fn load_avg() -> Vec<f64> {
    load_avg_impl().unwrap_or_default()
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn load_avg_impl() -> Option<Vec<f64>> {
    let text = std::fs::read_to_string("/proc/loadavg").ok()?;
    let vals: Vec<f64> = text
        .split_whitespace()
        .take(3)
        .filter_map(|s| s.parse::<f64>().ok())
        .collect();
    if vals.len() == 3 {
        Some(vals)
    } else {
        None
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
fn load_avg_impl() -> Option<Vec<f64>> {
    use std::os::raw::c_int;

    extern "C" {
        fn getloadavg(loadavg: *mut f64, nelem: c_int) -> c_int;
    }

    let mut buf = [0f64; 3];
    // SAFETY: `buf` has room for `nelem` doubles.
    let n = unsafe { getloadavg(buf.as_mut_ptr(), buf.len() as c_int) };
    if n == buf.len() as c_int {
        Some(buf.to_vec())
    } else {
        None
    }
}

#[cfg(not(unix))]
fn load_avg_impl() -> Option<Vec<f64>> {
    None
}

// ---------------------------------------------------------------------------
// Agent probing (local herdr API — best effort)
// ---------------------------------------------------------------------------

/// Per-call timeout for `server.agent_manifests` inside the resource
/// probe path. The typed `server_agent_manifests()` wrapper uses
/// [`api_client::DEFAULT_TIMEOUT`] (30 s); a wedged herdr socket that
/// accepts connections but never responds would then stall the
/// `hc-resources` sampler ~35 s per sample (5 s `agent.list` + 30 s
/// manifests), delaying [`ResourceMonitor::shutdown`] joins and
/// accumulating blocked `spawn_blocking` threads on bridge reconnects.
/// Manifests are cheap metadata — 3 s is generous for a local socket;
/// on timeout kinds degrade to `[]` exactly like any other API error.
pub const MANIFESTS_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Probe the local herdr API for `(agent_count, supported_kinds)`.
///
/// `agent_count` is the length of `agent.list`; `kinds` is
/// [`probe_agent_kinds`]. Any failure → `(0, [])`.
fn probe_agents() -> (u32, Vec<String>) {
    let client = match api_client::ApiClient::connect_default() {
        Ok(c) => c,
        Err(_) => return (0, Vec::new()),
    };
    let count = client
        .request_with_timeout("agent.list", &serde_json::json!({}), Duration::from_secs(5))
        .map(|v| items_len(&v) as u32)
        .unwrap_or(0);
    (count, agent_kinds_via(&client))
}

/// Agent kinds this machine can actually run: `server.agent_manifests`
/// kinds ∩ agent CLIs present on `PATH` (bead m6-resources /
/// m3-provisioning boundary — we detect presence only, never install or
/// touch credentials).
///
/// Best-effort: no herdr API socket, API error, or empty PATH → `[]`.
pub fn probe_agent_kinds() -> Vec<String> {
    let client = match api_client::ApiClient::connect_default() {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    agent_kinds_via(&client)
}

/// Manifest kinds filtered to CLIs found on `PATH`.
///
/// Uses [`MANIFESTS_PROBE_TIMEOUT`] rather than the client's
/// [`api_client::DEFAULT_TIMEOUT`] so a hung herdr socket cannot stall
/// the resource monitor (see the const's docs).
fn agent_kinds_via(client: &api_client::ApiClient) -> Vec<String> {
    let manifests = match client.request_with_timeout(
        "server.agent_manifests",
        &serde_json::json!({}),
        MANIFESTS_PROBE_TIMEOUT,
    ) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    manifest_kinds(&manifests)
        .into_iter()
        .filter(|k| cli_on_path(k))
        .collect()
}

/// Extract supported agent kinds from a `server.agent_manifests` response.
///
/// Mirrors the defensive walk in `cluster_fleet::spawn` (private there):
/// recursively collects string values under `kind`/`agent_kind`/`agent`/`id`/`slug`
/// keys plus string arrays under `kinds` keys. Upstream schema drift is
/// expected; when nothing parses we return empty and callers treat kinds
/// as unknown.
fn manifest_kinds(value: &Value) -> Vec<String> {
    use std::collections::BTreeSet;

    fn walk(v: &Value, out: &mut BTreeSet<String>) {
        match v {
            Value::Object(map) => {
                for key in ["kind", "agent_kind", "agent", "id", "slug"] {
                    if let Some(s) = map.get(key).and_then(Value::as_str) {
                        if !s.is_empty() {
                            out.insert(s.to_string());
                        }
                    }
                }
                if let Some(arr) = map.get("kinds").and_then(Value::as_array) {
                    for k in arr.iter().filter_map(Value::as_str) {
                        if !k.is_empty() {
                            out.insert(k.to_string());
                        }
                    }
                }
                for child in map.values() {
                    walk(child, out);
                }
            }
            Value::Array(items) => {
                for child in items {
                    walk(child, out);
                }
            }
            _ => {}
        }
    }

    let mut out = BTreeSet::new();
    walk(value, &mut out);
    out.into_iter().collect()
}

/// Count items in a list-style API response (`{"agents": [...]}`, bare
/// array, etc.). Mirrors `cluster_fleet::parse::items_array` (private).
fn items_len(value: &Value) -> usize {
    if let Some(a) = value.as_array() {
        return a.len();
    }
    const COLLECTION_KEYS: &[&str] = &[
        "agents",
        "workspaces",
        "panes",
        "worktrees",
        "items",
        "results",
        "manifests",
    ];
    for key in COLLECTION_KEYS {
        if let Some(a) = value.get(*key).and_then(Value::as_array) {
            return a.len();
        }
    }
    0
}

/// True when an executable named `kind` exists on `PATH`.
///
/// Best-effort: kind names are matched verbatim against PATH entries
/// (agent CLIs like `claude`, `codex`, `opencode` share their kind name).
/// Unusual name mappings are a provisioning-bead concern; here a miss just
/// means the kind isn't advertised.
fn cli_on_path(kind: &str) -> bool {
    if kind.is_empty() || kind.contains('/') || kind.contains('\\') {
        return false;
    }
    let paths = match std::env::var_os("PATH") {
        Some(p) => p,
        None => return false,
    };
    for dir in std::env::split_paths(&paths) {
        let candidate = dir.join(kind);
        let Ok(meta) = std::fs::metadata(&candidate) else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o111 == 0 {
                continue;
            }
        }
        return true;
    }
    false
}

/// Effective `max_agents` for the local machine: `cluster.json`
/// `machines."local".max_agents` override wins, else
/// [`DEFAULT_MAX_AGENTS`], clamped to [`MAX_AGENTS_CEILING`].
///
/// Reads `cluster.json` only when it already exists — a worker that never
/// ran `init` must not get a `cluster.json` created as a side effect of
/// sampling.
fn configured_max_agents() -> u32 {
    let path = match cluster_core::config::cluster_json_path() {
        Ok(p) => p,
        Err(_) => return DEFAULT_MAX_AGENTS,
    };
    if !path.exists() {
        return DEFAULT_MAX_AGENTS;
    }
    let reported = cluster_core::config::load_cluster()
        .ok()
        .and_then(|o| {
            o.value
                .machines
                .get(cluster_fleet::LOCAL_LABEL)
                .and_then(|m| m.max_agents)
        })
        .unwrap_or(DEFAULT_MAX_AGENTS);
    reported.min(MAX_AGENTS_CEILING)
}

// ---------------------------------------------------------------------------
// Change detection + periodic monitor
// ---------------------------------------------------------------------------

/// True when `next` differs from `prev` in a way the controller cares
/// about: agent count, kinds, cap, CPU count, total memory, or available
/// memory beyond [`MEM_CHANGE_THRESHOLD_MB`]. Load averages are excluded —
/// they fluctuate continuously and would defeat change detection.
pub fn materially_changed(prev: &Resources, next: &Resources) -> bool {
    prev.agent_count != next.agent_count
        || prev.kinds != next.kinds
        || prev.max_agents != next.max_agents
        || prev.cpu_cores != next.cpu_cores
        || prev.mem_total_mb != next.mem_total_mb
        || prev.mem_available_mb.abs_diff(next.mem_available_mb) > MEM_CHANGE_THRESHOLD_MB
}

/// Periodic resource sampler.
///
/// Spawns a dedicated thread (the probes are blocking API/socket calls —
/// keeping them off the async executor is deliberate). Behaviour:
///
/// - Samples every `check_interval = clamp(interval/4, 10ms, 2s)`.
/// - Invokes `on_update` when the sample is the first one, when
///   [`materially_changed`] reports a material difference, **or** when
///   `interval` has elapsed since the last emit — i.e. the callback fires
///   on each report tick *and* promptly on material change.
/// - [`ResourceMonitor::shutdown`] (also `Drop`) stops the thread; join
///   waits at most one `check_interval`.
pub struct ResourceMonitor {
    stop: Arc<AtomicBool>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl ResourceMonitor {
    /// Start sampling. `interval` is the periodic report cadence; change
    /// detection runs up to 4× more often (bounded, see struct docs).
    pub fn spawn(interval: Duration, on_update: impl Fn(Resources) + Send + 'static) -> Self {
        let interval = interval.max(Duration::from_millis(10));
        let check_every = (interval / 4).clamp(Duration::from_millis(10), Duration::from_secs(2));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();

        let spawned = std::thread::Builder::new()
            .name("hc-resources".to_string())
            .spawn(move || {
                let mut last_emit: Option<Resources> = None;
                let mut last_emit_at = Instant::now();
                while !stop_thread.load(Ordering::Relaxed) {
                    std::thread::sleep(check_every);
                    if stop_thread.load(Ordering::Relaxed) {
                        break;
                    }
                    let sample = sample_resources();
                    let emit = match &last_emit {
                        None => true,
                        Some(prev) => {
                            materially_changed(prev, &sample) || last_emit_at.elapsed() >= interval
                        }
                    };
                    if emit {
                        last_emit = Some(sample);
                        last_emit_at = Instant::now();
                        if let Some(r) = &last_emit {
                            on_update(r.clone());
                        }
                    }
                }
            });

        Self {
            stop,
            thread: Mutex::new(spawned.ok()),
        }
    }

    /// Stop the sampler thread and wait for it to exit (bounded by one
    /// check interval). Idempotent.
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.thread.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ResourceMonitor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn resources(agent_count: u32, mem_avail: u64, kinds: &[&str]) -> Resources {
        Resources {
            cpu_cores: 8,
            mem_total_mb: 16384,
            mem_available_mb: mem_avail,
            agent_count,
            max_agents: 4,
            load_avg: vec![0.1, 0.2, 0.3],
            kinds: kinds.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn manifest_kinds_reads_herdr_095_agent_field() {
        // Real `server.agent_manifests` shape from herdr 0.9.5 uses `agent`
        // for the kind name — regression for kinds[] reporting empty.
        let v = serde_json::json!({
            "type": "agent_manifest_status",
            "manifests": [
                {"agent": "claude", "source_kind": "remote"},
                {"agent": "codex", "source_kind": "remote"}
            ]
        });
        let kinds = manifest_kinds(&v);
        assert!(kinds.contains(&"claude".to_string()));
        assert!(kinds.contains(&"codex".to_string()));
        assert!(!kinds.contains(&"remote".to_string()));
    }

    #[cfg(unix)]
    #[test]
    fn agent_kinds_via_is_bounded_on_wedged_herdr_socket() {
        // Regression for issue #30: a herdr socket that accepts
        // connections but never responds must not stall the resource
        // probe for the full `DEFAULT_TIMEOUT` (30 s). The manifests
        // call goes through `request_with_timeout` with
        // `MANIFESTS_PROBE_TIMEOUT`, so kinds degrade to `[]` quickly.
        use std::os::unix::net::UnixListener;

        let dir = std::env::temp_dir().join(format!("hc-wedged-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("herdr.sock");
        let listener = UnixListener::bind(&sock).unwrap();

        // Accept every connection and never write a response.
        std::thread::spawn(move || {
            while let Ok((stream, _)) = listener.accept() {
                std::mem::forget(stream); // hold the fd open, stay silent
            }
        });

        let client = api_client::ApiClient::connect(&sock).unwrap();
        let start = Instant::now();
        let kinds = agent_kinds_via(&client);
        let elapsed = start.elapsed();

        assert!(kinds.is_empty(), "wedged socket must yield no kinds");
        assert!(
            elapsed < Duration::from_secs(10),
            "manifests probe took {elapsed:?}; expected ~{MANIFESTS_PROBE_TIMEOUT:?} \
             (DEFAULT_TIMEOUT is {} s)",
            api_client::DEFAULT_TIMEOUT.as_secs()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sample_resources_returns_sane_values() {
        let r = sample_resources();
        assert!(r.cpu_cores >= 1, "cpu_cores={}", r.cpu_cores);
        // Linux/macOS both report real memory; unknown platforms report 0,
        // which is still a sane (documented) value.
        assert!(r.mem_available_mb <= r.mem_total_mb.max(1) || r.mem_total_mb == 0);
        assert!(r.max_agents >= 1 && r.max_agents <= MAX_AGENTS_CEILING);
        assert!(r.load_avg.len() == 3 || r.load_avg.is_empty());
        // agent_count/kinds are best-effort; just ensure they're populated
        // shapes (possibly empty when herdr isn't running).
        assert!(r.kinds.iter().all(|k| !k.is_empty()));
    }

    #[test]
    fn materially_changed_detects_meaningful_diffs() {
        let base = resources(1, 8192, &["claude"]);
        assert!(!materially_changed(&base, &base));

        // Same values → false.
        assert!(!materially_changed(&base, &resources(1, 8192, &["claude"])));
        // Small mem drift below threshold → false.
        let mut drift = base.clone();
        drift.mem_available_mb += MEM_CHANGE_THRESHOLD_MB;
        assert!(!materially_changed(&base, &drift));
        // Agent count change → true.
        assert!(materially_changed(&base, &resources(2, 8192, &["claude"])));
        // Kinds change → true.
        assert!(materially_changed(
            &base,
            &resources(1, 8192, &["claude", "codex"])
        ));
        // Big mem drop → true.
        assert!(materially_changed(
            &base,
            &resources(1, 8192 - MEM_CHANGE_THRESHOLD_MB - 1, &["claude"])
        ));
        // Load avg alone is NOT material.
        let mut noisy = base.clone();
        noisy.load_avg = vec![99.0, 88.0, 77.0];
        assert!(!materially_changed(&base, &noisy));
    }

    #[test]
    fn monitor_fires_promptly_and_shutdown_is_clean() {
        let (tx, rx) = mpsc::channel();
        let monitor = ResourceMonitor::spawn(Duration::from_millis(60), move |r| {
            let _ = tx.send(r);
        });

        // First emit is immediate (first sample always fires).
        let first = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("monitor should emit promptly");
        assert!(first.cpu_cores >= 1);

        // Periodic emit: a second callback arrives within ~2 intervals.
        let second = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("monitor should emit on the report tick");
        assert_eq!(second.cpu_cores, first.cpu_cores);

        let start = Instant::now();
        monitor.shutdown();
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "shutdown should not hang"
        );
    }

    #[test]
    fn configured_max_agents_defaults_without_cluster_json() {
        let _g = STATE_DIR_ENV_LOCK.blocking_lock();
        let dir = std::env::temp_dir().join(format!("hc-res-{}-{}", "nojson", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var(cluster_core::config::ENV_STATE_DIR, &dir);
        assert_eq!(configured_max_agents(), DEFAULT_MAX_AGENTS);
        std::env::remove_var(cluster_core::config::ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn configured_max_agents_honors_local_override() {
        let _g = STATE_DIR_ENV_LOCK.blocking_lock();
        let dir =
            std::env::temp_dir().join(format!("hc-res-{}-{}", "override", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Minimal cluster.json with machines."local".max_agents = 7.
        let cluster = serde_json::json!({
            "cluster_id": "test",
            "name": "test",
            "port": 47820,
            "protocol_version": 1,
            "created_at": "2024-01-01T00:00:00Z",
            "machines": {
                "local": { "max_agents": 7 }
            }
        });
        std::fs::write(
            dir.join(cluster_core::registry::CLUSTER_FILE_NAME),
            serde_json::to_vec_pretty(&cluster).unwrap(),
        )
        .unwrap();
        std::env::set_var(cluster_core::config::ENV_STATE_DIR, &dir);
        assert_eq!(configured_max_agents(), 7);
        // And the ceiling still applies.
        let cluster = serde_json::json!({
            "cluster_id": "test", "name": "test", "port": 47820,
            "protocol_version": 1, "created_at": "2024-01-01T00:00:00Z",
            "machines": { "local": { "max_agents": 99 } }
        });
        std::fs::write(
            dir.join(cluster_core::registry::CLUSTER_FILE_NAME),
            serde_json::to_vec_pretty(&cluster).unwrap(),
        )
        .unwrap();
        assert_eq!(configured_max_agents(), MAX_AGENTS_CEILING);
        std::env::remove_var(cluster_core::config::ENV_STATE_DIR);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cli_on_path_semantics() {
        // "sh" exists on unix PATH; a bogus name does not.
        #[cfg(unix)]
        {
            assert!(cli_on_path("sh"));
        }
        assert!(!cli_on_path("definitely-not-a-real-cli-hc"));
        assert!(!cli_on_path(""));
        assert!(!cli_on_path("a/b"));
    }
}
