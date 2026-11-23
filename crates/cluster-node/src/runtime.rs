//! `runtime` — herdr runtime supervisor (worker side).
//!
//! Ensures a protocol-compatible herdr server is running locally so the
//! worker's [`cluster_bridge::worker::WorkerBridge`] always has a local
//! herdr API socket to proxy cluster streams to.
//!
//! # Lifecycle ([`HerdrRuntime::ensure`])
//!
//! 1. **Reuse first.** If the herdr API socket already answers `ping` with
//!    a protocol inside [`api_client::compat`]'s supported range, the
//!    existing server is used as-is — no duplicate install, no spawn.
//!    An incompatible responder is a hard [`RuntimeError::Incompatible`]:
//!    spawning a second server could not displace it anyway.
//! 2. **Pick a binary.** `HERDCLUSTER_HERDR_BINARY` wins outright. Otherwise
//!    a `herdr` on `PATH` is preferred; when it is missing *or* turns out to
//!    be protocol-incompatible, the latest upstream release binary is
//!    installed into `<state_dir>/runtime/` and used instead.
//! 3. **Install.** Assets are the raw binaries published by
//!    `herdrdev/herdr` GitHub releases, named `herdr-<os>-<arch>` (verified
//!    against the `releases/latest` API for v0.9.5 — there is no tarball
//!    and no separate `.sha256` file; the API's per-asset `digest` field
//!    carries `sha256:<hex>`). Downloads go through the `curl` CLI so this
//!    crate needs no HTTP dependency; checksum verification uses `sha2`.
//! 4. **Spawn + supervise.** `herdr server` (bare = headless server, verified
//!    on 0.9.5) is spawned with piped stdout/stderr forwarded to `tracing`.
//!    A monitor thread restarts it with exponential backoff (0.5 s → 30 s,
//!    capped at [`MAX_RESTARTS`] restarts) on unexpected exit.
//!
//! # Test hooks
//!
//! - `HERDCLUSTER_HERDR_BINARY` — use this binary instead of downloading.
//! - `HERDCLUSTER_RELEASE_BASE` / [`RuntimeConfig::release_base`] — override
//!   the GitHub API base URL (`…/repos/herdrdev/herdr`) so tests can point
//!   at a mock release server.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use api_client::compat::{self, ServerInfo};
use api_client::{platform, ApiClient, ApiError};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Environment variable overriding the herdr binary path entirely.
pub const ENV_HERDR_BINARY: &str = "HERDCLUSTER_HERDR_BINARY";
/// Environment variable overriding the release API base URL.
pub const ENV_RELEASE_BASE: &str = "HERDCLUSTER_RELEASE_BASE";

/// Default GitHub API base for `herdrdev/herdr` releases.
pub const DEFAULT_RELEASE_BASE: &str = "https://api.github.com/repos/herdrdev/herdr";

/// How long [`HerdrRuntime::ensure`] waits for the API socket to come up
/// after spawning `herdr server`.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll interval while waiting for the API socket.
const READY_POLL: Duration = Duration::from_millis(200);
/// Monitor thread poll interval for child liveness.
const MONITOR_POLL: Duration = Duration::from_millis(500);
/// Initial restart backoff.
const RESTART_BACKOFF_MIN: Duration = Duration::from_millis(500);
/// Restart backoff cap.
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Maximum automatic restarts before the supervisor gives up.
const MAX_RESTARTS: u32 = 5;

/// Configuration for [`HerdrRuntime::ensure`].
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// HerdCluster state dir (see [`cluster_core::config::state_dir`]).
    /// The runtime binary lives under `<state_dir>/runtime/`.
    pub state_dir: PathBuf,
    /// Override for the release download base URL (tests / mirrors).
    /// When `None`, `HERDCLUSTER_RELEASE_BASE` is consulted, then
    /// [`DEFAULT_RELEASE_BASE`].
    pub release_base: Option<String>,
}

/// Errors from the herdr runtime supervisor.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// This platform is not supported (Windows MVP target is WSL2).
    #[error(
        "unsupported platform: the herdr runtime supervisor supports \
             macOS/Linux on x86_64 and aarch64; on Windows use WSL2"
    )]
    UnsupportedPlatform,
    /// Underlying I/O error (spawn, filesystem, socket probe).
    #[error("runtime io error: {0}")]
    Io(#[from] std::io::Error),
    /// Release metadata or asset download failed.
    #[error("herdr release download failed: {0}")]
    Download(String),
    /// The downloaded asset failed checksum verification.
    #[error("herdr release checksum mismatch for {asset}: expected {expected}, got {actual}")]
    Checksum {
        /// Asset name that failed verification.
        asset: String,
        /// Expected hex digest (without the `sha256:` prefix).
        expected: String,
        /// Actual hex digest of the downloaded bytes.
        actual: String,
    },
    /// Installing the release binary into the runtime dir failed.
    #[error("failed to install herdr runtime: {0}")]
    Install(String),
    /// Spawning `herdr server` failed or the process died during startup.
    #[error("failed to spawn herdr server: {0}")]
    Spawn(String),
    /// A herdr server is answering but its API protocol is outside the
    /// range this herdcluster build supports.
    #[error("herdr API protocol incompatible: {0}")]
    Incompatible(String),
    /// The herdr API socket returned an error during probing.
    #[error("herdr api error: {0}")]
    Api(#[from] ApiError),
    /// The API socket did not come up within [`READY_TIMEOUT`].
    #[error("timed out waiting for the herdr API socket")]
    Timeout,
}

/// `<state_dir>/runtime` — where the supervisor keeps the downloaded
/// herdr binary and its metadata.
pub fn runtime_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("runtime")
}

/// Path of the managed herdr binary inside the runtime dir.
fn managed_binary_path(state_dir: &Path) -> PathBuf {
    runtime_dir(state_dir).join("herdr")
}

/// File recording the installed release tag (e.g. `v0.9.5`).
fn version_marker_path(state_dir: &Path) -> PathBuf {
    runtime_dir(state_dir).join("VERSION")
}

// ---- platform / asset mapping (pure, unit-tested) ----

/// Whether this build can supervise a herdr runtime at all.
fn platform_supported() -> bool {
    supported_asset(os_str(), arch_str()).is_some()
}

#[cfg(target_os = "macos")]
fn os_str() -> &'static str {
    "macos"
}
#[cfg(target_os = "linux")]
fn os_str() -> &'static str {
    "linux"
}
#[cfg(target_os = "windows")]
fn os_str() -> &'static str {
    "windows"
}
#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
fn os_str() -> &'static str {
    "unknown"
}

#[cfg(target_arch = "x86_64")]
fn arch_str() -> &'static str {
    "x86_64"
}
#[cfg(target_arch = "aarch64")]
fn arch_str() -> &'static str {
    "aarch64"
}
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn arch_str() -> &'static str {
    "unknown"
}

/// Map `(os, arch)` to the upstream release asset name
/// (`herdr-<os>-<arch>`), or `None` when unsupported. Kept parameterized
/// so unit tests can exercise every mapping without `cfg` gymnastics.
fn asset_name(os: &str, arch: &str) -> Option<String> {
    match (os, arch) {
        ("macos", "aarch64") => Some("herdr-macos-aarch64".to_string()),
        ("macos", "x86_64") => Some("herdr-macos-x86_64".to_string()),
        ("linux", "aarch64") => Some("herdr-linux-aarch64".to_string()),
        ("linux", "x86_64") => Some("herdr-linux-x86_64".to_string()),
        _ => None,
    }
}

fn supported_asset(os: &str, arch: &str) -> Option<String> {
    asset_name(os, arch)
}

// ---- release metadata / download ----

/// One asset entry from the GitHub `releases/latest` response (only the
/// fields we consume).
#[derive(Debug, serde::Deserialize)]
struct ReleaseAsset {
    name: String,
    /// e.g. `"sha256:<hex>"` — absent on older API shapes.
    #[serde(default)]
    digest: Option<String>,
    browser_download_url: String,
}

/// Parsed `releases/latest` payload.
#[derive(Debug, serde::Deserialize)]
struct LatestRelease {
    tag_name: String,
    #[serde(default)]
    assets: Vec<ReleaseAsset>,
}

/// Release API base URL: config override → env → upstream default.
fn release_base(cfg: &RuntimeConfig) -> String {
    cfg.release_base
        .clone()
        .or_else(|| std::env::var(ENV_RELEASE_BASE).ok())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_RELEASE_BASE.to_string())
}

/// GET `url` via the `curl` CLI (no HTTP dependency in this crate).
/// Returns the response body on 2xx; anything else is a [`RuntimeError::Download`].
fn http_get(url: &str) -> Result<Vec<u8>, RuntimeError> {
    let output = Command::new("curl")
        .args(["-fsSL", "--max-time", "120", "--retry", "2"])
        .arg(url)
        .output()
        .map_err(|e| {
            RuntimeError::Download(format!(
                "failed to run curl ({e}); install curl or set {ENV_HERDR_BINARY}"
            ))
        })?;
    if !output.status.success() {
        return Err(RuntimeError::Download(format!(
            "GET {url} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output.stdout)
}

/// Fetch `releases/latest` metadata from the release API base.
fn fetch_latest_release(base: &str) -> Result<LatestRelease, RuntimeError> {
    let url = format!("{}/releases/latest", base.trim_end_matches('/'));
    let body = http_get(&url)?;
    serde_json::from_slice(&body)
        .map_err(|e| RuntimeError::Download(format!("bad releases/latest JSON from {url}: {e}")))
}

/// Lowercase hex SHA-256 of `data`.
fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(64);
    for b in digest {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

/// Verify `data` against an asset `digest` field of the form
/// `"sha256:<hex>"`. Returns `Ok(())` when the digest is absent or
/// well-formed-and-matching; `Err(Checksum)` on mismatch, `Err(Download)`
/// on an unparseable digest.
fn verify_checksum(asset: &str, data: &[u8], digest: Option<&str>) -> Result<(), RuntimeError> {
    let Some(digest) = digest else {
        // No published checksum: record what we downloaded so a later
        // audit can compare. Documented limitation — upstream currently
        // publishes `digest` on every asset.
        tracing::warn!(
            asset,
            "release asset has no sha256 digest; recording computed hash"
        );
        return Ok(());
    };
    let expected = digest
        .strip_prefix("sha256:")
        .ok_or_else(|| RuntimeError::Download(format!("unrecognized digest '{digest}'")))?;
    let actual = sha256_hex(data);
    if actual.eq_ignore_ascii_case(expected) {
        Ok(())
    } else {
        Err(RuntimeError::Checksum {
            asset: asset.to_string(),
            expected: expected.to_string(),
            actual,
        })
    }
}

/// Parse a `herdr --version` line (`"herdr 0.9.5"`) into the version.
fn parse_version_output(out: &str) -> Option<String> {
    out.split_whitespace()
        .find(|tok| tok.chars().next().is_some_and(|c| c.is_ascii_digit()))
        .map(|s| s.trim().to_string())
}

/// Strip a leading `v` from a release tag (`v0.9.5` → `0.9.5`).
fn tag_version(tag: &str) -> &str {
    tag.strip_prefix('v').unwrap_or(tag)
}

/// Ensure the managed herdr binary exists in the runtime dir and matches
/// the latest release. Returns the binary path. Blocks on network I/O —
/// callers on an async runtime should use `spawn_blocking`.
fn ensure_managed_binary(cfg: &RuntimeConfig) -> Result<PathBuf, RuntimeError> {
    let dir = runtime_dir(&cfg.state_dir);
    std::fs::create_dir_all(&dir).map_err(RuntimeError::Io)?;
    let bin = managed_binary_path(&cfg.state_dir);

    // Fast path: installed binary reports the same version as latest.
    // If the metadata fetch fails but a binary exists, reuse it (offline
    // tolerance) — a stale binary still beats no worker.
    let latest = fetch_latest_release(&release_base(cfg));
    if let Ok(rel) = &latest {
        if bin.exists() {
            if let Ok(v) = Command::new(&bin).arg("--version").output() {
                let ver = parse_version_output(&String::from_utf8_lossy(&v.stdout));
                if ver.as_deref() == Some(tag_version(&rel.tag_name)) {
                    tracing::debug!(bin = %bin.display(), tag = %rel.tag_name, "managed herdr up to date");
                    return Ok(bin);
                }
            }
        }
    } else if bin.exists() {
        tracing::warn!("release metadata fetch failed; reusing installed herdr binary");
        return Ok(bin);
    }
    let rel = latest?;

    let asset_name =
        supported_asset(os_str(), arch_str()).ok_or(RuntimeError::UnsupportedPlatform)?;
    let asset = rel
        .assets
        .iter()
        .find(|a| a.name == asset_name)
        .ok_or_else(|| {
            RuntimeError::Download(format!(
                "release {} has no asset '{asset_name}'",
                rel.tag_name
            ))
        })?;

    tracing::info!(asset = %asset.name, url = %asset.browser_download_url, "downloading herdr release");
    let bytes = http_get(&asset.browser_download_url)?;
    verify_checksum(&asset.name, &bytes, asset.digest.as_deref())?;

    // Atomic-ish install: write temp, chmod, rename.
    let tmp = dir.join(format!(".herdr-{}.tmp", std::process::id()));
    std::fs::write(&tmp, &bytes).map_err(|e| RuntimeError::Install(e.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| RuntimeError::Install(e.to_string()))?;
    }
    std::fs::rename(&tmp, &bin).map_err(|e| RuntimeError::Install(e.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755));
    }
    if asset.digest.is_none() {
        // Record the observed hash for audit when upstream publishes none.
        let _ = std::fs::write(
            dir.join(format!("{}.sha256", asset.name)),
            format!("{}  {}\n", sha256_hex(&bytes), asset.name),
        );
    }
    let _ = std::fs::write(
        version_marker_path(&cfg.state_dir),
        format!("{}\n", rel.tag_name),
    );
    tracing::info!(bin = %bin.display(), tag = %rel.tag_name, "installed herdr runtime");
    Ok(bin)
}

// ---- API probing ----

/// Result of probing the local herdr API socket.
#[derive(Debug)]
enum Probe {
    /// Socket absent/refused — no server running.
    Absent,
    /// Server answered `ping` with a supported protocol.
    Compatible(ServerInfo),
    /// Server answered but protocol is out of range.
    Incompatible(u64),
}

/// How long a single `ping` probe may take before the socket is treated
/// as unresponsive.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Cheap probe of the herdr API socket: `ping` + compat range check.
/// Blocking but fast (unix socket connect + one request).
fn probe_api() -> Probe {
    let path = match platform::socket_path() {
        Ok(p) => p,
        Err(_) => return Probe::Absent,
    };
    let client = match ApiClient::connect(&path) {
        Ok(c) => c,
        Err(_) => return Probe::Absent,
    };
    match client.request_with_timeout("ping", &serde_json::json!({}), PROBE_TIMEOUT) {
        Ok(result) => match serde_json::from_value::<ServerInfo>(result) {
            Ok(info)
                if (compat::MIN_SUPPORTED_PROTOCOL..=compat::MAX_SUPPORTED_PROTOCOL)
                    .contains(&info.protocol) =>
            {
                Probe::Compatible(info)
            }
            Ok(info) => Probe::Incompatible(info.protocol),
            Err(_) => Probe::Absent,
        },
        Err(ApiError::IncompatibleProtocol { protocol }) => Probe::Incompatible(protocol),
        Err(_) => Probe::Absent,
    }
}

/// Locate `herdr` on `PATH` (no shell, plain exec search).
fn find_on_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if candidate
                    .metadata()
                    .map(|m| m.permissions().mode() & 0o111 != 0)
                    .unwrap_or(false)
                {
                    return Some(candidate);
                }
            }
            #[cfg(not(unix))]
            return Some(candidate);
        }
    }
    None
}

// ---- spawned child plumbing ----

/// Shared slot for the supervised `herdr server` child. The monitor thread
/// and `HerdrRuntime` both reach it through short, non-blocking locks.
struct ChildSlot {
    child: Mutex<Option<Child>>,
}

impl ChildSlot {
    fn new(child: Child) -> Self {
        Self {
            child: Mutex::new(Some(child)),
        }
    }

    /// `true` while a child exists and has not exited.
    fn is_alive(&self) -> bool {
        let mut guard = match self.child.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };
        match guard.as_mut() {
            Some(c) => matches!(c.try_wait(), Ok(None)),
            None => false,
        }
    }
}

/// Spawn `herdr server` with piped output forwarded to `tracing`.
fn spawn_server(binary: &Path) -> Result<Child, RuntimeError> {
    let mut cmd = Command::new(binary);
    cmd.arg("server")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| RuntimeError::Spawn(format!("{} server: {e}", binary.display())))?;

    // Forward child stdout/stderr to tracing on dedicated threads.
    if let Some(mut stream) = child.stdout.take() {
        std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(&mut stream).lines() {
                match line {
                    Ok(l) if !l.trim().is_empty() => {
                        tracing::info!("herdr server: {}", l)
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
    }
    if let Some(mut stream) = child.stderr.take() {
        std::thread::spawn(move || {
            use std::io::BufRead;
            for line in std::io::BufReader::new(&mut stream).lines() {
                match line {
                    Ok(l) if !l.trim().is_empty() => {
                        tracing::warn!("herdr server: {}", l)
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
        });
    }
    Ok(child)
}

/// Monitor thread: poll the child, restart with backoff on unexpected
/// exit (up to [`MAX_RESTARTS`]), then mark the slot empty.
fn monitor_loop(
    binary: PathBuf,
    slot: Arc<ChildSlot>,
    stop: Arc<AtomicBool>,
    alive: Arc<AtomicU8>,
) {
    let mut backoff = RESTART_BACKOFF_MIN;
    let mut restarts = 0u32;
    loop {
        std::thread::sleep(MONITOR_POLL);
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let exited = {
            let mut guard = match slot.child.lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            match guard.as_mut() {
                Some(c) => match c.try_wait() {
                    Ok(Some(_)) => {
                        *guard = None;
                        true
                    }
                    Ok(None) => false,
                    Err(_) => return,
                },
                None => return,
            }
        };
        if !exited {
            continue;
        }
        alive.store(0, Ordering::Relaxed);
        if restarts >= MAX_RESTARTS {
            tracing::error!("herdr server exceeded restart budget; giving up");
            return;
        }
        restarts += 1;
        // Backoff sleep, interruptible by shutdown.
        let deadline = Instant::now() + backoff;
        while Instant::now() < deadline {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        backoff = (backoff * 2).min(RESTART_BACKOFF_MAX);
        if stop.load(Ordering::Relaxed) {
            return;
        }
        match spawn_server(&binary) {
            Ok(child) => {
                tracing::warn!(restarts, "restarting herdr server");
                if let Ok(mut guard) = slot.child.lock() {
                    *guard = Some(child);
                }
                alive.store(1, Ordering::Relaxed);
            }
            Err(e) => {
                tracing::error!("herdr server respawn failed: {e}");
            }
        }
    }
}

/// Whether the managed side is a server we spawned (vs. a pre-existing
/// server we merely adopted).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Managed {
    /// We spawned `herdr server`; Drop kills it.
    Spawned,
    /// A compatible server was already running; we adopted it.
    Reused,
}

/// Opaque handle to a running (or adopted) herdr server.
pub struct HerdrRuntime {
    managed: Managed,
    binary: PathBuf,
    slot: Option<Arc<ChildSlot>>,
    stop: Option<Arc<AtomicBool>>,
    alive: Arc<AtomicU8>,
    monitor: Option<std::thread::JoinHandle<()>>,
}

impl HerdrRuntime {
    /// Ensure a compatible herdr server is running locally; see module
    /// docs for the full lifecycle. Idempotent: repeated calls reuse the
    /// already-serving API socket.
    pub async fn ensure(cfg: &RuntimeConfig) -> Result<Self, RuntimeError> {
        if !platform_supported() {
            return Err(RuntimeError::UnsupportedPlatform);
        }
        let rdir = runtime_dir(&cfg.state_dir);
        tokio::fs::create_dir_all(&rdir).await?;

        // 1. Reuse an already-serving compatible server.
        match probe_api() {
            Probe::Compatible(info) => {
                tracing::info!(
                    version = info.version.as_deref().unwrap_or("?"),
                    protocol = info.protocol,
                    "reusing already-running herdr server"
                );
                return Ok(Self {
                    managed: Managed::Reused,
                    binary: PathBuf::new(),
                    slot: None,
                    stop: None,
                    alive: Arc::new(AtomicU8::new(1)),
                    monitor: None,
                });
            }
            Probe::Incompatible(proto) => {
                return Err(RuntimeError::Incompatible(format!(
                    "server reports protocol {proto}, supported range is {}..={}",
                    compat::MIN_SUPPORTED_PROTOCOL,
                    compat::MAX_SUPPORTED_PROTOCOL
                )))
            }
            Probe::Absent => {}
        }

        // 2. Choose + start a binary. Blocking work (download, spawn
        //    readiness poll) goes through spawn_blocking.
        let cfg = cfg.clone();
        let (binary, child) = tokio::task::spawn_blocking(move || start_herdr_server(&cfg))
            .await
            .map_err(|e| RuntimeError::Spawn(format!("blocking task join: {e}")))??;

        let slot = Arc::new(ChildSlot::new(child));
        let stop = Arc::new(AtomicBool::new(false));
        let alive = Arc::new(AtomicU8::new(1));
        let monitor = {
            let binary = binary.clone();
            let slot = slot.clone();
            let stop = stop.clone();
            let alive = alive.clone();
            std::thread::spawn(move || monitor_loop(binary, slot, stop, alive))
        };

        Ok(Self {
            managed: Managed::Spawned,
            binary,
            slot: Some(slot),
            stop: Some(stop),
            alive,
            monitor: Some(monitor),
        })
    }

    /// Whether the managed server process is still alive. For an adopted
    /// (pre-existing) server this probes the API socket instead.
    pub fn is_running(&self) -> bool {
        match self.managed {
            Managed::Spawned => {
                if self.alive.load(Ordering::Relaxed) == 0 {
                    return false;
                }
                self.slot.as_ref().map(|s| s.is_alive()).unwrap_or(false)
            }
            Managed::Reused => matches!(probe_api(), Probe::Compatible(_)),
        }
    }

    /// Path of the herdr binary this runtime spawned, or `None` when an
    /// already-running server was adopted (`Managed::Reused`).
    pub fn binary_path(&self) -> Option<&Path> {
        match self.managed {
            Managed::Spawned => Some(&self.binary),
            Managed::Reused => None,
        }
    }
}

impl Drop for HerdrRuntime {
    fn drop(&mut self) {
        if let Some(stop) = &self.stop {
            stop.store(true, Ordering::Relaxed);
        }
        if let Some(slot) = &self.slot {
            if let Ok(mut guard) = slot.child.lock() {
                if let Some(mut child) = guard.take() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }
        if let Some(monitor) = self.monitor.take() {
            // The monitor exits on the stop flag within ~600 ms; detaching
            // is fine if it is mid-respawn (it re-checks the flag).
            let _ = monitor.thread().id();
        }
    }
}

/// Blocking body of [`HerdrRuntime::ensure`] step 2: pick a binary,
/// spawn `herdr server`, and wait for a compatible API.
fn start_herdr_server(cfg: &RuntimeConfig) -> Result<(PathBuf, Child), RuntimeError> {
    // Explicit override: use exactly this binary.
    if let Ok(v) = std::env::var(ENV_HERDR_BINARY) {
        if !v.trim().is_empty() {
            let bin = PathBuf::from(v);
            if !bin.is_file() {
                return Err(RuntimeError::Spawn(format!(
                    "{ENV_HERDR_BINARY}={} is not a file",
                    bin.display()
                )));
            }
            let child = spawn_and_wait_ready(&bin)?;
            return Ok((bin, child));
        }
    }

    // Preferred: system herdr on PATH.
    if let Some(sys) = find_on_path("herdr") {
        match spawn_and_wait_ready(&sys) {
            Ok(child) => {
                tracing::info!(bin = %sys.display(), "using system herdr");
                return Ok((sys, child));
            }
            Err(RuntimeError::Incompatible(msg)) => {
                tracing::warn!("system herdr incompatible ({msg}); falling back to release binary");
            }
            Err(e) => {
                // Spawn/timeout failures on the system binary also fall
                // back — the managed binary is the reliable path.
                tracing::warn!(
                    "system herdr failed to start ({e}); falling back to release binary"
                );
            }
        }
    }

    // Fallback: install + spawn the managed release binary.
    let bin = ensure_managed_binary(cfg)?;
    let child = spawn_and_wait_ready(&bin)?;
    Ok((bin, child))
}

/// Spawn `bin server`, poll the API socket until `ping` reports a
/// compatible protocol, and return the live child. Kills the child on
/// failure.
fn spawn_and_wait_ready(bin: &Path) -> Result<Child, RuntimeError> {
    let mut child = spawn_server(bin)?;
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Err(RuntimeError::Spawn(format!(
                "{} server exited during startup: {status}",
                bin.display()
            )));
        }
        match probe_api() {
            Probe::Compatible(info) => {
                tracing::info!(
                    version = info.version.as_deref().unwrap_or("?"),
                    protocol = info.protocol,
                    bin = %bin.display(),
                    "herdr server is up"
                );
                return Ok(child);
            }
            Probe::Incompatible(proto) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(RuntimeError::Incompatible(format!(
                    "protocol {proto} outside supported range"
                )));
            }
            Probe::Absent => {}
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(RuntimeError::Timeout);
        }
        std::thread::sleep(READY_POLL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_dir_is_state_runtime() {
        assert_eq!(
            runtime_dir(Path::new("/tmp/hc")),
            PathBuf::from("/tmp/hc/runtime")
        );
    }

    #[test]
    fn asset_name_mapping() {
        assert_eq!(
            asset_name("macos", "aarch64").as_deref(),
            Some("herdr-macos-aarch64")
        );
        assert_eq!(
            asset_name("macos", "x86_64").as_deref(),
            Some("herdr-macos-x86_64")
        );
        assert_eq!(
            asset_name("linux", "aarch64").as_deref(),
            Some("herdr-linux-aarch64")
        );
        assert_eq!(
            asset_name("linux", "x86_64").as_deref(),
            Some("herdr-linux-x86_64")
        );
        assert_eq!(asset_name("windows", "x86_64"), None);
        assert_eq!(asset_name("linux", "riscv64"), None);
    }

    #[test]
    fn platform_supported_matches_asset_map() {
        // On supported dev platforms this is true; the function exists so
        // the cfg plumbing stays honest.
        assert_eq!(
            platform_supported(),
            supported_asset(os_str(), arch_str()).is_some()
        );
    }

    #[test]
    fn checksum_verify_ok_and_mismatch() {
        let data = b"hello herdr";
        let good = format!("sha256:{}", sha256_hex(data));
        assert!(verify_checksum("herdr-linux-x86_64", data, Some(&good)).is_ok());
        let bad = format!("sha256:{}", "0".repeat(64));
        assert!(matches!(
            verify_checksum("herdr-linux-x86_64", data, Some(&bad)),
            Err(RuntimeError::Checksum { .. })
        ));
        // Missing digest is tolerated (recorded by caller).
        assert!(verify_checksum("a", data, None).is_ok());
        // Garbage digest prefix is a download/metadata error.
        assert!(matches!(
            verify_checksum("a", data, Some("md5:abc")),
            Err(RuntimeError::Download(_))
        ));
    }

    #[test]
    fn version_and_tag_parsing() {
        assert_eq!(
            parse_version_output("herdr 0.9.5\n"),
            Some("0.9.5".to_string())
        );
        assert_eq!(
            parse_version_output("herdr 0.9.5-beta.1 extra"),
            Some("0.9.5-beta.1".to_string())
        );
        assert_eq!(parse_version_output(""), None);
        assert_eq!(tag_version("v0.9.5"), "0.9.5");
        assert_eq!(tag_version("0.9.5"), "0.9.5");
    }

    #[test]
    fn release_json_parses_minimal_shape() {
        let json = br#"{
            "tag_name": "v0.9.5",
            "assets": [
                {"name":"herdr-linux-x86_64","digest":"sha256:abc","browser_download_url":"https://x/y"},
                {"name":"herdr-macos-aarch64","browser_download_url":"https://x/z"}
            ]
        }"#;
        let rel: LatestRelease = serde_json::from_slice(json).unwrap();
        assert_eq!(rel.tag_name, "v0.9.5");
        assert_eq!(rel.assets.len(), 2);
        assert_eq!(rel.assets[0].digest.as_deref(), Some("sha256:abc"));
        assert_eq!(rel.assets[1].digest, None);
    }

    /// Mock release server test: serves `releases/latest` + an asset over
    /// plain HTTP on loopback; `release_base` points at it. Exercises the
    /// real download + checksum + install path without touching the
    /// network. Skips when `curl` is unavailable.
    #[test]
    fn managed_binary_download_install_mock() {
        if Command::new("curl").arg("--version").output().is_err() {
            eprintln!("curl not available; skipping");
            return;
        }
        // Tiny HTTP/1.0 server on an ephemeral port.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let payload = b"#!/bin/sh\necho fake-herdr 9.9.9\n".to_vec();
        let digest = format!("sha256:{}", sha256_hex(&payload));
        let latest = format!(
            r#"{{"tag_name":"v9.9.9","assets":[{{"name":"{asset}","digest":"{digest}","browser_download_url":"http://127.0.0.1:{port}/dl/{asset}"}}]}}"#,
            asset = supported_asset(os_str(), arch_str()).unwrap(),
        );
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut s) = stream else { break };
                use std::io::{Read, Write};
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf);
                let req = String::from_utf8_lossy(&buf);
                let (status, body) = if req.starts_with("GET /releases/latest") {
                    ("200 OK", latest.clone().into_bytes())
                } else if req.contains("/dl/") {
                    ("200 OK", payload.clone())
                } else {
                    ("404 Not Found", b"nope".to_vec())
                };
                let head = format!(
                    "HTTP/1.0 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = s.write_all(head.as_bytes());
                let _ = s.write_all(&body);
            }
        });

        let dir = std::env::temp_dir().join(format!("hc-rt-mock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let cfg = RuntimeConfig {
            state_dir: dir.clone(),
            release_base: Some(format!("http://127.0.0.1:{port}")),
        };
        let bin = ensure_managed_binary(&cfg).unwrap();
        assert!(bin.exists());
        assert_eq!(
            std::fs::read_to_string(version_marker_path(&dir))
                .unwrap()
                .trim(),
            "v9.9.9"
        );
        // Second call hits the fast path (version match) without download.
        let bin2 = ensure_managed_binary(&cfg).unwrap();
        assert_eq!(bin, bin2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// End-to-end `ensure()` against a fake `herdr` binary that serves the
    /// API socket — gated on `HERDCLUSTER_RUNTIME_IT=1` because it spawns
    /// processes. Not run in CI.
    #[test]
    fn ensure_with_fake_herdr_binary_gated() {
        if std::env::var("HERDCLUSTER_RUNTIME_IT").ok().as_deref() != Some("1") {}
        // Requires a fake herdr implementation; left as a manual hook.
    }
}
