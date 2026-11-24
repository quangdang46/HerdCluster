//! Worker daemon + `herdcluster join` pairing client (bead `m3-worker`,
//! worker half of `m3-join-ux`; PLAN.md §3.5, §3.10).
//!
//! # Join flow ([`join`])
//!
//! 1. Resolve the controller: `--controller` hint wins; otherwise browse
//!    mDNS + UDP broadcast via [`cluster_net::discovery`] (multiple
//!    candidates → interactive pick, or `Ambiguous` error when
//!    non-interactive).
//! 2. Confirm with the user (skipped under `--yes`/`--json`).
//! 3. Pairing exchange over plain TCP to the controller port: the worker
//!    sends one [`Message::PairHello`] frame and waits for
//!    `PairAccept`/`PairReject`, driven by the
//!    [`cluster_core::pairing::PairingClient`] FSM. The pairing code is
//!    the bootstrap secret — it comes from the `HERDCLUSTER_PAIRING_CODE`
//!    env var or an interactive prompt (see [`ENV_PAIRING_CODE`]).
//! 4. On approval, decode the [`WorkerCredential`] carried inside
//!    `PairAccept.credential`, persist it via
//!    [`cluster_net::conn::save_worker_credential`] (0600), and record
//!    pairing metadata in `<state_dir>/worker.json`.
//!
//! # Daemon ([`run_daemon`])
//!
//! `herdcluster worker` (and the post-join daemon) runs
//! [`run_daemon`]/[`run_daemon_until`]:
//!
//! ```text
//! runtime::HerdrRuntime::ensure      (herdr server lifecycle — other bead)
//!   → api-client ping (warn-only verify of the local API socket)
//!   → WorkerClient::spawn(addr)       (TLS + mutual auth + reconnect loop)
//!   → per-session WorkerBridge        (cluster stream ↔ herdr API socket)
//!   → ResourceMonitor → Resources msgs on the control channel
//!   → run until SIGINT/SIGTERM (or the provided shutdown future)
//! ```
//!
//! Reconnect/resume is entirely inside [`cluster_net::conn::WorkerClient`]:
//! on every new [`Session`] the bridge supervisor rebuilds the
//! [`cluster_bridge::worker::WorkerBridge`] and a fresh resource sample is
//! queued. Dead sessions fail stream opens with `worker_unreachable`.
//!
//! # Pairing transport contract (for `controller.rs`)
//!
//! The worker expects the controller to accept a **plain TCP** connection
//! on the cluster port, read exactly one protocol frame
//! ([`cluster_core::protocol::encode_frame`] of `PairHello`), run its
//! [`cluster_core::pairing::PairingServer`], and reply with one frame:
//! `PairAccept { credential: base64(WorkerCredential JSON),
//! ca_fingerprint }` or `PairReject { reason }`. No TLS/mux — the worker
//! has no CA yet; the one-time code is the bootstrap secret and the
//! controller owns rate-limiting/confirm UX.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cluster_core::identity::Keypair;
use cluster_core::pairing::{self, ClientPhase, PairingClient, SystemClock};
use cluster_core::protocol::{self, Message, Resources};
use cluster_net::conn::{
    self, Session, WorkerClient, WorkerConfig, WorkerCredential, WorkerTuning,
};
use cluster_net::discovery::{self, DiscoveredController, DiscoveryError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

#[cfg(unix)]
use cluster_bridge::worker::WorkerBridge;

/// Env var carrying the pairing code for non-interactive joins
/// (`herdcluster join --yes/--json` or scripted flows). The code is the
/// one displayed by `herdcluster init` on the controller.
pub const ENV_PAIRING_CODE: &str = "HERDCLUSTER_PAIRING_CODE";

/// `<state_dir>/worker.json` — pairing metadata written by [`join`].
pub const WORKER_INFO_FILE: &str = "worker.json";
/// Schema version for [`WorkerInfo`].
pub const WORKER_INFO_VERSION: u32 = 1;

/// TCP connect deadline for the pairing exchange.
const PAIR_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Overall deadline for the PairHello → PairAccept/Reject exchange.
/// The controller's confirm window is 120 s ([`pairing::CONFIRM_TIMEOUT_MS`]);
/// 30 s of slack covers scheduling and the code TTL path.
const PAIR_ANSWER_TIMEOUT: Duration = Duration::from_millis(pairing::CONFIRM_TIMEOUT_MS + 30_000);

// ---------------------------------------------------------------------------
// Join
// ---------------------------------------------------------------------------

/// Configuration for [`join`]. Mirrors `herdcluster join` flags.
#[derive(Debug, Clone, Default)]
pub struct JoinConfig {
    /// Manual controller hint (`host`, `host:port`, or IP) — wins over
    /// discovery when set.
    pub controller: Option<String>,
    /// Non-interactive mode: never prompt; discovery ambiguity and a
    /// missing pairing code become errors.
    pub yes: bool,
    /// JSON output mode (also suppresses prompts/prints).
    pub json: bool,
}

/// Result of a successful-or-declined [`join`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinOutcome {
    /// Pairing approved; the worker credential is stored and the daemon
    /// can run via [`run_daemon`].
    Paired {
        /// Sanitized worker label (hostname-derived).
        worker_label: String,
        /// Controller's advertised cluster name, or `"herdcluster"` when
        /// the candidate didn't advertise one.
        cluster_name: String,
    },
    /// The user declined at an interactive prompt.
    Declined,
    /// The controller rejected the pairing (`PairReject`).
    Rejected {
        /// Controller-supplied reason.
        reason: String,
    },
}

/// Errors from [`join`].
#[derive(Debug, Error)]
pub enum JoinError {
    /// Underlying I/O error (stdin, state dir, socket).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// State-dir resolution failed.
    #[error("config error: {0}")]
    Config(String),
    /// Discovery failure (no candidates, bad `--controller` value, mDNS/UDP
    /// errors are non-fatal inside discovery itself).
    #[error("discovery error: {0}")]
    Discovery(#[from] DiscoveryError),
    /// Several controllers discovered and no interactive picker or
    /// `--controller` hint is available.
    #[error("multiple herdcluster controllers found; pick one with --controller")]
    Ambiguous {
        /// The candidates that were discovered.
        candidates: Vec<DiscoveredController>,
    },
    /// A pairing code is required but none was provided
    /// ([`ENV_PAIRING_CODE`] unset and non-interactive).
    #[error("pairing code required: set {ENV_PAIRING_CODE} or run interactively")]
    MissingPairingCode,
    /// This machine already holds a worker credential.
    #[error("already paired (credential at {0}); revoke or remove it before re-joining")]
    AlreadyPaired(PathBuf),
    /// Pairing transport or FSM failure (connect, timeout, protocol,
    /// unexpected message, expired session).
    #[error("pairing failed: {0}")]
    Pairing(String),
    /// Credential decode/persist failure.
    #[error("credential error: {0}")]
    Credential(String),
}

/// Run the full join flow against the default herdcluster state dir.
///
/// Pairing only — the long-running daemon is [`run_daemon`].
pub async fn join(cfg: JoinConfig) -> Result<JoinOutcome, JoinError> {
    let state_dir =
        cluster_core::config::state_dir().map_err(|e| JoinError::Config(e.to_string()))?;
    join_in_state_dir(cfg, state_dir).await
}

/// [`join`] against an explicit state dir — the testable core and the
/// entry point for callers that already resolved the dir.
pub async fn join_in_state_dir(
    cfg: JoinConfig,
    state_dir: PathBuf,
) -> Result<JoinOutcome, JoinError> {
    std::fs::create_dir_all(&state_dir)?;

    // Already paired → refuse rather than silently rotate the credential.
    if conn::load_worker_credential(&state_dir).is_ok() {
        return Err(JoinError::AlreadyPaired(conn::worker_credential_path(
            &state_dir,
        )));
    }

    // Identity is needed for the PairHello pubkey; created once and reused
    // by WorkerClient::load for the daemon's lifetime.
    let identity = Keypair::load_or_create(&state_dir.join("identity.key"))
        .map_err(|e| JoinError::Credential(e.to_string()))?;

    let (addr, cluster_name) = resolve_target(&cfg).await?;

    if interactive(&cfg) && !confirm_pairing(addr, cluster_name.as_deref())? {
        return Ok(JoinOutcome::Declined);
    }

    let code = pairing_code(&cfg)?;
    let label = worker_label();
    let caps = crate::resources::probe_agent_kinds();

    let exchange = pairing_exchange(addr, &identity, &code, &label, caps).await?;
    let credential = match exchange {
        PairingExchange::Accepted(c) => c,
        PairingExchange::Rejected { reason } => {
            return Ok(JoinOutcome::Rejected { reason });
        }
    };

    conn::save_worker_credential(&state_dir, &credential)
        .map_err(|e| JoinError::Credential(e.to_string()))?;
    let info = WorkerInfo {
        version: WORKER_INFO_VERSION,
        worker_label: label.clone(),
        controller_addr: addr.to_string(),
        paired_at_unix: unix_now(),
        cluster_name: cluster_name.clone(),
    };
    save_worker_info(&state_dir, &info).map_err(JoinError::Io)?;

    if !cfg.json {
        println!("Paired → worker '{}' online", label);
        println!(
            "  cluster: {}",
            cluster_name.as_deref().unwrap_or("herdcluster")
        );
        println!("  controller: {addr}");
        println!("Next: run `herdcluster worker` to start the worker daemon.");
    }

    Ok(JoinOutcome::Paired {
        worker_label: label,
        cluster_name: cluster_name.unwrap_or_else(|| "herdcluster".to_string()),
    })
}

fn interactive(cfg: &JoinConfig) -> bool {
    !(cfg.yes || cfg.json)
}

/// Resolve which controller to pair with: manual hint > single discovery
/// candidate > interactive pick > `Ambiguous` error.
async fn resolve_target(cfg: &JoinConfig) -> Result<(SocketAddr, Option<String>), JoinError> {
    if let Some(hint) = cfg.controller.as_deref() {
        let (host, port) = discovery::parse_controller_arg(hint)?;
        let addr = resolve_host_port(&host, port).await?;
        return Ok((addr, None));
    }

    let candidates = discovery::discover_controllers(discovery::DEFAULT_DISCOVERY_TIMEOUT).await;
    match candidates.len() {
        0 => Err(DiscoveryError::NoneFound.into()),
        1 => {
            let c = &candidates[0];
            Ok((c.addr, c.cluster_name.clone()))
        }
        _ => {
            if !interactive(cfg) {
                return Err(JoinError::Ambiguous { candidates });
            }
            print!("{}", discovery::format_candidates(&candidates));
            let idx = prompt_pick(candidates.len())?;
            let c = &candidates[idx];
            Ok((c.addr, c.cluster_name.clone()))
        }
    }
}

async fn resolve_host_port(host: &str, port: u16) -> Result<SocketAddr, JoinError> {
    let mut addrs = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| JoinError::Pairing(format!("cannot resolve controller '{host}': {e}")))?;
    addrs
        .next()
        .ok_or_else(|| JoinError::Pairing(format!("no address found for '{host}'")))
}

/// y/n confirmation before pairing. Returns `Ok(false)` on decline.
fn confirm_pairing(addr: SocketAddr, cluster_name: Option<&str>) -> Result<bool, JoinError> {
    let cluster = cluster_name.unwrap_or("herdcluster");
    print!("Pair this machine with {cluster} at {addr}? [y/N] ");
    use std::io::Write;
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    Ok(matches!(
        line.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

/// Interactive 1-based pick from `n` candidates.
fn prompt_pick(n: usize) -> Result<usize, JoinError> {
    use std::io::Write;
    print!("Select controller [1-{n}]: ");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    match line.trim().parse::<usize>() {
        Ok(i) if (1..=n).contains(&i) => Ok(i - 1),
        _ => Err(JoinError::Pairing(format!(
            "invalid selection '{line}' (expected 1-{n})"
        ))),
    }
}

/// Obtain the pairing code: env var first, interactive prompt otherwise.
fn pairing_code(cfg: &JoinConfig) -> Result<String, JoinError> {
    if let Ok(v) = std::env::var(ENV_PAIRING_CODE) {
        let v = v.trim().to_string();
        if !v.is_empty() {
            return Ok(v);
        }
    }
    if !interactive(cfg) {
        return Err(JoinError::MissingPairingCode);
    }
    use std::io::Write;
    print!("Pairing code shown on the controller: ");
    std::io::stdout().flush()?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line)?;
    let code = line.trim().to_string();
    if code.is_empty() {
        Err(JoinError::MissingPairingCode)
    } else {
        Ok(code)
    }
}

// ---------------------------------------------------------------------------
// Pairing wire exchange
// ---------------------------------------------------------------------------

/// Outcome of the PairHello → answer exchange.
#[derive(Debug)]
enum PairingExchange {
    /// `PairAccept` received; carries the decoded worker credential.
    Accepted(WorkerCredential),
    /// `PairReject` received.
    Rejected {
        /// Controller-supplied reason.
        reason: String,
    },
}

/// Perform the pairing exchange with a controller at `addr`.
///
/// Sends one `PairHello` frame, then reads frames until `PairAccept` or
/// `PairReject` (other control messages are ignored for forward
/// compatibility). Driven by [`PairingClient`] for phase/deadline
/// tracking per the m1-pairing FSM.
async fn pairing_exchange(
    addr: SocketAddr,
    identity: &Keypair,
    code: &str,
    hostname: &str,
    caps: Vec<String>,
) -> Result<PairingExchange, JoinError> {
    let mut stream = timeout(PAIR_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| JoinError::Pairing("connect timed out".to_string()))?
        .map_err(|e| JoinError::Pairing(format!("connect to {addr} failed: {e}")))?;

    let hello = Message::PairHello {
        code: code.to_string(),
        worker_pubkey: protocol::pubkey_to_hex(&identity.public_key_bytes()),
        hostname: hostname.to_string(),
        caps,
    };
    let frame = protocol::encode_frame(&hello)
        .map_err(|e| JoinError::Pairing(format!("encode PairHello: {e}")))?;
    timeout(PAIR_CONNECT_TIMEOUT, stream.write_all(&frame))
        .await
        .map_err(|_| JoinError::Pairing("send timed out".to_string()))?
        .map_err(JoinError::Io)?;

    let mut fsm = PairingClient::new(SystemClock);
    fsm.begin(PAIR_ANSWER_TIMEOUT.as_millis() as u64)
        .map_err(|e| JoinError::Pairing(format!("pairing client: {e}")))?;

    let result = timeout(
        PAIR_ANSWER_TIMEOUT,
        read_pairing_answer(&mut stream, &mut fsm),
    )
    .await;
    match result {
        Ok(inner) => inner,
        Err(_) => {
            // Drive the FSM to its Expired phase for a precise error.
            if matches!(fsm.tick(), ClientPhase::Expired) {
                Err(JoinError::Pairing(
                    "timed out waiting for controller answer".to_string(),
                ))
            } else {
                Err(JoinError::Pairing(
                    "timed out waiting for answer".to_string(),
                ))
            }
        }
    }
}

/// Read frames until the pairing FSM reaches a terminal phase.
async fn read_pairing_answer(
    stream: &mut TcpStream,
    fsm: &mut PairingClient,
) -> Result<PairingExchange, JoinError> {
    let mut decoder = protocol::FrameDecoder::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = stream.read(&mut buf).await.map_err(JoinError::Io)?;
        if n == 0 {
            return Err(JoinError::Pairing(
                "controller closed the connection".to_string(),
            ));
        }
        decoder.feed(&buf[..n]);
        loop {
            let msg = decoder
                .next_message()
                .map_err(|e| JoinError::Pairing(format!("bad frame: {e}")))?;
            let Some(msg) = msg else { break };
            match msg {
                Message::PairAccept {
                    credential,
                    ca_fingerprint,
                } => {
                    let raw = protocol::base64_to_bytes(&credential)
                        .map_err(|e| JoinError::Pairing(format!("bad credential: {e}")))?;
                    fsm.on_accept(raw.clone(), ca_fingerprint.clone())
                        .map_err(|e| JoinError::Pairing(format!("pairing fsm: {e}")))?;
                    if let ClientPhase::Accepted {
                        credential,
                        ca_fingerprint: fp,
                    } = fsm.phase().clone()
                    {
                        let cred = WorkerCredential::decode(&credential)
                            .map_err(|e| JoinError::Credential(e.to_string()))?;
                        // Defensive: the fingerprint inside the credential
                        // must match the one carried on the message.
                        if cred.ca_fingerprint != fp {
                            return Err(JoinError::Credential(
                                "ca_fingerprint mismatch between PairAccept and credential"
                                    .to_string(),
                            ));
                        }
                        return Ok(PairingExchange::Accepted(cred));
                    }
                    return Err(JoinError::Pairing("pairing FSM did not accept".to_string()));
                }
                Message::PairReject { reason } => {
                    fsm.on_reject(reason.clone())
                        .map_err(|e| JoinError::Pairing(format!("pairing fsm: {e}")))?;
                    return Ok(PairingExchange::Rejected { reason });
                }
                other => {
                    tracing::debug!(?other, "ignoring non-pairing control message");
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// worker.json (pairing metadata)
// ---------------------------------------------------------------------------

/// Pairing metadata persisted at `<state_dir>/worker.json`.
///
/// The credential itself lives in `worker_credential.json` (owned by
/// `cluster-net`); this file records the label + controller address the
/// daemon needs. `cluster-net`'s credential format has no label field, so
/// a separate small file is the documented choice (see module docs).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerInfo {
    /// Schema version ([`WORKER_INFO_VERSION`]).
    pub version: u32,
    /// Sanitized worker label advertised to the controller.
    pub worker_label: String,
    /// Controller address (`ip:port`) the daemon dials.
    pub controller_addr: String,
    /// Pairing time, seconds since UNIX epoch.
    pub paired_at_unix: u64,
    /// Cluster name advertised at discovery time, if any.
    #[serde(default)]
    pub cluster_name: Option<String>,
}

/// `<state_dir>/worker.json`.
pub fn worker_info_path(state_dir: &Path) -> PathBuf {
    state_dir.join(WORKER_INFO_FILE)
}

/// Save [`WorkerInfo`] atomically with `0600` permissions (unix).
pub fn save_worker_info(state_dir: &Path, info: &WorkerInfo) -> std::io::Result<()> {
    let json = serde_json::to_vec_pretty(info)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    atomic_write_600(&worker_info_path(state_dir), &json)
}

/// Load [`WorkerInfo`]; `ErrorKind::NotFound` when never paired.
pub fn load_worker_info(state_dir: &Path) -> std::io::Result<WorkerInfo> {
    let raw = std::fs::read(worker_info_path(state_dir))?;
    let info: WorkerInfo = serde_json::from_slice(&raw)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if info.version != WORKER_INFO_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported worker.json version {}", info.version),
        ));
    }
    Ok(info)
}

fn atomic_write_600(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut f = opts.open(&tmp)?;
        use std::io::Write;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Worker label / hostname
// ---------------------------------------------------------------------------

/// The worker label for a raw hostname: the fleet bridge-socket sanitize
/// convention (`cluster_fleet::endpoint::sanitize_label`), so the label is
/// always a safe socket basename.
pub fn worker_label_for(hostname: &str) -> String {
    cluster_fleet::endpoint::sanitize_label(hostname)
}

/// This machine's worker label (hostname, sanitized; `"worker"` fallback).
pub fn worker_label() -> String {
    worker_label_for(&local_hostname())
}

/// Best-effort local hostname: env vars → `gethostname(3)` on unix →
/// `"worker"`.
fn local_hostname() -> String {
    for var in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(v) = std::env::var(var) {
            let v = v.trim().to_string();
            if !v.is_empty() {
                return v;
            }
        }
    }
    #[cfg(unix)]
    if let Some(h) = unix_hostname() {
        return h;
    }
    "worker".to_string()
}

#[cfg(unix)]
fn unix_hostname() -> Option<String> {
    use std::os::raw::c_char;

    extern "C" {
        fn gethostname(name: *mut c_char, len: usize) -> i32;
    }

    let mut buf = [0i8; 256];
    // SAFETY: `buf` is a valid writable buffer of `len` bytes.
    let rc = unsafe { gethostname(buf.as_mut_ptr() as *mut c_char, buf.len()) };
    if rc != 0 {
        return None;
    }
    let s = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr() as *const c_char) }
        .to_string_lossy()
        .trim()
        .to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

// ---------------------------------------------------------------------------
// Daemon
// ---------------------------------------------------------------------------

/// Configuration for [`run_daemon`].
#[derive(Debug, Clone)]
pub struct WorkerDaemonConfig {
    /// Herdcluster state dir holding `identity.key`,
    /// `worker_credential.json`, and `worker.json`.
    pub state_dir: PathBuf,
    /// Controller address override; when `None`, read from `worker.json`.
    pub controller: Option<SocketAddr>,
    /// Reconnect/heartbeat tuning (tests). `None` → [`WorkerTuning::default`].
    pub tuning: Option<WorkerTuning>,
    /// Resource report cadence (tests). `None` →
    /// [`crate::resources::DEFAULT_REPORT_INTERVAL`] (15 s).
    pub resource_interval: Option<Duration>,
    /// Passed through to the herdr runtime supervisor (release asset base
    /// URL override; `None` → upstream default chosen by `runtime.rs`).
    pub release_base: Option<String>,
}

impl WorkerDaemonConfig {
    /// Config for `state_dir` with all tuning defaults.
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            state_dir: state_dir.into(),
            controller: None,
            tuning: None,
            resource_interval: None,
            release_base: None,
        }
    }
}

/// Errors from [`run_daemon`].
#[derive(Debug, Error)]
pub enum WorkerError {
    /// Underlying I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Herdcluster config/state-dir error.
    #[error("config error: {0}")]
    Config(String),
    /// Herdr runtime supervisor failure.
    #[error("herdr runtime error: {0}")]
    Runtime(#[from] crate::runtime::RuntimeError),
    /// Worker credential / connection-layer failure.
    #[error("cluster connection error: {0}")]
    Conn(#[from] conn::ConnError),
    /// No stored pairing credential.
    #[error("worker is not paired (missing credential at {0}); run `herdcluster join` first")]
    NotPaired(PathBuf),
    /// No controller address: `worker.json` missing/unreadable and no
    /// [`WorkerDaemonConfig::controller`] override.
    #[error(
        "no controller address: pair with `herdcluster join` or set WorkerDaemonConfig::controller"
    )]
    NoController,
}

/// Run the worker daemon until SIGINT/SIGTERM.
pub async fn run_daemon(cfg: WorkerDaemonConfig) -> Result<(), WorkerError> {
    run_daemon_until(cfg, shutdown_signal()).await
}

/// [`run_daemon`] with an explicit shutdown future — the testable core.
/// Returns `Ok(())` on clean shutdown; setup failures return early.
pub async fn run_daemon_until(
    cfg: WorkerDaemonConfig,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), WorkerError> {
    tokio::pin!(shutdown);
    std::fs::create_dir_all(&cfg.state_dir)?;

    // 1. Herdr runtime supervisor: ensure the local herdr server exists
    //    and is running before the bridge needs its API socket.
    let rt_state_dir = crate::runtime::runtime_dir(&cfg.state_dir);
    let runtime = crate::runtime::HerdrRuntime::ensure(&crate::runtime::RuntimeConfig {
        state_dir: rt_state_dir,
        release_base: cfg.release_base.clone(),
    })
    .await?;
    tracing::info!(running = runtime.is_running(), "herdr runtime ensured");

    // 2. Verify the local API socket (warn-only: the bridge reports
    //    `herdr_unavailable` per stream if it isn't up yet, and the
    //    resource monitor degrades to agent_count=0).
    verify_local_api();

    // 3. Resolve the controller address.
    let addr = resolve_daemon_addr(&cfg)?;

    // 4. Worker client + reconnect loop.
    let label = worker_label();
    let worker_cfg = WorkerConfig {
        hostname: label.clone(),
        caps: crate::resources::probe_agent_kinds(),
        resources: crate::resources::sample_resources(),
    };
    let tuning = cfg.tuning.clone().unwrap_or_default();
    let client = WorkerClient::load_with_tuning(&cfg.state_dir, worker_cfg, tuning).map_err(
        |e| match e {
            conn::ConnError::NotPaired => {
                WorkerError::NotPaired(conn::worker_credential_path(&cfg.state_dir))
            }
            other => WorkerError::Conn(other),
        },
    )?;
    let handle = Arc::new(client).spawn(addr);
    tracing::info!(%addr, %label, "worker daemon started");

    // 5. Resource monitor → Resources control messages on the current
    //    session. The monitor callback is sync; it forwards through an
    //    unbounded channel to the async sender task.
    let sess_rx = handle.session();
    let (res_tx, res_rx) = mpsc::unbounded_channel::<Resources>();
    let res_tx_monitor = res_tx.clone();
    let interval = cfg
        .resource_interval
        .unwrap_or(crate::resources::DEFAULT_REPORT_INTERVAL);
    let monitor = crate::resources::ResourceMonitor::spawn(interval, move |r| {
        let _ = res_tx_monitor.send(r);
    });
    spawn_resource_forwarder(res_rx, sess_rx.clone());

    // 6. Per-session bridge supervisor (unix only — the bridge is a unix
    //    socket proxy). Rebuilds WorkerBridge on every new Session and
    //    queues a fresh resource sample so the controller isn't stuck
    //    with the connect-time snapshot after a reconnect.
    #[cfg(unix)]
    spawn_bridge_supervisor(sess_rx.clone(), res_tx);
    #[cfg(not(unix))]
    {
        let _ = res_tx;
        let _ = sess_rx;
        tracing::warn!("cluster bridge requires unix; API streams unavailable");
    }

    // 7. Run until shutdown.
    shutdown.await;
    tracing::info!("worker daemon shutting down");
    handle.shutdown();
    monitor.shutdown();
    Ok(())
}

/// Resolve the controller address: explicit config wins, else `worker.json`.
fn resolve_daemon_addr(cfg: &WorkerDaemonConfig) -> Result<SocketAddr, WorkerError> {
    if let Some(addr) = cfg.controller {
        return Ok(addr);
    }
    match load_worker_info(&cfg.state_dir) {
        Ok(info) => info
            .controller_addr
            .parse()
            .map_err(|_| WorkerError::NoController),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(WorkerError::NoController),
        Err(e) => Err(WorkerError::Io(e)),
    }
}

/// Best-effort `ping` against the local herdr API socket. Warn-only —
/// see [`run_daemon_until`] step 2.
fn verify_local_api() {
    match api_client::ApiClient::connect_default() {
        Ok(client) => {
            if let Err(e) = client.ping() {
                tracing::warn!(error = %e, "herdr API ping failed; streams will report herdr_unavailable");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "herdr API socket not found yet; runtime may still be starting");
        }
    }
}

/// Forward sampled [`Resources`] onto the current session's control
/// channel as [`Message::Resources`]. Runs until the channel closes.
fn spawn_resource_forwarder(
    mut rx: mpsc::UnboundedReceiver<Resources>,
    sess_rx: watch::Receiver<Option<Arc<Session>>>,
) {
    tokio::spawn(async move {
        while let Some(resources) = rx.recv().await {
            let session = sess_rx.borrow().clone();
            let Some(session) = session else {
                continue; // offline/backing off — drop the report
            };
            let msg = Message::Resources {
                cpu_cores: resources.cpu_cores,
                mem_total_mb: resources.mem_total_mb,
                mem_available_mb: resources.mem_available_mb,
                agent_count: resources.agent_count,
                max_agents: resources.max_agents,
                load_avg: resources.load_avg.clone(),
                kinds: resources.kinds.clone(),
            };
            if let Err(e) = session.send_message(&msg).await {
                tracing::debug!(error = %e, "resource report send failed");
            }
        }
    });
}

/// Rebuild the [`WorkerBridge`] whenever the worker session changes.
///
/// A `None` session (offline/backoff) drops the bridge; a *new* `Arc`
/// session replaces it (the old session's streams are dead anyway).
/// On each new session a fresh [`crate::resources::sample_resources`]
/// is queued so the controller gets current data without waiting for the
/// next monitor tick.
#[cfg(unix)]
fn spawn_bridge_supervisor(
    mut sess_rx: watch::Receiver<Option<Arc<Session>>>,
    res_tx: mpsc::UnboundedSender<Resources>,
) {
    tokio::spawn(async move {
        let mut current: Option<(Arc<Session>, WorkerBridge)> = None;
        loop {
            let session = sess_rx.borrow().clone();
            match session {
                Some(s) => {
                    let same = current
                        .as_ref()
                        .map(|(old, _)| Arc::ptr_eq(old, &s))
                        .unwrap_or(false);
                    if !same {
                        match WorkerBridge::new(s.clone()) {
                            Ok(bridge) => {
                                current = Some((s, bridge));
                                let tx = res_tx.clone();
                                tokio::task::spawn_blocking(move || {
                                    let _ = tx.send(crate::resources::sample_resources());
                                });
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "worker bridge unavailable");
                                current = None;
                            }
                        }
                    }
                }
                None => current = None,
            }
            if sess_rx.changed().await.is_err() {
                break;
            }
        }
        // `current` drops here → WorkerBridge::shutdown on task exit.
    });
}

/// Resolve on SIGINT or SIGTERM (unix); SIGINT elsewhere.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = ctrl_c => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = ctrl_c.await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hc-worker-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // ---- worker label ----

    #[test]
    fn worker_label_for_sanitizes() {
        assert_eq!(worker_label_for("desktop-win"), "desktop-win");
        assert_eq!(worker_label_for("wsl2 box!"), "wsl2_box");
        assert_eq!(worker_label_for("!!!"), "worker");
        assert_eq!(worker_label_for("  "), "worker");
        assert_eq!(worker_label(), worker_label_for(&local_hostname()));
    }

    // ---- worker.json ----

    #[test]
    fn worker_info_roundtrip() {
        let dir = tmpdir("info");
        let info = WorkerInfo {
            version: WORKER_INFO_VERSION,
            worker_label: "desktop-win".into(),
            controller_addr: "192.168.1.10:47820".into(),
            paired_at_unix: 1_700_000_000,
            cluster_name: Some("home".into()),
        };
        save_worker_info(&dir, &info).unwrap();
        let loaded = load_worker_info(&dir).unwrap();
        assert_eq!(loaded, info);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(worker_info_path(&dir))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }

        // Missing file → NotFound.
        let empty = tmpdir("info-empty");
        assert_eq!(
            load_worker_info(&empty).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
    }

    #[test]
    fn worker_info_rejects_bad_version() {
        let dir = tmpdir("info-ver");
        std::fs::write(
            worker_info_path(&dir),
            br#"{"version":99,"worker_label":"x","controller_addr":"1.2.3.4:1","paired_at_unix":0}"#,
        )
        .unwrap();
        assert!(load_worker_info(&dir).is_err());
    }

    // ---- pairing exchange (loopback fake controller) ----

    /// Drive `pairing_exchange` against a fake controller that reads one
    /// frame and replies with the given message.
    async fn run_fake_controller(reply: Message) -> (SocketAddr, tokio::task::JoinHandle<Message>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Read one frame with an incremental decoder.
            let mut decoder = protocol::FrameDecoder::new();
            let mut buf = [0u8; 8192];
            let hello = loop {
                let n = sock.read(&mut buf).await.unwrap();
                assert!(n > 0, "eof before PairHello");
                decoder.feed(&buf[..n]);
                if let Some(m) = decoder.next_message().unwrap() {
                    break m;
                }
            };
            let frame = protocol::encode_frame(&reply).unwrap();
            sock.write_all(&frame).await.unwrap();
            sock.shutdown().await.unwrap();
            hello
        });
        (addr, handle)
    }

    fn test_credential() -> WorkerCredential {
        WorkerCredential {
            controller_pubkey: "ab".repeat(32),
            ca_fingerprint: "0123456789abcdef".to_string(),
            ca_cert_pem: "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
                .to_string(),
        }
    }

    #[tokio::test]
    async fn pairing_exchange_accept_stores_credential() {
        let cred = test_credential();
        let raw = cred.encode().unwrap();
        let reply = Message::PairAccept {
            credential: protocol::bytes_to_base64(&raw),
            ca_fingerprint: cred.ca_fingerprint.clone(),
        };
        let (addr, server) = run_fake_controller(reply).await;

        let identity = Keypair::from_seed(&[7u8; 32]);
        let exchange = pairing_exchange(
            addr,
            &identity,
            "K7QP2MXR",
            "test-host",
            vec!["claude".into()],
        )
        .await
        .unwrap();

        // Server saw a well-formed PairHello.
        let hello = server.await.unwrap();
        match hello {
            Message::PairHello {
                code,
                worker_pubkey,
                hostname,
                caps,
            } => {
                assert_eq!(code, "K7QP2MXR");
                assert_eq!(hostname, "test-host");
                assert_eq!(caps, vec!["claude".to_string()]);
                assert_eq!(
                    protocol::pubkey_from_hex(&worker_pubkey).unwrap(),
                    identity.public_key_bytes()
                );
            }
            other => panic!("expected PairHello, got {other:?}"),
        }

        match exchange {
            PairingExchange::Accepted(c) => {
                assert_eq!(c.controller_pubkey, cred.controller_pubkey);
                assert_eq!(c.ca_fingerprint, cred.ca_fingerprint);
            }
            PairingExchange::Rejected { reason } => {
                panic!("unexpected rejection: {reason}")
            }
        }
    }

    #[tokio::test]
    async fn pairing_exchange_reject_is_typed() {
        let reply = Message::PairReject {
            reason: "user denied".into(),
        };
        let (addr, _server) = run_fake_controller(reply).await;
        let identity = Keypair::from_seed(&[3u8; 32]);
        let exchange = pairing_exchange(addr, &identity, "CODE", "h", vec![])
            .await
            .unwrap();
        match exchange {
            PairingExchange::Rejected { reason } => assert_eq!(reason, "user denied"),
            PairingExchange::Accepted(_) => panic!("expected rejection"),
        }
    }

    #[tokio::test]
    async fn pairing_exchange_eof_is_error() {
        // Controller accepts then closes without answering.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            drop(sock);
        });
        let identity = Keypair::from_seed(&[9u8; 32]);
        let err = pairing_exchange(addr, &identity, "CODE", "h", vec![])
            .await
            .unwrap_err();
        // EOF/reset surfaces as Pairing (clean close) or Io (RST).
        assert!(
            matches!(err, JoinError::Pairing(_) | JoinError::Io(_)),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn join_config_defaults() {
        let cfg = JoinConfig::default();
        assert!(cfg.controller.is_none() && !cfg.yes && !cfg.json);
    }
}
