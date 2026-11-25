//! Connection manager: worker dial-out + controller accept side.
//!
//! # Session abstraction
//!
//! [`Session`] is what `cluster-bridge`/`cluster-node` program against. It
//! wraps a [`Mux`] plus the authenticated peer identity and offers:
//!
//! - `open_stream()` / `accept_stream()` — API byte streams (IDs ≥ 1);
//! - `send_message()` / `recv_message()` — the stream-0 control channel
//!   carrying `cluster_core::protocol::Message` frames;
//! - `spawn_heartbeat()` — periodic `Heartbeat` control messages;
//! - `wait_closed()` / `is_alive()` — connection liveness.
//!
//! # Worker side ([`WorkerClient`])
//!
//! Loads `<state_dir>/identity.key` (Ed25519 identity) and
//! `<state_dir>/worker_credential.json` (see [`WorkerCredential`], written
//! at pairing time by `cluster-node`). `connect_once()` performs:
//! TCP connect → TLS 1.3 (pinned cluster CA) → Ed25519 mutual-auth
//! challenge → `WorkerHello`. `spawn()` runs the reconnect loop:
//!
//! - heartbeat every [`WorkerTuning::heartbeat_interval`] (default 5 s);
//! - **degraded** after [`WorkerTuning::degraded_after`] (default 15 s = 3
//!   missed beats) with no inbound control traffic;
//! - **offline** after [`WorkerTuning::offline_after`] (default 30 s) →
//!   session dropped and reconnect attempted;
//! - exponential backoff [`WorkerTuning::backoff_min`] (0.5 s) doubling to
//!   [`WorkerTuning::backoff_max`] (30 s) with jitter;
//! - resume uses the stored credential — **no re-pairing**.
//!
//! On every reconnect the old [`Session`] (and therefore every
//! [`StreamHandle`] it handed out) is dead: `send`/`recv` fail with
//! [`MuxError::Closed`]/[`MuxError::ConnectionClosed`], and
//! `Session::open_stream` on a dead session fails with
//! [`ConnError::WorkerUnreachable`]. There is no silent stream resume —
//! the bridge must re-issue the API call on the new session. Use
//! [`is_unreachable_err`] to map stream errors to that typed outcome.
//!
//! # Controller side ([`Controller`])
//!
//! `bind()` loads TLS material + cluster identity, listens, and
//! `accept_loop()` spawns a task per connection: TLS accept →
//! `server_auth` → `WorkerHello` (validated: pubkey must match the
//! authenticated key, `protocol_version` must match) → session
//! registration → `ControllerEvent::WorkerConnected`. A watcher removes
//! the session and emits `WorkerDisconnected` (tagged with the dead
//! [`Session`]) when the connection dies — or when inbound control
//! traffic stays silent past the liveness threshold
//! ([`Controller::set_liveness_reap_after`]), which catches workers that
//! vanish without a TCP close. Liveness is measured on the control
//! channel, so the consumer spawned by `cluster-node`'s control pump
//! (or any `recv_message` loop) must be running for heartbeats to count.
//! Per-session heartbeats keep workers' liveness checks fed; disable with
//! [`Controller::disable_heartbeats`] (tests only).

use crate::mux::{Mux, MuxError, MuxRole, StreamHandle};
use crate::tls::{self, TlsError, TlsMaterial};
use bytes::Bytes;
use cluster_core::identity::Keypair;
use cluster_core::protocol::{self, Message, ProtocolError, Resources, PROTOCOL_VERSION};
use cluster_core::registry;
use rand_core::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;

// ---- credential (pairing artifact stored on the worker) ----

/// What the worker persists at pairing time. This is serialized as JSON
/// into the opaque `credential` bytes of `PairAccept` by `cluster-node`,
/// and stored locally at `<state_dir>/worker_credential.json` (mode 0600).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCredential {
    /// Controller's Ed25519 public key, hex-encoded — the key that signs
    /// the post-handshake auth challenge.
    pub controller_pubkey: String,
    /// Pinned cluster CA fingerprint (`PairAccept.ca_fingerprint`).
    pub ca_fingerprint: String,
    /// PEM-encoded cluster CA certificate — the TLS root store contents.
    pub ca_cert_pem: String,
}

impl WorkerCredential {
    /// Serialize to the opaque bytes carried in `PairAccept.credential`.
    pub fn encode(&self) -> Result<Vec<u8>, ConnError> {
        serde_json::to_vec(self).map_err(|e| ConnError::Credential(e.to_string()))
    }

    /// Parse from `PairAccept.credential` bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, ConnError> {
        let c: WorkerCredential =
            serde_json::from_slice(bytes).map_err(|e| ConnError::Credential(e.to_string()))?;
        c.validate()?;
        Ok(c)
    }

    fn validate(&self) -> Result<(), ConnError> {
        if self.controller_pubkey.len() != 64
            || !self
                .controller_pubkey
                .chars()
                .all(|c| c.is_ascii_hexdigit())
        {
            return Err(ConnError::Credential(
                "controller_pubkey is not 64 hex chars".to_string(),
            ));
        }
        if self.ca_fingerprint.len() != 16 || self.ca_cert_pem.is_empty() {
            return Err(ConnError::Credential(
                "incomplete CA pin material".to_string(),
            ));
        }
        Ok(())
    }

    /// The controller pubkey as raw bytes.
    pub fn controller_pubkey_bytes(&self) -> Result<[u8; 32], ConnError> {
        protocol::pubkey_from_hex(&self.controller_pubkey)
            .map_err(|e| ConnError::Credential(e.to_string()))
    }
}

/// `<state_dir>/worker_credential.json`.
pub fn worker_credential_path(state_dir: &Path) -> PathBuf {
    state_dir.join("worker_credential.json")
}

fn atomic_write_600(path: &Path, contents: &[u8]) -> Result<(), ConnError> {
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

/// Persist a worker credential (pairing result) for later reconnects.
pub fn save_worker_credential(state_dir: &Path, cred: &WorkerCredential) -> Result<(), ConnError> {
    atomic_write_600(&worker_credential_path(state_dir), &cred.encode()?)
}

/// Load the stored worker credential. `Err(ConnError::NotPaired)` when the
/// machine was never paired.
pub fn load_worker_credential(state_dir: &Path) -> Result<WorkerCredential, ConnError> {
    let path = worker_credential_path(state_dir);
    let raw = std::fs::read(&path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ConnError::NotPaired
        } else {
            ConnError::Io(e)
        }
    })?;
    WorkerCredential::decode(&raw)
}

// ---- errors ----

/// Errors from the connection layer.
#[derive(Debug, Error)]
pub enum ConnError {
    /// I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// TLS layer error.
    #[error("tls error: {0}")]
    Tls(#[from] TlsError),
    /// Mux error.
    #[error("mux error: {0}")]
    Mux(#[from] MuxError),
    /// Wire-protocol codec error.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    /// Credential encode/decode/validation failure.
    #[error("credential error: {0}")]
    Credential(String),
    /// This machine has no stored pairing credential.
    #[error("not paired: run `herdcluster join` first")]
    NotPaired,
    /// A handshake/IO step exceeded its deadline.
    #[error("timeout: {0}")]
    Timeout(&'static str),
    /// Peer spoke an incompatible protocol version.
    #[error("protocol version mismatch: peer={found} expected={expected}")]
    ProtocolVersion {
        /// Version the peer announced.
        found: u32,
        /// Version we require.
        expected: u32,
    },
    /// Unexpected control message where a specific one was required.
    #[error("unexpected control message: {0}")]
    UnexpectedMessage(String),
    /// The session/connection is gone — map to `worker_unreachable`.
    #[error("worker unreachable")]
    WorkerUnreachable,
}

/// Map a stream error observed after a reconnect to the typed
/// `worker_unreachable` outcome required by PLAN.md §3.6. Returns `true`
/// for errors that mean "the connection this stream belonged to is dead".
pub fn is_unreachable_err(err: &MuxError) -> bool {
    matches!(
        err,
        MuxError::Closed | MuxError::ConnectionClosed | MuxError::Reset
    )
}

// ---- session ----

/// A live authenticated cluster session (one TLS conn + one mux).
pub struct Session {
    mux: Mux,
    peer_pubkey: [u8; 32],
    closed: watch::Receiver<bool>,
    /// Elapsed millis (since session start) of the last inbound control
    /// message; 0 = none yet.
    last_inbound_ms: AtomicU64,
    started: Instant,
}

impl Session {
    pub(crate) fn new(mux: Mux, peer_pubkey: [u8; 32]) -> Self {
        Self {
            closed: mux.closed_watch(),
            mux,
            peer_pubkey,
            last_inbound_ms: AtomicU64::new(u64::MAX),
            started: Instant::now(),
        }
    }

    /// The authenticated peer's Ed25519 public key.
    pub fn peer_pubkey(&self) -> [u8; 32] {
        self.peer_pubkey
    }

    /// Time since the last inbound control message (or since connect).
    pub fn since_last_inbound(&self) -> Duration {
        let ms = self.last_inbound_ms.load(Ordering::Acquire);
        let now = self.started.elapsed();
        if ms == u64::MAX {
            now
        } else {
            now.saturating_sub(Duration::from_millis(ms))
        }
    }

    fn mark_inbound(&self) {
        self.last_inbound_ms
            .store(self.started.elapsed().as_millis() as u64, Ordering::Release);
    }

    /// Whether the underlying connection is still alive.
    pub fn is_alive(&self) -> bool {
        self.mux.is_alive()
    }

    /// Open a new API byte stream. Fails with
    /// [`ConnError::WorkerUnreachable`] once the session is dead — callers
    /// must re-resolve the session after a reconnect.
    pub fn open_stream(&self) -> Result<StreamHandle, ConnError> {
        if !self.is_alive() {
            return Err(ConnError::WorkerUnreachable);
        }
        self.mux.open_stream().map_err(|e| {
            if is_unreachable_err(&e) {
                ConnError::WorkerUnreachable
            } else {
                ConnError::Mux(e)
            }
        })
    }

    /// Accept the next peer-opened stream.
    pub async fn accept_stream(&self) -> Result<StreamHandle, ConnError> {
        self.mux.accept_stream().await.map_err(|e| match e {
            MuxError::Closed | MuxError::ConnectionClosed => ConnError::WorkerUnreachable,
            other => ConnError::Mux(other),
        })
    }

    /// Raw control-channel handle (stream 0).
    pub fn control(&self) -> StreamHandle {
        self.mux.control()
    }

    /// Send one protocol [`Message`] on the control channel.
    pub async fn send_message(&self, msg: &Message) -> Result<(), ConnError> {
        if !self.is_alive() {
            return Err(ConnError::WorkerUnreachable);
        }
        let frame = protocol::encode_frame(msg)?;
        if frame.len() > crate::mux::MAX_PAYLOAD {
            return Err(ConnError::UnexpectedMessage(
                "control message exceeds mux frame cap".to_string(),
            ));
        }
        self.control().send(Bytes::from(frame)).await.map_err(|e| {
            if is_unreachable_err(&e) {
                ConnError::WorkerUnreachable
            } else {
                ConnError::Mux(e)
            }
        })
    }

    /// Receive the next control-channel [`Message`]. Updates the inbound
    /// liveness timestamp.
    pub async fn recv_message(&self) -> Result<Message, ConnError> {
        let payload = self.control().recv().await.map_err(|e| match e {
            MuxError::ConnectionClosed | MuxError::Closed | MuxError::Reset => {
                ConnError::WorkerUnreachable
            }
            other => ConnError::Mux(other),
        })?;
        let Some(payload) = payload else {
            return Err(ConnError::WorkerUnreachable);
        };
        let msg = protocol::decode_frame(&payload)?;
        self.mark_inbound();
        Ok(msg)
    }

    /// Spawn a task sending `Heartbeat{seq}` every `every`. The task exits
    /// when the session dies. The returned handle can be aborted early.
    pub fn spawn_heartbeat(&self, every: Duration) -> JoinHandle<()> {
        let control = self.control();
        tokio::spawn(async move {
            let mut seq = 1u64;
            let mut iv = tokio::time::interval(every);
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                iv.tick().await;
                let frame = match protocol::encode_frame(&Message::Heartbeat { seq }) {
                    Ok(f) => f,
                    Err(_) => break,
                };
                seq += 1;
                if control.send(Bytes::from(frame)).await.is_err() {
                    break;
                }
            }
        })
    }

    /// A `watch` receiver that flips to `false` when the connection dies.
    pub(crate) fn mux_closed_watch(&self) -> watch::Receiver<bool> {
        self.closed.clone()
    }

    /// Wait for the connection to die.
    pub async fn wait_closed(&self) {
        let mut rx = self.closed.clone();
        let _ = rx.wait_for(|alive| !*alive).await;
    }

    /// Close the session (tears down the mux and transport).
    pub fn close(&self) {
        self.mux.close();
    }
}

// ---- worker side ----

/// Worker-reported static configuration sent in `WorkerHello`.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Hostname advertised to the controller.
    pub hostname: String,
    /// Advertised capabilities (agent kinds etc.).
    pub caps: Vec<String>,
    /// Initial resource snapshot.
    pub resources: Resources,
}

/// Timing/backoff knobs. Defaults match PLAN.md §3.5.
#[derive(Debug, Clone)]
pub struct WorkerTuning {
    /// Heartbeat interval (default 5 s).
    pub heartbeat_interval: Duration,
    /// No inbound traffic for this long → degraded (default 15 s).
    pub degraded_after: Duration,
    /// No inbound traffic for this long → reconnect (default 30 s).
    pub offline_after: Duration,
    /// Initial reconnect backoff (default 0.5 s).
    pub backoff_min: Duration,
    /// Backoff cap (default 30 s).
    pub backoff_max: Duration,
    /// Deadline for dial+TLS+auth+hello (default 15 s).
    pub connect_timeout: Duration,
}

impl Default for WorkerTuning {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(registry::HEARTBEAT_INTERVAL_SECS as u64),
            degraded_after: Duration::from_secs(registry::DEGRADED_AFTER_SECS as u64),
            offline_after: Duration::from_secs(registry::OFFLINE_AFTER_SECS as u64),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(15),
        }
    }
}

/// Worker link state exposed through [`WorkerHandle::status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkStatus {
    /// No live session (connecting or in backoff).
    Offline,
    /// Session established, heartbeats flowing.
    Online,
    /// Session up but inbound traffic stale past `degraded_after`.
    Degraded,
}

/// The paired worker endpoint: identity + credential + TLS material.
pub struct WorkerClient {
    identity: Keypair,
    credential: WorkerCredential,
    config: WorkerConfig,
    tuning: WorkerTuning,
}

impl WorkerClient {
    /// Load identity, credential, and TLS material from `state_dir`.
    /// Fails with [`ConnError::NotPaired`] when no credential exists.
    pub fn load(state_dir: &Path, config: WorkerConfig) -> Result<Self, ConnError> {
        Self::load_with_tuning(state_dir, config, WorkerTuning::default())
    }

    /// Like [`WorkerClient::load`] with custom timing knobs (tests).
    pub fn load_with_tuning(
        state_dir: &Path,
        config: WorkerConfig,
        tuning: WorkerTuning,
    ) -> Result<Self, ConnError> {
        std::fs::create_dir_all(state_dir)?;
        let identity = Keypair::load_or_create(&state_dir.join("identity.key"))
            .map_err(|e| ConnError::Credential(e.to_string()))?;
        let credential = load_worker_credential(state_dir)?;
        // Ensure TLS material exists (generates/rotates certs on first run).
        tls::ensure_tls_material(state_dir, &config.hostname)?;
        Ok(Self {
            identity,
            credential,
            config,
            tuning,
        })
    }

    /// The stored credential (for diagnostics / re-pair flows).
    pub fn credential(&self) -> &WorkerCredential {
        &self.credential
    }

    /// This worker's Ed25519 public key (what `WorkerHello` advertises).
    pub fn public_key(&self) -> [u8; 32] {
        self.identity.public_key_bytes()
    }

    /// One full connect attempt: TCP → TLS → mutual auth → `WorkerHello`.
    /// Does NOT loop; the reconnect policy lives in [`WorkerClient::spawn`].
    pub async fn connect_once(&self, addr: SocketAddr) -> Result<Session, ConnError> {
        let client_cfg = tls::client_config(
            &self.credential.ca_cert_pem,
            &self.credential.ca_fingerprint,
        )?;
        let ctrl_pub = self.credential.controller_pubkey_bytes()?;

        let mut stream = timeout(self.tuning.connect_timeout, tls::dial_tls(addr, client_cfg))
            .await
            .map_err(|_| ConnError::Timeout("tls dial"))??;

        timeout(
            self.tuning.connect_timeout,
            tls::client_auth(&mut stream, &self.identity, &ctrl_pub),
        )
        .await
        .map_err(|_| ConnError::Timeout("auth challenge"))??;

        let mux = Mux::new(stream, MuxRole::Dialer);
        let session = Session::new(mux, ctrl_pub);

        let hello = Message::WorkerHello {
            worker_pubkey: protocol::pubkey_to_hex(&self.identity.public_key_bytes()),
            hostname: self.config.hostname.clone(),
            caps: self.config.caps.clone(),
            resources: self.config.resources.clone(),
            protocol_version: PROTOCOL_VERSION,
        };
        timeout(self.tuning.connect_timeout, session.send_message(&hello))
            .await
            .map_err(|_| ConnError::Timeout("worker hello"))??;

        Ok(session)
    }

    /// Spawn the reconnect loop. Returns a handle exposing the current
    /// session, link status, and inbound control messages.
    pub fn spawn(self: Arc<Self>, addr: SocketAddr) -> WorkerHandle {
        let (status_tx, status_rx) = watch::channel(LinkStatus::Offline);
        let (sess_tx, sess_rx) = watch::channel(None::<Arc<Session>>);
        let (ev_tx, _) = broadcast::channel(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let last_error = Arc::new(Mutex::new(None::<String>));

        let client = self.clone();
        let le = last_error.clone();
        let ev_tx_loop = ev_tx.clone();
        tokio::spawn(async move {
            run_worker_loop(
                client,
                addr,
                status_tx,
                sess_tx,
                ev_tx_loop,
                shutdown_rx,
                le,
            )
            .await;
        });

        WorkerHandle {
            status_rx,
            sess_rx,
            events_rx: ev_tx.subscribe(),
            shutdown_tx,
            last_error,
        }
    }
}

/// Jittered backoff delay in [0.5x, 1.5x), capped at `cap`.
fn jittered_delay(backoff: Duration, cap: Duration) -> Duration {
    let mut b = [0u8; 8];
    rand_core::OsRng.fill_bytes(&mut b);
    let jitter = (u64::from_le_bytes(b) % 1000) as f64 / 1000.0;
    Duration::from_secs_f64(backoff.as_secs_f64() * (0.5 + jitter)).min(cap)
}

/// Sleep for the current jittered backoff, then double it (capped).
/// Returns the next backoff, or `None` when shutdown was requested
/// mid-sleep (caller must return).
async fn backoff_sleep(
    backoff: Duration,
    tuning: &WorkerTuning,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Option<Duration> {
    let delay = jittered_delay(backoff, tuning.backoff_max);
    tokio::select! {
        _ = tokio::time::sleep(delay) => {}
        _ = shutdown_rx.changed() => return None,
    }
    Some((backoff * 2).min(tuning.backoff_max))
}

async fn run_worker_loop(
    client: Arc<WorkerClient>,
    addr: SocketAddr,
    status_tx: watch::Sender<LinkStatus>,
    sess_tx: watch::Sender<Option<Arc<Session>>>,
    ev_tx: broadcast::Sender<Message>,
    mut shutdown_rx: watch::Receiver<bool>,
    last_error: Arc<Mutex<Option<String>>>,
) {
    let tuning = client.tuning.clone();
    let mut backoff = tuning.backoff_min;
    loop {
        if *shutdown_rx.borrow() {
            return;
        }
        let _ = status_tx.send(LinkStatus::Offline);
        let _ = sess_tx.send(None);

        match client.connect_once(addr).await {
            Ok(session) => {
                let session_started = Instant::now();
                let session = Arc::new(session);
                let _ = status_tx.send(LinkStatus::Online);
                let _ = sess_tx.send(Some(session.clone()));
                *last_error.lock().unwrap() = None;
                let _hb = session.spawn_heartbeat(tuning.heartbeat_interval);

                monitor_session(&session, &status_tx, &ev_tx, &tuning).await;

                // Session ended — close it so all streams fail, then backoff.
                session.close();
                let _ = sess_tx.send(None);
                let _ = status_tx.send(LinkStatus::Offline);

                // Only a session that lived long enough to be "real"
                // (> 2 heartbeats) earns a backoff reset. Instantly-dropped
                // sessions — controller version-mismatch close, registry
                // refusal, flaky link — must keep exponential backoff or we
                // reconnect-hammer the controller at ~backoff_min cadence.
                if session_started.elapsed() > tuning.heartbeat_interval * 2 {
                    backoff = tuning.backoff_min;
                } else {
                    match backoff_sleep(backoff, &tuning, &mut shutdown_rx).await {
                        Some(b) => backoff = b,
                        None => return,
                    }
                }
            }
            Err(e) => {
                *last_error.lock().unwrap() = Some(e.to_string());
                match backoff_sleep(backoff, &tuning, &mut shutdown_rx).await {
                    Some(b) => backoff = b,
                    None => return,
                }
            }
        }
    }
}

/// Watch a live session: forward non-heartbeat control messages, track
/// inbound liveness, flip Degraded/Online, return when the session dies.
async fn monitor_session(
    session: &Arc<Session>,
    status_tx: &watch::Sender<LinkStatus>,
    ev_tx: &broadcast::Sender<Message>,
    tuning: &WorkerTuning,
) {
    // Tick often enough to observe the degraded/offline thresholds even
    // when they are tuned down to milliseconds in tests.
    let tick_every = (tuning.degraded_after.min(tuning.offline_after) / 4)
        .clamp(Duration::from_millis(10), Duration::from_secs(1));
    let mut tick = tokio::time::interval(tick_every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut closed = session.mux_closed_watch();
    loop {
        tokio::select! {
            msg = session.recv_message() => {
                match msg {
                    Ok(m) => {
                        if !matches!(m, Message::Heartbeat { .. }) {
                            let _ = ev_tx.send(m);
                        }
                        if *status_tx.borrow() == LinkStatus::Degraded {
                            let _ = status_tx.send(LinkStatus::Online);
                        }
                    }
                    Err(_) => return,
                }
            }
            _ = tick.tick() => {
                let idle = session.since_last_inbound();
                if idle >= tuning.offline_after {
                    return;
                }
                if idle >= tuning.degraded_after
                    && *status_tx.borrow() != LinkStatus::Degraded
                {
                    let _ = status_tx.send(LinkStatus::Degraded);
                }
            }
            _ = closed.wait_for(|alive| !*alive) => return,
        }
    }
}

// ---- worker handle ----

/// Handle to a running [`WorkerClient::spawn`] reconnect loop.
pub struct WorkerHandle {
    status_rx: watch::Receiver<LinkStatus>,
    sess_rx: watch::Receiver<Option<Arc<Session>>>,
    events_rx: broadcast::Receiver<Message>,
    shutdown_tx: watch::Sender<bool>,
    last_error: Arc<Mutex<Option<String>>>,
}

impl WorkerHandle {
    /// Current link status.
    pub fn status(&self) -> watch::Receiver<LinkStatus> {
        self.status_rx.clone()
    }

    /// Watch the current session (`None` while offline/backing off).
    /// The `Arc<Session>` is replaced on every reconnect; streams from an
    /// older session are dead — always re-fetch after status changes.
    pub fn session(&self) -> watch::Receiver<Option<Arc<Session>>> {
        self.sess_rx.clone()
    }

    /// Inbound non-heartbeat control messages (Events, Resources, …).
    pub fn events(&self) -> broadcast::Receiver<Message> {
        self.events_rx.resubscribe()
    }

    /// Last connect error, if any.
    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().unwrap().clone()
    }

    /// Stop the reconnect loop.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

// ---- controller side ----

/// A registered worker session on the controller.
#[derive(Clone)]
pub struct WorkerSession {
    /// The live session (streams + control channel).
    pub session: Arc<Session>,
    /// Authenticated worker Ed25519 pubkey.
    pub worker_pubkey: [u8; 32],
    /// Worker's self-reported hostname (from `WorkerHello`).
    pub hostname: String,
    /// Advertised capabilities.
    pub caps: Vec<String>,
    /// Latest reported resources, if any `Resources` message arrived.
    pub resources: Option<Resources>,
    /// When the session was established.
    pub connected_at: Instant,
}

impl std::fmt::Debug for WorkerSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerSession")
            .field("worker_pubkey", &self.worker_pubkey)
            .field("hostname", &self.hostname)
            .finish_non_exhaustive()
    }
}

/// Controller-side events.
pub enum ControllerEvent {
    /// A worker completed auth + `WorkerHello` and registered.
    WorkerConnected(WorkerSession),
    /// A worker session ended (conn drop or explicit close).
    WorkerDisconnected {
        /// The worker's pubkey.
        worker_pubkey: [u8; 32],
        /// The exact session that ended. After a fast reconnect this can
        /// be a *stale* session already replaced in the session map —
        /// consumers must compare it (e.g. `Arc::ptr_eq`) against the
        /// session a bridge was built from before tearing anything down.
        session: Arc<Session>,
    },
}

impl std::fmt::Debug for ControllerEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WorkerConnected(ws) => f.debug_tuple("WorkerConnected").field(ws).finish(),
            Self::WorkerDisconnected {
                worker_pubkey,
                session,
            } => f
                .debug_struct("WorkerDisconnected")
                .field("worker_pubkey", worker_pubkey)
                .field("session_alive", &session.is_alive())
                .finish(),
        }
    }
}

/// Default controller-side reap threshold for inbound silence:
/// `offline_after` (30 s) plus slack for three missed worker heartbeats
/// (≈15 s) so a live-but-slow worker is never reaped.
fn default_liveness_reap_after() -> Duration {
    Duration::from_secs(
        (registry::OFFLINE_AFTER_SECS + 3 * registry::HEARTBEAT_INTERVAL_SECS) as u64,
    )
}

/// Upper bound on delivering one [`ControllerEvent`]. `send().await`
/// applies backpressure instead of the old `try_send`, which silently
/// dropped connect/disconnect events on a full queue (leaving a
/// registered worker with no bridge); the timeout only guards a wedged
/// consumer.
const EVENT_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Deliver a controller event with a bounded wait. A closed channel
/// (controller shutting down) or a consumer stalled past
/// [`EVENT_SEND_TIMEOUT`] drops the event — logged, never silent.
async fn send_controller_event(tx: &mpsc::Sender<ControllerEvent>, event: ControllerEvent) {
    match timeout(EVENT_SEND_TIMEOUT, tx.send(event)).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => tracing::debug!("controller event channel closed; event dropped"),
        Err(_) => tracing::warn!(
            "controller event queue did not drain within {EVENT_SEND_TIMEOUT:?}; event dropped"
        ),
    }
}

/// The controller endpoint: TLS listener + session registry.
pub struct Controller {
    listener: TcpListener,
    tls_config: Arc<rustls::ServerConfig>,
    cluster_key: Keypair,
    tls_material: TlsMaterial,
    heartbeat: Option<Duration>,
    /// Inbound-silence threshold after which the per-session watchdog
    /// reaps a session whose peer vanished without a TCP close.
    liveness_reap_after: Duration,
    sessions: Arc<Mutex<HashMap<[u8; 32], Arc<Session>>>>,
    event_tx: mpsc::Sender<ControllerEvent>,
    event_rx: tokio::sync::Mutex<mpsc::Receiver<ControllerEvent>>,
}

impl Controller {
    /// Bind `listen` and load TLS material + cluster identity from
    /// `state_dir`. Generates the cluster CA/server cert on first run.
    pub async fn bind(state_dir: &Path, listen: SocketAddr) -> Result<Self, ConnError> {
        let hostname = hostname_hint();
        let material = tls::ensure_tls_material(state_dir, &hostname)?;
        let tls_config = tls::server_config(&material)?;
        let cluster_key = Keypair::load_or_create(&state_dir.join("identity.key"))
            .map_err(|e| ConnError::Credential(e.to_string()))?;
        let listener = TcpListener::bind(listen).await?;
        let (event_tx, event_rx) = mpsc::channel(64);
        Ok(Self {
            listener,
            tls_config,
            cluster_key,
            tls_material: material,
            heartbeat: Some(Duration::from_secs(
                registry::HEARTBEAT_INTERVAL_SECS as u64,
            )),
            liveness_reap_after: default_liveness_reap_after(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            event_tx,
            event_rx: tokio::sync::Mutex::new(event_rx),
        })
    }

    /// The bound socket address (useful with port 0 in tests).
    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    /// The cluster identity pubkey (what workers pin in their credential).
    pub fn cluster_pubkey(&self) -> [u8; 32] {
        self.cluster_key.public_key_bytes()
    }

    /// The TLS material in use (CA cert PEM + fingerprint for pairing).
    pub fn tls_material(&self) -> &TlsMaterial {
        &self.tls_material
    }

    /// Disable per-session heartbeats (tests that need silence).
    pub fn disable_heartbeats(&mut self) {
        self.heartbeat = None;
    }

    /// Override the heartbeat interval.
    pub fn set_heartbeat_interval(&mut self, every: Duration) {
        self.heartbeat = Some(every);
    }

    /// Override the inbound-silence reap threshold (tests).
    ///
    /// Sessions whose control channel has delivered no message for
    /// longer than this are closed by the per-session watchdog — the
    /// controller-side counterpart of the worker's `offline_after`.
    /// Default is `OFFLINE_AFTER_SECS + 3 * HEARTBEAT_INTERVAL_SECS`
    /// (45 s), which tolerates several missed worker heartbeats.
    pub fn set_liveness_reap_after(&mut self, after: Duration) {
        self.liveness_reap_after = after;
    }

    /// The live session for a worker pubkey, if connected.
    pub fn session(&self, worker_pubkey: &[u8; 32]) -> Option<Arc<Session>> {
        self.sessions.lock().unwrap().get(worker_pubkey).cloned()
    }

    /// Snapshot of currently connected worker pubkeys.
    pub fn connected_workers(&self) -> Vec<[u8; 32]> {
        self.sessions.lock().unwrap().keys().copied().collect()
    }

    /// Next controller event (blocks until one arrives).
    pub async fn next_event(&self) -> ControllerEvent {
        let mut rx = self.event_rx.lock().await;
        rx.recv().await.expect("controller event channel open")
    }

    /// Accept connections forever. Spawn this; it never returns.
    pub async fn accept_loop(self: Arc<Self>) {
        loop {
            match self.listener.accept().await {
                Ok((tcp, _addr)) => {
                    let this = self.clone();
                    tokio::spawn(async move {
                        let _ = this.handle_conn(tcp).await;
                    });
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn handle_conn(&self, tcp: TcpStream) -> Result<(), ConnError> {
        let mut tls = timeout(
            tls::AUTH_TIMEOUT,
            tls::accept_tls(tcp, self.tls_config.clone()),
        )
        .await
        .map_err(|_| ConnError::Timeout("tls accept"))??;

        let worker_pub = timeout(
            tls::AUTH_TIMEOUT,
            tls::server_auth(&mut tls, &self.cluster_key),
        )
        .await
        .map_err(|_| ConnError::Timeout("auth challenge"))??;

        let mux = Mux::new(tls, MuxRole::Acceptor);
        let session = Arc::new(Session::new(mux, worker_pub));
        if let Some(every) = self.heartbeat {
            let _hb = session.spawn_heartbeat(every);
        }

        // First control message must be WorkerHello.
        let hello = timeout(Duration::from_secs(10), session.recv_message())
            .await
            .map_err(|_| ConnError::Timeout("worker hello"))??;

        let (hostname, caps, resources) = match hello {
            Message::WorkerHello {
                worker_pubkey,
                hostname,
                caps,
                resources,
                protocol_version,
            } => {
                if protocol_version != PROTOCOL_VERSION {
                    let _ = session
                        .send_message(&Message::Event {
                            kind: "cluster.error".to_string(),
                            payload: serde_json::json!({
                                "reason": format!("protocol version {protocol_version} unsupported")
                            }),
                        })
                        .await;
                    session.close();
                    return Err(ConnError::ProtocolVersion {
                        found: protocol_version,
                        expected: PROTOCOL_VERSION,
                    });
                }
                // The claimed pubkey must match the authenticated key.
                match protocol::pubkey_from_hex(&worker_pubkey) {
                    Ok(pk) if pk == worker_pub => {}
                    _ => {
                        session.close();
                        return Err(ConnError::UnexpectedMessage(
                            "WorkerHello pubkey does not match authenticated key".to_string(),
                        ));
                    }
                }
                (hostname, caps, resources)
            }
            other => {
                session.close();
                return Err(ConnError::UnexpectedMessage(format!(
                    "expected worker_hello, got {other:?}"
                )));
            }
        };

        // Register (replacing any stale session for this worker).
        let ws = WorkerSession {
            session: session.clone(),
            worker_pubkey: worker_pub,
            hostname,
            caps,
            resources: Some(resources),
            connected_at: Instant::now(),
        };
        {
            let mut map = self.sessions.lock().unwrap();
            if let Some(old) = map.insert(worker_pub, session.clone()) {
                old.close();
            }
        }
        send_controller_event(&self.event_tx, ControllerEvent::WorkerConnected(ws)).await;

        // Watcher: unregister + emit disconnect when the connection dies
        // OR when inbound control traffic goes silent past the reap
        // threshold — a worker host that powers off or loses NAT state
        // without a TCP FIN would otherwise leave this session (and its
        // bridge, registry row, unix listener) alive forever. Closing the
        // session here runs the exact same cleanup path as a real
        // disconnect: the mux closes, `wait_closed` resolves, and the
        // `WorkerDisconnected` event below fires.
        let sessions = self.sessions.clone();
        let event_tx = self.event_tx.clone();
        let watcher_session = session.clone();
        let reap_after = self.liveness_reap_after;
        tokio::spawn(async move {
            // Tick often enough to observe even test-tuned thresholds,
            // mirroring the worker-side `monitor_session` cadence.
            let tick_every =
                (reap_after / 4).clamp(Duration::from_millis(10), Duration::from_secs(5));
            let mut tick = tokio::time::interval(tick_every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = watcher_session.wait_closed() => break,
                    _ = tick.tick() => {
                        if watcher_session.since_last_inbound() >= reap_after {
                            tracing::warn!(
                                "worker session silent for {:?}; reaping (possible ungraceful disconnect)",
                                watcher_session.since_last_inbound()
                            );
                            watcher_session.close();
                            break;
                        }
                    }
                }
            }
            {
                let mut map = sessions.lock().unwrap();
                // Only remove if it's still *our* session (a reconnect may
                // have already replaced it).
                if map
                    .get(&worker_pub)
                    .map(|s| Arc::ptr_eq(s, &watcher_session))
                    .unwrap_or(false)
                {
                    map.remove(&worker_pub);
                }
            }
            // Tag the event with *this* session: after a fast reconnect
            // the map entry may already belong to a newer session, and
            // consumers must not tear down resources built from it.
            send_controller_event(
                &event_tx,
                ControllerEvent::WorkerDisconnected {
                    worker_pubkey: worker_pub,
                    session: watcher_session.clone(),
                },
            )
            .await;
        });

        // Block until the session dies; the watcher owns cleanup.
        session.wait_closed().await;
        Ok(())
    }
}

fn hostname_hint() -> String {
    for var in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(v) = std::env::var(var) {
            if !v.trim().is_empty() {
                return v.trim().to_string();
            }
        }
    }
    "localhost".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster_core::protocol::Resources;

    fn tempdir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn worker_cfg() -> WorkerConfig {
        WorkerConfig {
            hostname: "test-worker".to_string(),
            caps: vec!["claude".to_string()],
            resources: Resources {
                cpu_cores: 8,
                mem_total_mb: 32768,
                mem_available_mb: 16384,
                agent_count: 0,
                max_agents: 4,
                load_avg: vec![],
                kinds: vec!["claude".to_string()],
            },
        }
    }

    /// Spin up a Controller on loopback + a paired WorkerClient whose
    /// credential pins the controller's CA + cluster pubkey.
    async fn loopback_pair() -> (
        Arc<Controller>,
        WorkerClient,
        std::net::SocketAddr,
        tempfile::TempDir,
        tempfile::TempDir,
    ) {
        let ctrl_dir = tempdir();
        let work_dir = tempdir();

        let ctrl = Arc::new(
            Controller::bind(ctrl_dir.path(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let addr = ctrl.local_addr();
        let c = ctrl.clone();
        tokio::spawn(async move { c.accept_loop().await });

        // Worker credential: pin controller CA + cluster pubkey.
        let material = ctrl.tls_material();
        let cred = WorkerCredential {
            controller_pubkey: protocol::pubkey_to_hex(&ctrl.cluster_pubkey()),
            ca_fingerprint: material.ca_fingerprint(),
            ca_cert_pem: material.ca_cert_pem.clone(),
        };
        save_worker_credential(work_dir.path(), &cred).unwrap();

        let client = WorkerClient::load(work_dir.path(), worker_cfg()).unwrap();
        (ctrl, client, addr, ctrl_dir, work_dir)
    }

    async fn wait_status(handle: &WorkerHandle, want: LinkStatus) -> watch::Receiver<LinkStatus> {
        let mut rx = handle.status();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if *rx.borrow() == want {
                return rx;
            }
            assert!(Instant::now() < deadline, "timed out waiting for {want:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
            rx.changed().await.unwrap();
        }
    }

    #[tokio::test]
    async fn worker_connects_and_controller_sees_hello() {
        let (ctrl, client, addr, _cd, _wd) = loopback_pair().await;
        let session = client.connect_once(addr).await.unwrap();

        // Controller side: WorkerHello arrived → WorkerConnected event.
        let event = timeout(Duration::from_secs(5), ctrl.next_event())
            .await
            .unwrap();
        match event {
            ControllerEvent::WorkerConnected(ws) => {
                assert_eq!(ws.hostname, "test-worker");
                assert_eq!(ws.worker_pubkey, client.public_key());
                // From the worker's view the peer is the controller.
                assert_eq!(session.peer_pubkey(), ctrl.cluster_pubkey());
                assert_eq!(ws.caps, vec!["claude".to_string()]);
            }
            other => panic!("expected WorkerConnected, got {other:?}"),
        }

        // Bidirectional stream: worker opens, controller accepts.
        let w_stream = session.open_stream().unwrap();
        let workers = ctrl.connected_workers();
        assert_eq!(workers.len(), 1);
        let c_session = ctrl.session(&workers[0]).unwrap();
        let c_stream = timeout(Duration::from_secs(5), c_session.accept_stream())
            .await
            .unwrap()
            .unwrap();

        w_stream.send(Bytes::from_static(b"ping")).await.unwrap();
        let got = timeout(Duration::from_secs(5), c_stream.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(&got[..], b"ping");

        // Control channel still works both ways.
        session
            .send_message(&Message::Event {
                kind: "test.ping".to_string(),
                payload: serde_json::json!({}),
            })
            .await
            .unwrap();
        let msg = timeout(Duration::from_secs(5), c_session.recv_message())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(msg, Message::Event { .. }));

        session.close();
    }

    #[tokio::test]
    async fn spawn_reconnects_after_disconnect_without_repairing() {
        let (ctrl, client, addr, _cd, _wd) = loopback_pair().await;
        let client = Arc::new(client);
        let handle = client.spawn(addr);

        wait_status(&handle, LinkStatus::Online).await;
        // Wait for WorkerConnected on controller.
        let ev = timeout(Duration::from_secs(5), ctrl.next_event())
            .await
            .unwrap();
        let first_worker_pub = match ev {
            ControllerEvent::WorkerConnected(ws) => ws.worker_pubkey,
            other => panic!("expected connect, got {other:?}"),
        };

        // Kill the session server-side: close the registered session.
        let sess = ctrl.session(&first_worker_pub).unwrap();
        let old_session = handle.session().borrow().clone().unwrap();
        sess.close();

        // Worker detects dead conn → backoff → reconnect → new session.
        let mut sess_rx = handle.session();
        let deadline = Instant::now() + Duration::from_secs(15);
        let new_session = loop {
            if let Some(s) = sess_rx.borrow().clone() {
                if !Arc::ptr_eq(&s, &old_session) {
                    break s;
                }
            }
            assert!(Instant::now() < deadline, "no reconnect");
            sess_rx.changed().await.unwrap();
        };
        // Old session's streams must be dead.
        assert!(!old_session.is_alive());
        assert!(matches!(
            old_session.open_stream(),
            Err(ConnError::WorkerUnreachable)
        ));
        // New session is usable: round-trip a byte stream through the
        // controller's side of the reconnected session. Wait for the
        // second WorkerConnected event — it is emitted only after the
        // controller registers the new session.
        let s = new_session.open_stream().unwrap();
        s.send(Bytes::from_static(b"x")).await.unwrap();
        // Skip the disconnect event; the reconnect emits WorkerConnected.
        let c_session = loop {
            let ev = timeout(Duration::from_secs(5), ctrl.next_event())
                .await
                .unwrap();
            match ev {
                ControllerEvent::WorkerConnected(_) => {
                    break ctrl.session(&first_worker_pub).unwrap()
                }
                ControllerEvent::WorkerDisconnected { .. } => {}
            }
        };
        let c_stream = timeout(Duration::from_secs(5), c_session.accept_stream())
            .await
            .unwrap()
            .unwrap();
        let got = timeout(Duration::from_secs(5), c_stream.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(&got[..], b"x");
        c_stream.send(Bytes::from_static(b"y")).await.unwrap();
        let back = timeout(Duration::from_secs(5), s.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(&back[..], b"y");

        handle.shutdown();
    }

    #[tokio::test]
    async fn worker_degraded_then_offline_reconnect() {
        let ctrl_dir = tempdir();
        let work_dir = tempdir();
        let mut ctrl = Controller::bind(ctrl_dir.path(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        ctrl.disable_heartbeats(); // silence → worker goes stale
        let ctrl = Arc::new(ctrl);
        let addr = ctrl.local_addr();
        let c = ctrl.clone();
        tokio::spawn(async move { c.accept_loop().await });

        let material = ctrl.tls_material();
        let cred = WorkerCredential {
            controller_pubkey: protocol::pubkey_to_hex(&ctrl.cluster_pubkey()),
            ca_fingerprint: material.ca_fingerprint(),
            ca_cert_pem: material.ca_cert_pem.clone(),
        };
        save_worker_credential(work_dir.path(), &cred).unwrap();

        let tuning = WorkerTuning {
            heartbeat_interval: Duration::from_millis(50),
            degraded_after: Duration::from_millis(200),
            offline_after: Duration::from_millis(400),
            backoff_min: Duration::from_millis(50),
            backoff_max: Duration::from_millis(200),
            connect_timeout: Duration::from_secs(5),
        };
        let client = Arc::new(
            WorkerClient::load_with_tuning(work_dir.path(), worker_cfg(), tuning).unwrap(),
        );
        let handle = client.spawn(addr);

        wait_status(&handle, LinkStatus::Online).await;
        // With heartbeats disabled on the controller, inbound goes stale:
        // Degraded at ~200ms, Offline+reconnect at ~400ms.
        wait_status(&handle, LinkStatus::Degraded).await;
        wait_status(&handle, LinkStatus::Online).await; // reconnected

        handle.shutdown();
    }

    #[tokio::test]
    async fn heartbeats_keep_session_online_past_offline_threshold() {
        // Regression: `since_last_inbound` used to report elapsed-since-
        // connect rather than time-since-last-message, so every session
        // was declared offline ~`offline_after` after connect even with
        // heartbeats flowing — workers reconnected every ~30 s forever.
        let ctrl_dir = tempdir();
        let work_dir = tempdir();
        let mut ctrl = Controller::bind(ctrl_dir.path(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        ctrl.set_heartbeat_interval(Duration::from_millis(50));
        let ctrl = Arc::new(ctrl);
        let addr = ctrl.local_addr();
        let c = ctrl.clone();
        tokio::spawn(async move { c.accept_loop().await });

        let material = ctrl.tls_material();
        let cred = WorkerCredential {
            controller_pubkey: protocol::pubkey_to_hex(&ctrl.cluster_pubkey()),
            ca_fingerprint: material.ca_fingerprint(),
            ca_cert_pem: material.ca_cert_pem.clone(),
        };
        save_worker_credential(work_dir.path(), &cred).unwrap();

        let tuning = WorkerTuning {
            heartbeat_interval: Duration::from_millis(50),
            degraded_after: Duration::from_millis(300),
            offline_after: Duration::from_millis(600),
            backoff_min: Duration::from_millis(50),
            backoff_max: Duration::from_millis(200),
            connect_timeout: Duration::from_secs(5),
        };
        let client = Arc::new(
            WorkerClient::load_with_tuning(work_dir.path(), worker_cfg(), tuning).unwrap(),
        );
        let handle = client.spawn(addr);

        wait_status(&handle, LinkStatus::Online).await;
        let first = handle.session().borrow().clone().unwrap();

        // Well past several offline_after windows with heartbeats flowing.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        assert_eq!(*handle.status().borrow(), LinkStatus::Online);
        let current = handle.session().borrow().clone().unwrap();
        assert!(
            Arc::ptr_eq(&first, &current),
            "session reconnected despite heartbeats"
        );

        handle.shutdown();
    }

    /// Regression: reconnect backoff must not reset after sessions that
    /// die almost immediately (controller version-mismatch close,
    /// registry refusal, flaky link). The controller kills every session
    /// the moment it registers; the worker must back off exponentially
    /// instead of reconnect-hammering at ~backoff_min cadence.
    #[tokio::test]
    async fn short_lived_sessions_do_not_reset_backoff() {
        let ctrl_dir = tempdir();
        let work_dir = tempdir();
        let ctrl = Arc::new(
            Controller::bind(ctrl_dir.path(), "127.0.0.1:0".parse().unwrap())
                .await
                .unwrap(),
        );
        let addr = ctrl.local_addr();
        let c = ctrl.clone();
        tokio::spawn(async move { c.accept_loop().await });

        let material = ctrl.tls_material();
        let cred = WorkerCredential {
            controller_pubkey: protocol::pubkey_to_hex(&ctrl.cluster_pubkey()),
            ca_fingerprint: material.ca_fingerprint(),
            ca_cert_pem: material.ca_cert_pem.clone(),
        };
        save_worker_credential(work_dir.path(), &cred).unwrap();

        // heartbeat_interval*2 = 400ms is far longer than the ~ms a
        // loopback session lives here, so every session counts as "short".
        let tuning = WorkerTuning {
            heartbeat_interval: Duration::from_millis(200),
            degraded_after: Duration::from_secs(60),
            offline_after: Duration::from_secs(60),
            backoff_min: Duration::from_millis(30),
            backoff_max: Duration::from_millis(150),
            connect_timeout: Duration::from_secs(5),
        };
        let client = Arc::new(
            WorkerClient::load_with_tuning(work_dir.path(), worker_cfg(), tuning).unwrap(),
        );
        let handle = client.spawn(addr);

        // Kill each session on connect and count reconnects over a fixed
        // window. backoff_max=150ms bounds the steady-state rate to a
        // handful per second; the old reset-after-every-session behavior
        // reconnected with no delay at all.
        let mut connects = 0usize;
        let deadline = Instant::now() + Duration::from_millis(800);
        while Instant::now() < deadline {
            let ev = match timeout(
                deadline.saturating_duration_since(Instant::now()),
                ctrl.next_event(),
            )
            .await
            {
                Ok(ev) => ev,
                Err(_) => break,
            };
            match ev {
                ControllerEvent::WorkerConnected(ws) => {
                    connects += 1;
                    ws.session.close();
                }
                ControllerEvent::WorkerDisconnected { .. } => {}
            }
        }
        handle.shutdown();
        assert!(
            connects < 20,
            "instantly-dropped sessions caused a reconnect hammer: \
             {connects} connects in 800ms"
        );
    }

    /// Regression for #95: a worker that vanishes without a TCP close
    /// (power loss, NAT state drop) leaves the mux "alive" forever — the
    /// controller only learned of death via `wait_closed`. The per-session
    /// watchdog must reap sessions whose inbound control traffic stays
    /// silent past the reap threshold, running the same disconnect path
    /// (`WorkerDisconnected` + session-map removal) as a real close,
    /// while leaving heartbeat-fed sessions alone.
    #[tokio::test]
    async fn controller_reaps_session_when_inbound_goes_silent() {
        let ctrl_dir = tempdir();
        let work_dir = tempdir();
        let mut ctrl = Controller::bind(ctrl_dir.path(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        ctrl.set_liveness_reap_after(Duration::from_millis(300));
        let ctrl = Arc::new(ctrl);
        let addr = ctrl.local_addr();
        let c = ctrl.clone();
        tokio::spawn(async move { c.accept_loop().await });

        let material = ctrl.tls_material();
        let cred = WorkerCredential {
            controller_pubkey: protocol::pubkey_to_hex(&ctrl.cluster_pubkey()),
            ca_fingerprint: material.ca_fingerprint(),
            ca_cert_pem: material.ca_cert_pem.clone(),
        };
        save_worker_credential(work_dir.path(), &cred).unwrap();

        let client = WorkerClient::load(work_dir.path(), worker_cfg()).unwrap();
        let worker_pub = client.public_key();
        let session = client.connect_once(addr).await.unwrap();
        // Fast heartbeats so inbound traffic is dense relative to the
        // 300 ms reap threshold.
        let hb = session.spawn_heartbeat(Duration::from_millis(50));

        // Wait for registration, then drain the control channel — this
        // is what marks `since_last_inbound` fresh on heartbeats (the
        // cluster-node control pump plays this role in production).
        let c_session = match timeout(Duration::from_secs(5), ctrl.next_event())
            .await
            .unwrap()
        {
            ControllerEvent::WorkerConnected(ws) => {
                assert_eq!(ws.worker_pubkey, worker_pub);
                ws.session
            }
            other => panic!("expected WorkerConnected, got {other:?}"),
        };
        let pump = tokio::spawn({
            let s = c_session.clone();
            async move { while s.recv_message().await.is_ok() {} }
        });

        // Well past the reap threshold with heartbeats flowing: the
        // session must NOT be reaped (idle-but-alive stays alive).
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(c_session.is_alive(), "live session was reaped");
        assert!(ctrl.session(&worker_pub).is_some());

        // Simulate an ungraceful worker death: heartbeats stop but the
        // TCP connection stays open (no FIN/RST).
        hb.abort();

        match timeout(Duration::from_secs(5), ctrl.next_event())
            .await
            .unwrap()
        {
            ControllerEvent::WorkerDisconnected {
                worker_pubkey,
                session: dead,
            } => {
                assert_eq!(worker_pubkey, worker_pub);
                assert!(!dead.is_alive());
            }
            other => panic!("expected WorkerDisconnected, got {other:?}"),
        }
        // Same cleanup as a real disconnect: session-map entry gone.
        assert!(ctrl.session(&worker_pub).is_none());
        assert!(ctrl.connected_workers().is_empty());

        pump.abort();
        session.close();
    }

    #[tokio::test]
    async fn connect_without_credential_is_not_paired() {
        let dir = tempdir();
        let err = WorkerClient::load(dir.path(), worker_cfg()).err().unwrap();
        assert!(matches!(err, ConnError::NotPaired));
    }

    #[tokio::test]
    async fn wrong_ca_pin_rejected() {
        let (_ctrl, _client, addr, _cd, work_dir) = loopback_pair().await;
        // Corrupt the stored credential's fingerprint.
        let mut cred = load_worker_credential(work_dir.path()).unwrap();
        cred.ca_fingerprint = "0000000000000000".to_string();
        save_worker_credential(work_dir.path(), &cred).unwrap();
        let client = WorkerClient::load(work_dir.path(), worker_cfg()).unwrap();
        let err = client.connect_once(addr).await.err().unwrap();
        assert!(matches!(err, ConnError::Tls(TlsError::PinMismatch)));
    }

    #[tokio::test]
    async fn credential_roundtrip() {
        let dir = tempdir();
        let cred = WorkerCredential {
            controller_pubkey: "ab".repeat(32),
            ca_fingerprint: "0123456789abcdef".to_string(),
            ca_cert_pem: "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
                .to_string(),
        };
        save_worker_credential(dir.path(), &cred).unwrap();
        let loaded = load_worker_credential(dir.path()).unwrap();
        assert_eq!(loaded.controller_pubkey, cred.controller_pubkey);
        // File must be mode 0600 on unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(worker_credential_path(dir.path()))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
}
