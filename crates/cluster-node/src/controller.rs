//! `controller` — the `herdcluster init` controller service.
//!
//! Composes the already-built crates into the long-running controller:
//!
//! - `cluster_core::config` / `registry` — `cluster.json`, `workers.json`,
//!   state-dir layout (explicit `state_dir` injection for tests).
//! - `cluster_core::identity` — cluster Ed25519 keypair (`identity.key`,
//!   loaded inside [`cluster_net::conn::Controller::bind`]).
//! - `cluster_net::tls` + `conn::Controller` — cluster CA, TLS 1.3
//!   listener, post-handshake Ed25519 mutual auth, `WorkerHello`
//!   validation, per-worker [`cluster_net::conn::Session`]s.
//! - `cluster_net::discovery` — mDNS `_herdcluster._tcp.local.` advert plus
//!   the UDP announce/probe fallback (wire-compatible with
//!   `cluster_net::discovery::UdpBrowser`).
//! - `cluster_bridge::controller::ControllerBridge` — one private unix
//!   socket per paired worker (`<state_dir>/bridge/<label>.sock`).
//!
//! # Port demultiplexing (read before changing)
//!
//! The cluster port (`47820/TCP`) carries **two protocols**:
//!
//! 1. **Pairing** — plain TCP, one length-prefixed
//!    [`cluster_core::protocol`] frame each way (`PairHello` →
//!    `PairAccept`/`PairReject`). This is the contract `worker.rs::join`
//!    implements; a joining worker has no CA yet, so TLS/mux is impossible
//!    at this stage. The pairing code + on-controller user confirmation is
//!    the bootstrap authentication; the CA is pinned *after* pairing
//!    (TOFU), matching the plan's "pin CA fingerprint from pairing".
//! 2. **Worker sessions** — TLS 1.3 + Ed25519 mutual auth + mux, owned by
//!    [`cluster_net::conn::Controller`].
//!
//! `conn::Controller` binds its own listener and cannot accept sockets
//! from outside `cluster-net`, so this module owns the *public* listener
//! and demultiplexes on the first payload byte: a TLS ClientHello always
//! starts with record type `0x16` (handshake); a protocol frame starts
//! with a `u32` length whose top byte is `0x00`/`0x01`. `0x16` → raw TCP
//! proxy to the controller's loopback-only listener (TLS is end-to-end —
//! the proxy only shovels bytes); anything else → the pairing handler.
//!
//! [`pair_with_controller`] is the client half of the pairing exchange,
//! exposed for tests and for `worker.rs`-style callers.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use cluster_core::config::{self, ConfigError};
use cluster_core::identity;
use cluster_core::pairing::{
    self, Clock, JoinRequest, PairingPhase, PairingServer, SystemClock, PAIRING_TTL_MS,
};
use cluster_core::protocol::{self, Message, Resources};
use cluster_core::registry::{self, ClusterInfo, WorkerEntry, WorkerRegistry, WorkerStatus};
use cluster_net::conn::{
    ConnError, Controller, ControllerEvent, Session, WorkerCredential, WorkerSession,
};
use cluster_net::discovery::{self, MdnsAdvertiser};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::watch;
use tokio::task::JoinHandle;

#[cfg(unix)]
use cluster_bridge::controller::ControllerBridge;
#[cfg(unix)]
use std::collections::HashMap;

/// How often the UDP socket re-broadcasts its discovery announce.
const UDP_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(1);
/// How often the registry persist task checks the dirty flag.
const PERSIST_TICK: Duration = Duration::from_secs(2);
/// How often the pairing ticker checks for an expired pairing session.
const PAIRING_TICK: Duration = Duration::from_secs(30);
/// Deadline for reading the `PairHello` frame on a pairing connection.
const PAIR_HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// Overall deadline for one pairing exchange (hello → answer). The
/// controller-side confirm window is 120 s; this adds slack.
const PAIR_EXCHANGE_TIMEOUT: Duration = Duration::from_millis(pairing::CONFIRM_TIMEOUT_MS + 30_000);
/// First byte of a TLS handshake record (content type `handshake`).
const TLS_HANDSHAKE_BYTE: u8 = 0x16;

/// Configuration for [`ControllerHandle::run`].
#[derive(Debug, Clone, Default)]
pub struct ControllerConfig {
    /// Cluster display name; defaults to the hostname.
    pub name: Option<String>,
    /// Accepted for CLI compatibility — orchestration skills are
    /// provisioned by `cluster-node::skills`, not here.
    pub no_skills: bool,
    /// TCP listen port; defaults to [`config::DEFAULT_PORT`]. `0` binds an
    /// ephemeral port (tests) — the bound port is reflected in
    /// [`ControllerHandle::local_addr`].
    pub port: Option<u16>,
    /// Automatically accept every pairing request without prompting.
    /// Intended for tests and unattended `--yes` flows.
    pub auto_accept_pairing: bool,
    /// State dir override (tests). `None` → [`config::state_dir`]
    /// (`HERDCLUSTER_STATE_DIR` aware).
    pub state_dir: Option<PathBuf>,
}

/// Errors from [`ControllerHandle::run`].
#[derive(Debug, Error)]
pub enum ControllerError {
    /// Filesystem error during state-dir setup.
    #[error("controller io error: {0}")]
    Io(#[from] std::io::Error),
    /// Config/state-dir resolution error.
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    /// Registry load/save error.
    #[error("registry error: {0}")]
    Registry(#[from] registry::RegistryError),
    /// Identity keypair error.
    #[error("identity error: {0}")]
    Identity(#[from] identity::IdentityError),
    /// Transport layer error (TLS bind, listener).
    #[error("cluster transport error: {0}")]
    Conn(#[from] ConnError),
    /// Bridge socket error.
    #[cfg(unix)]
    #[error("bridge error: {0}")]
    Bridge(#[from] cluster_bridge::BridgeError),
    /// mDNS advertisement failed at startup (non-fatal — the UDP
    /// announce/probe fallback still provides discovery).
    #[error("discovery error: {0}")]
    Discovery(#[from] discovery::DiscoveryError),
}

// ---- shared service state ----

/// Everything the spawned tasks need, behind `Arc`.
struct Inner {
    controller: Arc<Controller>,
    registry: Arc<Mutex<WorkerRegistry>>,
    state_dir: PathBuf,
    cluster: ClusterInfo,
    /// The *public* cluster address workers dial (TLS + pairing share it).
    public_addr: SocketAddr,
    /// Current one-time pairing code (rotates when a session expires).
    pairing_code: Arc<Mutex<String>>,
    #[cfg(unix)]
    bridges: Arc<Mutex<HashMap<String, ControllerBridge>>>,
    shutdown_tx: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    /// Keeps the mDNS advertisement alive; unregisters on drop.
    _mdns: Option<MdnsAdvertiser>,
}

/// Opaque handle to a running controller service. Dropping does NOT stop
/// the service — call [`ControllerHandle::shutdown`] (or let the process
/// exit) for a clean stop.
pub struct ControllerHandle {
    inner: Arc<Inner>,
}

// ---- small helpers ----

fn hostname() -> String {
    for var in ["HOSTNAME", "COMPUTERNAME"] {
        if let Ok(v) = std::env::var(var) {
            if !v.trim().is_empty() {
                return v.trim().to_string();
            }
        }
    }
    #[cfg(unix)]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
            let s = s.trim().to_string();
            if !s.is_empty() {
                return s;
            }
        }
    }
    "localhost".to_string()
}

fn default_cluster_name() -> String {
    hostname()
}

/// Resolve the effective listen port: explicit config > `HERDCLUSTER_PORT`
/// env (memory-only, mirroring `config::load_cluster`) > `cluster.json`.
fn resolve_port(cfg_port: Option<u16>, persisted: u16) -> Result<u16, ConfigError> {
    if let Some(p) = cfg_port {
        return Ok(p);
    }
    if let Ok(raw) = std::env::var(config::ENV_PORT) {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return trimmed
                .parse::<u16>()
                .map_err(|_| ConfigError::InvalidPort(raw.clone()));
        }
    }
    Ok(persisted)
}

/// Pick a filesystem-unique worker label: `desired`, or `desired-2`,
/// `desired-3`, … when another registered worker already uses it.
fn unique_label(registry: &WorkerRegistry, desired: &str, own_pubkey: &str) -> String {
    let taken = |label: &str| {
        registry
            .list()
            .iter()
            .any(|w| w.label == label && w.pubkey != own_pubkey)
    };
    if !taken(desired) {
        return desired.to_string();
    }
    for n in 2..1000u32 {
        let candidate = format!("{desired}-{n}");
        if !taken(&candidate) {
            return candidate;
        }
    }
    format!("{desired}-x")
}

// ---- UDP discovery wire format (mirrors cluster_net::discovery) ----

/// JSON shape of UDP discovery datagrams. Field-compatible with the
/// private `UdpPacket` in `cluster_net::discovery` so `UdpBrowser` peers
/// interoperate.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UdpPacket {
    v: u32,
    kind: String,
    #[serde(default)]
    port: u16,
    #[serde(default)]
    cluster_name: String,
    #[serde(default)]
    protocol_version: u32,
    #[serde(default)]
    hostname: String,
}

fn announce_packet(port: u16, cluster_name: &str, host: &str) -> UdpPacket {
    UdpPacket {
        v: 1,
        kind: "announce".to_string(),
        port,
        cluster_name: cluster_name.to_string(),
        protocol_version: protocol::PROTOCOL_VERSION,
        hostname: host.to_string(),
    }
}

// ---- pairing shared state ----

/// Mutable pairing state guarded by one mutex: the FSM plus when the
/// current session started (for proactive TTL rotation).
struct PairingInner {
    server: PairingServer<SystemClock>,
    started: std::time::Instant,
}

/// Everything a pairing connection handler needs.
struct PairingShared {
    inner: Mutex<PairingInner>,
    registry: Arc<Mutex<WorkerRegistry>>,
    cluster_pubkey: [u8; 32],
    ca_fingerprint: String,
    ca_cert_pem: String,
    /// Current one-time code, mirrored for [`ControllerHandle::pairing_code`].
    code_slot: Arc<Mutex<String>>,
    auto_accept: bool,
}

impl PairingShared {
    /// Rotate to a fresh session when the current one is terminal.
    fn ensure_live(&self) {
        let mut g = self.inner.lock().unwrap();
        if g.server.phase().is_terminal() {
            rotate_locked(&mut g, &self.code_slot);
        }
    }

    /// Run `f` against the FSM, rotating first if the session is terminal.
    fn with_server<R>(&self, f: impl FnOnce(&mut PairingServer<SystemClock>) -> R) -> R {
        let mut g = self.inner.lock().unwrap();
        if g.server.phase().is_terminal() {
            rotate_locked(&mut g, &self.code_slot);
        }
        f(&mut g.server)
    }

    /// Proactively rotate an `AwaitingJoin` session past its TTL so the
    /// displayed code never silently dies.
    fn tick_expiry(&self) {
        let mut g = self.inner.lock().unwrap();
        if g.server.phase() == PairingPhase::AwaitingJoin
            && g.started.elapsed() > Duration::from_millis(PAIRING_TTL_MS + 5_000)
        {
            rotate_locked(&mut g, &self.code_slot);
        }
    }
}

/// Replace the FSM inside `g` with a fresh session and publish the new
/// code to the slot + stdout. Caller holds the mutex.
fn rotate_locked(g: &mut PairingInner, code_slot: &Arc<Mutex<String>>) {
    let mut rng = rand_core::OsRng;
    g.server = PairingServer::start(SystemClock, &mut rng);
    g.started = std::time::Instant::now();
    let code = g.server.code().to_string();
    *code_slot.lock().unwrap() = code.clone();
    println!("new pairing code: {code}");
    println!("pair with: herdcluster join --code {code}");
}

// ---- ControllerHandle ----

impl ControllerHandle {
    /// Initialize cluster identity + `cluster.json` + TLS material, bind
    /// the public listener, and run the pairing server, accept proxy,
    /// per-worker bridges, registry persistence, and discovery adverts
    /// until [`shutdown`](Self::shutdown) is called.
    ///
    /// Returns once all background tasks are spawned; the service keeps
    /// running until shutdown. Ctrl-C handling is the caller's job — wire
    /// it to [`shutdown`](Self::shutdown).
    pub async fn run(cfg: ControllerConfig) -> Result<Self, ControllerError> {
        let state_dir = match &cfg.state_dir {
            Some(d) => d.clone(),
            None => config::state_dir()?,
        };
        std::fs::create_dir_all(&state_dir)?;

        // ---- cluster.json ----
        let cluster_path = state_dir.join(registry::CLUSTER_FILE_NAME);
        let default_name = cfg.name.clone().unwrap_or_else(default_cluster_name);
        let outcome = registry::load_or_init_cluster(&cluster_path, &default_name)?;
        let mut cluster = outcome.value;
        let mut cluster_dirty = outcome.created;
        if let Some(name) = &cfg.name {
            if cluster.name != *name {
                cluster.name = name.clone();
                cluster_dirty = true;
            }
        }
        let bind_port = resolve_port(cfg.port, cluster.port)?;
        if cfg.port.is_some() && cluster.port != bind_port {
            cluster.port = bind_port;
            cluster_dirty = true;
        }
        if cluster_dirty {
            registry::save_cluster_info(&cluster_path, &cluster)?;
        }
        if let Some(w) = &outcome.warning {
            tracing::warn!("{w}");
        }

        // ---- workers.json ----
        let registry = Arc::new(Mutex::new(WorkerRegistry::open(
            state_dir.join(registry::WORKERS_FILE_NAME),
        )?));
        if let Some(w) = registry.lock().unwrap().load_warning.clone() {
            tracing::warn!("{w}");
            println!("warning: {w}");
        }

        // ---- TLS + identity on a loopback-only listener ----
        // The PUBLIC listener below demultiplexes pairing frames from TLS
        // records and proxies the latter here; see module docs.
        let internal_listen = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let controller = Arc::new(Controller::bind(&state_dir, internal_listen).await?);
        let internal_addr = controller.local_addr();

        let material = controller.tls_material();
        let ca_fingerprint = material.ca_fingerprint();
        let ca_cert_pem = material.ca_cert_pem.clone();
        let cluster_pubkey = controller.cluster_pubkey();

        // ---- public listener (pairing + TLS demux) ----
        let public_listener = TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            bind_port,
        ))
        .await?;
        let public_addr = public_listener.local_addr()?;

        // ---- shared task plumbing ----
        let (shutdown_tx, _) = watch::channel(false);
        let dirty = Arc::new(AtomicBool::new(false));
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();

        // ---- conn::Controller accept loop (internal listener) ----
        {
            let controller = controller.clone();
            tasks.push(tokio::spawn(async move {
                controller.accept_loop().await;
            }));
        }

        // ---- controller event loop: registry marks, bridges, status ----
        #[cfg(unix)]
        let bridges: Arc<Mutex<HashMap<String, ControllerBridge>>> =
            Arc::new(Mutex::new(HashMap::new()));
        {
            let controller = controller.clone();
            let registry = registry.clone();
            let dirty = dirty.clone();
            let state_dir = state_dir.clone();
            #[cfg(unix)]
            let bridges = bridges.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            tasks.push(tokio::spawn(async move {
                controller_event_loop(
                    controller,
                    registry,
                    dirty,
                    state_dir,
                    #[cfg(unix)]
                    bridges,
                    &mut shutdown_rx,
                )
                .await;
            }));
        }

        // ---- registry persist task (debounced writes) ----
        {
            let registry = registry.clone();
            let dirty = dirty.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            tasks.push(tokio::spawn(async move {
                persist_loop(registry, dirty, &mut shutdown_rx).await;
            }));
        }

        // ---- revocation watch: enforce `revoke` marker files ----
        // `cluster_node::revoke::revoke_worker` runs in a separate
        // process and cannot reach this in-memory state; it drops a
        // marker under `<state_dir>/revocations/` that this loop picks
        // up to close live sessions and drop the cached registry row.
        {
            let controller = controller.clone();
            let registry = registry.clone();
            let dirty = dirty.clone();
            let state_dir = state_dir.clone();
            #[cfg(unix)]
            let bridges = bridges.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            tasks.push(tokio::spawn(async move {
                revocation_watch_loop(
                    controller,
                    registry,
                    dirty,
                    state_dir,
                    #[cfg(unix)]
                    bridges,
                    &mut shutdown_rx,
                )
                .await;
            }));
        }

        // ---- pairing FSM + code slot ----
        let mut rng = rand_core::OsRng;
        let pairing_server = PairingServer::start(SystemClock, &mut rng);
        let initial_code = pairing_server.code().to_string();
        let code_slot: Arc<Mutex<String>> = Arc::new(Mutex::new(initial_code));
        let pairing = Arc::new(PairingShared {
            inner: Mutex::new(PairingInner {
                server: pairing_server,
                started: std::time::Instant::now(),
            }),
            registry: registry.clone(),
            cluster_pubkey,
            ca_fingerprint,
            ca_cert_pem,
            code_slot: code_slot.clone(),
            auto_accept: cfg.auto_accept_pairing,
        });

        // ---- public accept/demux loop ----
        {
            let pairing = pairing.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            tasks.push(tokio::spawn(async move {
                public_accept_loop(public_listener, internal_addr, pairing, &mut shutdown_rx).await;
            }));
        }

        // ---- pairing TTL ticker ----
        {
            let pairing = pairing.clone();
            let mut shutdown_rx = shutdown_tx.subscribe();
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(PAIRING_TICK);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = tick.tick() => pairing.tick_expiry(),
                        _ = shutdown_rx.changed() => break,
                    }
                }
            }));
        }

        // ---- UDP socket: discovery announce/probe only ----
        let udp = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, public_addr.port())).await?;
        udp.set_broadcast(true)?;
        {
            let udp = Arc::new(udp);
            let cluster_name = cluster.name.clone();
            let host = hostname();
            let mut shutdown_rx = shutdown_tx.subscribe();
            tasks.push(tokio::spawn(async move {
                udp_service_loop(
                    udp,
                    public_addr.port(),
                    &cluster_name,
                    &host,
                    &mut shutdown_rx,
                )
                .await;
            }));
        }

        // ---- mDNS advertisement (primary discovery) ----
        // Non-fatal: the UDP broadcast fallback still works without it.
        let mdns = match MdnsAdvertiser::advertise(&cluster.name, public_addr.port()) {
            Ok(a) => Some(a),
            Err(e) => {
                tracing::warn!("mDNS advertise failed ({e}); UDP discovery fallback still active");
                None
            }
        };

        // ---- keep-awake supervisor: inhibit local sleep while agents work ----
        // Polls the local herdr `agent.list`; holds a platform assertion
        // while any agent is `working` (cluster.json `keepawake`, default
        // on). The stop receiver fires on `shutdown()`; aborting the task
        // (also done by `shutdown()`) releases via the asserter's Drop.
        if cluster.keepawake {
            tasks.push(crate::keepawake::spawn(
                Arc::new(crate::keepawake::LocalHerdrProbe::new()),
                crate::keepawake::platform_asserter(),
                crate::keepawake::POLL_INTERVAL,
                crate::keepawake::RELEASE_GRACE,
                shutdown_tx.subscribe(),
            ));
        } else {
            tracing::debug!("keep-awake disabled by cluster config");
        }

        // ---- status lines ----
        println!(
            "HerdCluster controller '{}' listening on {}",
            cluster.name, public_addr
        );
        println!("run `herdcluster join` on another machine to add a worker");
        println!(
            "to pair a worker now, run `herdcluster join --code {}`",
            code_slot.lock().unwrap().clone()
        );

        Ok(Self {
            inner: Arc::new(Inner {
                controller,
                registry,
                state_dir,
                cluster,
                public_addr,
                pairing_code: code_slot,
                #[cfg(unix)]
                bridges,
                shutdown_tx,
                tasks,
                _mdns: mdns,
            }),
        })
    }

    /// Request graceful shutdown. Idempotent.
    pub fn shutdown(&self) {
        let _ = self.inner.shutdown_tx.send(true);
        for task in &self.inner.tasks {
            task.abort();
        }
        #[cfg(unix)]
        {
            let mut bridges = self.inner.bridges.lock().unwrap();
            for (_, bridge) in bridges.drain() {
                bridge.shutdown();
            }
        }
        if let Ok(mut reg) = self.inner.registry.lock() {
            reg.refresh_statuses(Utc::now());
            let _ = reg.save();
        }
    }

    /// The bound public socket address (actual port when `port` was 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.public_addr
    }

    /// The cluster display name from `cluster.json`.
    pub fn cluster_name(&self) -> &str {
        &self.inner.cluster.name
    }

    /// The herdcluster state dir in use.
    pub fn state_dir(&self) -> &Path {
        &self.inner.state_dir
    }

    /// The current one-time pairing code (rotates when a session expires).
    /// Display it as `herdcluster join --code <code>`.
    pub fn pairing_code(&self) -> String {
        self.inner
            .pairing_code
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Pubkeys of workers with a live session right now.
    pub fn connected_workers(&self) -> Vec<[u8; 32]> {
        self.inner.controller.connected_workers()
    }
}

// ---- background tasks ----

/// Public accept loop: demux each connection into TLS-proxy or pairing.
async fn public_accept_loop(
    listener: TcpListener,
    internal_addr: SocketAddr,
    pairing: Arc<PairingShared>,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    loop {
        let (stream, _peer) = tokio::select! {
            _ = shutdown_rx.changed() => break,
            res = listener.accept() => match res {
                Ok(v) => v,
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let pairing = pairing.clone();
        tokio::spawn(async move {
            handle_public_conn(stream, internal_addr, pairing).await;
        });
    }
}

/// Peek the first payload byte: `0x16` → TLS proxy to the internal
/// listener; anything else → the pairing exchange.
async fn handle_public_conn(
    mut stream: TcpStream,
    internal_addr: SocketAddr,
    pairing: Arc<PairingShared>,
) {
    let mut first = [0u8; 1];
    match tokio::time::timeout(PAIR_HELLO_TIMEOUT, stream.peek(&mut first)).await {
        Ok(Ok(1)) => {}
        _ => return, // closed or stalled before sending anything
    }
    if first[0] == TLS_HANDSHAKE_BYTE {
        // TLS session: proxy bytes to the loopback conn::Controller.
        match TcpStream::connect(internal_addr).await {
            Ok(mut internal) => {
                let _ = tokio::io::copy_bidirectional(&mut stream, &mut internal).await;
            }
            Err(e) => tracing::debug!("internal TLS connect failed: {e}"),
        }
        return;
    }
    pairing_conn(stream, pairing).await;
}

/// One pairing connection: read `PairHello`, drive the FSM (prompt +
/// confirm), write `PairAccept`/`PairReject`, close.
async fn pairing_conn(mut stream: TcpStream, shared: Arc<PairingShared>) {
    let exchange = tokio::time::timeout(PAIR_EXCHANGE_TIMEOUT, async {
        // Read exactly one frame; it must be PairHello.
        let hello = read_one_frame(&mut stream).await;
        let Some(Message::PairHello {
            code,
            worker_pubkey,
            hostname,
            ..
        }) = hello
        else {
            return;
        };
        let Ok(pk) = protocol::pubkey_from_hex(&worker_pubkey) else {
            return;
        };
        run_pairing_exchange(
            &mut stream,
            &shared,
            JoinRequest {
                code,
                worker_pubkey: pk,
                hostname,
            },
        )
        .await;
    });
    if exchange.await.is_err() {
        tracing::debug!("pairing exchange timed out");
    }
}

/// Read frames until the first complete message (or EOF/error).
async fn read_one_frame(stream: &mut TcpStream) -> Option<Message> {
    let mut decoder = protocol::FrameDecoder::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = stream.read(&mut buf).await.ok()?;
        if n == 0 {
            return None;
        }
        decoder.feed(&buf[..n]);
        match decoder.next_message() {
            Ok(Some(msg)) => return Some(msg),
            Ok(None) => continue,
            Err(_) => return None,
        }
    }
}

/// Drive one join request through the FSM and answer on the socket.
async fn run_pairing_exchange(
    stream: &mut TcpStream,
    shared: &Arc<PairingShared>,
    req: JoinRequest,
) {
    shared.ensure_live();
    let pending = shared.with_server(|server| server.handle_join(req));
    match pending {
        Ok(pending) => {
            let fingerprint = identity::fingerprint_of_pubkey(&pending.worker_pubkey);
            println!(
                "Worker '{}' (key {}) wants to join — accept? [y/N]",
                pending.hostname, fingerprint
            );
            let approved = if shared.auto_accept {
                true
            } else {
                confirm_prompt(&pending).await
            };
            let response = if approved {
                shared.with_server(|server| {
                    server.confirm().map(|join| {
                        register_worker(&join, shared);
                        build_pair_accept(shared)
                    })
                })
            } else {
                Ok(shared.with_server(|server| {
                    let _ = server.deny("denied by controller user");
                    Some(Message::PairReject {
                        reason: "join denied by controller user".to_string(),
                    })
                }))
            };
            match response {
                Ok(Some(msg)) => {
                    write_pairing_frame(stream, &msg).await;
                    if let Message::PairAccept { .. } = msg {
                        println!("worker paired successfully");
                    }
                }
                Ok(None) => {
                    write_pairing_frame(
                        stream,
                        &Message::PairReject {
                            reason: "pairing session error".to_string(),
                        },
                    )
                    .await;
                }
                Err(e) => {
                    tracing::debug!("pairing confirm failed: {e}");
                    write_pairing_frame(
                        stream,
                        &Message::PairReject {
                            reason: "pairing session expired".to_string(),
                        },
                    )
                    .await;
                }
            }
        }
        Err(pairing::PairingError::InvalidCode) => {
            write_pairing_frame(
                stream,
                &Message::PairReject {
                    reason: "invalid pairing code".to_string(),
                },
            )
            .await;
        }
        Err(pairing::PairingError::LockedOut) => {
            write_pairing_frame(
                stream,
                &Message::PairReject {
                    reason: "too many failed attempts; try again later".to_string(),
                },
            )
            .await;
        }
        Err(pairing::PairingError::Expired) => {
            // Rotate so the next code is fresh; tell the worker it.
            shared.with_server(|_| ());
            let new_code = shared.code_slot.lock().unwrap().clone();
            write_pairing_frame(
                stream,
                &Message::PairReject {
                    reason: format!("pairing code expired — new code: {new_code}"),
                },
            )
            .await;
        }
        Err(pairing::PairingError::WrongState) => {
            write_pairing_frame(
                stream,
                &Message::PairReject {
                    reason: "another pairing is awaiting confirmation".to_string(),
                },
            )
            .await;
        }
        Err(e) => {
            write_pairing_frame(
                stream,
                &Message::PairReject {
                    reason: format!("pairing error: {e}"),
                },
            )
            .await;
        }
    }
}

/// Encode + write one pairing response frame, then half-close.
async fn write_pairing_frame(stream: &mut TcpStream, msg: &Message) {
    match protocol::encode_frame(msg) {
        Ok(frame) => {
            if stream.write_all(&frame).await.is_ok() {
                let _ = stream.shutdown().await;
            }
        }
        Err(e) => tracing::warn!("pairing response encode failed: {e}"),
    }
}

/// Prompt the local user `[y/N]` until the confirm deadline. Non-TTY
/// stdin always denies (use `auto_accept_pairing` for unattended flows).
async fn confirm_prompt(pending: &pairing::PendingJoin) -> bool {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        println!("non-interactive session: auto-denying join (re-run with --yes to accept)");
        return false;
    }
    let now = SystemClock.now_ms();
    let remaining_ms = pending
        .confirm_deadline_ms
        .saturating_sub(now)
        .min(pairing::CONFIRM_TIMEOUT_MS);
    if remaining_ms == 0 {
        return false;
    }
    print!("accept? [y/N] ");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let deadline = Duration::from_millis(remaining_ms);
    let answer = tokio::time::timeout(deadline, tokio::task::spawn_blocking(read_stdin_line)).await;
    match answer {
        Ok(Ok(line)) => matches!(line.trim().to_ascii_lowercase().as_str(), "y" | "yes"),
        _ => false,
    }
}

fn read_stdin_line() -> String {
    use std::io::BufRead;
    let stdin = std::io::stdin();
    let mut line = String::new();
    let _ = stdin.lock().read_line(&mut line);
    line
}

/// Register an approved worker in `workers.json` and persist immediately
/// (pairing is rare — debounce is only needed for heartbeat churn).
fn register_worker(join: &pairing::ApprovedJoin, shared: &PairingShared) {
    let pubkey_hex = protocol::pubkey_to_hex(&join.worker_pubkey);
    let mut reg = shared.registry.lock().unwrap();
    let label = unique_label(&reg, &join.hostname, &pubkey_hex);
    reg.upsert(WorkerEntry {
        worker_id: registry::random_id(16),
        label,
        pubkey: pubkey_hex,
        paired_at: Utc::now(),
        last_seen: None,
        status: WorkerStatus::Offline,
        resources: None,
    });
    if let Err(e) = reg.save() {
        tracing::warn!("workers.json save after pairing failed: {e}");
    }
}

/// Build the `PairAccept` message carrying the worker credential.
fn build_pair_accept(shared: &PairingShared) -> Option<Message> {
    let credential = WorkerCredential {
        controller_pubkey: protocol::pubkey_to_hex(&shared.cluster_pubkey),
        ca_fingerprint: shared.ca_fingerprint.clone(),
        ca_cert_pem: shared.ca_cert_pem.clone(),
    };
    match credential.encode() {
        Ok(bytes) => Some(Message::PairAccept {
            credential: protocol::bytes_to_base64(&bytes),
            ca_fingerprint: shared.ca_fingerprint.clone(),
        }),
        Err(e) => {
            tracing::error!("failed to encode worker credential: {e}");
            None
        }
    }
}

/// Consume [`Controller::next_event`] forever: mark workers seen in the
/// registry, (unix) spawn a [`ControllerBridge`] per connected worker, and
/// tear bridges down on disconnect.
#[allow(clippy::too_many_arguments)]
async fn controller_event_loop(
    controller: Arc<Controller>,
    registry: Arc<Mutex<WorkerRegistry>>,
    dirty: Arc<AtomicBool>,
    state_dir: PathBuf,
    #[cfg(unix)] bridges: Arc<Mutex<HashMap<String, ControllerBridge>>>,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    loop {
        let event = tokio::select! {
            _ = shutdown_rx.changed() => break,
            ev = controller.next_event() => ev,
        };
        match event {
            ControllerEvent::WorkerConnected(ws) => {
                let registry = registry.clone();
                let dirty = dirty.clone();
                let state_dir = state_dir.clone();
                #[cfg(unix)]
                let bridges = bridges.clone();
                tokio::spawn(async move {
                    on_worker_connected(
                        ws,
                        registry,
                        dirty,
                        state_dir,
                        #[cfg(unix)]
                        bridges,
                    )
                    .await;
                });
            }
            ControllerEvent::WorkerDisconnected { worker_pubkey } => {
                on_worker_disconnected(
                    worker_pubkey,
                    &registry,
                    #[cfg(unix)]
                    &bridges,
                    &dirty,
                );
            }
        }
    }
}

/// Look up the registry row for a worker pubkey (hex). Returns
/// `(worker_id, label)` or `None` when the worker is not paired.
fn find_worker(registry: &WorkerRegistry, pubkey: &[u8; 32]) -> Option<(String, String)> {
    let hex = protocol::pubkey_to_hex(pubkey);
    registry
        .list()
        .iter()
        .find(|w| w.pubkey == hex)
        .map(|w| (w.worker_id.clone(), w.label.clone()))
}

/// A worker completed TLS + auth + `WorkerHello`: verify it is paired,
/// mark it seen, spawn its bridge, and start the control-message pump.
#[allow(clippy::too_many_arguments)]
async fn on_worker_connected(
    ws: WorkerSession,
    registry: Arc<Mutex<WorkerRegistry>>,
    dirty: Arc<AtomicBool>,
    state_dir: PathBuf,
    #[cfg(unix)] bridges: Arc<Mutex<HashMap<String, ControllerBridge>>>,
) {
    let found = {
        let reg = registry.lock().unwrap();
        find_worker(&reg, &ws.worker_pubkey)
    };
    let Some((worker_id, label)) = found else {
        // conn::Controller's auth proves key *possession*, not pairing —
        // an unknown pubkey means revoked/unpaired. Refuse the session.
        tracing::warn!(
            pubkey = %protocol::pubkey_to_hex(&ws.worker_pubkey),
            "unpaired worker connected; closing session"
        );
        ws.session.close();
        return;
    };

    {
        let mut reg = registry.lock().unwrap();
        if reg
            .mark_seen(&worker_id, ws.resources.clone(), Utc::now())
            .is_ok()
        {
            dirty.store(true, Ordering::Relaxed);
        }
    }
    println!("worker '{label}' connected");

    // Control-channel pump: Resources reports feed the registry; other
    // messages are logged. Without this consumer the mux control stream
    // would stall once its buffer filled.
    spawn_control_pump(
        ws.session.clone(),
        worker_id.clone(),
        registry.clone(),
        dirty.clone(),
    );

    #[cfg(unix)]
    {
        match ControllerBridge::new(&state_dir, &label, ws.session.clone()).await {
            Ok(bridge) => {
                let mut map = bridges.lock().unwrap();
                if let Some(old) = map.insert(worker_id.clone(), bridge) {
                    old.shutdown();
                }
            }
            Err(e) => {
                tracing::warn!(worker = %label, "bridge bind failed: {e}");
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (state_dir, ws);
    }
}

/// Drain a worker session's control channel: `Resources` → registry,
/// `Heartbeat` → liveness only, everything else → trace log.
fn spawn_control_pump(
    session: Arc<Session>,
    worker_id: String,
    registry: Arc<Mutex<WorkerRegistry>>,
    dirty: Arc<AtomicBool>,
) {
    tokio::spawn(async move {
        loop {
            match session.recv_message().await {
                Ok(Message::Resources {
                    cpu_cores,
                    mem_total_mb,
                    mem_available_mb,
                    agent_count,
                    max_agents,
                    load_avg,
                    kinds,
                }) => {
                    let resources = Resources {
                        cpu_cores,
                        mem_total_mb,
                        mem_available_mb,
                        agent_count,
                        max_agents,
                        load_avg,
                        kinds,
                    };
                    let mut reg = registry.lock().unwrap();
                    if reg
                        .mark_seen(&worker_id, Some(resources), Utc::now())
                        .is_ok()
                    {
                        dirty.store(true, Ordering::Relaxed);
                    }
                }
                Ok(Message::Heartbeat { .. }) => {}
                Ok(Message::Event { kind, .. }) => {
                    tracing::debug!(worker = %worker_id, kind, "worker event");
                }
                Ok(other) => {
                    tracing::trace!(worker = %worker_id, ?other, "unexpected control message");
                }
                Err(_) => break,
            }
        }
    });
}

/// A worker session ended: tear down its bridge; the registry row stays —
/// `derive_status` flips it to degraded/offline as `last_seen` ages.
fn on_worker_disconnected(
    worker_pubkey: [u8; 32],
    registry: &Arc<Mutex<WorkerRegistry>>,
    #[cfg(unix)] bridges: &Arc<Mutex<HashMap<String, ControllerBridge>>>,
    dirty: &Arc<AtomicBool>,
) {
    let found = {
        let reg = registry.lock().unwrap();
        find_worker(&reg, &worker_pubkey)
    };
    if let Some((worker_id, label)) = found {
        println!("worker '{label}' disconnected");
        #[cfg(unix)]
        if let Some(bridge) = bridges.lock().unwrap().remove(&worker_id) {
            bridge.shutdown();
        }
        dirty.store(true, Ordering::Relaxed);
    }
}

/// How often the revocation watch scans `<state_dir>/revocations/`.
const REVOKE_SCAN_INTERVAL: Duration = Duration::from_millis(500);

/// Poll for revocation markers written by `cluster_node::revoke` and
/// enforce them against live state. See [`process_revocations`].
#[allow(clippy::too_many_arguments)]
async fn revocation_watch_loop(
    controller: Arc<Controller>,
    registry: Arc<Mutex<WorkerRegistry>>,
    dirty: Arc<AtomicBool>,
    state_dir: PathBuf,
    #[cfg(unix)] bridges: Arc<Mutex<HashMap<String, ControllerBridge>>>,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    let dir = crate::revoke::revocations_dir(&state_dir);
    let mut tick = tokio::time::interval(REVOKE_SCAN_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = shutdown_rx.changed() => break,
        }
        process_revocations(
            &controller,
            &registry,
            &dirty,
            &state_dir,
            #[cfg(unix)]
            &bridges,
            &dir,
        );
    }
}

/// Enforce every pending revocation marker in `dir`:
///
/// 1. Close the live [`Session`] for the marker's pubkey (the worker's
///    reconnect will then be refused by `on_worker_connected`'s
///    registry check — revocation is server-side, not cosmetic).
/// 2. Remove the row from the **in-memory** registry — required because
///    this process caches `workers.json` and `persist_loop` would
///    otherwise resurrect the row `revoke_worker` deleted on disk.
/// 3. Drop the worker's [`ControllerBridge`] (unix) so the bridge socket
///    stops serving.
/// 4. Consume the marker file.
///
/// Synchronous and idempotent: markers for already-removed workers are
/// still consumed (their pubkey session close is a no-op when absent).
#[allow(clippy::too_many_arguments)]
fn process_revocations(
    controller: &Arc<Controller>,
    registry: &Arc<Mutex<WorkerRegistry>>,
    dirty: &AtomicBool,
    state_dir: &Path,
    #[cfg(unix)] bridges: &Arc<Mutex<HashMap<String, ControllerBridge>>>,
    dir: &Path,
) {
    let Ok(read) = std::fs::read_dir(dir) else {
        return; // no revocations dir yet — the common case
    };
    for item in read.flatten() {
        let path = item.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(raw) = std::fs::read(&path) else {
            continue;
        };
        let marker: crate::revoke::RevocationMarker = match serde_json::from_slice(&raw) {
            Ok(m) => m,
            Err(_) => {
                // Corrupt marker: quarantine rather than wedge the loop.
                let _ = std::fs::rename(&path, path.with_extension("json.bad"));
                continue;
            }
        };
        if marker.version != crate::revoke::REVOCATION_MARKER_VERSION {
            // Unknown future version — leave it for a newer controller.
            continue;
        }

        // 1. Kill the live session, if any.
        if let Ok(pk) = protocol::pubkey_from_hex(&marker.pubkey) {
            if let Some(sess) = controller.session(&pk) {
                sess.close();
            }
        }

        // 2+3. Registry row + bridge, keyed by worker_id with a pubkey
        // fallback (the marker is authoritative for both).
        let removed_id = {
            let mut reg = registry.lock().unwrap();
            let target = if reg.get(&marker.worker_id).is_some() {
                Some(marker.worker_id.clone())
            } else {
                reg.list()
                    .iter()
                    .find(|w| w.pubkey == marker.pubkey)
                    .map(|w| w.worker_id.clone())
            };
            match target {
                Some(id) if reg.remove(&id) => {
                    dirty.store(true, Ordering::Relaxed);
                    // Revocation is rare — persist immediately instead of
                    // waiting for the debounced tick.
                    if let Err(e) = reg.save() {
                        tracing::warn!("workers.json save after revocation failed: {e}");
                    }
                    Some(id)
                }
                _ => None,
            }
        };
        #[cfg(unix)]
        if let Some(id) = &removed_id {
            if let Some(bridge) = bridges.lock().unwrap().remove(id) {
                bridge.shutdown();
            }
        }
        #[cfg(not(unix))]
        let _ = removed_id;

        // 4. Consume the marker.
        if let Err(e) = crate::revoke::consume_marker(state_dir, &marker.worker_id) {
            tracing::debug!(error = %e, "revocation marker cleanup failed");
        }
        println!("worker '{}' revoked", marker.label);
    }
}

/// Debounced `workers.json` persistence: every [`PERSIST_TICK`] save when
/// dirty, refreshing derived statuses first.
async fn persist_loop(
    registry: Arc<Mutex<WorkerRegistry>>,
    dirty: Arc<AtomicBool>,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(PERSIST_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if dirty.swap(false, Ordering::Relaxed) {
                    let mut reg = registry.lock().unwrap();
                    reg.refresh_statuses(Utc::now());
                    if let Err(e) = reg.save() {
                        tracing::warn!("workers.json save failed: {e}");
                        dirty.store(true, Ordering::Relaxed);
                    }
                }
            }
            _ = shutdown_rx.changed() => break,
        }
    }
}

// ---- UDP service: discovery announce/probe ----

/// UDP loop owning the cluster UDP port: periodic announces + probe
/// replies, wire-compatible with `cluster_net::discovery::UdpBrowser`.
async fn udp_service_loop(
    udp: Arc<UdpSocket>,
    port: u16,
    cluster_name: &str,
    host: &str,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    let announce = match serde_json::to_vec(&announce_packet(port, cluster_name, host)) {
        Ok(v) => v,
        Err(_) => return,
    };
    let broadcast = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), port);
    let loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

    let mut tick = tokio::time::interval(UDP_ANNOUNCE_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut buf = [0u8; 2048];
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let _ = udp.send_to(&announce, broadcast).await;
                let _ = udp.send_to(&announce, loopback).await;
            }
            _ = shutdown_rx.changed() => break,
            res = udp.recv_from(&mut buf) => {
                let Ok((n, src)) = res else { break };
                if let Ok(p) = serde_json::from_slice::<UdpPacket>(&buf[..n]) {
                    if p.v == 1 && p.kind == "probe" {
                        let _ = udp.send_to(&announce, src).await;
                    }
                }
            }
        }
    }
}

// ---- worker-side pairing client (used by tests / worker.rs-style callers) ----

/// Outcome of a successful [`pair_with_controller`] exchange.
#[derive(Debug)]
pub enum PairingClientOutcome {
    /// Controller approved; carries the opaque credential bytes (decode
    /// with [`WorkerCredential::decode`]) and the pinned CA fingerprint.
    Accepted {
        /// Serialized [`WorkerCredential`] bytes.
        credential: Vec<u8>,
        /// Cluster CA fingerprint for pinning.
        ca_fingerprint: String,
    },
    /// Controller rejected the join (bad code, denied, busy, …).
    Rejected {
        /// Human-readable reason from the controller.
        reason: String,
    },
}

/// Errors from [`pair_with_controller`].
#[derive(Debug, Error)]
pub enum PairingClientError {
    /// Socket I/O failure.
    #[error("pairing io error: {0}")]
    Io(#[from] std::io::Error),
    /// Connect or answer deadline elapsed.
    #[error("timed out waiting for the controller's pairing answer")]
    Timeout,
    /// A response arrived but could not be decoded.
    #[error("invalid pairing response: {0}")]
    Protocol(String),
}

/// Pair with a controller over plain TCP — the exact contract
/// `worker.rs::join` implements: connect, write one `PairHello` frame,
/// read frames until `PairAccept`/`PairReject`.
///
/// `worker_pubkey` is this machine's Ed25519 public key
/// ([`cluster_core::identity::Keypair::public_key_bytes`]). `controller`
/// is the cluster address (`ip:port`) discovered via mDNS/UDP/manual.
pub async fn pair_with_controller(
    controller: SocketAddr,
    code: &str,
    worker_pubkey: &[u8; 32],
    hostname: &str,
    caps: &[String],
    timeout: Duration,
) -> Result<PairingClientOutcome, PairingClientError> {
    let deadline = std::time::Instant::now() + timeout;
    let connect_deadline = deadline.min(std::time::Instant::now() + PAIR_HELLO_TIMEOUT);
    let remaining = connect_deadline.saturating_duration_since(std::time::Instant::now());
    let mut stream = tokio::time::timeout(remaining, TcpStream::connect(controller))
        .await
        .map_err(|_| PairingClientError::Timeout)?
        .map_err(PairingClientError::Io)?;

    let hello = Message::PairHello {
        code: code.to_string(),
        worker_pubkey: protocol::pubkey_to_hex(worker_pubkey),
        hostname: hostname.to_string(),
        caps: caps.to_vec(),
    };
    let frame =
        protocol::encode_frame(&hello).map_err(|e| PairingClientError::Protocol(e.to_string()))?;
    stream.write_all(&frame).await?;

    let mut decoder = protocol::FrameDecoder::new();
    let mut buf = [0u8; 8192];
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(PairingClientError::Timeout);
        }
        let n = match tokio::time::timeout(remaining, stream.read(&mut buf)).await {
            Ok(Ok(0)) => {
                return Err(PairingClientError::Protocol(
                    "controller closed the connection".to_string(),
                ))
            }
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(PairingClientError::Io(e)),
            Err(_) => return Err(PairingClientError::Timeout),
        };
        decoder.feed(&buf[..n]);
        loop {
            let msg = decoder
                .next_message()
                .map_err(|e| PairingClientError::Protocol(e.to_string()))?;
            let Some(msg) = msg else { break };
            match msg {
                Message::PairAccept {
                    credential,
                    ca_fingerprint,
                } => {
                    let bytes = protocol::base64_to_bytes(&credential)
                        .map_err(|e| PairingClientError::Protocol(e.to_string()))?;
                    return Ok(PairingClientOutcome::Accepted {
                        credential: bytes,
                        ca_fingerprint,
                    });
                }
                Message::PairReject { reason } => {
                    return Ok(PairingClientOutcome::Rejected { reason });
                }
                _ => {} // forward-compatible: ignore other messages
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster_core::identity::Keypair;
    use cluster_net::conn::{self, save_worker_credential, WorkerConfig};

    fn tempdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hc-ctrl-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn worker_cfg(hostname: &str) -> WorkerConfig {
        WorkerConfig {
            hostname: hostname.to_string(),
            caps: vec!["claude".to_string()],
            resources: Resources {
                cpu_cores: 4,
                mem_total_mb: 8192,
                mem_available_mb: 4096,
                agent_count: 0,
                max_agents: 4,
                load_avg: vec![],
                kinds: vec!["claude".to_string()],
            },
        }
    }

    /// Full loopback: init controller (auto-accept) → TCP pairing →
    /// credential → TLS WorkerClient connect → registry row + bridge sock.
    #[cfg(unix)]
    #[tokio::test]
    async fn ctrl_pairing_then_worker_connects() {
        let state_dir = tempdir("e2e");
        let worker_dir = tempdir("worker");

        let handle = ControllerHandle::run(ControllerConfig {
            name: Some("test-cluster".to_string()),
            port: Some(0),
            auto_accept_pairing: true,
            state_dir: Some(state_dir.clone()),
            ..Default::default()
        })
        .await
        .expect("controller run");
        let addr = handle.local_addr();
        assert_ne!(addr.port(), 0);
        assert_eq!(handle.cluster_name(), "test-cluster");

        // Worker identity (created before pairing, as worker.rs will do).
        let identity = Keypair::load_or_create(&worker_dir.join("identity.key")).unwrap();
        let pubkey = identity.public_key_bytes();

        // Pair over plain TCP using the shared helper (same contract as
        // worker.rs::join's pairing_exchange).
        let outcome = pair_with_controller(
            addr,
            &handle.pairing_code(),
            &pubkey,
            "test-worker",
            &["claude".to_string()],
            Duration::from_secs(15),
        )
        .await
        .expect("pair_with_controller");
        let PairingClientOutcome::Accepted {
            credential,
            ca_fingerprint,
        } = outcome
        else {
            panic!("expected PairAccept, got {outcome:?}");
        };

        // Persist the credential and connect as a real worker — the TLS
        // path must traverse the public demux listener + proxy.
        let cred = WorkerCredential::decode(&credential).unwrap();
        assert_eq!(cred.ca_fingerprint, ca_fingerprint);
        save_worker_credential(&worker_dir, &cred).unwrap();
        let client = conn::WorkerClient::load(&worker_dir, worker_cfg("test-worker")).unwrap();
        let session = client.connect_once(addr).await.expect("worker connect");
        assert!(session.is_alive());

        // Registry: worker row exists with our pubkey.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            {
                let reg = WorkerRegistry::open(state_dir.join("workers.json")).unwrap();
                if !reg.list().is_empty() {
                    let w = &reg.list()[0];
                    assert_eq!(w.pubkey, protocol::pubkey_to_hex(&pubkey));
                    assert_eq!(w.label, "test-worker");
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker never registered"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Bridge socket exists for the worker label.
        let sock = state_dir.join("bridge").join("test-worker.sock");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !sock.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "bridge socket missing"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        session.close();
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&state_dir);
        let _ = std::fs::remove_dir_all(&worker_dir);
    }

    /// Wrong pairing code → PairReject, worker never registers.
    #[cfg(unix)]
    #[tokio::test]
    async fn ctrl_pairing_rejects_bad_code() {
        let state_dir = tempdir("badcode");
        let handle = ControllerHandle::run(ControllerConfig {
            port: Some(0),
            auto_accept_pairing: true,
            state_dir: Some(state_dir.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        let addr = handle.local_addr();
        let kp = Keypair::generate();

        let outcome = pair_with_controller(
            addr,
            "ZZZZZZZZ", // not the displayed code
            &kp.public_key_bytes(),
            "bad-worker",
            &[],
            Duration::from_secs(15),
        )
        .await
        .unwrap();
        match outcome {
            PairingClientOutcome::Rejected { reason } => {
                assert!(reason.contains("invalid pairing code"), "{reason}");
            }
            other => panic!("expected rejection, got {other:?}"),
        }
        let reg = WorkerRegistry::open(state_dir.join("workers.json")).unwrap();
        assert!(reg.list().is_empty());
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&state_dir);
    }

    /// cluster.json + identity.key + TLS material all land under the
    /// injected state dir.
    #[cfg(unix)]
    #[tokio::test]
    async fn ctrl_state_layout_created() {
        let state_dir = tempdir("layout");
        let handle = ControllerHandle::run(ControllerConfig {
            name: Some("layout-cluster".to_string()),
            port: Some(0),
            auto_accept_pairing: true,
            state_dir: Some(state_dir.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        for p in [
            "cluster.json",
            "identity.key",
            "tls/ca.crt",
            "tls/server.crt",
        ] {
            assert!(state_dir.join(p).exists(), "missing {p}");
        }
        assert_eq!(handle.pairing_code().len(), 8);
        handle.shutdown();
        let _ = std::fs::remove_dir_all(&state_dir);
    }

    /// `revoke_worker_in_state_dir` against a *running* controller:
    /// the marker file must kill the live session, drop the in-memory
    /// registry row (which persist_loop would otherwise resurrect),
    /// remove the bridge, and be consumed — all without touching
    /// controller internals.
    #[cfg(unix)]
    #[tokio::test]
    async fn ctrl_revoke_kills_live_session_and_row() {
        let state_dir = tempdir("revoke");
        let worker_dir = tempdir("revoke-worker");

        let handle = ControllerHandle::run(ControllerConfig {
            port: Some(0),
            auto_accept_pairing: true,
            state_dir: Some(state_dir.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        let addr = handle.local_addr();

        let identity = Keypair::load_or_create(&worker_dir.join("identity.key")).unwrap();
        let pubkey = identity.public_key_bytes();
        let outcome = pair_with_controller(
            addr,
            &handle.pairing_code(),
            &pubkey,
            "revoke-me",
            &[],
            Duration::from_secs(15),
        )
        .await
        .unwrap();
        let PairingClientOutcome::Accepted { credential, .. } = outcome else {
            panic!("expected PairAccept");
        };
        let cred = WorkerCredential::decode(&credential).unwrap();
        save_worker_credential(&worker_dir, &cred).unwrap();
        let client = conn::WorkerClient::load(&worker_dir, worker_cfg("revoke-me")).unwrap();
        let session = client.connect_once(addr).await.expect("worker connect");
        assert!(session.is_alive());

        // Wait for the registry row so we know the worker_id for the
        // marker-consumed assertion.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let worker_id = loop {
            {
                let reg = WorkerRegistry::open(state_dir.join("workers.json")).unwrap();
                if let Some(w) = reg.list().first() {
                    break w.worker_id.clone();
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker never registered"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };

        // Revoke from "another process" — file-level only.
        let outcome =
            crate::revoke::revoke_worker_in_state_dir("revoke-me", &state_dir).expect("revoke");
        assert!(outcome.removed_registry);
        assert!(outcome.disconnected_live);

        // The watch loop (500 ms cadence) must: close the session,
        // consume the marker, and keep workers.json empty.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let session_dead = !session.is_alive();
            let marker_gone =
                !crate::revoke::revocation_marker_path(&state_dir, &worker_id).exists();
            let reg_empty = WorkerRegistry::open(state_dir.join("workers.json"))
                .unwrap()
                .list()
                .is_empty();
            if session_dead && marker_gone && reg_empty {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "revoke not enforced: session_dead={session_dead} marker_gone={marker_gone} reg_empty={reg_empty}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Bridge socket for the label is gone too.
        assert!(!state_dir.join("bridge").join("revoke-me.sock").exists());

        handle.shutdown();
        let _ = std::fs::remove_dir_all(&state_dir);
        let _ = std::fs::remove_dir_all(&worker_dir);
    }
}
