//! LAN discovery: mDNS primary + UDP broadcast fallback.
//!
//! # What is discovered
//!
//! A HerdCluster **controller** advertises itself so workers (`join`) can
//! find it without typing an address:
//!
//! - **mDNS** (primary): service type `_herdcluster._tcp.local.` with TXT
//!   records `port`, `cluster_name`, `protocol_version` (see
//!   [`MdnsAdvertiser`]/[`MdnsBrowser`]). `mdns-sd` does the heavy lifting.
//! - **UDP broadcast** (fallback for mDNS-filtered LANs, e.g. some WSL2
//!   NAT setups): announcers listen on UDP [`CLUSTER_PORT`] and answer
//!   `{"kind":"probe"}` datagrams plus emit periodic
//!   `{"kind":"announce", …}` broadcasts (see [`UdpAnnouncer`]/
//!   [`UdpBrowser`]). Directed `127.0.0.1` probes are also sent so
//!   localhost tests and same-machine dev flows work without multicast.
//!
//! # Usage
//!
//! - Controller (`init`): call [`discover_controllers`] is NOT needed;
//!   instead [`MdnsAdvertiser::advertise`] + [`UdpAnnouncer::start`] keep
//!   the advertisement alive for the process lifetime.
//! - Worker (`join`): [`discover_controllers`] returns every candidate
//!   found by both mechanisms (deduplicated). Pass the result through
//!   [`resolve_controller`] together with the optional `--controller`
//!   manual override. Multiple candidates without an override produce a
//!   [`DiscoveryError::Ambiguous`] listing them — print with
//!   [`format_candidates`] (the `--discover` debug view).
//!
//! # WSL2 caveat
//!
//! WSL2's default NAT does not forward multicast/broadcast between the
//! Windows host and the VM. Mirrored networking mode fixes this; otherwise
//! the worker should use the manual `--controller <windows-host-ip>`
//! override (see PLAN.md §3.5 risk table).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;

/// mDNS service type advertised by controllers.
pub const MDNS_SERVICE_TYPE: &str = "_herdcluster._tcp.local.";

/// Cluster service port — TCP for the TLS connection **and** UDP for
/// discovery broadcast (PLAN.md OQ-4: `47820/TCP` + `47820/UDP`).
/// Mirrors `cluster_core::config::DEFAULT_PORT`.
pub const CLUSTER_PORT: u16 = cluster_core::config::DEFAULT_PORT;

/// How often the UDP announcer repeats its broadcast announce.
const UDP_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(1);

/// Default browse window for [`discover_controllers`].
pub const DEFAULT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);

// ---- candidate model ----

/// Where a [`DiscoveredController`] was learned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoverySource {
    /// Learned via mDNS (`_herdcluster._tcp.local.`).
    Mdns,
    /// Learned via the UDP broadcast fallback.
    UdpBroadcast,
    /// Supplied manually via `--controller` (not discovered).
    Manual,
}

/// A discovered (or manually supplied) cluster controller endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredController {
    /// Controller hostname (mDNS instance host or UDP `hostname` field).
    pub hostname: String,
    /// Controller's self-reported cluster name, if advertised.
    pub cluster_name: Option<String>,
    /// Advertised wire protocol version, if known.
    pub protocol_version: Option<u32>,
    /// Address to dial (TCP [`CLUSTER_PORT`] for real controllers).
    pub addr: SocketAddr,
    /// Which mechanism produced this candidate.
    pub source: DiscoverySource,
}

impl fmt::Display for DiscoveredController {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.addr, self.source_label())
    }
}

impl DiscoveredController {
    fn source_label(&self) -> &'static str {
        match self.source {
            DiscoverySource::Mdns => "mdns",
            DiscoverySource::UdpBroadcast => "udp",
            DiscoverySource::Manual => "manual",
        }
    }
}

/// Errors from discovery.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    /// I/O error (socket bind/send, mDNS daemon).
    #[error("discovery io error: {0}")]
    Io(#[from] std::io::Error),
    /// mDNS daemon error.
    #[error("mdns error: {0}")]
    Mdns(String),
    /// No controllers were found within the browse window.
    #[error("no herdcluster controllers found on this network")]
    NoneFound,
    /// Several controllers found and no `--controller` override was given.
    #[error("multiple controllers found; pick one with --controller")]
    Ambiguous {
        /// The candidates the user must choose from.
        candidates: Vec<DiscoveredController>,
    },
    /// A `--controller` argument could not be parsed.
    #[error("invalid --controller value '{input}': {reason}")]
    BadManualInput {
        /// The raw argument.
        input: String,
        /// Why it failed.
        reason: String,
    },
}

// ---- manual override parsing ----

/// Parse a `--controller` value: `host`, `host:port`, or a bare IP.
/// Returns `(host, port)`; port defaults to [`CLUSTER_PORT`]. The caller
/// resolves `host` (e.g. `tokio::net::lookup_host`).
pub fn parse_controller_arg(input: &str) -> Result<(String, u16), DiscoveryError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(DiscoveryError::BadManualInput {
            input: input.to_string(),
            reason: "empty".to_string(),
        });
    }
    // Bare SocketAddr ("1.2.3.4:47820" or "[::1]:47820").
    if let Ok(sa) = input.parse::<SocketAddr>() {
        return Ok((sa.ip().to_string(), sa.port()));
    }
    // "host:port" — split on the last ':' only when what follows parses
    // as a port, so IPv6 literals without brackets fall through to host.
    if let Some((host, port_str)) = input.rsplit_once(':') {
        if !host.is_empty() && !host.contains(':') {
            if let Ok(port) = port_str.parse::<u16>() {
                return Ok((host.to_string(), port));
            }
        }
    }
    // Bare host/IP → default cluster port.
    Ok((input.to_string(), CLUSTER_PORT))
}

/// Choose the controller to dial.
///
/// - `manual` (a raw `--controller` string) always wins.
/// - Otherwise exactly one discovered candidate is used.
/// - Zero candidates → [`DiscoveryError::NoneFound`]; several →
///   [`DiscoveryError::Ambiguous`] carrying the list to display.
pub fn resolve_controller(
    manual: Option<&str>,
    candidates: &[DiscoveredController],
) -> Result<(String, u16), DiscoveryError> {
    if let Some(m) = manual {
        return parse_controller_arg(m);
    }
    match candidates.len() {
        0 => Err(DiscoveryError::NoneFound),
        1 => {
            let c = &candidates[0];
            Ok((c.addr.ip().to_string(), c.addr.port()))
        }
        _ => Err(DiscoveryError::Ambiguous {
            candidates: candidates.to_vec(),
        }),
    }
}

/// `--discover` debug rendering: one line per candidate.
pub fn format_candidates(candidates: &[DiscoveredController]) -> String {
    if candidates.is_empty() {
        return "no controllers discovered".to_string();
    }
    let mut out = String::new();
    for c in candidates {
        out.push_str(&format!(
            "  {}  hostname={} cluster={} proto={} src={}\n",
            c.addr,
            c.hostname,
            c.cluster_name.as_deref().unwrap_or("-"),
            c.protocol_version
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string()),
            c.source_label(),
        ));
    }
    out
}

/// Debug helper for `herdcluster --discover`: browse both mechanisms and
/// print the candidate list to stdout.
pub async fn debug_discover(timeout: Duration) {
    let candidates = discover_controllers(timeout).await;
    print!("{}", format_candidates(&candidates));
}

// ---- combined entry point ----

/// Browse mDNS **and** UDP broadcast for controllers, returning a
/// deduplicated candidate list (keyed by socket address).
pub async fn discover_controllers(timeout: Duration) -> Vec<DiscoveredController> {
    // Run UDP browse (async) and mDNS browse (sync daemon + channel)
    // concurrently by splitting the window: UDP gets the full window,
    // mDNS is collected in a blocking task for the same window.
    let udp = UdpBrowser::browse(timeout);
    let mdns = tokio::task::spawn_blocking(move || mdns_browse(timeout));
    let (udp_res, mdns_res) = tokio::join!(udp, mdns);
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for c in udp_res.into_iter().chain(mdns_res.unwrap_or_default()) {
        if seen.insert(c.addr) {
            out.push(c);
        }
    }
    out
}

// ---- mDNS ----

/// Keeps a controller's mDNS advertisement alive. Drop to withdraw
/// (the daemon unregisters on shutdown).
pub struct MdnsAdvertiser {
    daemon: mdns_sd::ServiceDaemon,
    fullname: String,
}

impl MdnsAdvertiser {
    /// Advertise `_herdcluster._tcp.local.` for `cluster_name` on `port`.
    /// TXT records: `port`, `cluster_name`, `protocol_version`.
    pub fn advertise(cluster_name: &str, port: u16) -> Result<Self, DiscoveryError> {
        let daemon =
            mdns_sd::ServiceDaemon::new().map_err(|e| DiscoveryError::Mdns(e.to_string()))?;
        let instance = instance_name();
        let host = hostname_domain();
        let ip = primary_ipv4().unwrap_or(Ipv4Addr::LOCALHOST);
        let txt = [
            ("port", port.to_string()),
            ("cluster_name", cluster_name.to_string()),
            (
                "protocol_version",
                cluster_core::protocol::PROTOCOL_VERSION.to_string(),
            ),
        ];
        let info = mdns_sd::ServiceInfo::new(
            MDNS_SERVICE_TYPE,
            &instance,
            &host,
            IpAddr::V4(ip),
            port,
            &txt[..],
        )
        .map_err(|e| DiscoveryError::Mdns(e.to_string()))?;
        let fullname = info.get_fullname().to_string();
        daemon
            .register(info)
            .map_err(|e| DiscoveryError::Mdns(e.to_string()))?;
        Ok(Self { daemon, fullname })
    }
}

impl Drop for MdnsAdvertiser {
    fn drop(&mut self) {
        let _ = self.daemon.unregister(&self.fullname);
        let _ = self.daemon.shutdown();
    }
}

fn mdns_browse(window: Duration) -> Vec<DiscoveredController> {
    let daemon = match mdns_sd::ServiceDaemon::new() {
        Ok(d) => d,
        Err(_) => return Vec::new(), // mDNS unavailable — UDP fallback covers us.
    };
    let rx = match daemon.browse(MDNS_SERVICE_TYPE) {
        Ok(rx) => rx,
        Err(_) => return Vec::new(),
    };
    let deadline = Instant::now() + window;
    let mut out = Vec::new();
    // Collect resolved services until the window closes.
    while Instant::now() < deadline {
        // flume::Receiver has try_recv + recv_timeout; use try_recv with a
        // short sleep to avoid naming the flume error type.
        match rx.try_recv() {
            Ok(mdns_sd::ServiceEvent::ServiceResolved(svc)) => {
                out.extend(resolved_to_candidates(&svc));
            }
            Ok(_) => {}
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    let _ = daemon.shutdown();
    out
}

fn resolved_to_candidates(svc: &mdns_sd::ResolvedService) -> Vec<DiscoveredController> {
    let port = svc.get_port();
    let hostname = svc.get_hostname().trim_end_matches('.').to_string();
    let cluster_name = svc
        .get_property_val_str("cluster_name")
        .map(|s| s.to_string());
    let protocol_version = svc
        .get_property_val_str("protocol_version")
        .and_then(|s| s.parse::<u32>().ok());
    let mut out = Vec::new();
    for ip in svc.get_addresses_v4() {
        out.push(DiscoveredController {
            hostname: hostname.clone(),
            cluster_name: cluster_name.clone(),
            protocol_version,
            addr: SocketAddr::new(IpAddr::V4(ip), port),
            source: DiscoverySource::Mdns,
        });
    }
    out
}

fn instance_name() -> String {
    let host = hostname_domain();
    host.trim_end_matches('.').to_string()
}

fn hostname_domain() -> String {
    // Prefer FQDN-ish local hostname; mdns-sd accepts bare names too.
    let raw = ["HOSTNAME", "COMPUTERNAME"]
        .iter()
        .find_map(|v| std::env::var(v).ok())
        .unwrap_or_else(|| "herdcluster".to_string());
    let raw = raw.trim().to_string();
    if raw.ends_with(".local.") || raw.contains('.') {
        raw
    } else {
        format!("{raw}.local.")
    }
}

/// First non-loopback IPv4 address, if any.
fn primary_ipv4() -> Option<Ipv4Addr> {
    use std::net::ToSocketAddrs;
    // Try resolving the local hostname; fall back to interface enumeration
    // via a UDP socket trick is unreliable, so loopback is the last resort
    // handled by the caller.
    if let Ok(name) = std::env::var("HOSTNAME").or_else(|_| std::env::var("COMPUTERNAME")) {
        if let Ok(addrs) = (name.as_str(), 0u16).to_socket_addrs() {
            for a in addrs {
                if let IpAddr::V4(v4) = a.ip() {
                    if !v4.is_loopback() {
                        return Some(v4);
                    }
                }
            }
        }
    }
    None
}

// ---- UDP broadcast fallback ----

/// Wire format for UDP discovery datagrams.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UdpPacket {
    /// Schema version (currently 1).
    v: u32,
    /// `"probe"` (browser → announcers) or `"announce"` (announcer → all).
    kind: String,
    /// Announce only: TCP port of the cluster service.
    #[serde(default)]
    port: u16,
    /// Announce only: cluster display name.
    #[serde(default)]
    cluster_name: String,
    /// Announce only: wire protocol version.
    #[serde(default)]
    protocol_version: u32,
    /// Announce only: controller hostname.
    #[serde(default)]
    hostname: String,
}

fn encode_packet(p: &UdpPacket) -> Vec<u8> {
    serde_json::to_vec(p).unwrap_or_default()
}

fn decode_packet(buf: &[u8]) -> Option<UdpPacket> {
    let p: UdpPacket = serde_json::from_slice(buf).ok()?;
    if p.v != 1 {
        return None;
    }
    Some(p)
}

/// Controller-side UDP announcer. Bind once via [`UdpAnnouncer::start`];
/// the background task answers probes and re-broadcasts announces.
pub struct UdpAnnouncer {
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
    udp_port: u16,
}

impl UdpAnnouncer {
    /// Start announcing on the default [`CLUSTER_PORT`].
    pub async fn start(cluster_name: &str, hostname: &str) -> Result<Self, DiscoveryError> {
        Self::start_on(cluster_name, hostname, CLUSTER_PORT, UDP_ANNOUNCE_INTERVAL).await
    }

    /// Start announcing on a custom UDP port (tests, port conflicts).
    /// `announce_interval` controls broadcast cadence.
    pub async fn start_on(
        cluster_name: &str,
        hostname: &str,
        udp_port: u16,
        announce_interval: Duration,
    ) -> Result<Self, DiscoveryError> {
        let sock = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, udp_port)).await?;
        sock.set_broadcast(true)?;
        let packet = Arc::new(UdpPacket {
            v: 1,
            kind: "announce".to_string(),
            port: CLUSTER_PORT,
            cluster_name: cluster_name.to_string(),
            protocol_version: cluster_core::protocol::PROTOCOL_VERSION,
            hostname: hostname.to_string(),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_announcer(
            sock,
            udp_port,
            packet,
            announce_interval,
            shutdown_rx,
        ));
        Ok(Self {
            shutdown: shutdown_tx,
            task,
            udp_port,
        })
    }

    /// The UDP port this announcer listens on.
    pub fn local_port(&self) -> u16 {
        self.udp_port
    }
}

impl Drop for UdpAnnouncer {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        self.task.abort();
    }
}

async fn run_announcer(
    sock: UdpSocket,
    udp_port: u16,
    packet: Arc<UdpPacket>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let bcast = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), udp_port);
    let loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), udp_port);
    let payload = encode_packet(&packet);
    let mut buf = [0u8; 2048];
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                // Broadcast announce; also send to loopback so same-machine
                // browsers reliably see it even where broadcast is filtered.
                let _ = sock.send_to(&payload, bcast).await;
                let _ = sock.send_to(&payload, loopback).await;
            }
            res = sock.recv_from(&mut buf) => {
                match res {
                    Ok((n, src)) => {
                        if let Some(p) = decode_packet(&buf[..n]) {
                            if p.kind == "probe" && p.v == 1 {
                                // Reply unicast to the prober's address.
                                let _ = sock.send_to(&payload, src).await;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            _ = shutdown.changed() => break,
        }
    }
}

/// Worker-side UDP browser. Stateless; [`UdpBrowser::browse`] sends a
/// probe and collects announces for the window.
pub struct UdpBrowser;

impl UdpBrowser {
    /// Browse the default [`CLUSTER_PORT`] for `window`.
    pub async fn browse(window: Duration) -> Vec<DiscoveredController> {
        Self::browse_ports(window, &[CLUSTER_PORT]).await
    }

    /// Browse a set of UDP ports (tests use custom ports). For each port a
    /// probe is broadcast *and* sent to `127.0.0.1` so same-host instances
    /// are found even when multicast is unavailable.
    pub async fn browse_ports(window: Duration, ports: &[u16]) -> Vec<DiscoveredController> {
        let sock = match UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let _ = sock.set_broadcast(true);
        let probe = encode_packet(&UdpPacket {
            v: 1,
            kind: "probe".to_string(),
            port: 0,
            cluster_name: String::new(),
            protocol_version: 0,
            hostname: String::new(),
        });
        let mut targets: Vec<SocketAddr> = Vec::new();
        for &p in ports {
            targets.push(SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), p));
            targets.push(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), p));
        }
        for t in &targets {
            let _ = sock.send_to(&probe, *t).await;
        }

        let deadline = Instant::now() + window;
        let mut buf = [0u8; 2048];
        // Dedupe by (controller IP, TCP port, hostname) — two controllers
        // on the same host share the TCP port but differ by hostname.
        let mut seen = BTreeMap::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, sock.recv_from(&mut buf)).await {
                Ok(Ok((n, src))) => {
                    if let Some(p) = decode_packet(&buf[..n]) {
                        if p.kind == "announce" && p.port != 0 {
                            // The datagram's source IP is the controller's
                            // address; `port` is its TCP service port.
                            let addr = SocketAddr::new(src.ip(), p.port);
                            let host_key = if p.hostname.is_empty() {
                                src.ip().to_string()
                            } else {
                                p.hostname.clone()
                            };
                            seen.entry((addr, host_key))
                                .or_insert_with(|| DiscoveredController {
                                    hostname: if p.hostname.is_empty() {
                                        src.ip().to_string()
                                    } else {
                                        p.hostname.clone()
                                    },
                                    cluster_name: if p.cluster_name.is_empty() {
                                        None
                                    } else {
                                        Some(p.cluster_name.clone())
                                    },
                                    protocol_version: if p.protocol_version == 0 {
                                        None
                                    } else {
                                        Some(p.protocol_version)
                                    },
                                    addr,
                                    source: DiscoverySource::UdpBroadcast,
                                });
                        }
                    }
                }
                _ => break,
            }
        }
        seen.into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn free_udp_port() -> u16 {
        // Bind :0 to learn a free port, then drop it (small race, fine
        // for tests).
        let s = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        s.local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn udp_announcer_answered_by_browser() {
        let port = free_udp_port();
        let ann = UdpAnnouncer::start_on(
            "test-cluster",
            "test-host",
            port,
            Duration::from_millis(100),
        )
        .await
        .unwrap();
        assert_eq!(ann.local_port(), port);

        // Browse only this port; window long enough for ≥1 announce tick.
        let found = UdpBrowser::browse_ports(Duration::from_millis(600), &[port]).await;
        assert_eq!(found.len(), 1, "expected exactly one candidate: {found:?}");
        let c = &found[0];
        assert_eq!(c.cluster_name.as_deref(), Some("test-cluster"));
        assert_eq!(c.hostname, "test-host");
        assert_eq!(c.source, DiscoverySource::UdpBroadcast);
        assert_eq!(c.addr.port(), CLUSTER_PORT);
        assert_eq!(c.addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(
            c.protocol_version,
            Some(cluster_core::protocol::PROTOCOL_VERSION)
        );
    }

    #[tokio::test]
    async fn udp_two_instances_both_discovered() {
        // "Two localhost instances": two announcers on distinct ports, one
        // browser that probes both.
        let p1 = free_udp_port();
        let p2 = free_udp_port();
        let a1 = UdpAnnouncer::start_on("cluster-a", "host-a", p1, Duration::from_millis(80))
            .await
            .unwrap();
        let a2 = UdpAnnouncer::start_on("cluster-b", "host-b", p2, Duration::from_millis(80))
            .await
            .unwrap();

        let found = UdpBrowser::browse_ports(Duration::from_millis(700), &[p1, p2]).await;
        let names: Vec<_> = found.iter().map(|c| c.cluster_name.clone()).collect();
        assert!(names.contains(&Some("cluster-a".to_string())), "{found:?}");
        assert!(names.contains(&Some("cluster-b".to_string())), "{found:?}");
        drop(a1);
        drop(a2);
    }

    #[tokio::test]
    async fn udp_probe_gets_unicast_reply() {
        // Announce interval long enough that only the probe reply can
        // produce a candidate within the window.
        let port = free_udp_port();
        let _ann = UdpAnnouncer::start_on("quiet", "quiet-host", port, Duration::from_secs(60))
            .await
            .unwrap();
        let found = UdpBrowser::browse_ports(Duration::from_millis(500), &[port]).await;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].hostname, "quiet-host");
    }

    #[test]
    fn parse_controller_arg_variants() {
        assert_eq!(
            parse_controller_arg("192.168.1.10:47821").unwrap(),
            ("192.168.1.10".to_string(), 47821)
        );
        assert_eq!(
            parse_controller_arg("macbook.local").unwrap(),
            ("macbook.local".to_string(), CLUSTER_PORT)
        );
        assert_eq!(
            parse_controller_arg("10.0.0.5").unwrap(),
            ("10.0.0.5".to_string(), CLUSTER_PORT)
        );
        assert!(parse_controller_arg("").is_err());
    }

    #[test]
    fn resolve_controller_prefers_manual() {
        let cands = vec![DiscoveredController {
            hostname: "h".into(),
            cluster_name: None,
            protocol_version: None,
            addr: "192.168.1.2:47820".parse().unwrap(),
            source: DiscoverySource::UdpBroadcast,
        }];
        // Manual override wins even with candidates present.
        assert_eq!(
            resolve_controller(Some("192.168.9.9:1234"), &cands).unwrap(),
            ("192.168.9.9".to_string(), 1234)
        );
        // Single candidate auto-selected.
        assert_eq!(
            resolve_controller(None, &cands).unwrap(),
            ("192.168.1.2".to_string(), 47820)
        );
        // Empty → NoneFound.
        assert!(matches!(
            resolve_controller(None, &[]),
            Err(DiscoveryError::NoneFound)
        ));
        // Two → Ambiguous carrying both.
        let mut two = cands.clone();
        two.push(DiscoveredController {
            hostname: "h2".into(),
            cluster_name: None,
            protocol_version: None,
            addr: "192.168.1.3:47820".parse().unwrap(),
            source: DiscoverySource::Mdns,
        });
        match resolve_controller(None, &two) {
            Err(DiscoveryError::Ambiguous { candidates }) => {
                assert_eq!(candidates.len(), 2);
            }
            other => panic!("expected Ambiguous, got {other:?}"),
        }
    }

    #[test]
    fn format_candidates_renders_list() {
        let c = DiscoveredController {
            hostname: "mac".into(),
            cluster_name: Some("homelab".into()),
            protocol_version: Some(1),
            addr: "192.168.1.5:47820".parse().unwrap(),
            source: DiscoverySource::Mdns,
        };
        let s = format_candidates(&[c]);
        assert!(s.contains("192.168.1.5:47820"));
        assert!(s.contains("homelab"));
        assert!(s.contains("mdns"));
        assert!(format_candidates(&[]).contains("no controllers"));
    }

    #[test]
    fn udp_packet_roundtrip_and_version_gate() {
        let p = UdpPacket {
            v: 1,
            kind: "announce".into(),
            port: 47820,
            cluster_name: "c".into(),
            protocol_version: 1,
            hostname: "h".into(),
        };
        let decoded = decode_packet(&encode_packet(&p)).unwrap();
        assert_eq!(decoded.kind, "announce");
        assert_eq!(decoded.port, 47820);
        let mut bad = p.clone();
        bad.v = 2;
        assert!(decode_packet(&encode_packet(&bad)).is_none());
    }

    /// mDNS advertise+browse on loopback. Requires a working mDNS daemon;
    /// ignored by default — run manually with `--ignored`.
    #[test]
    #[ignore = "requires multicast-capable network; run manually"]
    fn mdns_advertise_and_browse() {
        let adv = MdnsAdvertiser::advertise("mdns-test", CLUSTER_PORT).unwrap();
        std::thread::sleep(Duration::from_millis(500));
        let found = mdns_browse(Duration::from_secs(3));
        assert!(found.iter().any(|c| c.source == DiscoverySource::Mdns));
        drop(adv);
    }

    #[test]
    fn mdns_service_info_validates_txt() {
        // Construction must succeed with our TXT record set (no daemon).
        let txt = [
            ("port", "47820".to_string()),
            ("cluster_name", "x".to_string()),
            ("protocol_version", "1".to_string()),
        ];
        let info = mdns_sd::ServiceInfo::new(
            MDNS_SERVICE_TYPE,
            "test-instance",
            "test-host.local.",
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            CLUSTER_PORT,
            &txt[..],
        )
        .unwrap();
        assert_eq!(info.get_port(), CLUSTER_PORT);
        assert_eq!(info.get_property_val_str("cluster_name"), Some("x"));
    }
}
