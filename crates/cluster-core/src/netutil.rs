//! Small network helpers shared by controller/worker UX.

use std::net::IpAddr;

/// Best-effort guess of this machine's LAN IPv4 address — the address a
/// worker on the same network should dial. Uses a UDP "connect" to a
/// public IP: the kernel picks the outbound interface without sending
/// any packets, so it works offline and never touches the network.
///
/// Returns `None` when no IPv4 route exists (e.g. only IPv6/loopback).
pub fn lan_ipv4() -> Option<IpAddr> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    // Any routed destination works; 192.0.2.1 is TEST-NET-1 (never sent).
    sock.connect("192.0.2.1:80").ok()?;
    let addr = sock.local_addr().ok()?.ip();
    match addr {
        IpAddr::V4(v4) if !v4.is_loopback() => Some(IpAddr::V4(v4)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lan_ipv4_is_non_loopback_when_available() {
        // Environment-dependent: on machines with a LAN route this must
        // return a non-loopback IPv4; on isolated hosts None is fine.
        if let Some(ip) = lan_ipv4() {
            assert!(!ip.is_loopback());
            assert!(ip.is_ipv4());
        }
    }
}
