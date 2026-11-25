//! TLS 1.3 layer for cluster connections.
//!
//! # Design
//!
//! - **Cluster CA**: an X.509 ECDSA P-256 CA generated with [`rcgen`] at
//!   `init` time and persisted under `<state_dir>/tls/` (`ca.crt`, `ca.key`,
//!   `server.crt`, `server.key`). Writes are atomic; keys are mode `0600`,
//!   certs `0644`, the directory `0700`. rcgen (maintained by the rustls
//!   org) was chosen over hand-rolled ASN.1: it emits rustls-compatible
//!   DER/PEM directly and keeps the dependency surface small.
//! - **Server certificate**: leaf signed by the cluster CA, SANs =
//!   `localhost` + the machine hostname, valid for [`SERVER_CERT_DAYS`]
//!   (30 days). The CA itself is valid for [`CA_CERT_DAYS`] (10 years).
//!   The server cert is auto-rotated on startup when missing or older than
//!   [`SERVER_CERT_DAYS`] - [`ROTATE_BEFORE_DAYS`] days (tracked via file
//!   mtime — the cert is only ever written by this module). The CA is only
//!   rotated via [`rotate_ca`], which invalidates every worker pin and
//!   therefore requires re-pairing; that is the documented rotation path.
//! - **Pinning**: workers pin the cluster CA certificate fingerprint —
//!   the first 16 lowercase hex chars of SHA-256 over the CA cert DER, the
//!   same fingerprint shape `cluster_core::identity` uses for pubkeys —
//!   received in `PairAccept.ca_fingerprint`, plus the CA certificate PEM
//!   carried inside the opaque pairing credential (see
//!   [`crate::conn::WorkerCredential`]). [`client_config`] builds its root
//!   store from *only* that CA certificate, so any other CA is rejected
//!   during the TLS handshake, and the fingerprint is checked against the
//!   stored PEM before the handshake so a corrupted credential fails fast.
//! - **Server name**: cluster peers dial by IP/hostname, but every cert
//!   SAN set contains `localhost`, so clients always verify with
//!   `ServerName::"localhost"`. This is safe because authentication is
//!   anchored by the pinned private CA, not by public PKI names.
//! - **Mutual auth**: TLS authenticates the channel; cluster identity is
//!   proven by an Ed25519 challenge immediately after the handshake
//!   ([`client_auth`] / [`server_auth`]). The worker signs with its
//!   `identity::Keypair`; the controller signs with the cluster identity
//!   keypair (`<state_dir>/identity.key` via `cluster_core::config`).
//!   Nonces are fresh per connection, so proofs cannot be replayed.
//!
//! Only TLS 1.3 is negotiated (`rustls::version::TLS13`).

use cluster_core::identity::Keypair;
use rand_core::RngCore;
use rcgen::{
    BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose, PublicKeyData,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::version::TLS13;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::{client::TlsStream as ClientTlsStream, server::TlsStream as ServerTlsStream};

/// Server certificate validity: 30 days (PLAN.md §3.5).
pub const SERVER_CERT_DAYS: i64 = 30;
/// Days before server-cert expiry at which it is rotated.
pub const ROTATE_BEFORE_DAYS: i64 = 7;
/// Cluster CA validity: 10 years. Rotating the CA invalidates all worker
/// pins, so it is deliberately long-lived; see the module docs.
pub const CA_CERT_DAYS: i64 = 3650;
/// Deadline callers should wrap the post-handshake auth exchange in.
pub const AUTH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
/// Domain-separation prefix for the post-handshake Ed25519 proof.
pub const AUTH_DOMAIN: &[u8] = b"herdcluster/auth/v1";
/// Challenge nonce length in bytes.
pub const NONCE_LEN: usize = 32;
/// `ServerName` used for all cluster TLS connections (always a SAN).
const TLS_SERVER_NAME: &str = "localhost";

// ---- persisted material ----

/// The TLS material persisted under `<state_dir>/tls/`.
#[derive(Debug, Clone)]
pub struct TlsMaterial {
    /// PEM-encoded cluster CA certificate.
    pub ca_cert_pem: String,
    /// PEM-encoded cluster CA private key (never leaves the state dir).
    pub ca_key_pem: String,
    /// DER bytes of the CA certificate.
    pub ca_cert_der: Vec<u8>,
    /// PEM-encoded server certificate.
    pub server_cert_pem: String,
    /// PEM-encoded server private key.
    pub server_key_pem: String,
}

impl TlsMaterial {
    /// The pinned-CA fingerprint: 16 lowercase hex chars of SHA-256 over the
    /// CA certificate DER. This is the value exchanged in
    /// `PairAccept.ca_fingerprint` and stored in the worker credential.
    pub fn ca_fingerprint(&self) -> String {
        cert_fingerprint(&self.ca_cert_der)
    }
}

/// Errors from the TLS layer.
#[derive(Debug, Error)]
pub enum TlsError {
    /// Filesystem error.
    #[error("tls io error: {0}")]
    Io(#[from] std::io::Error),
    /// Certificate generation/parsing failed.
    #[error("tls certificate error: {0}")]
    Cert(#[from] rcgen::Error),
    /// rustls handshake/configuration error.
    #[error("tls error: {0}")]
    Rustls(#[from] rustls::Error),
    /// PEM/DER decoding failed.
    #[error("tls pem error: {0}")]
    Pem(String),
    /// The stored CA certificate does not match the pinned fingerprint.
    #[error("cluster CA fingerprint mismatch (pinning violation)")]
    PinMismatch,
    /// Post-handshake mutual-auth proof failed or was malformed.
    #[error("cluster auth failed: {0}")]
    Auth(String),
}

/// `<state_dir>/tls` — directory holding CA + server cert material.
pub fn tls_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("tls")
}

fn ca_cert_path(dir: &Path) -> PathBuf {
    dir.join("ca.crt")
}
fn ca_key_path(dir: &Path) -> PathBuf {
    dir.join("ca.key")
}
fn server_cert_path(dir: &Path) -> PathBuf {
    dir.join("server.crt")
}
fn server_key_path(dir: &Path) -> PathBuf {
    dir.join("server.key")
}

fn ensure_dir_700(dir: &Path) -> Result<(), TlsError> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Atomic write with explicit Unix mode (0600 for keys, 0644 for certs).
fn atomic_write_mode(path: &Path, contents: &[u8], mode: u32) -> Result<(), TlsError> {
    let tmp = path.with_extension("tmp");
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(mode);
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
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    }
    Ok(())
}

fn write_private(path: &Path, contents: &str) -> Result<(), TlsError> {
    atomic_write_mode(path, contents.as_bytes(), 0o600)
}

fn write_cert(path: &Path, contents: &str) -> Result<(), TlsError> {
    atomic_write_mode(path, contents.as_bytes(), 0o644)
}

// ---- certificate generation ----

fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

fn ca_params() -> CertificateParams {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::CrlSign,
    ];
    params.not_before = now() - TimeDuration::hours(1);
    params.not_after = now() + TimeDuration::days(CA_CERT_DAYS);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "herdcluster-cluster-ca");
    params
}

/// Generate a fresh cluster CA: params + key + self-signed cert.
fn generate_ca() -> Result<(CertificateParams, KeyPair, rcgen::Certificate), TlsError> {
    let params = ca_params();
    let key = KeyPair::generate()?;
    let cert = params.self_signed(&key)?;
    Ok((params, key, cert))
}

/// Generate a server leaf cert signed by `ca`, valid for `days` days with
/// SANs `localhost` + `hostname`.
fn generate_server_cert(
    ca_params: &CertificateParams,
    ca_key: &KeyPair,
    hostname: &str,
    days: i64,
) -> Result<(CertificateParams, KeyPair, rcgen::Certificate), TlsError> {
    let mut sans = vec![TLS_SERVER_NAME.to_string()];
    let host = hostname.trim();
    if !host.is_empty() && host != TLS_SERVER_NAME {
        sans.push(host.to_string());
    }
    let mut params = CertificateParams::new(sans)?;
    params.is_ca = IsCa::ExplicitNoCa;
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    params.not_before = now() - TimeDuration::hours(1);
    params.not_after = now() + TimeDuration::days(days);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "herdcluster-controller");
    let key = KeyPair::generate()?;
    let issuer = Issuer::from_params(ca_params, ca_key);
    let cert = params.signed_by(&key, &issuer)?;
    Ok((params, key, cert))
}

/// Decode the first certificate PEM block to DER.
fn pem_to_der(pem: &str) -> Result<Vec<u8>, TlsError> {
    let mut reader = std::io::BufReader::new(pem.as_bytes());
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(|e| TlsError::Pem(format!("cert pem: {e}")))?;
    certs
        .into_iter()
        .next()
        .map(|c| c.as_ref().to_vec())
        .ok_or_else(|| TlsError::Pem("no certificate found in PEM".to_string()))
}

fn der_to_cert_der(der: &[u8]) -> CertificateDer<'static> {
    CertificateDer::from(der.to_vec())
}

/// 16 lowercase hex chars of SHA-256 over a certificate DER — the cluster
/// CA fingerprint exchanged at pairing time.
pub fn cert_fingerprint(cert_der: &[u8]) -> String {
    let digest = Sha256::digest(cert_der);
    hex_encode(&digest[..8])
}

// cluster-core's fsutil is private; tiny local hex helper instead.
mod fsutil_hex {
    pub fn hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push(HEX[(b >> 4) as usize] as char);
            s.push(HEX[(b & 0x0f) as usize] as char);
        }
        s
    }
}
use fsutil_hex::hex as hex_encode;

/// Load or create the cluster CA under `dir`. Returns the reconstructed
/// params (sufficient for [`Issuer`]), the parsed signing key, and the cert
/// PEM + DER.
fn load_or_create_ca(
    dir: &Path,
) -> Result<(CertificateParams, KeyPair, String, Vec<u8>), TlsError> {
    let cert_path = ca_cert_path(dir);
    let key_path = ca_key_path(dir);
    if cert_path.exists() && key_path.exists() {
        let cert_pem = std::fs::read_to_string(&cert_path)?;
        let key_pem = std::fs::read_to_string(&key_path)?;
        let key = KeyPair::from_pem(&key_pem)?;
        let der = pem_to_der(&cert_pem)?;
        if ca_key_matches_cert(&key, &der) {
            // Params only need the fields `Issuer` reads: DN + key usages +
            // key-identifier method. Reconstruct deterministically.
            let params = ca_params();
            return Ok((params, key, cert_pem, der));
        }
        // Mismatched ca.crt/ca.key pair — the signature of a crash between
        // the two atomic renames in `rotate_ca`. Both files exist so this
        // would otherwise load silently and break every TLS handshake.
        // Regenerate the pair below; the server cert is signed by the CA
        // key, so it must be re-issued too (its mtime looks fresh).
        tracing::warn!(
            "cluster CA cert/key mismatch under {}; regenerating",
            dir.display()
        );
        let _ = std::fs::remove_file(server_cert_path(dir));
        let _ = std::fs::remove_file(server_key_path(dir));
    }
    let (params, key, cert) = generate_ca()?;
    let cert_pem = cert.pem();
    write_cert(&cert_path, &cert_pem)?;
    write_private(&key_path, &key.serialize_pem())?;
    let der = pem_to_der(&cert_pem)?;
    Ok((params, key, cert_pem, der))
}

/// True when `key` is the private half of the CA certificate whose DER is
/// `cert_der` — i.e. the certificate's SubjectPublicKeyInfo equals the
/// key's. Guards [`load_or_create_ca`] against a mismatched pair left by
/// a crash between the `ca.crt`/`ca.key` atomic renames in [`rotate_ca`].
fn ca_key_matches_cert(key: &KeyPair, cert_der: &[u8]) -> bool {
    let cert = der_to_cert_der(cert_der);
    let Ok(parsed) = rustls::server::ParsedCertificate::try_from(&cert) else {
        return false;
    };
    parsed.subject_public_key_info().as_ref() == key.subject_public_key_info().as_slice()
}

/// Whether the persisted server cert should be regenerated. Uses file mtime
/// as the validity proxy (documented in the module docs).
fn server_cert_stale(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return true;
    };
    let Ok(mtime) = meta.modified() else {
        return true;
    };
    let age = mtime.elapsed().unwrap_or_default();
    let max_age = std::time::Duration::from_secs(
        ((SERVER_CERT_DAYS - ROTATE_BEFORE_DAYS) * 24 * 60 * 60) as u64,
    );
    age >= max_age
}

fn write_server_cert(
    dir: &Path,
    ca_params: &CertificateParams,
    ca_key: &KeyPair,
    hostname: &str,
) -> Result<(String, String), TlsError> {
    let (_params, key, cert) = generate_server_cert(ca_params, ca_key, hostname, SERVER_CERT_DAYS)?;
    let cert_pem = cert.pem();
    write_cert(&server_cert_path(dir), &cert_pem)?;
    write_private(&server_key_path(dir), &key.serialize_pem())?;
    Ok((cert_pem, key.serialize_pem()))
}

/// Load persisted TLS material, generating a fresh cluster CA + server cert
/// on first run and rotating the server cert when stale. Idempotent.
pub fn ensure_tls_material(state_dir: &Path, hostname: &str) -> Result<TlsMaterial, TlsError> {
    let dir = tls_dir(state_dir);
    ensure_dir_700(&dir)?;

    let (ca_params, ca_key, ca_cert_pem, ca_cert_der) = load_or_create_ca(&dir)?;

    let (server_cert_pem, server_key_pem) = if server_cert_stale(&server_cert_path(&dir)) {
        write_server_cert(&dir, &ca_params, &ca_key, hostname)?
    } else {
        (
            std::fs::read_to_string(server_cert_path(&dir))?,
            std::fs::read_to_string(server_key_path(&dir))?,
        )
    };

    Ok(TlsMaterial {
        ca_cert_pem,
        ca_key_pem: std::fs::read_to_string(ca_key_path(&dir))?,
        ca_cert_der,
        server_cert_pem,
        server_key_pem,
    })
}

/// Rotate the cluster CA: generates a fresh CA + server cert, overwriting
/// the persisted material. **All workers must re-pair** afterwards because
/// their pinned CA fingerprint no longer matches. Returns the new
/// [`TlsMaterial`].
pub fn rotate_ca(state_dir: &Path, hostname: &str) -> Result<TlsMaterial, TlsError> {
    let dir = tls_dir(state_dir);
    ensure_dir_700(&dir)?;
    let (ca_params, ca_key, cert) = generate_ca()?;
    let ca_cert_pem = cert.pem();
    write_cert(&ca_cert_path(&dir), &ca_cert_pem)?;
    write_private(&ca_key_path(&dir), &ca_key.serialize_pem())?;
    let (server_cert_pem, server_key_pem) = write_server_cert(&dir, &ca_params, &ca_key, hostname)?;
    let ca_cert_der = pem_to_der(&ca_cert_pem)?;
    Ok(TlsMaterial {
        ca_cert_pem,
        ca_key_pem: ca_key.serialize_pem(),
        ca_cert_der,
        server_cert_pem,
        server_key_pem,
    })
}

// ---- rustls configs ----

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Build the controller-side [`ServerConfig`]: TLS 1.3 only, no client
/// certs (worker identity is proven by the post-handshake challenge).
pub fn server_config(material: &TlsMaterial) -> Result<Arc<ServerConfig>, TlsError> {
    let cert = der_to_cert_der(&pem_to_der(&material.server_cert_pem)?);
    let key = parse_private_key_pem(&material.server_key_pem)?;
    let config = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&TLS13])?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    Ok(Arc::new(config))
}

/// Build the worker-side [`ClientConfig`]: TLS 1.3 only, root store
/// containing *only* the pinned cluster CA. `ca_cert_pem` is the CA cert
/// stored in the worker credential; `pinned_fingerprint` is the
/// `ca_fingerprint` from pairing — a mismatch is rejected before any
/// handshake bytes are sent.
pub fn client_config(
    ca_cert_pem: &str,
    pinned_fingerprint: &str,
) -> Result<Arc<ClientConfig>, TlsError> {
    let ca_der = pem_to_der(ca_cert_pem)?;
    if cert_fingerprint(&ca_der) != pinned_fingerprint {
        return Err(TlsError::PinMismatch);
    }
    let mut roots = RootCertStore::empty();
    roots.add(der_to_cert_der(&ca_der))?;
    let config = ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&TLS13])?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(config))
}

fn parse_private_key_pem(pem: &str) -> Result<PrivateKeyDer<'static>, TlsError> {
    let mut reader = std::io::BufReader::new(pem.as_bytes());
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| TlsError::Pem(format!("key pem: {e}")))?
        .ok_or_else(|| TlsError::Pem("no private key found in PEM".to_string()))?;
    Ok(key)
}

// ---- handshakes ----

/// Dial `addr` and perform the TLS 1.3 handshake as a cluster worker.
pub async fn dial_tls(
    addr: std::net::SocketAddr,
    config: Arc<ClientConfig>,
) -> Result<ClientTlsStream<TcpStream>, TlsError> {
    let tcp = TcpStream::connect(addr).await?;
    let name = ServerName::try_from(TLS_SERVER_NAME.to_string())
        .map_err(|e| TlsError::Pem(format!("server name: {e}")))?;
    let stream = tokio_rustls::TlsConnector::from(config)
        .connect(name, tcp)
        .await?;
    Ok(stream)
}

/// Accept a TCP connection and perform the TLS 1.3 handshake as the
/// cluster controller.
pub async fn accept_tls(
    tcp: TcpStream,
    config: Arc<ServerConfig>,
) -> Result<ServerTlsStream<TcpStream>, TlsError> {
    let stream = tokio_rustls::TlsAcceptor::from(config).accept(tcp).await?;
    Ok(stream)
}

// ---- mutual auth (post-handshake Ed25519 challenge) ----

fn random_nonce() -> [u8; NONCE_LEN] {
    let mut nonce = [0u8; NONCE_LEN];
    rand_core::OsRng.fill_bytes(&mut nonce);
    nonce
}

fn auth_payload(role: &[u8], nonce_a: &[u8], nonce_b: &[u8], pubkey: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(AUTH_DOMAIN.len() + 16 + 2 * NONCE_LEN + 32);
    v.extend_from_slice(AUTH_DOMAIN);
    v.push(0);
    v.extend_from_slice(role);
    v.extend_from_slice(nonce_a);
    v.extend_from_slice(nonce_b);
    v.extend_from_slice(pubkey);
    v
}

/// Worker side of the mutual-auth exchange. `stream` must be the
/// established TLS stream *before* the mux starts. `pinned_controller_pubkey`
/// is the controller's Ed25519 public key stored in the worker credential;
/// the peer's claimed pubkey must match it and its signature must verify.
pub async fn client_auth<S>(
    stream: &mut S,
    worker_keypair: &Keypair,
    pinned_controller_pubkey: &[u8; 32],
) -> Result<(), TlsError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // 1. Send our nonce.
    let nonce_w = random_nonce();
    stream.write_all(&nonce_w).await?;

    // 2. Receive controller nonce + pubkey + signature (32+32+64 bytes).
    let mut buf = [0u8; NONCE_LEN * 2 + 64];
    stream.read_exact(&mut buf).await?;
    let (nonce_c, rest) = buf.split_at(NONCE_LEN);
    let (ctrl_pub, sig_c) = rest.split_at(32);
    let mut ctrl_pub_arr = [0u8; 32];
    ctrl_pub_arr.copy_from_slice(ctrl_pub);
    let mut sig_c_arr = [0u8; 64];
    sig_c_arr.copy_from_slice(sig_c);

    if &ctrl_pub_arr != pinned_controller_pubkey {
        return Err(TlsError::Auth(
            "controller pubkey does not match pinned credential".to_string(),
        ));
    }
    let payload_c = auth_payload(b"controller", nonce_c, &nonce_w, &ctrl_pub_arr);
    if !Keypair::verify(&ctrl_pub_arr, &payload_c, &sig_c_arr) {
        return Err(TlsError::Auth(
            "controller signature verification failed".to_string(),
        ));
    }

    // 3. Send our pubkey + signature over both nonces.
    let worker_pub = worker_keypair.public_key_bytes();
    let payload_w = auth_payload(b"worker", &nonce_w, nonce_c, &worker_pub);
    let sig_w = worker_keypair.sign_challenge(&payload_w);
    stream.write_all(&worker_pub).await?;
    stream.write_all(&sig_w).await?;
    Ok(())
}

/// Controller side of the mutual-auth exchange. Returns the authenticated
/// worker Ed25519 public key. The caller must ensure the returned key is
/// the one the worker later claims in `WorkerHello` (or a registered
/// worker) before trusting the session.
pub async fn server_auth<S>(stream: &mut S, cluster_keypair: &Keypair) -> Result<[u8; 32], TlsError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // 1. Receive worker nonce.
    let mut nonce_w = [0u8; NONCE_LEN];
    stream.read_exact(&mut nonce_w).await?;

    // 2. Send our nonce + pubkey + signature.
    let nonce_c = random_nonce();
    let cluster_pub = cluster_keypair.public_key_bytes();
    let payload_c = auth_payload(b"controller", &nonce_c, &nonce_w, &cluster_pub);
    let sig_c = cluster_keypair.sign_challenge(&payload_c);
    stream.write_all(&nonce_c).await?;
    stream.write_all(&cluster_pub).await?;
    stream.write_all(&sig_c).await?;

    // 3. Receive worker pubkey + signature and verify.
    let mut buf = [0u8; 32 + 64];
    stream.read_exact(&mut buf).await?;
    let (worker_pub, sig_w) = buf.split_at(32);
    let mut worker_pub_arr = [0u8; 32];
    worker_pub_arr.copy_from_slice(worker_pub);
    let mut sig_w_arr = [0u8; 64];
    sig_w_arr.copy_from_slice(sig_w);
    let payload_w = auth_payload(b"worker", &nonce_w, &nonce_c, &worker_pub_arr);
    if !Keypair::verify(&worker_pub_arr, &payload_w, &sig_w_arr) {
        return Err(TlsError::Auth(
            "worker signature verification failed".to_string(),
        ));
    }
    Ok(worker_pub_arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn tempdir() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn material_persisted_with_right_perms() {
        let dir = tempdir();
        let m = ensure_tls_material(dir.path(), "test-host").unwrap();
        assert_eq!(m.ca_fingerprint().len(), 16);
        let tls = tls_dir(dir.path());
        assert!(tls.join("ca.crt").exists());
        assert!(tls.join("server.crt").exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(&tls.join("ca.key")), 0o600);
            assert_eq!(mode(&tls.join("server.key")), 0o600);
            assert_eq!(mode(&tls.join("ca.crt")), 0o644);
            assert_eq!(mode(&tls), 0o700);
        }
        // Second call reuses the same CA (stable fingerprint).
        let m2 = ensure_tls_material(dir.path(), "test-host").unwrap();
        assert_eq!(m.ca_fingerprint(), m2.ca_fingerprint());
    }

    #[test]
    fn rotate_ca_changes_fingerprint() {
        let dir = tempdir();
        let m1 = ensure_tls_material(dir.path(), "h").unwrap();
        let m2 = rotate_ca(dir.path(), "h").unwrap();
        assert_ne!(m1.ca_fingerprint(), m2.ca_fingerprint());
    }

    /// Regression: a crash between the `ca.crt` and `ca.key` atomic
    /// renames in `rotate_ca` leaves a mismatched pair that used to load
    /// silently and break every TLS handshake. Load must detect the
    /// mismatch and regenerate a consistent pair (and a server cert
    /// signed by it).
    #[test]
    fn mismatched_ca_pair_regenerates() {
        let dir = tempdir();
        let m1 = ensure_tls_material(dir.path(), "h").unwrap();

        // Simulate the torn write: swap in a *different* CA's cert while
        // keeping the original ca.key.
        let other = tempdir();
        let m_other = ensure_tls_material(other.path(), "h").unwrap();
        std::fs::write(tls_dir(dir.path()).join("ca.crt"), &m_other.ca_cert_pem).unwrap();

        let m2 = ensure_tls_material(dir.path(), "h").unwrap();
        // A fresh CA was generated — neither the old nor the foreign one.
        assert_ne!(m2.ca_fingerprint(), m1.ca_fingerprint());
        assert_ne!(m2.ca_fingerprint(), m_other.ca_fingerprint());
        // The regenerated pair is self-consistent and stable on reload.
        let m3 = ensure_tls_material(dir.path(), "h").unwrap();
        assert_eq!(m2.ca_fingerprint(), m3.ca_fingerprint());
        // Material still builds working rustls configs (server cert was
        // re-signed by the new CA).
        server_config(&m2).unwrap();
        client_config(&m2.ca_cert_pem, &m2.ca_fingerprint()).unwrap();
    }

    #[tokio::test]
    async fn loopback_handshake_and_mutual_auth() {
        let dir = tempdir();
        let material = ensure_tls_material(dir.path(), "localhost").unwrap();
        let server_cfg = server_config(&material).unwrap();
        let client_cfg = client_config(&material.ca_cert_pem, &material.ca_fingerprint()).unwrap();

        let cluster = Keypair::generate();
        let worker = Keypair::generate();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ctrl_pub = cluster.public_key_bytes();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = accept_tls(tcp, server_cfg).await.unwrap();
            let peer = server_auth(&mut tls, &cluster).await.unwrap();
            (tls, peer)
        });

        let mut client = dial_tls(addr, client_cfg).await.unwrap();
        client_auth(&mut client, &worker, &ctrl_pub).await.unwrap();

        let (server_tls, peer_pub) = server.await.unwrap();
        assert_eq!(peer_pub, worker.public_key_bytes());
        drop(server_tls);
        drop(client);
    }

    #[tokio::test]
    async fn wrong_pin_rejected_at_config_time() {
        let dir = tempdir();
        let material = ensure_tls_material(dir.path(), "localhost").unwrap();
        // Fingerprint of a *different* CA must fail before dialing.
        let dir2 = tempdir();
        let m2 = ensure_tls_material(dir2.path(), "localhost").unwrap();
        assert!(client_config(&m2.ca_cert_pem, &material.ca_fingerprint()).is_err());
        // And the right pem with wrong fingerprint string fails too.
        assert!(client_config(&material.ca_cert_pem, "0000000000000000").is_err());
    }

    #[tokio::test]
    async fn expired_cert_rejected() {
        // Build an expired server cert signed by a fresh CA.
        let (ca_params, ca_key, ca_cert) = generate_ca().unwrap();
        let (_p, server_key, server_cert) =
            generate_server_cert(&ca_params, &ca_key, "localhost", -1).unwrap();
        let ca_pem = ca_cert.pem();
        let ca_der = pem_to_der(&ca_pem).unwrap();
        let fp = cert_fingerprint(&ca_der);

        let server_cfg = {
            let cert = der_to_cert_der(&pem_to_der(&server_cert.pem()).unwrap());
            let key_pem = server_key.serialize_pem();
            let key = parse_private_key_pem(&key_pem).unwrap();
            Arc::new(
                ServerConfig::builder_with_provider(provider())
                    .with_protocol_versions(&[&TLS13])
                    .unwrap()
                    .with_no_client_auth()
                    .with_single_cert(vec![cert], key)
                    .unwrap(),
            )
        };
        let client_cfg = client_config(&ca_pem, &fp).unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            accept_tls(tcp, server_cfg).await
        });
        let res = dial_tls(addr, client_cfg).await;
        assert!(res.is_err(), "expired cert must be rejected");
        let _ = server.await;
    }

    #[tokio::test]
    async fn foreign_ca_rejected() {
        // Server uses CA-A; client pins CA-B → handshake must fail.
        let dir_a = tempdir();
        let dir_b = tempdir();
        let ma = ensure_tls_material(dir_a.path(), "localhost").unwrap();
        let mb = ensure_tls_material(dir_b.path(), "localhost").unwrap();
        let server_cfg = server_config(&ma).unwrap();
        // Client trusts B's CA but presents B's fingerprint while server
        // cert chains to A → webpki unknown issuer.
        let client_cfg = client_config(&mb.ca_cert_pem, &mb.ca_fingerprint()).unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            accept_tls(tcp, server_cfg).await
        });
        let res = dial_tls(addr, client_cfg).await;
        assert!(res.is_err());
        let _ = server.await;
    }

    #[tokio::test]
    async fn bad_worker_signature_rejected() {
        let dir = tempdir();
        let material = ensure_tls_material(dir.path(), "localhost").unwrap();
        let server_cfg = server_config(&material).unwrap();
        let client_cfg = client_config(&material.ca_cert_pem, &material.ca_fingerprint()).unwrap();

        let cluster = Keypair::generate();
        let impostor = Keypair::generate(); // signs with wrong key
        let _ctrl_pub = cluster.public_key_bytes();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = accept_tls(tcp, server_cfg).await.unwrap();
            server_auth(&mut tls, &cluster).await
        });

        let mut client = dial_tls(addr, client_cfg).await.unwrap();
        // client_auth with a *different* pinned pubkey would fail client-side;
        // instead run the raw exchange signing with impostor's key.
        let nonce_w = random_nonce();
        client.write_all(&nonce_w).await.unwrap();
        let mut buf = [0u8; NONCE_LEN * 2 + 64];
        client.read_exact(&mut buf).await.unwrap();
        let (nonce_c, rest) = buf.split_at(NONCE_LEN);
        let (cp, sc) = rest.split_at(32);
        let mut cp_arr = [0u8; 32];
        cp_arr.copy_from_slice(cp);
        let mut sc_arr = [0u8; 64];
        sc_arr.copy_from_slice(sc);
        let payload_c = auth_payload(b"controller", nonce_c, &nonce_w, &cp_arr);
        assert!(Keypair::verify(&cp_arr, &payload_c, &sc_arr));
        // Worker claims impostor pubkey but signs payload correctly for it —
        // signature is valid *for impostor's key*, so server accepts the
        // proof but the returned pubkey is the impostor's (not registered).
        // For a *bad signature* test, corrupt the signature instead:
        let worker_pub = impostor.public_key_bytes();
        let payload_w = auth_payload(b"worker", &nonce_w, nonce_c, &worker_pub);
        let mut sig_w = impostor.sign_challenge(&payload_w);
        sig_w[0] ^= 0xff;
        client.write_all(&worker_pub).await.unwrap();
        client.write_all(&sig_w).await.unwrap();

        let res = server.await.unwrap();
        assert!(matches!(res, Err(TlsError::Auth(_))));
    }

    #[tokio::test]
    async fn auth_wrong_pinned_pubkey_fails_client_side() {
        let dir = tempdir();
        let material = ensure_tls_material(dir.path(), "localhost").unwrap();
        let server_cfg = server_config(&material).unwrap();
        let client_cfg = client_config(&material.ca_cert_pem, &material.ca_fingerprint()).unwrap();

        let cluster = Keypair::generate();
        let worker = Keypair::generate();
        let wrong_pin = Keypair::generate().public_key_bytes();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = accept_tls(tcp, server_cfg).await.unwrap();
            server_auth(&mut tls, &cluster).await
        });

        let mut client = dial_tls(addr, client_cfg).await.unwrap();
        let res = client_auth(&mut client, &worker, &wrong_pin).await;
        assert!(matches!(res, Err(TlsError::Auth(_))));
        // client_auth bailed before sending its proof; closing the stream
        // unblocks the server's read so the task can finish.
        drop(client);
        let _ = server.await;
    }

    #[tokio::test]
    async fn tls_material_files_parse() {
        // Ensure generated PEMs round-trip through rustls config builders.
        let dir = tempdir();
        let m = ensure_tls_material(dir.path(), "localhost").unwrap();
        let _ = server_config(&m).unwrap();
        let _ = client_config(&m.ca_cert_pem, &m.ca_fingerprint()).unwrap();
        // And the private key parses as PKCS#8 for rustls.
        parse_private_key_pem(&m.server_key_pem).unwrap();
    }
}
