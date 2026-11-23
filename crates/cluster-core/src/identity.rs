//! Ed25519 identities for cluster (controller) and worker keys.
//!
//! Each machine holds one long-lived Ed25519 keypair used for post-pairing
//! mutual authentication (challenge/response over TLS). Keys are stored as a
//! 32-byte seed, hex-encoded, in a file created with mode `0600` and written
//! atomically. The *pairing code* is a separate, short-lived secret — see
//! [`crate::pairing`].

use crate::fsutil;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::OsRng;
use sha2::{Digest, Sha256};
use std::path::Path;
use thiserror::Error;

/// Length in hex characters of a [`Keypair::fingerprint`].
pub const FINGERPRINT_HEX_LEN: usize = 16;
/// Length in bytes of an Ed25519 public key.
pub const PUBLIC_KEY_LEN: usize = 32;
/// Length in bytes of an Ed25519 signature.
pub const SIGNATURE_LEN: usize = 64;
/// Length in bytes of the secret seed persisted to disk.
pub const SEED_LEN: usize = 32;

/// Errors produced by identity load/generate/verify operations.
#[derive(Debug, Error)]
pub enum IdentityError {
    /// Underlying filesystem error.
    #[error("identity io error: {0}")]
    Io(#[from] std::io::Error),
    /// The key file exists but does not contain a valid key.
    #[error("identity key file is corrupt or has an invalid format")]
    CorruptKey,
    /// Signature bytes could not be parsed or verification failed.
    #[error("signature verification failed")]
    BadSignature,
}

/// An Ed25519 keypair: the machine's cluster/worker identity.
///
/// The private half never leaves this process except through [`Keypair::save`]
/// into a `0600` file under the state dir.
pub struct Keypair {
    signing: SigningKey,
    verifying: VerifyingKey,
}

// SigningKey is not Clone in a useful way here; manual impl not needed.
// (Keypair is intentionally not Clone: key material should be handled once.)

impl Keypair {
    /// Generate a fresh random keypair from the OS CSPRNG.
    pub fn generate() -> Self {
        let signing = SigningKey::generate(&mut OsRng);
        let verifying = signing.verifying_key();
        Self { signing, verifying }
    }

    /// Rebuild a keypair from a 32-byte secret seed. Used by [`Keypair::load`]
    /// and by tests that need deterministic keys.
    pub fn from_seed(seed: &[u8; SEED_LEN]) -> Self {
        let signing = SigningKey::from_bytes(seed);
        let verifying = signing.verifying_key();
        Self { signing, verifying }
    }

    /// The raw 32-byte public key.
    pub fn public_key_bytes(&self) -> [u8; PUBLIC_KEY_LEN] {
        self.verifying.to_bytes()
    }

    /// The raw 32-byte secret seed (for persistence only — never log this).
    pub fn secret_seed(&self) -> [u8; SEED_LEN] {
        self.signing.to_bytes()
    }

    /// Short human-usable fingerprint: first 16 lowercase hex chars of
    /// SHA-256 over the raw public key. Stable for a given keypair and safe
    /// to display in `machines` output.
    pub fn fingerprint(&self) -> String {
        fingerprint_of_pubkey(&self.verifying.to_bytes())
    }

    /// Sign `msg`, returning the 64-byte Ed25519 signature.
    pub fn sign(&self, msg: &[u8]) -> [u8; SIGNATURE_LEN] {
        let sig: Signature = self.signing.sign(msg);
        sig.to_bytes()
    }

    /// Verify `sig` over `msg` against `pubkey_bytes`. Returns `false` for
    /// malformed keys/signatures rather than erroring.
    pub fn verify(pubkey_bytes: &[u8; PUBLIC_KEY_LEN], msg: &[u8], sig: &[u8; SIGNATURE_LEN]) -> bool {
        let Ok(verifying) = VerifyingKey::from_bytes(pubkey_bytes) else {
            return false;
        };
        let sig = Signature::from_bytes(sig);
        verifying.verify(msg, &sig).is_ok()
    }

    /// Sign a challenge/response: convenience wrapper over [`Keypair::sign`]
    /// documented for the post-pairing auth flow.
    pub fn sign_challenge(&self, challenge: &[u8]) -> [u8; SIGNATURE_LEN] {
        self.sign(challenge)
    }

    /// Persist this keypair to `path` (hex-encoded seed), atomically, mode
    /// `0600`. The parent directory must exist.
    pub fn save(&self, path: &Path) -> Result<(), IdentityError> {
        let hex = fsutil::hex_encode(&self.secret_seed());
        fsutil::atomic_write(path, format!("{hex}\n").as_bytes())?;
        Ok(())
    }

    /// Load a keypair from `path`. Returns [`IdentityError::CorruptKey`] if
    /// the file exists but is not a valid hex-encoded 32-byte seed.
    pub fn load(path: &Path) -> Result<Self, IdentityError> {
        let raw = std::fs::read_to_string(path)?;
        let trimmed = raw.trim();
        let seed_bytes = fsutil::hex_decode(trimmed).ok_or(IdentityError::CorruptKey)?;
        let seed: &[u8; SEED_LEN] = seed_bytes
            .as_slice()
            .try_into()
            .map_err(|_| IdentityError::CorruptKey)?;
        Ok(Self::from_seed(seed))
    }

    /// Load the keypair at `path`, generating and persisting a fresh one if
    /// the file does not exist yet.
    pub fn load_or_create(path: &Path) -> Result<Self, IdentityError> {
        match Self::load(path) {
            Ok(kp) => Ok(kp),
            Err(IdentityError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                let kp = Self::generate();
                kp.save(path)?;
                Ok(kp)
            }
            Err(e) => Err(e),
        }
    }
}

/// Compute the [`Keypair::fingerprint`] for a raw public key. Exposed for
/// verifying a peer's claimed fingerprint without holding its private key.
pub fn fingerprint_of_pubkey(pubkey: &[u8]) -> String {
    let digest = Sha256::digest(pubkey);
    fsutil::hex_encode(&digest[..8])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hc-identity-{}", std::process::id()));
        let _ = fsutil::ensure_dir(&dir);
        dir.join(name)
    }

    #[test]
    fn keygen_roundtrip_save_load() {
        let path = temp_path("roundtrip.key");
        let kp = Keypair::generate();
        kp.save(&path).unwrap();
        let loaded = Keypair::load(&path).unwrap();
        assert_eq!(kp.public_key_bytes(), loaded.public_key_bytes());
        assert_eq!(kp.secret_seed(), loaded.secret_seed());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_or_create_generates_then_reuses() {
        let path = temp_path("load_or_create.key");
        let _ = std::fs::remove_file(&path);
        let first = Keypair::load_or_create(&path).unwrap();
        assert!(path.exists());
        let second = Keypair::load_or_create(&path).unwrap();
        assert_eq!(first.public_key_bytes(), second.public_key_bytes());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn sign_verify_ok() {
        let kp = Keypair::generate();
        let msg = b"challenge: nonce-123 cluster-auth";
        let sig = kp.sign(msg);
        assert!(Keypair::verify(&kp.public_key_bytes(), msg, &sig));
    }

    #[test]
    fn wrong_key_rejected() {
        let a = Keypair::generate();
        let b = Keypair::generate();
        let sig = a.sign(b"hello");
        assert!(!Keypair::verify(&b.public_key_bytes(), b"hello", &sig));
    }

    #[test]
    fn tampered_message_or_signature_rejected() {
        let kp = Keypair::generate();
        let sig = kp.sign(b"original");
        assert!(!Keypair::verify(&kp.public_key_bytes(), b"tampered", &sig));
        let mut bad_sig = sig;
        bad_sig[0] ^= 0xff;
        assert!(!Keypair::verify(&kp.public_key_bytes(), b"original", &bad_sig));
    }

    #[test]
    fn corrupt_key_file_detected() {
        let path = temp_path("corrupt.key");
        std::fs::write(&path, "not-hex!!!").unwrap();
        assert!(matches!(Keypair::load(&path), Err(IdentityError::CorruptKey)));
        // Right length hex but wrong seed length is also corrupt.
        std::fs::write(&path, "deadbeef\n").unwrap();
        assert!(matches!(Keypair::load(&path), Err(IdentityError::CorruptKey)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn fingerprint_stable_and_wellformed() {
        let kp = Keypair::generate();
        let fp1 = kp.fingerprint();
        let fp2 = fingerprint_of_pubkey(&kp.public_key_bytes());
        assert_eq!(fp1, fp2);
        assert_eq!(fp1.len(), FINGERPRINT_HEX_LEN);
        assert!(fp1.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        // A different key must (practically) produce a different fingerprint.
        let other = Keypair::generate();
        assert_ne!(fp1, other.fingerprint());
    }

    #[cfg(unix)]
    #[test]
    fn key_file_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let path = temp_path("perms.key");
        let kp = Keypair::generate();
        kp.save(&path).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let _ = std::fs::remove_file(&path);
    }
}
