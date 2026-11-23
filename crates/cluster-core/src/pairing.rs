//! Pairing finite-state machine — pure logic, no I/O, no real time.
//!
//! Two machines live here:
//!
//! - [`PairingServer`] — the controller side. Generates a one-time 8-char
//!   code (~40 bits, unambiguous alphabet), accepts join requests, asks the
//!   local user to confirm, and approves or rejects.
//! - [`PairingClient`] — the worker side. Waits for `PairAccept`/`PairReject`
//!   after sending its hello, with a caller-chosen timeout.
//!
//! Time is injected via the [`Clock`] trait and randomness via
//! [`rand_core::RngCore`], so every transition is unit-testable.
//!
//! Security properties (per PLAN.md §3.5 and the m1-pairing spec):
//! - Code alphabet excludes ambiguous characters `0 O 1 I L`.
//! - Codes are single-use and expire after [`PAIRING_TTL_MS`] (5 min).
//! - Code comparison is constant-time (via [`subtle::ConstantTimeEq`]).
//! - At most [`MAX_JOIN_ATTEMPTS`] (5) wrong codes per window; the 6th
//!   attempt... precisely: after 5 failures the server locks out for
//!   [`LOCKOUT_MS`] (60 s), then grants a fresh window of 5.
//! - Controller must confirm within [`CONFIRM_TIMEOUT_MS`] (120 s) of a
//!   valid join or the whole session expires (documented choice: the
//!   session is single-use, so an unconfirmed join kills it rather than
//!   silently returning to `AwaitingJoin`).

use rand_core::RngCore;
use subtle::ConstantTimeEq;
use thiserror::Error;

/// One-time pairing code length in characters.
pub const CODE_LENGTH: usize = 8;
/// Pairing code time-to-live: 5 minutes.
pub const PAIRING_TTL_MS: u64 = 5 * 60 * 1000;
/// Controller confirm timeout after a valid join: 120 s.
pub const CONFIRM_TIMEOUT_MS: u64 = 120 * 1000;
/// Max failed code attempts before lockout.
pub const MAX_JOIN_ATTEMPTS: u32 = 5;
/// Lockout duration after too many failures: 60 s.
pub const LOCKOUT_MS: u64 = 60 * 1000;
/// Default worker-side wait for a pairing answer.
pub const CLIENT_DEFAULT_TIMEOUT_MS: u64 = 15 * 60 * 1000;

/// Unambiguous alphabet: Crockford base32 minus `0 O 1 I L` (and `U`, kept
/// excluded per Crockford). 30 symbols → 8 chars ≈ 39.25 bits.
pub const CODE_ALPHABET: &str = "23456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Injected clock. Implementations must return milliseconds since some
/// fixed epoch (only differences matter).
pub trait Clock {
    /// Current time in milliseconds.
    fn now_ms(&self) -> u64;
}

/// Wall clock backed by [`std::time::SystemTime`].
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// A clock that never advances — useful for tests and deterministic callers.
pub struct FixedClock(pub u64);

impl Clock for FixedClock {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

/// Server-side pairing phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairingPhase {
    /// Waiting for a worker to present the code.
    AwaitingJoin,
    /// A valid code was presented; waiting for the local user to confirm.
    PendingConfirm,
    /// User confirmed; pairing approved.
    Approved,
    /// User denied the join.
    Rejected,
    /// Code TTL or confirm deadline elapsed.
    Expired,
}

impl PairingPhase {
    /// Whether this phase is terminal (no further transitions).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            PairingPhase::Approved | PairingPhase::Rejected | PairingPhase::Expired
        )
    }
}

/// Worker-side pairing phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientPhase {
    /// Not started.
    Idle,
    /// Hello sent, awaiting controller answer.
    Waiting,
    /// Controller approved; carries the opaque cluster credential and the
    /// pinned CA fingerprint from `PairAccept`.
    Accepted {
        /// Opaque credential bytes (base64 on the wire).
        credential: Vec<u8>,
        /// Pinned cluster CA fingerprint.
        ca_fingerprint: String,
    },
    /// Controller rejected with a human-readable reason.
    Rejected {
        /// Rejection reason.
        reason: String,
    },
    /// Wait deadline elapsed without an answer.
    Expired,
}

/// Errors returned by pairing operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PairingError {
    /// Session/client already reached a terminal phase.
    #[error("pairing session is already finished")]
    AlreadyFinished,
    /// Session expired (code TTL or confirm deadline).
    #[error("pairing session expired")]
    Expired,
    /// Too many failed attempts; retry after the lockout ends.
    #[error("too many failed attempts; locked out")]
    LockedOut,
    /// Code did not match (counts as a failed attempt).
    #[error("invalid pairing code")]
    InvalidCode,
    /// Operation not valid in the current phase.
    #[error("invalid pairing state for this operation")]
    WrongState,
}

/// What a successful [`PairingServer::handle_join`] produces: the controller
/// must surface `hostname`/`worker_pubkey` to the user and call
/// [`PairingServer::confirm`] or [`PairingServer::deny`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingJoin {
    /// Worker's raw 32-byte Ed25519 public key.
    pub worker_pubkey: [u8; 32],
    /// Worker-reported hostname.
    pub hostname: String,
    /// Epoch-ms deadline by which `confirm`/`deny` must happen.
    pub confirm_deadline_ms: u64,
}

/// Result of a successful [`PairingServer::confirm`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovedJoin {
    /// Approved worker's raw public key.
    pub worker_pubkey: [u8; 32],
    /// Approved worker's hostname.
    pub hostname: String,
    /// Time (ms) at which approval happened, per the injected clock.
    pub approved_at_ms: u64,
}

/// A join request presented by a worker (fields mirror `PairHello`).
#[derive(Debug, Clone)]
pub struct JoinRequest {
    /// The user-typed pairing code.
    pub code: String,
    /// Worker's raw 32-byte Ed25519 public key.
    pub worker_pubkey: [u8; 32],
    /// Worker-reported hostname.
    pub hostname: String,
}

/// Generate a one-time pairing code: [`CODE_LENGTH`] chars from
/// [`CODE_ALPHABET`], rejection-sampled to avoid modulo bias.
pub fn generate_code<R: RngCore + ?Sized>(rng: &mut R) -> String {
    // Largest multiple of alphabet len that fits in u32; values at or above
    // it are rejected so every symbol is equally likely.
    const ALPHABET_LEN: u32 = CODE_ALPHABET.len() as u32;
    const LIMIT: u32 = u32::MAX - (u32::MAX % ALPHABET_LEN);
    let alphabet: &[u8] = CODE_ALPHABET.as_bytes();
    let mut code = String::with_capacity(CODE_LENGTH);
    while code.len() < CODE_LENGTH {
        let v = loop {
            let v = rng.next_u32();
            if v < LIMIT {
                break v;
            }
        };
        code.push(alphabet[(v % ALPHABET_LEN) as usize] as char);
    }
    code
}

/// Normalize user input for comparison: uppercase and strip everything that
/// is not alphanumeric (so `2345-6789`, `2345 6789`, lowercase all work).
fn normalize_code(input: &str) -> String {
    input
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

/// Constant-time string equality on the normalized code bytes.
fn code_eq(a: &str, b: &str) -> bool {
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

/// Controller-side pairing session.
pub struct PairingServer<C: Clock = SystemClock> {
    clock: C,
    code: String,
    phase: PairingPhase,
    created_ms: u64,
    failed_attempts: u32,
    lockout_until_ms: Option<u64>,
    pending: Option<PendingJoin>,
}

impl<C: Clock> PairingServer<C> {
    /// Start a new pairing session with a fresh random code. `rng` is the
    /// caller's entropy source (`rand_core::OsRng` in production, a
    /// deterministic rng in tests).
    pub fn start<R: RngCore + ?Sized>(clock: C, rng: &mut R) -> Self {
        let code = generate_code(rng);
        let created_ms = clock.now_ms();
        Self {
            clock,
            code,
            phase: PairingPhase::AwaitingJoin,
            created_ms,
            failed_attempts: 0,
            lockout_until_ms: None,
            pending: None,
        }
    }

    /// Start a session with a caller-chosen code (tests, or a code shown by
    /// another channel). The code is normalized before storage.
    pub fn start_with_code(clock: C, code: &str) -> Self {
        let created_ms = clock.now_ms();
        Self {
            clock,
            code: normalize_code(code),
            phase: PairingPhase::AwaitingJoin,
            created_ms,
            failed_attempts: 0,
            lockout_until_ms: None,
            pending: None,
        }
    }

    /// The pairing code to display to the user (e.g. `join --code XXXX-XXXX`).
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Current phase.
    pub fn phase(&self) -> PairingPhase {
        self.phase
    }

    /// The pending join awaiting confirmation, if any.
    pub fn pending(&self) -> Option<&PendingJoin> {
        self.pending.as_ref()
    }

    /// Lazily advance expiry: `AwaitingJoin` dies at `created + PAIRING_TTL_MS`,
    /// `PendingConfirm` dies at its confirm deadline. Called at the top of
    /// every public method.
    fn refresh(&mut self) {
        if self.phase.is_terminal() {
            return;
        }
        let now = self.clock.now_ms();
        let expired = match self.phase {
            PairingPhase::AwaitingJoin => now.saturating_sub(self.created_ms) >= PAIRING_TTL_MS,
            PairingPhase::PendingConfirm => self
                .pending
                .as_ref()
                .map(|p| now >= p.confirm_deadline_ms)
                .unwrap_or(true),
            _ => false,
        };
        if expired {
            self.phase = PairingPhase::Expired;
            self.pending = None;
        }
    }

    /// Handle a worker's join request. On success the phase becomes
    /// `PendingConfirm` and the caller must confirm or deny.
    pub fn handle_join(&mut self, req: JoinRequest) -> Result<PendingJoin, PairingError> {
        self.refresh();
        match self.phase {
            PairingPhase::AwaitingJoin => {}
            PairingPhase::PendingConfirm
            | PairingPhase::Approved
            | PairingPhase::Rejected
            | PairingPhase::Expired => {
                return Err(if self.phase == PairingPhase::Expired {
                    PairingError::Expired
                } else {
                    PairingError::WrongState
                })
            }
        }

        let now = self.clock.now_ms();
        if let Some(until) = self.lockout_until_ms {
            if now < until {
                return Err(PairingError::LockedOut);
            }
            // Lockout expired: fresh attempt window.
            self.lockout_until_ms = None;
            self.failed_attempts = 0;
        }

        let presented = normalize_code(&req.code);
        // Constant-time compare; length differences are not secret (the code
        // length is public knowledge).
        if !code_eq(&presented, &self.code) {
            self.failed_attempts += 1;
            if self.failed_attempts >= MAX_JOIN_ATTEMPTS {
                self.lockout_until_ms = Some(now + LOCKOUT_MS);
                self.failed_attempts = 0;
            }
            return Err(PairingError::InvalidCode);
        }

        let pending = PendingJoin {
            worker_pubkey: req.worker_pubkey,
            hostname: req.hostname,
            confirm_deadline_ms: now + CONFIRM_TIMEOUT_MS,
        };
        self.pending = Some(pending.clone());
        self.phase = PairingPhase::PendingConfirm;
        Ok(pending)
    }

    /// Confirm the pending join. Returns the approval record; the caller is
    /// responsible for sending `PairAccept` with cluster credentials.
    pub fn confirm(&mut self) -> Result<ApprovedJoin, PairingError> {
        self.refresh();
        if self.phase != PairingPhase::PendingConfirm {
            return Err(match self.phase {
                PairingPhase::Expired => PairingError::Expired,
                PairingPhase::Approved | PairingPhase::Rejected => PairingError::AlreadyFinished,
                _ => PairingError::WrongState,
            });
        }
        let pending = self.pending.take().expect("pending join present");
        self.phase = PairingPhase::Approved;
        Ok(ApprovedJoin {
            worker_pubkey: pending.worker_pubkey,
            hostname: pending.hostname,
            approved_at_ms: self.clock.now_ms(),
        })
    }

    /// Deny the pending join.
    pub fn deny(&mut self, _reason: impl Into<String>) -> Result<(), PairingError> {
        self.refresh();
        if self.phase != PairingPhase::PendingConfirm {
            return Err(match self.phase {
                PairingPhase::Expired => PairingError::Expired,
                PairingPhase::Approved | PairingPhase::Rejected => PairingError::AlreadyFinished,
                _ => PairingError::WrongState,
            });
        }
        self.pending = None;
        self.phase = PairingPhase::Rejected;
        Ok(())
    }
}

/// Worker-side pairing client.
pub struct PairingClient<C: Clock = SystemClock> {
    clock: C,
    phase: ClientPhase,
    deadline_ms: Option<u64>,
}

impl<C: Clock> PairingClient<C> {
    /// Create a client in `Idle` phase. Call [`PairingClient::begin`] once the
    /// `PairHello` has been sent.
    pub fn new(clock: C) -> Self {
        Self {
            clock,
            phase: ClientPhase::Idle,
            deadline_ms: None,
        }
    }

    /// Mark the hello as sent; start the answer deadline.
    pub fn begin(&mut self, timeout_ms: u64) -> Result<(), PairingError> {
        if self.phase != ClientPhase::Idle {
            return Err(PairingError::WrongState);
        }
        self.deadline_ms = Some(self.clock.now_ms() + timeout_ms);
        self.phase = ClientPhase::Waiting;
        Ok(())
    }

    /// Handle a `PairAccept` message.
    pub fn on_accept(&mut self, credential: Vec<u8>, ca_fingerprint: String) -> Result<(), PairingError> {
        self.refresh()?;
        if self.phase != ClientPhase::Waiting {
            return Err(PairingError::WrongState);
        }
        self.phase = ClientPhase::Accepted {
            credential,
            ca_fingerprint,
        };
        Ok(())
    }

    /// Handle a `PairReject` message.
    pub fn on_reject(&mut self, reason: impl Into<String>) -> Result<(), PairingError> {
        self.refresh()?;
        if self.phase != ClientPhase::Waiting {
            return Err(PairingError::WrongState);
        }
        self.phase = ClientPhase::Rejected {
            reason: reason.into(),
        };
        Ok(())
    }

    /// Current phase.
    pub fn phase(&self) -> &ClientPhase {
        &self.phase
    }

    /// Whether the client reached a terminal phase.
    pub fn is_finished(&self) -> bool {
        !matches!(self.phase, ClientPhase::Idle | ClientPhase::Waiting)
    }

    fn refresh(&mut self) -> Result<(), PairingError> {
        if let (ClientPhase::Waiting, Some(deadline)) = (&self.phase, self.deadline_ms) {
            if self.clock.now_ms() >= deadline {
                self.phase = ClientPhase::Expired;
            }
        }
        Ok(())
    }

    /// Drive expiry; call periodically or before reading `phase`.
    pub fn tick(&mut self) -> ClientPhase {
        let _ = self.refresh();
        self.phase.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    /// Deterministic RNG: xorshift64*.
    struct StepRng(u64);
    impl RngCore for StepRng {
        fn next_u32(&mut self) -> u32 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            (self.0.wrapping_mul(0x2545F4914F6CDD1D) >> 32) as u32
        }
        fn next_u64(&mut self) -> u64 {
            ((self.next_u32() as u64) << 32) | self.next_u32() as u64
        }
        fn fill_bytes(&mut self, dest: &mut [u8]) {
            for chunk in dest.chunks_mut(8) {
                let v = self.next_u64().to_le_bytes();
                chunk.copy_from_slice(&v[..chunk.len()]);
            }
        }
        fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand_core::Error> {
            self.fill_bytes(dest);
            Ok(())
        }
    }

    /// Mutable clock shareable between test and the FSM under test.
    #[derive(Clone)]
    struct TestClock(Rc<Cell<u64>>);
    impl TestClock {
        fn new(t: u64) -> Self {
            Self(Rc::new(Cell::new(t)))
        }
        fn set(&self, t: u64) {
            self.0.set(t);
        }
        fn advance(&self, ms: u64) {
            self.0.set(self.0.get() + ms);
        }
    }
    impl Clock for TestClock {
        fn now_ms(&self) -> u64 {
            self.0.get()
        }
    }

    fn pubkey(seed: u8) -> [u8; 32] {
        [seed; 32]
    }

    fn join(code: &str, seed: u8) -> JoinRequest {
        JoinRequest {
            code: code.to_string(),
            worker_pubkey: pubkey(seed),
            hostname: format!("worker-{seed}"),
        }
    }

    #[test]
    fn code_alphabet_has_no_ambiguous_chars() {
        for bad in ['0', 'O', '1', 'I', 'L', 'o', 'l', 'i'] {
            assert!(!CODE_ALPHABET.contains(bad), "alphabet contains {bad}");
        }
        assert_eq!(CODE_ALPHABET.len(), 30);
        // ~40 bits of entropy.
        let bits = (CODE_ALPHABET.len() as f64).log2() * CODE_LENGTH as f64;
        assert!(bits > 38.0 && bits < 41.0, "entropy {bits} bits");
    }

    #[test]
    fn generated_codes_use_alphabet_and_are_unique() {
        let mut rng = StepRng(0x12345678);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..500 {
            let code = generate_code(&mut rng);
            assert_eq!(code.len(), CODE_LENGTH);
            assert!(code.chars().all(|c| CODE_ALPHABET.contains(c)));
            seen.insert(code);
        }
        assert_eq!(seen.len(), 500, "codes should be effectively unique");
    }

    #[test]
    fn happy_path_join_confirm() {
        let clock = TestClock::new(1_000_000);
        let mut rng = StepRng(42);
        let mut server = PairingServer::start(clock.clone(), &mut rng);
        let code = server.code().to_string();
        assert_eq!(server.phase(), PairingPhase::AwaitingJoin);

        let pending = server.handle_join(join(&code, 7)).unwrap();
        assert_eq!(pending.hostname, "worker-7");
        assert_eq!(pending.worker_pubkey, pubkey(7));
        assert_eq!(server.phase(), PairingPhase::PendingConfirm);

        let approved = server.confirm().unwrap();
        assert_eq!(approved.worker_pubkey, pubkey(7));
        assert_eq!(approved.hostname, "worker-7");
        assert_eq!(server.phase(), PairingPhase::Approved);
        assert!(server.phase().is_terminal());
    }

    #[test]
    fn deny_path() {
        let mut server = PairingServer::start_with_code(TestClock::new(0), "ABCD2345");
        server.handle_join(join("abcd-2345", 1)).unwrap(); // normalization
        server.deny("nope").unwrap();
        assert_eq!(server.phase(), PairingPhase::Rejected);
    }

    #[test]
    fn code_is_case_and_separator_insensitive() {
        let mut server = PairingServer::start_with_code(TestClock::new(0), "wxyz6789");
        server.handle_join(join("WXYZ-6789", 3)).unwrap();
        assert_eq!(server.phase(), PairingPhase::PendingConfirm);
    }

    #[test]
    fn wrong_code_counts_attempts_then_locks_out() {
        let mut server = PairingServer::start_with_code(TestClock::new(0), "ABCD2345");
        for _ in 0..MAX_JOIN_ATTEMPTS {
            assert_eq!(
                server.handle_join(join("WRONGCODE", 9)),
                Err(PairingError::InvalidCode)
            );
        }
        // Next attempt during lockout → LockedOut (not InvalidCode).
        assert_eq!(
            server.handle_join(join("WRONGCODE", 9)),
            Err(PairingError::LockedOut)
        );
        // Even the correct code is refused during lockout.
        assert_eq!(
            server.handle_join(join("ABCD2345", 9)),
            Err(PairingError::LockedOut)
        );
    }

    #[test]
    fn lockout_expires_and_grants_fresh_window() {
        let clock = TestClock::new(0);
        let mut server = PairingServer::start_with_code(clock.clone(), "ABCD2345");
        for _ in 0..MAX_JOIN_ATTEMPTS {
            let _ = server.handle_join(join("WRONG", 1));
        }
        assert_eq!(
            server.handle_join(join("WRONG", 1)),
            Err(PairingError::LockedOut)
        );
        // Advance past the 60s lockout.
        clock.advance(LOCKOUT_MS + 1);
        // Fresh window: the correct code works again.
        server.handle_join(join("ABCD2345", 5)).unwrap();
        assert_eq!(server.phase(), PairingPhase::PendingConfirm);
    }

    #[test]
    fn code_ttl_expires_session() {
        let clock = TestClock::new(0);
        let mut server = PairingServer::start_with_code(clock.clone(), "ABCD2345");
        clock.set(PAIRING_TTL_MS);
        assert_eq!(
            server.handle_join(join("ABCD2345", 1)),
            Err(PairingError::Expired)
        );
        assert_eq!(server.phase(), PairingPhase::Expired);
    }

    #[test]
    fn confirm_timeout_expires_pending_session() {
        let clock = TestClock::new(0);
        let mut server = PairingServer::start_with_code(clock.clone(), "ABCD2345");
        server.handle_join(join("ABCD2345", 2)).unwrap();
        clock.set(CONFIRM_TIMEOUT_MS);
        assert_eq!(server.confirm(), Err(PairingError::Expired));
        assert_eq!(server.phase(), PairingPhase::Expired);
    }

    #[test]
    fn single_use_code_cannot_be_replayed() {
        let mut server = PairingServer::start_with_code(TestClock::new(0), "ABCD2345");
        server.handle_join(join("ABCD2345", 1)).unwrap();
        server.confirm().unwrap();
        assert_eq!(server.phase(), PairingPhase::Approved);
        // Approved is terminal; a second join with the same code fails.
        assert!(matches!(
            server.handle_join(join("ABCD2345", 2)),
            Err(PairingError::WrongState)
        ));
        // confirm again also fails.
        assert_eq!(server.confirm(), Err(PairingError::AlreadyFinished));
    }

    #[test]
    fn join_while_pending_confirm_is_rejected() {
        let mut server = PairingServer::start_with_code(TestClock::new(0), "ABCD2345");
        server.handle_join(join("ABCD2345", 1)).unwrap();
        assert_eq!(
            server.handle_join(join("ABCD2345", 2)),
            Err(PairingError::WrongState)
        );
        // And a wrong code during PendingConfirm is not counted as an attempt.
        assert_eq!(
            server.handle_join(join("WRONG", 2)),
            Err(PairingError::WrongState)
        );
    }

    #[test]
    fn client_accept_reject_expire() {
        let clock = TestClock::new(0);
        let mut client = PairingClient::new(clock.clone());
        client.begin(1000).unwrap();
        assert_eq!(client.phase(), &ClientPhase::Waiting);

        client.on_accept(b"cred".to_vec(), "fp".into()).unwrap();
        assert!(matches!(client.phase(), ClientPhase::Accepted { .. }));
        assert!(client.is_finished());

        // Reject path.
        let mut c2 = PairingClient::new(clock.clone());
        c2.begin(1000).unwrap();
        c2.on_reject("denied by user").unwrap();
        assert_eq!(
            c2.phase(),
            &ClientPhase::Rejected {
                reason: "denied by user".into()
            }
        );

        // Expiry path.
        let mut c3 = PairingClient::new(clock.clone());
        c3.begin(500).unwrap();
        clock.set(600);
        assert_eq!(c3.tick(), ClientPhase::Expired);
    }

    #[test]
    fn client_cannot_begin_twice_or_accept_while_idle() {
        let mut client = PairingClient::new(TestClock::new(0));
        // Accept before begin → WrongState.
        assert_eq!(
            client.on_accept(vec![], "fp".into()),
            Err(PairingError::WrongState)
        );
        client.begin(1000).unwrap();
        assert_eq!(client.begin(1000), Err(PairingError::WrongState));
    }
}
