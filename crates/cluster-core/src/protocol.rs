//! Cluster wire protocol codec — pure encoding/decoding, no sockets.
//!
//! Wire format: each frame on the wire is
//!
//! ```text
//! u32 BE payload_len || payload_len bytes of UTF-8 JSON
//! ```
//!
//! `payload_len` includes only the JSON document and is capped at
//! [`MAX_FRAME_LEN`] (16 MiB). Every JSON payload is an envelope:
//!
//! ```json
//! { "version": 1, "message": { "type": "heartbeat", "seq": 7 } }
//! ```
//!
//! The `message` object is a [`Message`] internally tagged by `"type"`
//! (`snake_case` variant names). Decoding rejects frames whose `version`
//! differs from [`PROTOCOL_VERSION`], oversize frames, truncated frames, and
//! malformed JSON.
//!
//! Binary blobs inside messages use text encodings (documented per field):
//! - `worker_pubkey`, `cluster_pubkey`: 64-char lowercase hex (32 bytes).
//! - `PairAccept.credential`, `ApiStreamData.data`: **base64** (standard
//!   alphabet, with padding) — chosen over hex for ~33% smaller frames on
//!   bulk API stream data.
//!
//! Stream IDs: `0` is reserved for control-plane messages; API byte streams
//! use IDs `1..=u32::MAX`. The mux layer (cluster-net) owns ordering,
//! buffering, and the 64-stream cap — this module only defines the message
//! vocabulary.

use crate::fsutil;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Wire protocol version implemented by this codec.
pub const PROTOCOL_VERSION: u32 = 1;
/// Maximum JSON payload size per frame: 16 MiB.
pub const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
/// Stream ID reserved for control-plane messages.
pub const CONTROL_STREAM_ID: u32 = 0;
/// Length in bytes of the frame length prefix.
pub const FRAME_HEADER_LEN: usize = 4;

/// Resource snapshot reported by workers (and the local machine) in
/// `WorkerHello`, `Resources` messages, and the worker registry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    /// CPU core count visible to the worker.
    pub cpu_cores: u32,
    /// Total physical memory in MiB.
    pub mem_total_mb: u64,
    /// Currently available memory in MiB.
    pub mem_available_mb: u64,
    /// Agents currently running on this machine.
    pub agent_count: u32,
    /// Configured cap on concurrent agents.
    pub max_agents: u32,
    /// Load averages (1/5/15 min where available); empty if unknown.
    #[serde(default)]
    pub load_avg: Vec<f64>,
    /// Agent kinds this machine can run (e.g. `["claude","codex"]`).
    #[serde(default)]
    pub kinds: Vec<String>,
}

/// Cluster protocol messages. `snake_case` `type` tag on the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    /// Worker → controller: first message of a pairing attempt.
    /// `worker_pubkey` is 64-char lowercase hex.
    PairHello {
        /// One-time pairing code entered by the user.
        code: String,
        /// Worker's Ed25519 public key, hex-encoded.
        worker_pubkey: String,
        /// Worker-reported hostname.
        hostname: String,
        /// Agent kinds/capabilities the worker advertises.
        #[serde(default)]
        caps: Vec<String>,
    },
    /// Controller → worker: pairing approved. `credential` is base64;
    /// `ca_fingerprint` pins the cluster CA.
    PairAccept {
        /// Opaque cluster credential issued to this worker, base64.
        credential: String,
        /// Cluster CA fingerprint (see `identity::fingerprint_of_pubkey`).
        ca_fingerprint: String,
    },
    /// Controller → worker: pairing denied.
    PairReject {
        /// Human-readable rejection reason.
        reason: String,
    },
    /// Worker → controller: retract a pairing the controller already
    /// accepted — e.g. the worker declined the CA-fingerprint TOFU check
    /// or failed to persist its credential (issue #110). `signature` is
    /// base64 Ed25519 over [`crate::identity::pair_decline_challenge`];
    /// the controller only drops a registry row that has never completed
    /// a session (`last_seen` still `None`).
    PairDecline {
        /// Worker's Ed25519 public key, hex-encoded.
        worker_pubkey: String,
        /// Ed25519 signature proving possession of the worker key, base64.
        signature: String,
    },
    /// Worker → controller: post-pairing hello with identity + resources.
    WorkerHello {
        /// Worker's Ed25519 public key, hex-encoded.
        worker_pubkey: String,
        /// Worker-reported hostname.
        hostname: String,
        /// Advertised capabilities.
        #[serde(default)]
        caps: Vec<String>,
        /// Initial resource snapshot.
        resources: Resources,
        /// Protocol version the worker speaks.
        protocol_version: u32,
    },
    /// Either side → open an API byte stream. `stream_id` must be > 0.
    ApiStreamOpen {
        /// Stream identifier (> [`CONTROL_STREAM_ID`]).
        stream_id: u32,
    },
    /// Either side → chunk of API bytes. Payload is base64.
    ApiStreamData {
        /// Owning stream identifier.
        stream_id: u32,
        /// Raw bytes of the tunneled herdr API stream, base64-encoded.
        data: String,
    },
    /// Either side → close a stream. `half_close` propagates FIN semantics:
    /// the sender will send no more data but may still receive.
    ApiStreamClose {
        /// Owning stream identifier.
        stream_id: u32,
        /// True for half-close (FIN), false for full close.
        half_close: bool,
    },
    /// Periodic liveness probe. `seq` lets peers detect missed beats.
    Heartbeat {
        /// Sender-chosen monotonically increasing sequence number.
        seq: u64,
    },
    /// Worker → controller: periodic resource report (~15 s + on change).
    Resources {
        /// CPU core count.
        cpu_cores: u32,
        /// Total memory in MiB.
        mem_total_mb: u64,
        /// Available memory in MiB.
        mem_available_mb: u64,
        /// Running agent count.
        agent_count: u32,
        /// Agent cap.
        max_agents: u32,
        /// Load averages (may be empty).
        #[serde(default)]
        load_avg: Vec<f64>,
        /// Supported agent kinds.
        #[serde(default)]
        kinds: Vec<String>,
    },
    /// Generic cluster event (worker joined/left, agent state change, …).
    /// `payload` is an arbitrary JSON object for forward compatibility.
    Event {
        /// Dotted event kind, e.g. `"worker.online"`.
        kind: String,
        /// Event-specific JSON payload.
        #[serde(default)]
        payload: Value,
    },
}

/// Errors from the codec.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    /// Frame header declares a payload larger than [`MAX_FRAME_LEN`].
    #[error("frame too large: {len} bytes (max {MAX_FRAME_LEN})")]
    FrameTooLarge {
        /// Declared payload length.
        len: usize,
    },
    /// Buffer ended before a complete frame was available.
    #[error("incomplete frame: need {needed} more byte(s)")]
    Truncated {
        /// Bytes still needed.
        needed: usize,
    },
    /// Payload was not valid JSON / did not match the schema.
    #[error("invalid frame payload: {0}")]
    InvalidPayload(String),
    /// `version` field missing or not equal to [`PROTOCOL_VERSION`].
    #[error("unsupported protocol version {found} (expected {PROTOCOL_VERSION})")]
    UnsupportedVersion {
        /// Version found in the envelope.
        found: u32,
    },
}

/// The on-the-wire envelope: `{ "version": N, "message": { … } }`.
#[derive(Debug, Serialize, Deserialize)]
struct Envelope {
    version: u32,
    message: Message,
}

/// Encode a [`Message`] into a complete wire frame
/// (`u32 BE len || JSON`). Errors only if the payload exceeds
/// [`MAX_FRAME_LEN`].
pub fn encode_frame(msg: &Message) -> Result<Vec<u8>, ProtocolError> {
    let payload = encode_payload(msg)?;
    if payload.len() > MAX_FRAME_LEN {
        return Err(ProtocolError::FrameTooLarge { len: payload.len() });
    }
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Encode just the JSON payload (no length prefix).
pub fn encode_payload(msg: &Message) -> Result<Vec<u8>, ProtocolError> {
    let envelope = Envelope {
        version: PROTOCOL_VERSION,
        message: msg.clone(),
    };
    serde_json::to_vec(&envelope).map_err(|e| ProtocolError::InvalidPayload(e.to_string()))
}

/// Decode a complete frame buffer into a [`Message`]. `buf` must contain
/// exactly one frame (header + payload); trailing bytes are rejected as
/// [`ProtocolError::InvalidPayload`]-adjacent misuse — use [`FrameDecoder`]
/// for streams.
pub fn decode_frame(buf: &[u8]) -> Result<Message, ProtocolError> {
    if buf.len() < FRAME_HEADER_LEN {
        return Err(ProtocolError::Truncated {
            needed: FRAME_HEADER_LEN - buf.len(),
        });
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len > MAX_FRAME_LEN {
        return Err(ProtocolError::FrameTooLarge { len });
    }
    if buf.len() < FRAME_HEADER_LEN + len {
        return Err(ProtocolError::Truncated {
            needed: FRAME_HEADER_LEN + len - buf.len(),
        });
    }
    if buf.len() != FRAME_HEADER_LEN + len {
        return Err(ProtocolError::InvalidPayload(format!(
            "frame buffer has {} trailing byte(s)",
            buf.len() - FRAME_HEADER_LEN - len
        )));
    }
    decode_payload(&buf[FRAME_HEADER_LEN..])
}

/// Decode a bare JSON payload (no length prefix).
pub fn decode_payload(payload: &[u8]) -> Result<Message, ProtocolError> {
    let envelope: Envelope = serde_json::from_slice(payload)
        .map_err(|e| ProtocolError::InvalidPayload(e.to_string()))?;
    if envelope.version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion {
            found: envelope.version,
        });
    }
    Ok(envelope.message)
}

/// Incremental frame decoder for a byte stream. Feed arbitrary chunks with
/// [`FrameDecoder::feed`], then pull complete messages with
/// [`FrameDecoder::next_message`]. An oversize *declared* length is rejected
/// as soon as the 4-byte header arrives, before the payload is read.
#[derive(Default)]
pub struct FrameDecoder {
    buf: Vec<u8>,
}

impl FrameDecoder {
    /// Create an empty decoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append received bytes to the internal buffer.
    pub fn feed(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Bytes currently buffered (for diagnostics/backpressure).
    pub fn buffered_len(&self) -> usize {
        self.buf.len()
    }

    /// Try to decode the next complete frame.
    ///
    /// - `Ok(Some(msg))` — a full frame was decoded and consumed.
    /// - `Ok(None)` — buffer holds an incomplete frame; feed more bytes.
    /// - `Err(_)` — malformed/oversize/version-mismatched frame. The decoder
    ///   state is unspecified afterwards; discard the connection.
    pub fn next_message(&mut self) -> Result<Option<Message>, ProtocolError> {
        if self.buf.len() < FRAME_HEADER_LEN {
            return Ok(None);
        }
        let len = u32::from_be_bytes([self.buf[0], self.buf[1], self.buf[2], self.buf[3]]) as usize;
        if len > MAX_FRAME_LEN {
            return Err(ProtocolError::FrameTooLarge { len });
        }
        if self.buf.len() < FRAME_HEADER_LEN + len {
            return Ok(None);
        }
        let payload: Vec<u8> = self.buf[FRAME_HEADER_LEN..FRAME_HEADER_LEN + len].to_vec();
        self.buf.drain(..FRAME_HEADER_LEN + len);
        decode_payload(&payload).map(Some)
    }
}

/// Encode a 32-byte public key as 64 lowercase hex chars.
pub fn pubkey_to_hex(pubkey: &[u8; 32]) -> String {
    fsutil::hex_encode(pubkey)
}

/// Decode a hex public key. Errors on wrong length or non-hex input.
pub fn pubkey_from_hex(s: &str) -> Result<[u8; 32], ProtocolError> {
    let bytes = fsutil::hex_decode(s)
        .ok_or_else(|| ProtocolError::InvalidPayload("pubkey is not valid hex".to_string()))?;
    if bytes.len() != 32 {
        return Err(ProtocolError::InvalidPayload(format!(
            "pubkey must be 32 bytes, got {}",
            bytes.len()
        )));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Base64-encode arbitrary bytes (standard alphabet, padded).
pub fn bytes_to_base64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Decode base64 produced by [`bytes_to_base64`].
pub fn base64_to_bytes(s: &str) -> Result<Vec<u8>, ProtocolError> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| ProtocolError::InvalidPayload(format!("invalid base64: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_messages() -> Vec<Message> {
        vec![
            Message::PairHello {
                code: "K7QP-2MXR".into(),
                worker_pubkey: pubkey_to_hex(&[0xAB; 32]),
                hostname: "desktop-win".into(),
                caps: vec!["claude".into(), "codex".into()],
            },
            Message::PairAccept {
                credential: bytes_to_base64(&[1, 2, 3, 250]),
                ca_fingerprint: "0123456789abcdef".into(),
            },
            Message::PairReject {
                reason: "user denied".into(),
            },
            Message::PairDecline {
                worker_pubkey: pubkey_to_hex(&[0x22; 32]),
                signature: bytes_to_base64(&[0x33; 64]),
            },
            Message::WorkerHello {
                worker_pubkey: pubkey_to_hex(&[0x11; 32]),
                hostname: "wsl2".into(),
                caps: vec![],
                resources: Resources {
                    cpu_cores: 8,
                    mem_total_mb: 16384,
                    mem_available_mb: 5120,
                    agent_count: 1,
                    max_agents: 4,
                    load_avg: vec![0.5, 0.7, 0.9],
                    kinds: vec!["claude".into()],
                },
                protocol_version: PROTOCOL_VERSION,
            },
            Message::ApiStreamOpen { stream_id: 1 },
            Message::ApiStreamData {
                stream_id: 1,
                data: bytes_to_base64(&[0u8; 1024]),
            },
            Message::ApiStreamClose {
                stream_id: 1,
                half_close: true,
            },
            Message::Heartbeat { seq: 42 },
            Message::Resources {
                cpu_cores: 4,
                mem_total_mb: 8192,
                mem_available_mb: 2048,
                agent_count: 0,
                max_agents: 4,
                load_avg: vec![],
                kinds: vec!["codex".into()],
            },
            Message::Event {
                kind: "worker.online".into(),
                payload: serde_json::json!({"worker_id": "w1", "label": "desktop"}),
            },
        ]
    }

    #[test]
    fn roundtrip_every_variant() {
        for msg in sample_messages() {
            let frame = encode_frame(&msg).unwrap();
            let decoded = decode_frame(&frame).unwrap();
            assert_eq!(decoded, msg, "roundtrip failed for {msg:?}");
        }
    }

    #[test]
    fn frame_has_version_and_length_prefix() {
        let msg = Message::Heartbeat { seq: 1 };
        let frame = encode_frame(&msg).unwrap();
        let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        assert_eq!(len, frame.len() - FRAME_HEADER_LEN);
        let payload: Value = serde_json::from_slice(&frame[FRAME_HEADER_LEN..]).unwrap();
        assert_eq!(payload["version"], PROTOCOL_VERSION);
        assert_eq!(payload["message"]["type"], "heartbeat");
    }

    #[test]
    fn truncated_frame_errors() {
        let msg = Message::Heartbeat { seq: 9 };
        let frame = encode_frame(&msg).unwrap();
        // Missing payload entirely.
        assert!(matches!(
            decode_frame(&frame[..FRAME_HEADER_LEN]),
            Err(ProtocolError::Truncated { .. })
        ));
        // Partial payload.
        assert!(matches!(
            decode_frame(&frame[..frame.len() - 1]),
            Err(ProtocolError::Truncated { .. })
        ));
        // Less than header.
        assert!(matches!(
            decode_frame(&frame[..2]),
            Err(ProtocolError::Truncated { .. })
        ));
    }

    #[test]
    fn garbage_payload_errors() {
        let mut frame = Vec::new();
        let payload = b"this is not json";
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        assert!(matches!(
            decode_frame(&frame),
            Err(ProtocolError::InvalidPayload(_))
        ));
    }

    #[test]
    fn oversize_frame_rejected_before_read() {
        // Declared length beyond the cap must be rejected from the header
        // alone — decoder must not wait for 16MiB+ of payload.
        let mut dec = FrameDecoder::new();
        dec.feed(&(MAX_FRAME_LEN as u32 + 1).to_be_bytes());
        assert!(matches!(
            dec.next_message(),
            Err(ProtocolError::FrameTooLarge { .. })
        ));
        // decode_frame path too.
        let mut buf = (MAX_FRAME_LEN as u32 + 1).to_be_bytes().to_vec();
        buf.extend_from_slice(b"{}");
        assert!(matches!(
            decode_frame(&buf),
            Err(ProtocolError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn version_mismatch_rejected() {
        let payload = br#"{"version":99,"message":{"type":"heartbeat","seq":1}}"#;
        assert!(matches!(
            decode_payload(payload),
            Err(ProtocolError::UnsupportedVersion { found: 99 })
        ));
        // Missing version → invalid payload (serde error).
        assert!(matches!(
            decode_payload(br#"{"message":{"type":"heartbeat","seq":1}}"#),
            Err(ProtocolError::InvalidPayload(_))
        ));
    }

    #[test]
    fn decoder_handles_split_and_coalesced_frames() {
        let mut dec = FrameDecoder::new();
        let f1 = encode_frame(&Message::Heartbeat { seq: 1 }).unwrap();
        let f2 = encode_frame(&Message::Heartbeat { seq: 2 }).unwrap();
        // Feed byte-by-byte to exercise incremental decoding.
        for &b in f1.iter().chain(f2.iter()) {
            dec.feed(&[b]);
        }
        assert_eq!(
            dec.next_message().unwrap(),
            Some(Message::Heartbeat { seq: 1 })
        );
        assert_eq!(
            dec.next_message().unwrap(),
            Some(Message::Heartbeat { seq: 2 })
        );
        assert_eq!(dec.next_message().unwrap(), None);
    }

    #[test]
    fn decoder_waits_for_incomplete_frame() {
        let mut dec = FrameDecoder::new();
        let frame = encode_frame(&Message::Heartbeat { seq: 3 }).unwrap();
        dec.feed(&frame[..10]);
        assert_eq!(dec.next_message().unwrap(), None);
        dec.feed(&frame[10..]);
        assert_eq!(
            dec.next_message().unwrap(),
            Some(Message::Heartbeat { seq: 3 })
        );
    }

    #[test]
    fn pubkey_hex_helpers() {
        let key = [0x5Au8; 32];
        let hex = pubkey_to_hex(&key);
        assert_eq!(hex.len(), 64);
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(pubkey_from_hex(&hex).unwrap(), key);
        assert!(pubkey_from_hex("zz").is_err());
        assert!(pubkey_from_hex(&hex[..62]).is_err());
    }

    #[test]
    fn stream_data_base64_roundtrip() {
        let raw: Vec<u8> = (0..=255).cycle().take(10_000).collect();
        let msg = Message::ApiStreamData {
            stream_id: 7,
            data: bytes_to_base64(&raw),
        };
        let frame = encode_frame(&msg).unwrap();
        let decoded = decode_frame(&frame).unwrap();
        match decoded {
            Message::ApiStreamData { stream_id, data } => {
                assert_eq!(stream_id, 7);
                assert_eq!(base64_to_bytes(&data).unwrap(), raw);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
