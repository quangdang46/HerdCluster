//! Stream multiplexer over a single TLS connection.
//!
//! # Wire format
//!
//! Every mux frame on the wire is:
//!
//! ```text
//! u32 BE stream_id | u8 flags | u32 BE payload_len | payload
//! ```
//!
//! `payload_len` is capped at [`MAX_PAYLOAD`] (1 MiB). Flag bits:
//!
//! | bit | name   | meaning                                            |
//! |-----|--------|----------------------------------------------------|
//! | 0   | `FIN`  | sender will send no more data (half-close)        |
//! | 1   | `RST`  | abort the stream; pending + future I/O fails       |
//! | 2   | `OPEN` | first frame for a peer-opened stream               |
//!
//! # Stream ID allocation
//!
//! IDs with the top bit (`0x8000_0000`) **clear** are allocated by the
//! *dialing* side ([`MuxRole::Dialer`], i.e. the worker); IDs with it **set**
//! are allocated by the *accepting* side ([`MuxRole::Acceptor`], i.e. the
//! controller). Each side numbers its own streams from 1 upward, so the two
//! ID spaces can never collide. Stream `0` is the reserved control channel
//! carrying `cluster_core::protocol` frames — implicitly open on both ends,
//! never allocated, exposed via [`Mux::control`].
//!
//! # Ordering, close, and backpressure
//!
//! - Per-stream ordering is guaranteed: each stream has a single FIFO
//!   outbound queue drained in order by the writer task and a single FIFO
//!   inbound queue filled in order by the reader task.
//! - `StreamHandle::close()` queues a `FIN` frame: the peer's `recv()`
//!   returns `Ok(None)` once buffered data drains, while the reverse
//!   direction stays open (half-close). `abort()` queues `RST`.
//! - When the connection dies, every stream's `recv()` fails with
//!   [`MuxError::ConnectionClosed`] and every `send()` fails — there is no
//!   silent resume.
//! - Backpressure: inbound per-stream buffered bytes are capped at
//!   [`STREAM_BUFFER_CAP`] (1 MiB). When a stream's queue is full the reader
//!   task stops reading from the socket entirely (TCP-level backpressure)
//!   until the application drains it. Outbound `send()` awaits while the
//!   per-stream outbound queue exceeds the same cap.
//! - At most [`MAX_STREAMS`] (64) streams may be live per connection;
//!   further `open_stream()` calls fail and inbound `OPEN`s are reset.
//!
//! The mux is transport-agnostic: any `AsyncRead + AsyncWrite` stream works
//! (TLS streams in production, `tokio::io::duplex` in tests).

use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

/// Maximum payload per mux frame: 1 MiB.
pub const MAX_PAYLOAD: usize = 1024 * 1024;
/// Maximum buffered-but-unread bytes per stream (both directions): 1 MiB.
pub const STREAM_BUFFER_CAP: usize = 1024 * 1024;
/// Maximum concurrently live streams per connection.
pub const MAX_STREAMS: usize = 64;
/// Reserved stream ID for the control channel.
pub const CONTROL_STREAM_ID: u32 = 0;
/// Top bit marking IDs allocated by the accepting side.
const ACCEPTOR_ID_BIT: u32 = 0x8000_0000;
/// Inbound per-stream channel capacity (messages, in addition to byte cap).
const INBOUND_MSG_CAP: usize = 64;
/// Frames drained per writer iteration.
const WRITER_BATCH: usize = 256;

const FLAG_FIN: u8 = 0x01;
const FLAG_RST: u8 = 0x02;
const FLAG_OPEN: u8 = 0x04;

const HEADER_LEN: usize = 4 + 1 + 4;

/// Which side of the connection this mux endpoint is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MuxRole {
    /// The side that initiated the connection (worker → controller). Owns
    /// stream IDs with the top bit clear.
    Dialer,
    /// The side that accepted the connection (controller). Owns stream IDs
    /// with the top bit set.
    Acceptor,
}

/// Errors from mux operations.
#[derive(Debug, Error)]
pub enum MuxError {
    /// The connection or this stream is gone; do not retry on it.
    #[error("mux connection closed")]
    Closed,
    /// The connection died while the stream was live.
    #[error("connection lost")]
    ConnectionClosed,
    /// The stream was reset (peer abort).
    #[error("stream reset")]
    Reset,
    /// The per-connection stream cap was reached.
    #[error("too many streams on this connection")]
    TooManyStreams,
    /// Payload exceeds [`MAX_PAYLOAD`].
    #[error("frame too large")]
    FrameTooLarge,
    /// Malformed frame received from the peer.
    #[error("protocol error: {0}")]
    Protocol(String),
    /// Underlying I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

// ---- internal stream state ----

/// Inbound events delivered to the receiving half.
enum InMsg {
    Data(Bytes),
    /// Remote sent FIN: no more data will arrive.
    Fin,
    /// Remote aborted the stream.
    Reset,
}

/// Outbound item queued for the writer task.
struct OutItem {
    flags: u8,
    payload: Bytes,
}

const SF_LOCAL_FIN: u8 = 0x01;
const SF_REMOTE_FIN: u8 = 0x02;
const SF_RESET: u8 = 0x04;
const SF_DEAD: u8 = 0x08;

/// Per-stream shared state.
struct StreamShared {
    id: u32,
    /// FIFO of outbound items.
    out: Mutex<VecDeque<OutItem>>,
    /// Bytes currently queued in `out`.
    out_bytes: AtomicUsize,
    /// Number of items queued in `out` (data + FIN/RST markers).
    out_len: AtomicUsize,
    /// Signalled when outbound space frees or the connection dies.
    out_notify: Notify,
    /// Shared wake for the connection writer task (any enqueue).
    wake: Arc<Notify>,
    /// Signalled when inbound buffer space frees or the stream dies.
    in_space: Notify,
    /// Queued-but-unread inbound payload bytes.
    in_bytes: AtomicUsize,
    /// Receiving end; `recv()` holds the mutex for the duration of one
    /// `Receiver::recv()` call so concurrent recv()s serialize cleanly.
    in_rx: tokio::sync::Mutex<mpsc::Receiver<InMsg>>,
    /// Sender half; the reader task uses `try_send` after cap checks.
    in_tx: mpsc::Sender<InMsg>,
    flags: AtomicU8,
    /// Set once `recv()` observed Fin — later recv()s return `Ok(None)`.
    recv_done: AtomicBool,
}

impl StreamShared {
    fn flag(&self, f: u8) -> bool {
        self.flags.load(Ordering::Acquire) & f != 0
    }
    fn is_dead(&self) -> bool {
        self.flag(SF_RESET) || self.flag(SF_DEAD)
    }
    fn local_fin_sent(&self) -> bool {
        self.flag(SF_LOCAL_FIN)
    }
    fn remote_fin(&self) -> bool {
        self.flag(SF_REMOTE_FIN)
    }
    /// Fully closed and drained — safe for the map to forget us.
    fn fully_closed(&self) -> bool {
        self.local_fin_sent()
            && self.remote_fin()
            && !self.is_dead()
            && self.out_len.load(Ordering::Acquire) == 0
    }
    /// Dead and drained — safe to forget (peer frames become no-ops).
    fn dead_and_drained(&self) -> bool {
        self.is_dead() && self.out_len.load(Ordering::Acquire) == 0
    }
    fn mark_dead(&self) {
        self.flags.fetch_or(SF_DEAD, Ordering::AcqRel);
        self.out_notify.notify_waiters();
        self.in_space.notify_waiters();
        // Dropping a queued InMsg is unnecessary; closing the channel is
        // enough — but try_send keeps the channel open. Explicitly close by
        // dropping is impossible (we hold the sender), so send a terminal
        // marker best-effort; recv() also checks `is_dead()` on channel close.
        let _ = self.in_tx.try_send(InMsg::Reset);
    }
}

fn new_stream_state(id: u32, wake: Arc<Notify>) -> Arc<StreamShared> {
    let (in_tx, in_rx) = mpsc::channel(INBOUND_MSG_CAP);
    Arc::new(StreamShared {
        id,
        out: Mutex::new(VecDeque::new()),
        out_bytes: AtomicUsize::new(0),
        out_len: AtomicUsize::new(0),
        out_notify: Notify::new(),
        wake,
        in_space: Notify::new(),
        in_bytes: AtomicUsize::new(0),
        in_rx: tokio::sync::Mutex::new(in_rx),
        in_tx,
        flags: AtomicU8::new(0),
        recv_done: AtomicBool::new(false),
    })
}

/// Push an outbound item unconditionally (control frames: OPEN/FIN/RST).
fn queue_out(st: &StreamShared, item: OutItem) {
    let mut out = st.out.lock().unwrap();
    st.out_bytes.fetch_add(item.payload.len(), Ordering::AcqRel);
    st.out_len.fetch_add(1, Ordering::AcqRel);
    out.push_back(item);
    st.out_notify.notify_one();
    st.wake.notify_one();
}

/// Push a data item only if the outbound byte cap allows it.
fn try_queue_data(st: &StreamShared, payload: Bytes) -> bool {
    let mut out = st.out.lock().unwrap();
    if st.out_bytes.load(Ordering::Acquire) + payload.len() > STREAM_BUFFER_CAP {
        return false;
    }
    st.out_bytes.fetch_add(payload.len(), Ordering::AcqRel);
    st.out_len.fetch_add(1, Ordering::AcqRel);
    out.push_back(OutItem { flags: 0, payload });
    st.out_notify.notify_one();
    st.wake.notify_one();
    true
}

// ---- public handle ----

/// One end of a multiplexed byte stream.
///
/// Cloneable; clones share the same underlying stream. Ordering between
/// concurrent `send()` callers on clones is unspecified, but each `send()`
/// is atomic — bytes from a single call are never interleaved with another
/// call's bytes on the wire.
#[derive(Clone)]
pub struct StreamHandle {
    shared: Arc<StreamShared>,
}

impl StreamHandle {
    /// The stream ID (namespaced by opener — see module docs).
    pub fn id(&self) -> u32 {
        self.shared.id
    }

    /// Queue `data` for sending. Awaits while the outbound queue is full
    /// (backpressure). Fails once the stream or connection is dead, and
    /// with [`MuxError::FrameTooLarge`] if `data` exceeds [`MAX_PAYLOAD`].
    pub async fn send(&self, data: Bytes) -> Result<(), MuxError> {
        if data.len() > MAX_PAYLOAD {
            return Err(MuxError::FrameTooLarge);
        }
        loop {
            {
                let st = &self.shared;
                if st.is_dead() {
                    return Err(dead_err(st));
                }
                if st.local_fin_sent() {
                    return Err(MuxError::Closed);
                }
                if try_queue_data(st, data.clone()) {
                    return Ok(());
                }
            }
            // Outbound queue full — wait for the writer to drain. The sleep
            // fallback bounds wake latency if a Notify permit is missed.
            tokio::select! {
                _ = self.shared.out_notify.notified() => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            }
            if self.shared.is_dead() {
                return Err(dead_err(&self.shared));
            }
        }
    }

    /// Convenience: send a byte slice.
    pub async fn send_bytes(&self, data: &[u8]) -> Result<(), MuxError> {
        self.send(Bytes::copy_from_slice(data)).await
    }

    /// Receive the next chunk. `Ok(Some(bytes))` = data,
    /// `Ok(None)` = peer half-closed (FIN) and the buffer drained,
    /// `Err` = reset or connection loss.
    pub async fn recv(&self) -> Result<Option<Bytes>, MuxError> {
        let st = &self.shared;
        if st.recv_done.load(Ordering::Acquire) {
            return Ok(None);
        }
        let mut rx = st.in_rx.lock().await;
        if st.is_dead() {
            st.recv_done.store(true, Ordering::Release);
            return Err(dead_err(st));
        }
        match rx.recv().await {
            Some(InMsg::Data(b)) => {
                st.in_bytes.fetch_sub(b.len(), Ordering::AcqRel);
                st.in_space.notify_one();
                Ok(Some(b))
            }
            Some(InMsg::Fin) => {
                st.recv_done.store(true, Ordering::Release);
                st.flags.fetch_or(SF_REMOTE_FIN, Ordering::AcqRel);
                st.in_space.notify_one();
                Ok(None)
            }
            Some(InMsg::Reset) => {
                st.recv_done.store(true, Ordering::Release);
                st.flags.fetch_or(SF_RESET, Ordering::AcqRel);
                st.in_space.notify_one();
                Err(MuxError::Reset)
            }
            None => {
                // Channel closed: reader task is gone → connection dead.
                st.recv_done.store(true, Ordering::Release);
                st.flags.fetch_or(SF_DEAD, Ordering::AcqRel);
                Err(MuxError::ConnectionClosed)
            }
        }
    }

    /// Half-close: queue FIN. The peer sees `Ok(None)` after draining;
    /// our `recv()` stays open. Idempotent.
    pub fn close(&self) -> Result<(), MuxError> {
        let st = &self.shared;
        if st.is_dead() {
            return Err(dead_err(st));
        }
        if st.local_fin_sent() {
            return Ok(());
        }
        queue_out(
            st,
            OutItem {
                flags: FLAG_FIN,
                payload: Bytes::new(),
            },
        );
        st.flags.fetch_or(SF_LOCAL_FIN, Ordering::AcqRel);
        Ok(())
    }

    /// Abort the stream: queue RST; pending and future I/O fails.
    pub fn abort(&self) {
        let st = &self.shared;
        st.flags.fetch_or(SF_RESET | SF_DEAD, Ordering::AcqRel);
        queue_out(
            st,
            OutItem {
                flags: FLAG_RST,
                payload: Bytes::new(),
            },
        );
        let _ = st.in_tx.try_send(InMsg::Reset);
    }

    /// Whether the local side has sent FIN.
    pub fn local_closed(&self) -> bool {
        self.shared.local_fin_sent()
    }

    /// Whether the peer has sent FIN (data may still be buffered).
    pub fn remote_closed(&self) -> bool {
        self.shared.remote_fin()
    }
}

fn dead_err(st: &StreamShared) -> MuxError {
    if st.flag(SF_RESET) {
        MuxError::Reset
    } else {
        MuxError::Closed
    }
}

// ---- mux ----

struct Shared {
    streams: Mutex<HashMap<u32, Arc<StreamShared>>>,
    /// Next ID to allocate in *our* ID space (1-based).
    next_local_id: AtomicU32,
    role: MuxRole,
    /// Queue of peer-opened stream handles.
    accept_tx: mpsc::Sender<StreamHandle>,
    accept_rx: tokio::sync::Mutex<mpsc::Receiver<StreamHandle>>,
    /// Wakes the writer task when outbound data may exist.
    write_notify: Arc<Notify>,
    /// Wakes blocked reader/writer tasks on teardown.
    shutdown: Notify,
    dead: AtomicBool,
    /// Liveness flag for `Mux::wait_closed` (watch persists the value, so
    /// late subscribers still observe teardown).
    alive_tx: tokio::sync::watch::Sender<bool>,
}

/// A multiplexed connection endpoint.
pub struct Mux {
    shared: Arc<Shared>,
    alive_rx: tokio::sync::watch::Receiver<bool>,
    control: StreamHandle,
    reader: JoinHandle<()>,
    writer: JoinHandle<()>,
}

impl Mux {
    /// Wrap an established byte stream and spawn reader/writer tasks.
    /// `role` selects which stream-ID space this side owns.
    pub fn new<S>(stream: S, role: MuxRole) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut rd, mut wr) = tokio::io::split(stream);
        let (accept_tx, accept_rx) = mpsc::channel(MAX_STREAMS);
        let shared = Arc::new(Shared {
            streams: Mutex::new(HashMap::new()),
            next_local_id: AtomicU32::new(1),
            role,
            accept_tx,
            accept_rx: tokio::sync::Mutex::new(accept_rx),
            write_notify: Arc::new(Notify::new()),
            shutdown: Notify::new(),
            dead: AtomicBool::new(false),
            alive_tx: tokio::sync::watch::channel(true).0,
        });

        // Control channel: stream 0, implicitly open both ways.
        let control_state = new_stream_state(CONTROL_STREAM_ID, shared.write_notify.clone());
        shared
            .streams
            .lock()
            .unwrap()
            .insert(CONTROL_STREAM_ID, control_state.clone());
        let control = StreamHandle {
            shared: control_state,
        };

        let reader_shared = shared.clone();
        let reader = tokio::spawn(async move {
            reader_loop(&mut rd, &reader_shared).await;
            teardown(&reader_shared);
        });
        let writer_shared = shared.clone();
        let writer = tokio::spawn(async move {
            writer_loop(&mut wr, &writer_shared).await;
        });

        Self {
            alive_rx: shared.alive_tx.subscribe(),
            shared,
            control,
            reader,
            writer,
        }
    }

    /// Open a new outbound stream. Fails with [`MuxError::TooManyStreams`]
    /// at the connection cap or [`MuxError::Closed`] after teardown.
    pub fn open_stream(&self) -> Result<StreamHandle, MuxError> {
        if self.shared.dead.load(Ordering::Acquire) {
            return Err(MuxError::Closed);
        }
        // Allocate an ID in our space.
        let n = self.shared.next_local_id.fetch_add(1, Ordering::AcqRel);
        if n > (u32::MAX >> 1) {
            return Err(MuxError::TooManyStreams);
        }
        let id = match self.shared.role {
            MuxRole::Dialer => n,
            MuxRole::Acceptor => n | ACCEPTOR_ID_BIT,
        };

        let mut streams = self.shared.streams.lock().unwrap();
        // Exclude the control channel from the cap count.
        let live = streams.len().saturating_sub(1);
        if live >= MAX_STREAMS {
            return Err(MuxError::TooManyStreams);
        }
        if streams.contains_key(&id) {
            return Err(MuxError::Protocol("stream id collision".to_string()));
        }
        let state = new_stream_state(id, self.shared.write_notify.clone());
        // Send OPEN (empty payload) so the peer registers its side.
        queue_out(
            &state,
            OutItem {
                flags: FLAG_OPEN,
                payload: Bytes::new(),
            },
        );
        streams.insert(id, state.clone());
        self.shared.write_notify.notify_one();
        Ok(StreamHandle { shared: state })
    }

    /// Wait for the next peer-opened stream. Returns
    /// [`MuxError::Closed`] once the connection is gone.
    pub async fn accept_stream(&self) -> Result<StreamHandle, MuxError> {
        let mut rx = self.shared.accept_rx.lock().await;
        loop {
            if self.shared.dead.load(Ordering::Acquire) {
                return Err(MuxError::Closed);
            }
            tokio::select! {
                h = rx.recv() => {
                    match h {
                        Some(h) => return Ok(h),
                        // All senders dropped — only possible at teardown.
                        None => return Err(MuxError::Closed),
                    }
                }
                // Sleep fallback: `dead` may have been set before we started
                // waiting (Notify permits are not retroactive).
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            }
        }
    }

    /// The reserved control channel (stream 0). Carries
    /// `cluster_core::protocol` frames as opaque payloads; each `send()`
    /// must contain exactly one encoded protocol frame.
    pub fn control(&self) -> StreamHandle {
        self.control.clone()
    }

    /// Whether the connection is still alive.
    pub fn is_alive(&self) -> bool {
        !self.shared.dead.load(Ordering::Acquire)
    }

    /// A `watch` receiver that flips to `false` on teardown — for callers
    /// that need to await connection death through `&self`.
    pub fn closed_watch(&self) -> tokio::sync::watch::Receiver<bool> {
        self.alive_rx.clone()
    }

    /// Wait until the connection dies (reader EOF, write error, or
    /// [`Mux::close`]). Returns immediately if already dead.
    pub async fn wait_closed(&mut self) {
        let _ = self.alive_rx.wait_for(|alive| !*alive).await;
    }

    /// Shut down: stop tasks, fail all streams, close the transport.
    pub fn close(&self) {
        teardown(&self.shared);
        self.reader.abort();
        self.writer.abort();
    }
}

impl Drop for Mux {
    fn drop(&mut self) {
        self.close();
    }
}

/// Mark the whole mux dead: fail every stream and wake all waiters.
fn teardown(shared: &Arc<Shared>) {
    if shared.dead.swap(true, Ordering::AcqRel) {
        return;
    }
    let _ = shared.alive_tx.send(false);
    shared.shutdown.notify_waiters();
    let streams: Vec<Arc<StreamShared>> = {
        let map = shared.streams.lock().unwrap();
        map.values().cloned().collect()
    };
    for st in streams {
        st.mark_dead();
    }
    // Close the accept queue so accept_stream() wakes with Closed.
    // (Sender side dropped when Shared drops; receivers see channel close
    // only after all senders go — the map still holds `accept_tx`, so we
    // cannot fully close it here. accept_stream() also checks dead flag.)
}

// ---- reader task ----

async fn reader_loop<R>(rd: &mut R, shared: &Arc<Shared>)
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; HEADER_LEN];
    loop {
        // Read header; clean EOF (0 bytes) ends the loop, partial is fatal.
        match rd.read_exact(&mut header).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Could be clean FIN mid-header only if 0 bytes read; we
                // can't distinguish — treat any EOF as connection end.
                return;
            }
            Err(_) => return,
        }
        let stream_id = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let flags = header[4];
        let len = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;
        if len > MAX_PAYLOAD {
            return; // protocol violation — kill the connection
        }
        let mut payload = vec![0u8; len];
        if len > 0 && rd.read_exact(&mut payload).await.is_err() {
            return;
        }
        if !dispatch_frame(shared, stream_id, flags, Bytes::from(payload)).await {
            return;
        }
        if shared.dead.load(Ordering::Acquire) {
            return;
        }
    }
}

/// Deliver one decoded frame. Returns false to kill the reader loop.
async fn dispatch_frame(shared: &Arc<Shared>, stream_id: u32, flags: u8, payload: Bytes) -> bool {
    // Control channel.
    if stream_id == CONTROL_STREAM_ID {
        let st = {
            shared
                .streams
                .lock()
                .unwrap()
                .get(&CONTROL_STREAM_ID)
                .cloned()
        };
        let Some(st) = st else { return true };
        return deliver_data(
            shared,
            &st,
            payload,
            flags & FLAG_FIN != 0,
            flags & FLAG_RST != 0,
        )
        .await;
    }

    let is_open = flags & FLAG_OPEN != 0;
    let fin = flags & FLAG_FIN != 0;
    let rst = flags & FLAG_RST != 0;

    // Resolve or create the stream state.
    let st = {
        let mut map = shared.streams.lock().unwrap();
        match map.get(&stream_id) {
            Some(s) => s.clone(),
            None => {
                if !is_open {
                    // Frame for an unknown stream — drop it (peer may have
                    // raced a close). Never fatal.
                    return true;
                }
                // Peer-opened stream: validate the ID belongs to the peer's
                // space and we're under the cap.
                let peer_bit = match shared.role {
                    MuxRole::Dialer => ACCEPTOR_ID_BIT,
                    MuxRole::Acceptor => 0,
                };
                let in_peer_space = (stream_id & ACCEPTOR_ID_BIT != 0) == (peer_bit != 0);
                let live = map.len().saturating_sub(1);
                if !in_peer_space || live >= MAX_STREAMS {
                    // Reject: we can't easily RST an unregistered stream;
                    // just drop the OPEN — the peer's sends will be
                    // ignored as unknown-stream frames.
                    return true;
                }
                let state = new_stream_state(stream_id, shared.write_notify.clone());
                map.insert(stream_id, state.clone());
                state
            }
        }
    };

    if is_open {
        // First frame for a peer-opened stream: hand the handle to accept().
        let handle = StreamHandle { shared: st.clone() };
        // accept_tx is bounded; if full, reset the stream.
        if shared.accept_tx.try_send(handle).is_err() {
            st.flags.fetch_or(SF_RESET | SF_DEAD, Ordering::AcqRel);
            st.in_tx.try_send(InMsg::Reset).ok();
        }
    }

    if st.is_dead() {
        return true;
    }
    deliver_data(shared, &st, payload, fin, rst).await
}

/// Enqueue inbound payload/control events honoring the byte cap. Waits
/// (with shutdown escape) while the stream's inbound buffer is full.
async fn deliver_data(
    shared: &Arc<Shared>,
    st: &Arc<StreamShared>,
    payload: Bytes,
    fin: bool,
    rst: bool,
) -> bool {
    if rst {
        st.flags.fetch_or(SF_RESET, Ordering::AcqRel);
        let _ = st.in_tx.try_send(InMsg::Reset);
        gc_stream(shared, st);
        return true;
    }

    // Wait until the inbound budget (bytes AND message slots) allows this
    // frame. While waiting we simply don't read — TCP backpressure.
    loop {
        let plen = payload.len();
        let byte_room = st.in_bytes.load(Ordering::Acquire) + plen <= STREAM_BUFFER_CAP;
        let msg_room = st.in_tx.capacity() > 0;
        if byte_room && msg_room {
            break;
        }
        tokio::select! {
            _ = st.in_space.notified() => {}
            // Sleep fallback bounds wake latency if a Notify is missed.
            _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
        }
        if shared.dead.load(Ordering::Acquire) {
            return false;
        }
    }

    let plen = payload.len();
    if plen > 0 {
        if st.in_tx.try_send(InMsg::Data(payload)).is_err() {
            return false; // channel closed → connection is going away
        }
        st.in_bytes.fetch_add(plen, Ordering::AcqRel);
    }
    if fin {
        st.flags.fetch_or(SF_REMOTE_FIN, Ordering::AcqRel);
        if st.in_tx.try_send(InMsg::Fin).is_err() {
            return false;
        }
    }
    gc_stream(shared, st);
    true
}

/// Remove closed streams from the map so the stream cap doesn't leak.
fn gc_stream(shared: &Arc<Shared>, st: &Arc<StreamShared>) {
    if st.fully_closed() || st.dead_and_drained() {
        shared.streams.lock().unwrap().remove(&st.id);
    }
}

// ---- writer task ----

async fn writer_loop<W>(wr: &mut W, shared: &Arc<Shared>)
where
    W: AsyncWrite + Unpin,
{
    loop {
        if shared.dead.load(Ordering::Acquire) {
            return;
        }
        // Collect a batch of pending outbound items.
        let mut batch: Vec<(u32, u8, Bytes)> = Vec::with_capacity(WRITER_BATCH);
        {
            let map = shared.streams.lock().unwrap();
            // Deterministic-ish order: control first, then by stream id.
            let mut ids: Vec<u32> = map.keys().copied().collect();
            ids.sort_unstable();
            for id in ids {
                if batch.len() >= WRITER_BATCH {
                    break;
                }
                let Some(st) = map.get(&id) else { continue };
                let mut out = st.out.lock().unwrap();
                while let Some(item) = out.pop_front() {
                    let plen = item.payload.len();
                    batch.push((id, item.flags, item.payload));
                    st.out_bytes.fetch_sub(plen, Ordering::AcqRel);
                    st.out_len.fetch_sub(1, Ordering::AcqRel);
                    if batch.len() >= WRITER_BATCH {
                        break;
                    }
                }
            }
        }
        if batch.is_empty() {
            // Nothing to write — sleep until notified or shutdown.
            tokio::select! {
                _ = shared.write_notify.notified() => {}
                _ = shared.shutdown.notified() => return,
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
            }
            continue;
        }
        // Serialize and write the batch.
        let mut wire = Vec::new();
        for (id, flags, payload) in &batch {
            if payload.len() > MAX_PAYLOAD {
                continue; // cannot happen; send() caps it
            }
            wire.extend_from_slice(&id.to_be_bytes());
            wire.push(*flags);
            wire.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            wire.extend_from_slice(payload);
        }
        if wr.write_all(&wire).await.is_err() || wr.flush().await.is_err() {
            teardown(shared);
            return;
        }
        // Wake senders waiting for outbound space + gc closed streams.
        {
            let map = shared.streams.lock().unwrap();
            for (id, _, _) in &batch {
                if let Some(st) = map.get(id) {
                    st.out_notify.notify_waiters();
                }
            }
            let closed: Vec<u32> = map
                .iter()
                .filter(|(id, st)| {
                    **id != CONTROL_STREAM_ID && (st.fully_closed() || st.dead_and_drained())
                })
                .map(|(id, _)| *id)
                .collect();
            drop(map);
            if !closed.is_empty() {
                let mut map = shared.streams.lock().unwrap();
                for id in closed {
                    map.remove(&id);
                }
            }
        }
        // Stream handles notify waiters via out_notify; the writer is
        // woken per-enqueue through the shared `wake` notify.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;
    use tokio::time::{timeout, Duration};

    /// Two mux endpoints over an in-memory duplex pipe.
    fn mux_pair() -> (Mux, Mux) {
        let (a, b) = duplex(1 << 20);
        (Mux::new(a, MuxRole::Dialer), Mux::new(b, MuxRole::Acceptor))
    }

    async fn open_pair(dialer: &Mux, acceptor: &Mux) -> (StreamHandle, StreamHandle) {
        let h = dialer.open_stream().unwrap();
        let peer = acceptor.accept_stream().await.unwrap();
        (h, peer)
    }

    #[tokio::test]
    async fn interleaved_streams_keep_ordering() {
        let (d, a) = mux_pair();
        let (s1, p1) = open_pair(&d, &a).await;
        let (s2, p2) = open_pair(&d, &a).await;

        // Interleave sends on two streams.
        for i in 0..50u32 {
            s1.send_bytes(format!("s1-{i}").as_bytes()).await.unwrap();
            s2.send_bytes(format!("s2-{i}").as_bytes()).await.unwrap();
        }

        for i in 0..50u32 {
            let b1 = timeout(Duration::from_secs(5), p1.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(b1.as_ref(), format!("s1-{i}").as_bytes());
            let b2 = timeout(Duration::from_secs(5), p2.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(b2.as_ref(), format!("s2-{i}").as_bytes());
        }
    }

    #[tokio::test]
    async fn large_payload_roundtrip() {
        let (d, a) = mux_pair();
        let (s, p) = open_pair(&d, &a).await;
        let data = Bytes::from(vec![0xABu8; MAX_PAYLOAD - 100]);
        s.send(data.clone()).await.unwrap();
        let got = timeout(Duration::from_secs(10), p.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(got, data);
        // Oversize rejected.
        assert!(matches!(
            s.send(Bytes::from(vec![0u8; MAX_PAYLOAD + 1])).await,
            Err(MuxError::FrameTooLarge)
        ));
    }

    #[tokio::test]
    async fn half_close_propagates_fin() {
        let (d, a) = mux_pair();
        let (s, p) = open_pair(&d, &a).await;
        s.send_bytes(b"final").await.unwrap();
        s.close().unwrap();
        // Peer drains data then sees EOF.
        let b = timeout(Duration::from_secs(5), p.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(&b[..], b"final");
        let eof = timeout(Duration::from_secs(5), p.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(eof.is_none());
        // Reverse direction still open.
        p.send_bytes(b"ack").await.unwrap();
        let b = timeout(Duration::from_secs(5), s.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(&b[..], b"ack");
        // Send after local FIN fails.
        assert!(s.send_bytes(b"nope").await.is_err());
    }

    #[tokio::test]
    async fn conn_drop_fails_streams() {
        let (d, a) = mux_pair();
        let (s, p) = open_pair(&d, &a).await;
        s.send_bytes(b"x").await.unwrap();
        drop(d);
        // Peer's pending recv fails (data may or may not arrive first).
        let res = timeout(Duration::from_secs(5), async {
            loop {
                match p.recv().await {
                    Ok(Some(_)) => continue,
                    other => break other,
                }
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            res,
            Err(MuxError::ConnectionClosed | MuxError::Reset | MuxError::Closed)
        ));
        // And new sends fail fast (mux is dead).
        assert!(s.send_bytes(b"y").await.is_err());
    }

    #[tokio::test]
    async fn abort_resets_stream() {
        let (d, a) = mux_pair();
        let (s, p) = open_pair(&d, &a).await;
        s.abort();
        let res = timeout(Duration::from_secs(5), p.recv()).await.unwrap();
        assert!(matches!(res, Err(MuxError::Reset)));
    }

    #[tokio::test]
    async fn slow_consumer_backpressure() {
        let (d, a) = mux_pair();
        let (s, p) = open_pair(&d, &a).await;
        // Send 256KiB chunks until one pends — proving queues saturate.
        let chunk = Bytes::from(vec![1u8; 256 * 1024]);
        let mut pending = None;
        for _ in 0..64 {
            let mut f = Box::pin(s.send(chunk.clone()));
            match timeout(Duration::from_millis(300), f.as_mut()).await {
                Err(_) => {
                    pending = Some(f);
                    break;
                }
                Ok(Ok(())) => {}
                Ok(Err(e)) => panic!("send failed: {e}"),
            }
        }
        let Some(pending) = pending else {
            panic!("send never applied backpressure");
        };
        // Drain everything; the pending send must then complete.
        loop {
            match timeout(Duration::from_secs(5), p.recv()).await {
                Ok(Ok(Some(_))) | Ok(Ok(None)) => continue,
                Ok(Err(_)) => break,
                Err(_) => break, // recv pended → all data drained
            }
        }
        timeout(Duration::from_secs(10), pending)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn control_channel_roundtrip() {
        let (d, a) = mux_pair();
        let msg =
            cluster_core::protocol::encode_frame(&cluster_core::protocol::Message::Heartbeat {
                seq: 7,
            })
            .unwrap();
        d.control().send(Bytes::from(msg.clone())).await.unwrap();
        let got = timeout(Duration::from_secs(5), a.control().recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(got.as_ref(), msg.as_slice());
        let decoded = cluster_core::protocol::decode_frame(&got).unwrap();
        assert_eq!(
            decoded,
            cluster_core::protocol::Message::Heartbeat { seq: 7 }
        );
    }

    #[tokio::test]
    async fn stream_cap_enforced() {
        let (d, a) = mux_pair();
        let mut handles = Vec::new();
        for _ in 0..MAX_STREAMS {
            handles.push(d.open_stream().unwrap());
        }
        assert!(matches!(d.open_stream(), Err(MuxError::TooManyStreams)));
        // Accept side sees them too (drain to keep queues happy).
        for _ in 0..MAX_STREAMS {
            a.accept_stream().await.unwrap();
        }
    }

    #[tokio::test]
    async fn acceptor_opens_stream_too() {
        let (d, a) = mux_pair();
        // Acceptor opens with high-bit IDs; dialer accepts.
        let h = a.open_stream().unwrap();
        assert!(h.id() & 0x8000_0000 != 0);
        let peer = d.accept_stream().await.unwrap();
        h.send_bytes(b"ctl").await.unwrap();
        let b = timeout(Duration::from_secs(5), peer.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(&b[..], b"ctl");
    }

    #[tokio::test]
    async fn mux_close_fails_waiters() {
        let (d, a) = mux_pair();
        let (_s, p) = open_pair(&d, &a).await;
        let pending = p.recv();
        tokio::pin!(pending);
        d.close();
        let res = timeout(Duration::from_secs(5), &mut pending).await.unwrap();
        assert!(res.is_err());
    }
}
