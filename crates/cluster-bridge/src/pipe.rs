//! Shared bidirectional byte pump: local unix socket ↔ mux [`StreamHandle`].
//!
//! Used by both bridge halves — the controller pipes `ApiClient` socket ↔
//! worker stream, the worker pipes cluster stream ↔ local herdr API socket.
//! The two directions are symmetric, so one function serves both.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::watch;

use cluster_net::mux::{MuxError, StreamHandle};

/// Read chunk size. 64 KiB keeps each mux frame well under
/// `mux::MAX_PAYLOAD` (1 MiB) while amortizing frame overhead.
const BUF_SIZE: usize = 64 * 1024;

/// Shared last-activity clock for both pipe directions. The idle timeout
/// applies to the *whole pipe*, not one direction: bytes moving
/// socket→stream or stream→socket reset the deadline, so a long one-way
/// upload (or download) is not reaped while it is still making progress.
///
/// `last_ms` carries the deadline math; `tick` wakes both directions so a
/// pending idle sleep is re-armed with the extended deadline instead of
/// firing mid-transfer.
struct PipeActivity {
    /// Reference epoch for `last_ms` timestamps.
    start: Instant,
    /// Milliseconds since `start` at the last observed byte movement.
    last_ms: AtomicU64,
    /// Bumped (with the new timestamp) on every [`Self::touch`].
    tick: watch::Sender<u64>,
}

impl PipeActivity {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            last_ms: AtomicU64::new(0),
            tick: watch::channel(0).0,
        }
    }

    /// Record that bytes moved through the pipe (seen on either side).
    fn touch(&self) {
        let ms = self.start.elapsed().as_millis() as u64;
        self.last_ms.store(ms, Ordering::Release);
        // Receivers only need the wakeup; the timestamp itself is read
        // back via `remaining()`. Send can only fail with no receivers,
        // which cannot happen while the pipe is running.
        let _ = self.tick.send(ms);
    }

    /// Subscribe to activity notifications for one pipe direction.
    fn subscribe(&self) -> watch::Receiver<u64> {
        self.tick.subscribe()
    }

    /// Time left before the pipe counts as idle; zero means already idle.
    fn remaining(&self, idle: Duration) -> Duration {
        let now = self.start.elapsed();
        let last = Duration::from_millis(self.last_ms.load(Ordering::Acquire));
        idle.saturating_sub(now.saturating_sub(last))
    }
}

/// Wait for an outbound write (`send()` / `write_all()`) under the shared
/// idle deadline. The write future is polled to completion while:
///
/// - activity in *either* pipe direction re-arms the deadline (same rule
///   as the read side — a legitimately slow drain keeps the pipe alive),
/// - `idle` elapses with no bytes moving anywhere → `Err(())`, and the
///   caller takes the same abort path as a stalled reader.
///
/// `fut` must be cancel-safe to re-poll: it is the *same* pinned future
/// across wakeups, so a partially-written `write_all` resumes rather
/// than restarting, and `StreamHandle::send` only mutates state once it
/// returns `Ok`.
async fn await_outbound<F, T>(
    activity: &PipeActivity,
    act_rx: &mut watch::Receiver<u64>,
    idle: Duration,
    fut: &mut std::pin::Pin<&mut F>,
) -> Result<T, ()>
where
    F: std::future::Future<Output = T>,
{
    loop {
        tokio::select! {
            r = fut.as_mut() => return Ok(r),
            r = act_rx.changed() => match r {
                // Opposite direction moved bytes: re-arm the deadline.
                Ok(()) => continue,
                // Sender drop is unreachable while a direction task holds
                // an `Arc` clone; treat it as idle like the read side.
                Err(_) => return Err(()),
            },
            _ = tokio::time::sleep(activity.remaining(idle)) => return Err(()),
        }
    }
}

/// Pipe bytes both ways between `sock` and `stream` until both directions
/// finish, an error occurs, or `idle` elapses with no bytes moving in
/// either direction.
///
/// # Close semantics
///
/// - Local socket read returns 0 (peer EOF / half-close) →
///   `stream.close()` queues a mux `FIN`; the stream→socket direction keeps
///   draining so a late response still reaches the local peer.
/// - `stream.recv()` returns `Ok(None)` (remote `FIN`) → the socket write
///   half is shut down so the local peer sees EOF after draining; the
///   socket→stream direction stays open until the local peer closes.
/// - Socket/stream error or idle timeout → `stream.abort()` (mux `RST`) and
///   a best-effort socket write shutdown, so the surviving side always sees
///   a clean EOF rather than a hang.
///
/// Outbound `send()`/`write_all()` calls are bounded by the *same* shared
/// [`PipeActivity`] deadline as the reads ([`await_outbound`]): a peer
/// that stops draining can no longer pin the task forever. Activity in
/// either direction re-arms the timer, so backpressure on a large
/// transfer is not mistaken for abandonment — only a fully silent pipe
/// for `idle` trips the abort path.
pub(crate) async fn pipe_unix_stream(sock: UnixStream, stream: StreamHandle, idle: Duration) {
    let (mut sock_rd, mut sock_wr) = tokio::io::split(sock);
    let send_end = stream.clone();
    let activity = Arc::new(PipeActivity::new());

    // Local socket -> cluster stream.
    let to_stream_activity = activity.clone();
    let to_stream = tokio::spawn(async move {
        /// Outcome of one select round: inbound bytes, an activity wakeup
        /// from the opposite direction, or shared-idle expiry.
        enum Step {
            Read(std::io::Result<usize>),
            Wake,
            Idle,
        }
        let mut act_rx = to_stream_activity.subscribe();
        let mut buf = vec![0u8; BUF_SIZE];
        loop {
            let step = tokio::select! {
                r = sock_rd.read(&mut buf) => Step::Read(r),
                // Sender drop is unreachable while this task holds an
                // `Arc` clone; treat it as idle rather than spin on Wake.
                r = act_rx.changed() => match r {
                    Ok(()) => Step::Wake,
                    Err(_) => Step::Idle,
                },
                _ = tokio::time::sleep(to_stream_activity.remaining(idle)) => Step::Idle,
            };
            let n = match step {
                // Opposite direction moved bytes: re-arm with the
                // extended deadline.
                Step::Wake => continue,
                // Pipe silent for `idle` in both directions: abandon.
                Step::Idle => {
                    send_end.abort();
                    break;
                }
                // Local EOF: half-close upstream, keep draining downstream.
                Step::Read(Ok(0)) => break,
                Step::Read(Ok(n)) => {
                    to_stream_activity.touch();
                    n
                }
                // Read error: abandon the stream.
                Step::Read(Err(_)) => {
                    send_end.abort();
                    break;
                }
            };
            // Bounded by the shared idle deadline: a worker that stops
            // draining the mux stream trips the same abort path as a
            // stalled reader instead of pinning this task forever.
            let mut send_fut = std::pin::pin!(send_end.send(Bytes::copy_from_slice(&buf[..n])));
            match await_outbound(&to_stream_activity, &mut act_rx, idle, &mut send_fut).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => break,
                Err(()) => {
                    send_end.abort();
                    break;
                }
            }
        }
        // Idempotent: no-op if we already aborted or FIN was sent.
        let _ = send_end.close();
    });

    // Cluster stream -> local socket.
    let to_socket = tokio::spawn(async move {
        enum Step {
            Recv(Result<Option<Bytes>, MuxError>),
            Wake,
            Idle,
        }
        let mut act_rx = activity.subscribe();
        loop {
            let step = tokio::select! {
                r = stream.recv() => Step::Recv(r),
                r = act_rx.changed() => match r {
                    Ok(()) => Step::Wake,
                    Err(_) => Step::Idle,
                },
                _ = tokio::time::sleep(activity.remaining(idle)) => Step::Idle,
            };
            match step {
                // Opposite direction moved bytes: re-arm the deadline.
                Step::Wake => {}
                // Pipe idle: nobody sent in *either* direction for `idle`
                // — reap the abandoned stream.
                Step::Idle => {
                    stream.abort();
                    let _ = sock_wr.shutdown().await;
                    break;
                }
                Step::Recv(Ok(Some(chunk))) => {
                    activity.touch();
                    // Same shared idle deadline as the read side: a local
                    // peer that stops reading the socket must not pin the
                    // pipe forever. The pinned future resumes partial
                    // writes, so re-polling on activity wakeups is safe.
                    let mut wr_fut = std::pin::pin!(sock_wr.write_all(&chunk));
                    match await_outbound(&activity, &mut act_rx, idle, &mut wr_fut).await {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => break,
                        Err(()) => {
                            stream.abort();
                            let _ = sock_wr.shutdown().await;
                            break;
                        }
                    }
                }
                // Remote FIN: propagate EOF to the local peer's reader.
                // Stream dead (RST/conn loss): surface EOF locally.
                Step::Recv(Ok(None)) | Step::Recv(Err(_)) => {
                    let _ = sock_wr.shutdown().await;
                    break;
                }
            }
        }
    });

    // Wait for both directions. Each loop iteration races its inbound
    // read/recv against the shared idle deadline (re-armed on every
    // activity wakeup), so the join completes once the whole pipe has been
    // silent for `idle` (or either side closes).
    let _ = tokio::join!(to_stream, to_socket);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cluster_net::mux::{Mux, MuxRole};
    use tokio::io::duplex;
    use tokio::net::UnixListener;
    use tokio::time::timeout;

    /// Regression test for #25: a one-way upload lasting longer than `idle`
    /// must not be reaped. Socket→stream activity keeps the whole pipe
    /// alive even while the stream→socket direction is completely silent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_way_upload_outlives_idle_timeout() {
        let idle = Duration::from_millis(150);

        // Real unix socket pair standing in for the local API socket.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("api.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let mut client = UnixStream::connect(&path).await.unwrap();
        let (sock, _) = listener.accept().await.unwrap();

        // Mux pair over an in-memory duplex; the peer end only receives.
        let (a, b) = duplex(1 << 20);
        let dialer = Mux::new(a, MuxRole::Dialer);
        let acceptor = Mux::new(b, MuxRole::Acceptor);
        let local = dialer.open_stream().unwrap();
        let peer = acceptor.accept_stream().await.unwrap();

        let pipe = tokio::spawn(pipe_unix_stream(sock, local, idle));

        // Upload for ~4x `idle` while downstream stays silent. Chunk
        // boundaries are not guaranteed, so verify the byte stream.
        const CHUNKS: usize = 20;
        const CHUNK_LEN: usize = 1024;
        let mut expected = Vec::with_capacity(CHUNKS * CHUNK_LEN);
        for i in 0..CHUNKS {
            let payload = vec![(i % 256) as u8; CHUNK_LEN];
            expected.extend_from_slice(&payload);
            client.write_all(&payload).await.unwrap();
            tokio::time::sleep(Duration::from_millis(40)).await;
        }

        let mut received = Vec::new();
        timeout(Duration::from_secs(10), async {
            while received.len() < expected.len() {
                let chunk = peer.recv().await.unwrap().expect("stream reset");
                received.extend_from_slice(&chunk);
            }
        })
        .await
        .expect("upload did not complete — pipe was reaped mid-transfer");
        assert_eq!(received, expected);

        // Local EOF must still propagate as mux FIN (half-close preserved).
        drop(client);
        let fin = timeout(Duration::from_secs(5), peer.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(fin.is_none());

        // Pipe finishes once both directions close/idle out.
        timeout(Duration::from_secs(5), pipe)
            .await
            .unwrap()
            .unwrap();
    }

    /// Regression test for #69: when *both* directions' outbound writes
    /// stall — the remote peer stops draining the mux stream and the
    /// local client stops reading the socket — the shared idle deadline
    /// must still reap the pipe. Previously the unbounded
    /// `send()`/`write_all()` awaits pinned both tasks forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_outbound_writes_are_reaped_by_idle_timeout() {
        let idle = Duration::from_millis(200);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("api.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let client = UnixStream::connect(&path).await.unwrap();
        let (sock, _) = listener.accept().await.unwrap();

        let (a, b) = duplex(1 << 20);
        let dialer = Mux::new(a, MuxRole::Dialer);
        let acceptor = Mux::new(b, MuxRole::Acceptor);
        let local = dialer.open_stream().unwrap();
        let peer = acceptor.accept_stream().await.unwrap();

        let pipe = tokio::spawn(pipe_unix_stream(sock, local, idle));

        // Remote peer floods the stream and never reads our side: once
        // the pipe's inbound window and the socket buffer fill, the
        // stream→socket write_all stalls.
        let flood_peer = peer.clone();
        let flood = tokio::spawn(async move {
            let chunk = Bytes::from(vec![0xCDu8; 64 * 1024]);
            while flood_peer.send(chunk.clone()).await.is_ok() {}
        });

        // Local client streams data and never reads: once the peer's
        // inbound window fills, the socket→stream send stalls. Both
        // directions end up blocked inside outbound writes.
        let upload = tokio::spawn(async move {
            let mut client = client;
            let chunk = vec![0xEFu8; 64 * 1024];
            while client.write_all(&chunk).await.is_ok() {}
        });

        // The pipe must die on the shared idle deadline, not hang.
        timeout(Duration::from_secs(10), pipe)
            .await
            .expect("pipe pinned by stalled outbound writes")
            .unwrap();

        // Remote side observes the abort (RST) after draining buffered
        // data, not a hang.
        let outcome = timeout(Duration::from_secs(10), async {
            loop {
                match peer.recv().await {
                    Ok(Some(_)) => continue,
                    other => break other,
                }
            }
        })
        .await
        .expect("peer did not observe stream abort");
        assert!(
            matches!(outcome, Ok(None) | Err(_)),
            "expected FIN/RST after idle abort, got {outcome:?}"
        );

        flood.abort();
        upload.abort();
    }
}
