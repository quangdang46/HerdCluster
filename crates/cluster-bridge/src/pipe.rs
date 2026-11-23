//! Shared bidirectional byte pump: local unix socket ↔ mux [`StreamHandle`].
//!
//! Used by both bridge halves — the controller pipes `ApiClient` socket ↔
//! worker stream, the worker pipes cluster stream ↔ local herdr API socket.
//! The two directions are symmetric, so one function serves both.

use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::time::timeout;

use cluster_net::mux::StreamHandle;

/// Read chunk size. 64 KiB keeps each mux frame well under
/// `mux::MAX_PAYLOAD` (1 MiB) while amortizing frame overhead.
const BUF_SIZE: usize = 64 * 1024;

/// Pipe bytes both ways between `sock` and `stream` until both directions
/// finish, an error occurs, or `idle` elapses with no inbound bytes.
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
/// Outbound `send()`/`write_all()` calls are deliberately *not* wrapped in
/// the idle timeout: a blocked write is legitimate TCP/mux backpressure,
/// not abandonment. Inbound waits are what the idle timeout measures.
pub(crate) async fn pipe_unix_stream(sock: UnixStream, stream: StreamHandle, idle: Duration) {
    let (mut sock_rd, mut sock_wr) = tokio::io::split(sock);
    let send_end = stream.clone();

    // Local socket -> cluster stream.
    let to_stream = tokio::spawn(async move {
        let mut buf = vec![0u8; BUF_SIZE];
        loop {
            let n = match timeout(idle, sock_rd.read(&mut buf)).await {
                // Local EOF: half-close upstream, keep draining downstream.
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => n,
                // Read error or idle timeout: abandon the stream.
                Ok(Err(_)) | Err(_) => {
                    send_end.abort();
                    break;
                }
            };
            if send_end
                .send(Bytes::copy_from_slice(&buf[..n]))
                .await
                .is_err()
            {
                break;
            }
        }
        // Idempotent: no-op if we already aborted or FIN was sent.
        let _ = send_end.close();
    });

    // Cluster stream -> local socket.
    let to_socket = tokio::spawn(async move {
        loop {
            match timeout(idle, stream.recv()).await {
                Ok(Ok(Some(chunk))) => {
                    if sock_wr.write_all(&chunk).await.is_err() {
                        break;
                    }
                }
                // Remote FIN: propagate EOF to the local peer's reader.
                Ok(Ok(None)) => {
                    let _ = sock_wr.shutdown().await;
                    break;
                }
                // Stream dead (RST/conn loss): surface EOF locally.
                Ok(Err(_)) => {
                    let _ = sock_wr.shutdown().await;
                    break;
                }
                // Idle: nobody is sending — reap the abandoned stream.
                Err(_) => {
                    stream.abort();
                    let _ = sock_wr.shutdown().await;
                    break;
                }
            }
        }
    });

    // Wait for both directions. The slower one is bounded by `idle` because
    // every loop iteration awaits an inbound read/recv under the timeout.
    let _ = tokio::join!(to_stream, to_socket);
}
