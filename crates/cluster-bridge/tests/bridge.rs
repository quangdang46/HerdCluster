//! Loopback integration tests for cluster-bridge.
//!
//! Topology under test (all in one process, real sockets):
//!
//! ```text
//! ApiClient ──► ControllerBridge.sock ──mux──► WorkerBridge ──► fake herdr.sock
//!    (controller side)                     (worker side)
//! ```
//!
//! The cluster transport is a real `cluster_net` TLS loopback
//! (`Controller::bind` + `WorkerClient::connect_once`), identical to
//! cluster-net's own conn tests. The "herdr API" is a fake NDJSON unix
//! socket that answers one `{"id","result"}` line per connection and closes,
//! matching the verified herdr 0.9.5 contract.

#![cfg(unix)]

use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use api_client::{ApiClient, ApiError};
use cluster_bridge::controller::{ControllerBridge, DEFAULT_IDLE_TIMEOUT};
use cluster_bridge::worker::WorkerBridge;
use cluster_core::protocol::Resources;
use cluster_net::conn::{
    save_worker_credential, Controller, ControllerEvent, WorkerClient, WorkerConfig,
    WorkerCredential,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::time::timeout;

// ---------- fixtures ----------

fn tempdir() -> TempDir {
    tempfile::tempdir().unwrap()
}

fn worker_cfg() -> WorkerConfig {
    WorkerConfig {
        hostname: "test-worker".to_string(),
        caps: vec!["claude".to_string()],
        resources: Resources {
            cpu_cores: 8,
            mem_total_mb: 32768,
            mem_available_mb: 16384,
            agent_count: 0,
            max_agents: 4,
            load_avg: vec![],
            kinds: vec!["claude".to_string()],
        },
    }
}

/// Real TLS loopback: controller listener + paired worker client, mirroring
/// `cluster_net::conn` tests. Returns (controller, worker client, addr, dirs).
async fn loopback_pair() -> (Arc<Controller>, WorkerClient, SocketAddr, TempDir, TempDir) {
    let ctrl_dir = tempdir();
    let work_dir = tempdir();

    let ctrl = Arc::new(
        Controller::bind(ctrl_dir.path(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let addr = ctrl.local_addr();
    let c = ctrl.clone();
    tokio::spawn(async move { c.accept_loop().await });

    let material = ctrl.tls_material();
    let cred = WorkerCredential {
        controller_pubkey: cluster_core::protocol::pubkey_to_hex(&ctrl.cluster_pubkey()),
        ca_fingerprint: material.ca_fingerprint(),
        ca_cert_pem: material.ca_cert_pem.clone(),
    };
    save_worker_credential(work_dir.path(), &cred).unwrap();

    let client = WorkerClient::load(work_dir.path(), worker_cfg()).unwrap();
    (ctrl, client, addr, ctrl_dir, work_dir)
}

/// Connect the worker, wait for controller registration, and return
/// (worker Session, controller Session for that worker).
async fn paired_sessions(
    ctrl: &Arc<Controller>,
    client: &WorkerClient,
    addr: SocketAddr,
) -> (
    Arc<cluster_net::conn::Session>,
    Arc<cluster_net::conn::Session>,
) {
    let worker_session = client.connect_once(addr).await.unwrap();
    let worker_pubkey = client.public_key();

    let event = timeout(Duration::from_secs(10), ctrl.next_event())
        .await
        .expect("timed out waiting for WorkerConnected");
    match event {
        ControllerEvent::WorkerConnected(ws) => {
            assert_eq!(ws.worker_pubkey, worker_pubkey);
        }
        other => panic!("expected WorkerConnected, got {other:?}"),
    }

    // The registry insert happens before the event is emitted, but poll
    // briefly to be safe.
    let mut c_session = None;
    for _ in 0..100 {
        if let Some(s) = ctrl.session(&worker_pubkey) {
            c_session = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let c_session = c_session.expect("controller session registered");
    (Arc::new(worker_session), c_session)
}

/// Fake herdr API server: one `{"id","result"}` line per connection, then
/// close — the verified herdr 0.9.5 contract. Returns the socket path.
async fn spawn_fake_herdr(dir: &Path) -> PathBuf {
    let path = dir.join("herdr.sock");
    let listener = UnixListener::bind(&path).unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let (rd, mut wr) = sock.into_split();
                let mut lines = BufReader::new(rd).lines();
                // Read the single request line.
                let req = match timeout(Duration::from_secs(10), lines.next_line()).await {
                    Ok(Ok(Some(l))) => l,
                    _ => return,
                };
                let parsed: Value = serde_json::from_str(&req).unwrap_or(json!({}));
                let resp = json!({
                    "id": parsed.get("id").cloned().unwrap_or(Value::Null),
                    "result": {
                        "ok": true,
                        "method": parsed.get("method").cloned().unwrap_or(Value::Null),
                    },
                });
                let mut line = serde_json::to_vec(&resp).unwrap();
                line.push(b'\n');
                let _ = wr.write_all(&line).await;
                let _ = wr.shutdown().await;
                // Drop → close, like the real server.
            });
        }
    });
    path
}

/// Full bridge stack: loopback pair + ControllerBridge + WorkerBridge.
/// Returns everything the tests need to keep alive.
struct Stack {
    _ctrl_dir: TempDir,
    _work_dir: TempDir,
    _state_dir: TempDir,
    _herdr_dir: TempDir,
    c_session: Arc<cluster_net::conn::Session>,
    w_session: Arc<cluster_net::conn::Session>,
    bridge: ControllerBridge,
    worker_bridge: WorkerBridge,
}

async fn full_stack(idle: Duration) -> Stack {
    full_stack_with(idle, None).await
}

async fn full_stack_with(idle: Duration, herdr_override: Option<PathBuf>) -> Stack {
    let (ctrl, client, addr, ctrl_dir, work_dir) = loopback_pair().await;
    let (w_session, c_session) = paired_sessions(&ctrl, &client, addr).await;

    let state_dir = tempdir();
    let herdr_dir = tempdir();
    let herdr_path = match herdr_override {
        Some(p) => p,
        None => spawn_fake_herdr(herdr_dir.path()).await,
    };

    let bridge =
        ControllerBridge::with_idle_timeout(state_dir.path(), "alpha", c_session.clone(), idle)
            .await
            .unwrap();
    let worker_bridge = WorkerBridge::with_options(
        w_session.clone(),
        herdr_path.clone(),
        idle,
        Duration::from_secs(5),
    );

    Stack {
        _ctrl_dir: ctrl_dir,
        _work_dir: work_dir,
        _state_dir: state_dir,
        _herdr_dir: herdr_dir,
        c_session,
        w_session,
        bridge,
        worker_bridge,
    }
}

fn blocking_request(path: &Path, method: &str) -> Result<Value, ApiError> {
    let client = ApiClient::connect(path).unwrap();
    client.request(method, &json!({}))
}

// ---------- tests ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn api_client_roundtrip_through_bridge() {
    let stack = full_stack(DEFAULT_IDLE_TIMEOUT).await;

    let sock = stack.bridge.socket_path().to_path_buf();
    let result = tokio::task::spawn_blocking(move || blocking_request(&sock, "ping"))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result["ok"], true);
    assert_eq!(result["method"], "ping");

    stack.bridge.shutdown();
    stack.worker_bridge.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_no_crosstalk() {
    let stack = full_stack(DEFAULT_IDLE_TIMEOUT).await;
    let sock = stack.bridge.socket_path().to_path_buf();

    let mut handles = Vec::new();
    for i in 0..3 {
        let p = sock.clone();
        handles.push(tokio::task::spawn_blocking(move || {
            blocking_request(&p, &format!("method-{i}"))
        }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        let result = h.await.unwrap().unwrap();
        assert_eq!(result["ok"], true);
        assert_eq!(result["method"], format!("method-{i}"));
    }

    stack.bridge.shutdown();
    stack.worker_bridge.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_unreachable_is_typed_error_not_hang() {
    let stack = full_stack(DEFAULT_IDLE_TIMEOUT).await;
    let sock = stack.bridge.socket_path().to_path_buf();

    // Kill the cluster session: open_stream must fail and the bridge must
    // answer with a typed NDJSON error envelope.
    stack.c_session.close();
    stack.w_session.close();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let err = tokio::task::spawn_blocking(move || blocking_request(&sock, "ping"))
        .await
        .unwrap()
        .unwrap_err();
    match err {
        ApiError::Server { code, .. } => assert_eq!(code, "worker_unreachable"),
        other => panic!("expected typed worker_unreachable, got {other:?}"),
    }

    stack.bridge.shutdown();
    stack.worker_bridge.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_herdr_socket_is_typed_error() {
    // Point the worker bridge at a socket path that does not exist.
    let missing = tempdir().path().join("no-such-herdr.sock").to_path_buf();
    let stack = full_stack_with(DEFAULT_IDLE_TIMEOUT, Some(missing)).await;
    let sock = stack.bridge.socket_path().to_path_buf();

    let err = tokio::task::spawn_blocking(move || blocking_request(&sock, "ping"))
        .await
        .unwrap()
        .unwrap_err();
    match err {
        ApiError::Server { code, .. } => assert_eq!(code, "herdr_unavailable"),
        other => panic!("expected typed herdr_unavailable, got {other:?}"),
    }

    stack.bridge.shutdown();
    stack.worker_bridge.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn socket_cleanup_and_stale_sweep() {
    let (ctrl, client, addr, ctrl_dir, work_dir) = loopback_pair().await;
    let (w_session, c_session) = paired_sessions(&ctrl, &client, addr).await;
    let state_dir = tempdir();
    let sock_path = state_dir.path().join("bridge").join("alpha.sock");

    // Bridge dir is created 0700, socket 0600.
    let bridge = ControllerBridge::new(state_dir.path(), "alpha", c_session.clone())
        .await
        .unwrap();
    assert_eq!(
        std::fs::metadata(state_dir.path().join("bridge"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&sock_path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    // A second bridge on the same live socket must refuse to steal it.
    let dup = ControllerBridge::new(state_dir.path(), "alpha", c_session.clone()).await;
    assert!(matches!(
        dup,
        Err(cluster_bridge::BridgeError::SocketInUse(_))
    ));

    // Shutdown removes the socket file.
    bridge.shutdown();
    assert!(!sock_path.exists());

    // Simulate a crash: a stale socket file (listener bound then dropped)
    // must be swept, not block rebinding.
    {
        let _l = tokio::net::UnixListener::bind(&sock_path).unwrap();
        // _l dropped at scope end → file remains, connect refused.
    }
    assert!(sock_path.exists());
    let bridge2 = ControllerBridge::new(state_dir.path(), "alpha", c_session.clone())
        .await
        .unwrap();
    assert!(sock_path.exists());
    // And it actually works. Keep the fake-herdr tempdir alive for the
    // duration of the request.
    let herdr_dir2 = tempdir();
    let herdr_path2 = spawn_fake_herdr(herdr_dir2.path()).await;
    let p = bridge2.socket_path().to_path_buf();
    let worker_bridge = WorkerBridge::with_options(
        w_session.clone(),
        herdr_path2,
        DEFAULT_IDLE_TIMEOUT,
        Duration::from_secs(5),
    );
    let result = tokio::task::spawn_blocking(move || blocking_request(&p, "ping"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result["ok"], true);

    bridge2.shutdown();
    worker_bridge.shutdown();
    drop((ctrl_dir, work_dir, herdr_dir2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_timeout_reaps_silent_connection() {
    let idle = Duration::from_millis(150);
    let stack = full_stack(idle).await;

    // Raw client: connect, send nothing. The pipe's idle timeout must kill
    // the stream and close the socket — the read must see EOF, not hang.
    let sock = stack.bridge.socket_path().to_path_buf();
    let (mut rd, mut wr) = tokio::net::UnixStream::connect(&sock)
        .await
        .unwrap()
        .into_split();
    let _ = &mut wr;
    let read = timeout(Duration::from_secs(5), async {
        let mut buf = [0u8; 16];
        rd.read(&mut buf).await
    })
    .await
    .expect("idle connection was not reaped");
    match read {
        Ok(0) | Err(_) => {} // clean EOF or reset — both fine
        other => panic!("expected EOF, got {other:?}"),
    }

    stack.bridge.shutdown();
    stack.worker_bridge.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_half_close_still_gets_response() {
    let stack = full_stack(DEFAULT_IDLE_TIMEOUT).await;
    let sock = stack.bridge.socket_path().to_path_buf();

    // std (blocking) client in spawn_blocking: send request, shutdown write
    // side (FIN), then read the response + EOF.
    let response = tokio::task::spawn_blocking(move || {
        use std::io::{BufRead, BufReader, Write};
        use std::net::Shutdown;
        use std::os::unix::net::UnixStream;

        let stream = UnixStream::connect(&sock).unwrap();
        let mut w = stream.try_clone().unwrap();
        w.write_all(b"{\"id\":\"hc-1\",\"method\":\"ping\",\"params\":{}}\n")
            .unwrap();
        w.flush().unwrap();
        // Half-close: we will send no more, but still read the response.
        stream.shutdown(Shutdown::Write).unwrap();

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        (line, reader.read_line(&mut String::new()).unwrap_or(0))
    })
    .await
    .unwrap();

    let parsed: Value = serde_json::from_str(response.0.trim()).unwrap();
    assert_eq!(parsed["result"]["ok"], true);
    assert_eq!(parsed["result"]["method"], "ping");
    // Second read hits EOF (0 bytes) — server closed after one response.
    assert_eq!(response.1, 0);

    stack.bridge.shutdown();
    stack.worker_bridge.shutdown();
}
