//! Integration tests for `api-client` using a fake NDJSON unix-socket
//! server. All tests are `#[cfg(unix)]`-gated implicitly by only running
//! where unix sockets exist (the crate's transport is unix-only for now).

#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use api_client::methods::{AgentStartParams, PaneSplitParams, SplitDirection};
use api_client::{ApiClient, ApiError};
use serde_json::{json, Value};

/// Spawn a fake herdr API server on a unix socket in a tempdir.
///
/// `handler` is invoked once per accepted connection with the raw request
/// line; it returns the raw response line to write back. The listener
/// accepts connections until `shutdown` is signalled or the returned
/// receiver yields `()`. Received request lines are forwarded over the
/// returned channel so tests can assert on the wire bytes.
struct FakeServer {
    socket_path: PathBuf,
    requests: mpsc::Receiver<String>,
    shutdown: mpsc::Sender<()>,
    _dir: tempfile::TempDir,
}

impl FakeServer {
    /// Start a server that replies to every request with `response_for`,
    /// which maps the parsed request `Value` to a response `Value` written
    /// as `{"id": <req id>, ...response_for(req)}`.
    fn start(response_for: impl Fn(&Value) -> Value + Send + Sync + 'static) -> FakeServer {
        Self::start_raw(move |req_line| {
            let req: Value = serde_json::from_str(req_line).unwrap();
            let id = req.get("id").cloned().unwrap_or(Value::Null);
            let mut resp = response_for(&req);
            // Envelope: caller returns either {"result": ...} or {"error": ...}
            if let Some(obj) = resp.as_object_mut() {
                obj.insert("id".to_string(), id);
            }
            serde_json::to_string(&resp).unwrap()
        })
    }

    /// Start a server with full control over the raw response bytes.
    /// `handler` receives the request line and returns the exact bytes to
    /// write back (including trailing newline if desired).
    fn start_raw(handler: impl Fn(&str) -> String + Send + Sync + 'static) -> FakeServer {
        let handler = Arc::new(handler);
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        listener.set_nonblocking(true).unwrap();

        let (req_tx, req_rx) = mpsc::channel::<String>();
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>();

        thread::spawn(move || {
            // Accept loop: one connection per request (mirrors real herdr).
            loop {
                if shutdown_rx.try_recv().is_ok() {
                    break;
                }
                match listener.accept() {
                    Ok((stream, _)) => {
                        let req_tx = req_tx.clone();
                        let handler = Arc::clone(&handler);
                        thread::spawn(move || {
                            let mut reader = BufReader::new(stream.try_clone().unwrap());
                            let mut writer = stream;
                            let mut line = String::new();
                            if reader.read_line(&mut line).is_ok() && !line.is_empty() {
                                let _ = req_tx.send(line.trim_end().to_string());
                                let response = handler(line.trim_end());
                                let _ = writer.write_all(response.as_bytes());
                                let _ = writer.write_all(b"\n");
                                let _ = writer.flush();
                            }
                        });
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });

        FakeServer {
            socket_path,
            requests: req_rx,
            shutdown: shutdown_tx,
            _dir: dir,
        }
    }

    fn client(&self) -> ApiClient {
        ApiClient::connect(&self.socket_path).unwrap()
    }
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
    }
}

fn recv_request(server: &FakeServer) -> Value {
    let line = server
        .requests
        .recv_timeout(Duration::from_secs(5))
        .expect("timed out waiting for request");
    serde_json::from_str(&line).unwrap()
}

// ---------------------------------------------------------------------------
// Core NDJSON client
// ---------------------------------------------------------------------------

#[test]
fn request_success_returns_result() {
    let server = FakeServer::start(|req| {
        assert_eq!(req["method"], "ping");
        json!({"result": {"type": "pong", "protocol": 22}})
    });
    let client = server.client();
    let result = client.request("ping", &json!({})).unwrap();
    assert_eq!(result["type"], "pong");
    assert_eq!(result["protocol"], 22);
}

#[test]
fn request_error_envelope_maps_to_server_error() {
    let server =
        FakeServer::start(|_| json!({"error": {"code": "not_found", "message": "no such agent"}}));
    let client = server.client();
    match client.agent_get("ghost") {
        Err(ApiError::Server { code, message }) => {
            assert_eq!(code, "not_found");
            assert_eq!(message, "no such agent");
        }
        other => panic!("expected Server error, got {other:?}"),
    }
}

#[test]
fn request_ids_are_unique_and_strings() {
    let server = FakeServer::start(|_| json!({"result": null}));
    let client = server.client();
    client.request("ping", &json!({})).unwrap();
    client.request("ping", &json!({})).unwrap();
    let first = recv_request(&server);
    let second = recv_request(&server);
    assert!(first["id"].is_string());
    assert!(second["id"].is_string());
    assert_ne!(first["id"], second["id"]);
}

#[test]
fn malformed_json_response_is_json_error() {
    let server = FakeServer::start_raw(|_| "this is not json".to_string());
    let client = server.client();
    match client.request("ping", &json!({})) {
        Err(ApiError::Json(_)) => {}
        other => panic!("expected Json error, got {other:?}"),
    }
}

#[test]
fn connection_refused_is_server_not_running() {
    // Bind then immediately drop a listener so the path is gone — connect
    // to a path that was never created.
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope.sock");
    match ApiClient::connect(&missing) {
        Err(ApiError::ServerNotRunning { path }) => assert_eq!(path, missing),
        other => panic!("expected ServerNotRunning, got {other:?}"),
    }
}

#[test]
fn timeout_when_server_never_responds() {
    // Server accepts but never writes a response.
    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("herdr.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    thread::spawn(move || {
        for stream in listener.incoming() {
            // Hold the connection open without responding.
            thread::spawn(move || {
                let _held = stream;
                thread::sleep(Duration::from_secs(30));
            });
        }
    });
    let client = ApiClient::connect(&socket_path).unwrap();
    match client.request_with_timeout("ping", &json!({}), Duration::from_millis(200)) {
        Err(ApiError::Timeout) => {}
        other => panic!("expected Timeout, got {other:?}"),
    }
}

#[test]
fn wire_shape_is_id_method_params() {
    let server = FakeServer::start(|_| json!({"result": {}}));
    let client = server.client();
    client.agent_list().unwrap();
    let req = recv_request(&server);
    assert!(req["id"].is_string());
    assert_eq!(req["method"], "agent.list");
    assert_eq!(req["params"], json!({}));
    // Ensure no extra top-level keys beyond id/method/params.
    let keys: Vec<&str> = req
        .as_object()
        .unwrap()
        .keys()
        .map(|s| s.as_str())
        .collect();
    assert_eq!(keys.len(), 3);
}

// ---------------------------------------------------------------------------
// methods.rs wrappers — serialization shape
// ---------------------------------------------------------------------------

#[test]
fn agent_start_serializes_required_and_optional_params() {
    let server = FakeServer::start(|_| json!({"result": {"ok": true}}));
    let client = server.client();
    client
        .agent_start(&AgentStartParams {
            pane_id: "p1".into(),
            kind: "claude".into(),
            name: "worker".into(),
            args: Some(vec!["--flag".into()]),
            timeout_ms: Some(10_000),
        })
        .unwrap();
    let req = recv_request(&server);
    assert_eq!(req["method"], "agent.start");
    assert_eq!(
        req["params"],
        json!({
            "pane_id": "p1",
            "kind": "claude",
            "name": "worker",
            "args": ["--flag"],
            "timeout_ms": 10_000
        })
    );
}

#[test]
fn agent_start_omits_absent_optionals() {
    let server = FakeServer::start(|_| json!({"result": {}}));
    let client = server.client();
    client
        .agent_start(&AgentStartParams {
            pane_id: "p1".into(),
            kind: "codex".into(),
            name: "n".into(),
            args: None,
            timeout_ms: None,
        })
        .unwrap();
    let req = recv_request(&server);
    assert_eq!(
        req["params"],
        json!({"pane_id": "p1", "kind": "codex", "name": "n"})
    );
}

#[test]
fn agent_wait_serializes_target_timeout_until() {
    let server = FakeServer::start(|_| json!({"result": {"status": "idle"}}));
    let client = server.client();
    client
        .agent_wait_with_timeout(
            "my-agent",
            Some(api_client::methods::WaitOptions {
                timeout_ms: Some(5_000),
                until: Some(vec![api_client::methods::AgentStatus::Idle]),
            }),
            Duration::from_secs(10),
        )
        .unwrap();
    let req = recv_request(&server);
    assert_eq!(req["method"], "agent.wait");
    assert_eq!(
        req["params"],
        json!({
            "target": "my-agent",
            "timeout_ms": 5_000,
            "until": ["idle"]
        })
    );
}

#[test]
fn pane_split_serializes_direction() {
    let server = FakeServer::start(|_| json!({"result": {}}));
    let client = server.client();
    client
        .pane_split(&PaneSplitParams {
            direction: SplitDirection::Down,
            target_pane_id: None,
            cwd: Some("/tmp/x".into()),
            env: None,
            focus: None,
            ratio: None,
            workspace_id: None,
        })
        .unwrap();
    let req = recv_request(&server);
    assert_eq!(req["method"], "pane.split");
    assert_eq!(req["params"]["direction"], "down");
    assert_eq!(req["params"]["cwd"], "/tmp/x");
}

#[test]
fn api_snapshot_uses_session_snapshot_method() {
    let server = FakeServer::start(|req| {
        assert_eq!(req["method"], "session.snapshot");
        json!({"result": {"snap": true}})
    });
    let client = server.client();
    let v = client.api_snapshot().unwrap();
    assert_eq!(v["snap"], true);
}

// ---------------------------------------------------------------------------
// compat.rs
// ---------------------------------------------------------------------------

#[test]
fn compat_accepts_supported_protocol() {
    let server = FakeServer::start(
        |_| json!({"result": {"type": "pong", "version": "0.9.5", "protocol": 22}}),
    );
    let client = server.client();
    let info = api_client::compat::check_compat(&client).unwrap();
    assert_eq!(info.protocol, 22);
    assert_eq!(info.version.as_deref(), Some("0.9.5"));
}

#[test]
fn compat_rejects_out_of_range_protocol() {
    for protocol in [0u64, 21, 23, 99] {
        let server = FakeServer::start(
            move |_| json!({"result": {"type": "pong", "version": "9.9.9", "protocol": protocol}}),
        );
        let client = server.client();
        match api_client::compat::check_compat(&client) {
            Err(ApiError::IncompatibleProtocol { protocol: p }) => {
                assert_eq!(p, protocol)
            }
            other => panic!("protocol {protocol}: expected IncompatibleProtocol, got {other:?}"),
        }
    }
}

#[test]
fn compat_error_message_mentions_update() {
    let server = FakeServer::start(
        |_| json!({"result": {"type": "pong", "version": "0.8.0", "protocol": 19}}),
    );
    let client = server.client();
    let err = api_client::compat::check_compat(&client).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("not supported"), "msg: {msg}");
    assert!(msg.contains("update herdcluster"), "msg: {msg}");
}

// ---------------------------------------------------------------------------
// platform socket-path discovery (env override)
// ---------------------------------------------------------------------------

#[test]
fn socket_path_candidates_honor_herdr_socket_path() {
    // Env mutation is process-global; keep all env assertions in this one
    // test so parallel tests can't race on HERDR_SOCKET_PATH.
    let _lock = env_lock();
    let custom = Path::new("/tmp/custom-herdr.sock");
    std::env::set_var("HERDR_SOCKET_PATH", custom);
    let candidates = api_client::platform::socket_path_candidates();
    std::env::remove_var("HERDR_SOCKET_PATH");
    assert_eq!(candidates[0], custom);
    // Default candidate is always last.
    assert!(candidates.last().unwrap().ends_with("herdr/herdr.sock"));
}

#[test]
fn socket_path_candidates_session_env_adds_candidate() {
    let _lock = env_lock();
    std::env::set_var("HERDR_SESSION", "my-session");
    std::env::remove_var("HERDR_SOCKET_PATH");
    let candidates = api_client::platform::socket_path_candidates();
    std::env::remove_var("HERDR_SESSION");
    assert!(candidates
        .iter()
        .any(|p| p.to_string_lossy().contains("my-session")));
}

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------
// Live smoke test against a real herdr, if one is running.
// ---------------------------------------------------------------------------

#[test]
fn live_ping_against_real_herdr_if_present() {
    let Ok(path) = api_client::platform::socket_path() else {
        return; // discovery unsupported on this platform
    };
    if !path.exists() {
        eprintln!("skipping live test: no herdr socket at {}", path.display());
        return;
    }
    // A stale socket file can exist without a live server — skip rather
    // than fail when the endpoint doesn't answer.
    let Ok(client) = ApiClient::connect(&path) else {
        eprintln!(
            "skipping live test: herdr socket not accepting at {}",
            path.display()
        );
        return;
    };
    let Ok(info) = api_client::compat::check_compat(&client) else {
        eprintln!(
            "skipping live test: herdr at {} did not answer compat check",
            path.display()
        );
        return;
    };
    assert!(info.protocol >= api_client::compat::MIN_SUPPORTED_PROTOCOL);
}
