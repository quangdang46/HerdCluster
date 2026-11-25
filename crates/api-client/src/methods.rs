//! Typed wrappers for the herdr API method subset herdcluster needs.
//!
//! Method and parameter names follow the upstream schema verified against
//! herdr **0.9.5** (`herdr api schema`, protocol 22). Where earlier docs
//! referenced `status.server` / `api.snapshot`, upstream renamed them to
//! `ping` / `session.snapshot` — see [`ApiClient::ping`] and
//! [`ApiClient::api_snapshot`].
//!
//! All wrappers are thin: they serialize a snake_case params struct and
//! call [`ApiClient::request`]. Responses are returned as
//! `serde_json::Value` — herdcluster consumes the JSON directly rather than
//! mirroring the full upstream response schema.
//!
//! Parameter shapes were taken from the upstream JSON schema; optional
//! fields use `skip_serializing_if` so absent values are omitted entirely
//! (matching upstream's optional/nullable conventions).

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::platform;
use crate::{ApiClient, ApiError, DEFAULT_TIMEOUT};

// ---------------------------------------------------------------------------
// Shared param building blocks
// ---------------------------------------------------------------------------

/// Params for methods that take no arguments (`agent.list`,
/// `workspace.list`, `server.agent_manifests`, `session.snapshot`, ...).
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct EmptyParams {}

/// Agent status values accepted by `agent.wait` / `agent.prompt` wait-until.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    /// Agent is idle at its prompt.
    Idle,
    /// Agent is actively working.
    Working,
    /// Agent is blocked waiting for input.
    Blocked,
    /// Agent finished its task.
    Done,
    /// Status could not be determined.
    Unknown,
}

impl AgentStatus {
    /// Every status in the upstream enum, for error messages and
    /// validation that need to enumerate the valid vocabulary.
    pub const ALL: [AgentStatus; 5] = [
        AgentStatus::Idle,
        AgentStatus::Working,
        AgentStatus::Blocked,
        AgentStatus::Done,
        AgentStatus::Unknown,
    ];

    /// Parse a lowercase status string (`"idle"`, `"working"`, ...).
    ///
    /// Returns `None` for anything outside the upstream enum — callers
    /// that match on raw status strings should treat that as "cannot be
    /// expressed in `agent.wait`/`events.wait` params" and keep their
    /// string-based path.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "idle" => AgentStatus::Idle,
            "working" => AgentStatus::Working,
            "blocked" => AgentStatus::Blocked,
            "done" => AgentStatus::Done,
            "unknown" => AgentStatus::Unknown,
            _ => return None,
        })
    }

    /// The wire string (inverse of [`AgentStatus::parse`]).
    pub fn as_str(&self) -> &'static str {
        match self {
            AgentStatus::Idle => "idle",
            AgentStatus::Working => "working",
            AgentStatus::Blocked => "blocked",
            AgentStatus::Done => "done",
            AgentStatus::Unknown => "unknown",
        }
    }
}

/// Pane read source (upstream `ReadSource`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadSource {
    /// The currently visible viewport.
    Visible,
    /// Recent scrollback.
    Recent,
    /// Recent scrollback with wrapped lines unwrapped.
    RecentUnwrapped,
    /// The bottom-buffer state used by agent detection.
    Detection,
}

/// Pane read format (upstream `ReadFormat`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadFormat {
    /// Plain text.
    Text,
    /// Text with ANSI styling preserved.
    Ansi,
}

/// Direction for `pane.split` (upstream `SplitDirection`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SplitDirection {
    /// Split to the right.
    Right,
    /// Split downward.
    Down,
}

/// Wait options shared by `agent.wait` and `agent.prompt`.
#[derive(Debug, Default, Clone, Serialize)]
pub struct WaitOptions {
    /// Give up after this many milliseconds (`null` = wait forever, still
    /// bounded by the client-side request timeout).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    /// Return early once the agent reaches one of these statuses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until: Option<Vec<AgentStatus>>,
}

fn req(
    client: &ApiClient,
    method: &str,
    params: &impl Serialize,
) -> Result<serde_json::Value, ApiError> {
    client.request(method, &serde_json::to_value(params)?)
}

fn req_timeout(
    client: &ApiClient,
    method: &str,
    params: &impl Serialize,
    timeout: Duration,
) -> Result<serde_json::Value, ApiError> {
    client.request_with_timeout(method, &serde_json::to_value(params)?, timeout)
}

// ---------------------------------------------------------------------------
// agent.*
// ---------------------------------------------------------------------------

/// Parameters for [`ApiClient::agent_start`].
///
/// Upstream requires `pane_id` + `kind` + `name`; `args` and
/// `timeout_ms` (3000 < t <= 300000) are optional.
#[derive(Debug, Clone, Serialize)]
pub struct AgentStartParams {
    /// Pane that will host the agent (from `pane.split`/`pane.list`).
    pub pane_id: String,
    /// Agent kind, e.g. `"claude"`, `"codex"` — see
    /// `server.agent_manifests` for the live list.
    pub kind: String,
    /// Display name for the agent.
    pub name: String,
    /// Extra CLI arguments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    /// Startup timeout in milliseconds (3000..=300000).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

impl ApiClient {
    /// `agent.list` — list all known agents. Params: `{}`.
    pub fn agent_list(&self) -> Result<serde_json::Value, ApiError> {
        req(self, "agent.list", &EmptyParams {})
    }

    /// `agent.get` — fetch one agent by target (name or terminal id).
    pub fn agent_get(&self, target: &str) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            target: &'a str,
        }
        req(self, "agent.get", &P { target })
    }

    /// `agent.read` — read an agent pane's terminal content.
    ///
    /// Upstream requires `target` and `source`; `format`, `lines`, and
    /// `strip_ansi` are optional.
    pub fn agent_read(
        &self,
        target: &str,
        source: ReadSource,
        format: Option<ReadFormat>,
        lines: Option<u32>,
        strip_ansi: Option<bool>,
    ) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            target: &'a str,
            source: ReadSource,
            #[serde(skip_serializing_if = "Option::is_none")]
            format: Option<ReadFormat>,
            #[serde(skip_serializing_if = "Option::is_none")]
            lines: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            strip_ansi: Option<bool>,
        }
        req(
            self,
            "agent.read",
            &P {
                target,
                source,
                format,
                lines,
                strip_ansi,
            },
        )
    }

    /// `agent.wait` — block until the agent reaches a status or timeout.
    ///
    /// Uses [`DEFAULT_TIMEOUT`] for the client-side call; for long waits use
    /// [`agent_wait_with_timeout`](Self::agent_wait_with_timeout).
    pub fn agent_wait(
        &self,
        target: &str,
        wait: Option<WaitOptions>,
    ) -> Result<serde_json::Value, ApiError> {
        self.agent_wait_with_timeout(target, wait, DEFAULT_TIMEOUT)
    }

    /// `agent.wait` with an explicit client-side timeout. The client timeout
    /// must exceed `wait.timeout_ms` or the client gives up first.
    pub fn agent_wait_with_timeout(
        &self,
        target: &str,
        wait: Option<WaitOptions>,
        client_timeout: Duration,
    ) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            target: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            timeout_ms: Option<u64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            until: Option<Vec<AgentStatus>>,
        }
        let (timeout_ms, until) = wait
            .map(|w| (w.timeout_ms, w.until))
            .unwrap_or((None, None));
        req_timeout(
            self,
            "agent.wait",
            &P {
                target,
                timeout_ms,
                until,
            },
            client_timeout,
        )
    }

    /// `agent.prompt` — send a prompt to an agent, optionally waiting for a
    /// resulting status.
    pub fn agent_prompt(
        &self,
        target: &str,
        text: &str,
        wait: Option<WaitOptions>,
    ) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            target: &'a str,
            text: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            wait: Option<WaitOptions>,
        }
        req(self, "agent.prompt", &P { target, text, wait })
    }

    /// `agent.send_keys` — send key names (e.g. `["Enter"]`, `["C-c"]`) to an
    /// agent's pane.
    pub fn agent_send_keys(
        &self,
        target: &str,
        keys: &[String],
    ) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            target: &'a str,
            keys: &'a [String],
        }
        req(self, "agent.send_keys", &P { target, keys })
    }

    /// `agent.start` — launch an agent in an existing pane.
    pub fn agent_start(&self, params: &AgentStartParams) -> Result<serde_json::Value, ApiError> {
        req(self, "agent.start", params)
    }

    /// `agent.rename` — rename an agent (`name: null` clears the name).
    pub fn agent_rename(
        &self,
        target: &str,
        name: Option<&str>,
    ) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            target: &'a str,
            name: Option<&'a str>,
        }
        req(self, "agent.rename", &P { target, name })
    }

    /// `agent.focus` — focus the pane hosting an agent.
    pub fn agent_focus(&self, target: &str) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            target: &'a str,
        }
        req(self, "agent.focus", &P { target })
    }

    /// `agent.explain` — fetch the detection explanation for an agent
    /// (useful for debugging detection drift).
    pub fn agent_explain(&self, target: &str) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            target: &'a str,
        }
        req(self, "agent.explain", &P { target })
    }

    /// `pane.close` — close a pane (terminating any agent running in it).
    /// There is no dedicated `agent.stop`/`agent.close` upstream method;
    /// closing the hosting pane is the teardown path.
    pub fn pane_close(&self, pane_id: &str) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            pane_id: &'a str,
        }
        req(self, "pane.close", &P { pane_id })
    }
}

// ---------------------------------------------------------------------------
// workspace.*
// ---------------------------------------------------------------------------

/// Parameters for [`ApiClient::workspace_create`].
#[derive(Debug, Default, Clone, Serialize)]
pub struct WorkspaceCreateParams {
    /// Working directory for the workspace's initial pane.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Environment variables for the workspace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<HashMap<String, String>>,
    /// Focus the new workspace (upstream default: false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub focus: Option<bool>,
    /// Workspace label.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Workspace whose focused pane supplies the `follow` cwd policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_workspace_id: Option<String>,
}

impl ApiClient {
    /// `workspace.create`.
    pub fn workspace_create(
        &self,
        params: &WorkspaceCreateParams,
    ) -> Result<serde_json::Value, ApiError> {
        req(self, "workspace.create", params)
    }

    /// `workspace.list` — params `{}`.
    pub fn workspace_list(&self) -> Result<serde_json::Value, ApiError> {
        req(self, "workspace.list", &EmptyParams {})
    }

    /// `workspace.get` — fetch one workspace by id.
    pub fn workspace_get(&self, workspace_id: &str) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            workspace_id: &'a str,
        }
        req(self, "workspace.get", &P { workspace_id })
    }

    /// `workspace.close` — close a workspace (and optionally its whole
    /// worktree group via `close_group`). Closes Herdr state only; it
    /// does not delete files on disk.
    pub fn workspace_close(
        &self,
        workspace_id: &str,
        close_group: Option<bool>,
    ) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            workspace_id: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            close_group: Option<bool>,
        }
        req(
            self,
            "workspace.close",
            &P {
                workspace_id,
                close_group,
            },
        )
    }
}

// ---------------------------------------------------------------------------
// pane.*
// ---------------------------------------------------------------------------

/// Parameters for [`ApiClient::pane_split`]. Only `direction` is
/// required upstream.
#[derive(Debug, Clone, Serialize)]
pub struct PaneSplitParams {
    /// Split direction (required).
    pub direction: SplitDirection,
    /// Pane to split; `None` splits the focused pane.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_pane_id: Option<String>,
    /// Working directory for the new pane.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Environment variables for the new pane.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<HashMap<String, String>>,
    /// Focus the new pane (upstream default: false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub focus: Option<bool>,
    /// Split ratio (0.0..=1.0); `None` uses the server default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ratio: Option<f64>,
    /// Workspace to split in; `None` uses the focused workspace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
}

impl ApiClient {
    /// `pane.list` — optionally filtered to one workspace.
    pub fn pane_list(&self, workspace_id: Option<&str>) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            workspace_id: Option<&'a str>,
        }
        req(self, "pane.list", &P { workspace_id })
    }

    /// `pane.split` — create a new pane next to an existing one.
    pub fn pane_split(&self, params: &PaneSplitParams) -> Result<serde_json::Value, ApiError> {
        req(self, "pane.split", params)
    }

    /// `pane.get` — fetch one pane by id.
    pub fn pane_get(&self, pane_id: &str) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            pane_id: &'a str,
        }
        // Upstream `PaneTarget` uses `pane_id`; verify shape is a bare id
        // string field — schema shows `{pane_id: string}` required.
        req(self, "pane.get", &P { pane_id })
    }

    /// `pane.read` — read a pane's terminal content directly.
    ///
    /// Unlike [`agent_read`](Self::agent_read), this takes a `pane_id`
    /// and works even when no agent is registered on the pane — used for
    /// spawn timeout diagnostics after the agent CLI may have died.
    pub fn pane_read(
        &self,
        pane_id: &str,
        source: ReadSource,
        format: Option<ReadFormat>,
        lines: Option<u32>,
        strip_ansi: Option<bool>,
    ) -> Result<serde_json::Value, ApiError> {
        #[derive(Serialize)]
        struct P<'a> {
            pane_id: &'a str,
            source: ReadSource,
            #[serde(skip_serializing_if = "Option::is_none")]
            format: Option<ReadFormat>,
            #[serde(skip_serializing_if = "Option::is_none")]
            lines: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            strip_ansi: Option<bool>,
        }
        req(
            self,
            "pane.read",
            &P {
                pane_id,
                source,
                format,
                lines,
                strip_ansi,
            },
        )
    }

    /// `pane.current` — the focused pane.
    pub fn pane_current(&self) -> Result<serde_json::Value, ApiError> {
        req(self, "pane.current", &EmptyParams {})
    }
}

// ---------------------------------------------------------------------------
// worktree.*
// ---------------------------------------------------------------------------

/// Parameters for [`ApiClient::worktree_list`]. All fields optional
/// upstream.
#[derive(Debug, Default, Clone, Serialize)]
pub struct WorktreeListParams {
    /// Repository working directory to inspect.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Trust the repository for worktree operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trust_repository: Option<bool>,
    /// Scope listing to one workspace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
}

/// Parameters for [`ApiClient::worktree_create`]. Only `trust_repository`
/// is a plain bool upstream; everything else is optional/nullable.
#[derive(Debug, Clone, Serialize)]
pub struct WorktreeCreateParams {
    /// Base ref/branch to create from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    /// New branch name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Repository working directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Focus the new worktree's workspace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub focus: Option<bool>,
    /// Workspace label.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Filesystem path for the worktree.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Trust the repository for worktree operations.
    pub trust_repository: bool,
    /// Attach to an existing workspace instead of creating one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
}

impl ApiClient {
    /// `worktree.list`.
    pub fn worktree_list(
        &self,
        params: &WorktreeListParams,
    ) -> Result<serde_json::Value, ApiError> {
        req(self, "worktree.list", params)
    }

    /// `worktree.create`.
    pub fn worktree_create(
        &self,
        params: &WorktreeCreateParams,
    ) -> Result<serde_json::Value, ApiError> {
        req(self, "worktree.create", params)
    }
}

// ---------------------------------------------------------------------------
// server.* / session.* / api snapshot
// ---------------------------------------------------------------------------

impl ApiClient {
    /// `server.agent_manifests` — live list of supported agent kinds and
    /// their manifest versions. Params: `{}`.
    pub fn server_agent_manifests(&self) -> Result<serde_json::Value, ApiError> {
        req(self, "server.agent_manifests", &EmptyParams {})
    }

    /// `ping` — handshake/status probe. Returns
    /// `{"type":"pong","version","protocol","capabilities":{...}}`.
    ///
    /// This is the upstream replacement for the older `status.server`
    /// method; [`crate::compat::check_compat`] uses it for version gating.
    pub fn ping(&self) -> Result<serde_json::Value, ApiError> {
        req(self, "ping", &EmptyParams {})
    }

    /// `session.snapshot` — the live session snapshot.
    ///
    /// Upstream renamed `api.snapshot` → `session.snapshot` (verified on
    /// 0.9.5, which has no `api.snapshot` method). Kept under the
    /// herdcluster-facing name `api_snapshot` for call-site clarity.
    pub fn api_snapshot(&self) -> Result<serde_json::Value, ApiError> {
        req(self, "session.snapshot", &EmptyParams {})
    }
}

// ---------------------------------------------------------------------------
// events.*
// ---------------------------------------------------------------------------

/// One subscription filter for [`ApiClient::events_subscribe`].
///
/// Upstream `Subscription` has many more variants (workspace/tab/pane
/// lifecycle, `pane.output_matched`, `layout.updated`, ...); only the
/// ones herdcluster consumes are modeled here. Wire names are the
/// dotted upstream strings (`pane.agent_status_changed`, not the
/// underscored event names used in streamed envelopes).
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum Subscription {
    /// Agent status changes on one pane. `agent_status: None` matches
    /// every transition; `Some` filters server-side.
    #[serde(rename = "pane.agent_status_changed")]
    PaneAgentStatusChanged {
        /// Pane to watch (from `agent.get`'s `pane_id`).
        pane_id: String,
        /// Optional status filter.
        #[serde(skip_serializing_if = "Option::is_none")]
        agent_status: Option<AgentStatus>,
    },
    /// A pane's process exited (global subscription — filter by
    /// `data.pane_id` client-side).
    #[serde(rename = "pane.exited")]
    PaneExited,
    /// A pane was closed (global subscription — filter by
    /// `data.pane_id` client-side).
    #[serde(rename = "pane.closed")]
    PaneClosed,
}

/// Params for [`ApiClient::events_subscribe`].
#[derive(Debug, Clone, Serialize)]
pub struct EventsSubscribeParams {
    /// Subscription filters; events matching any of them are streamed.
    pub subscriptions: Vec<Subscription>,
}

/// A live `events.subscribe` stream.
///
/// Unlike [`ApiClient::request`], the connection stays open and yields
/// one subscription-event envelope per line. Verified on herdr 0.9.5:
/// the first response line is the ack
/// `{"id": ..., "result": {"type": "subscription_started"}}` (consumed
/// by [`ApiClient::events_subscribe`]); every subsequent line is a bare
/// `{"event": ..., "data": {...}}` envelope with **no** `id`/`result`
/// wrapper (see the `subscription_event` schema in `herdr api schema`).
pub struct EventStream {
    reader: BufReader<Box<dyn platform::IoStream>>,
    /// Bytes of the current line read so far, accumulated across
    /// `next_event` calls. The stream's per-read timeout can fire
    /// mid-line; without this buffer the partial bytes would be dropped
    /// while the line's tail stayed buffered, so the next call would try
    /// to parse the tail as a fresh JSON envelope (issue #87).
    line_buf: String,
}

impl EventStream {
    /// Read the next streamed event envelope.
    ///
    /// * `Ok(Some(value))` — one `{"event","data"}` envelope.
    /// * `Ok(None)` — clean EOF: the server (or bridge) closed the
    ///   stream. Callers may resubscribe.
    /// * `Err(ApiError::Timeout)` — no line arrived within the read
    ///   timeout the stream was opened with; treat as a tick and call
    ///   `next_event` again. Any partial line already read is retained
    ///   and completed by a later call.
    /// * `Err(_)` — transport or parse failure; the stream is unusable.
    pub fn next_event(&mut self) -> Result<Option<serde_json::Value>, ApiError> {
        loop {
            // `read_line` appends to `line_buf` rather than replacing it,
            // so bytes consumed before a WouldBlock/TimedOut survive in
            // the buffer until the rest of the line arrives.
            match self.reader.read_line(&mut self.line_buf) {
                Ok(0) => {
                    if self.line_buf.is_empty() {
                        // Clean EOF between lines.
                        return Ok(None);
                    }
                    // EOF mid-line: parse whatever arrived — truncated
                    // JSON surfaces as `ApiError::Json` (recoverable
                    // upstream) rather than being silently dropped.
                    let line = std::mem::take(&mut self.line_buf);
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    return Ok(Some(serde_json::from_str(trimmed)?));
                }
                Ok(_) => {
                    let line = std::mem::take(&mut self.line_buf);
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    return Ok(Some(serde_json::from_str(trimmed)?));
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    // Tick: keep `line_buf` — it holds the partial line.
                    return Err(ApiError::Timeout);
                }
                Err(e) => return Err(ApiError::Io(e)),
            }
        }
    }
}

impl ApiClient {
    /// `events.subscribe` — open a streaming event subscription.
    ///
    /// `timeout` becomes the stream's per-read idle timeout: once the
    /// ack is consumed, [`EventStream::next_event`] returns
    /// [`ApiError::Timeout`] whenever no event arrives within `timeout`,
    /// which callers can use as a periodic tick (e.g. for stuck-agent
    /// warnings) without dropping the subscription.
    ///
    /// On endpoints that don't implement `events.subscribe` (older
    /// herdr, restricted bridges) this fails with
    /// [`ApiError::Server`] — callers should treat that as "fall back to
    /// polling".
    pub fn events_subscribe(
        &self,
        params: &EventsSubscribeParams,
        timeout: Duration,
    ) -> Result<EventStream, ApiError> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let request = serde_json::json!({
            "id": format!("hc-{id}"),
            "method": "events.subscribe",
            "params": serde_json::to_value(params)?,
        });
        let mut line = serde_json::to_vec(&request)?;
        line.push(b'\n');

        let mut stream = platform::connect(&self.socket_path, timeout)?;
        stream.write_all(&line)?;
        stream.flush()?;

        // Consume the ack line, which shares the request/response
        // envelope: {"id","result":{"type":"subscription_started"}} or
        // {"id","error":{...}}.
        let mut reader = BufReader::new(stream);
        let mut ack = String::new();
        match reader.read_line(&mut ack) {
            Ok(0) => {
                return Err(ApiError::Server {
                    code: "connection_closed".to_string(),
                    message: "herdr closed the connection without a response".to_string(),
                })
            }
            Ok(_) => {}
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                return Err(ApiError::Timeout)
            }
            Err(e) => return Err(ApiError::Io(e)),
        }
        let envelope: serde_json::Value = serde_json::from_str(ack.trim_end())?;
        if let Some(error) = envelope.get("error") {
            let code = error
                .get("code")
                .and_then(|c| c.as_str())
                .unwrap_or("unknown")
                .to_string();
            let message = error
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("herdr API error")
                .to_string();
            return Err(ApiError::Server { code, message });
        }
        if envelope.get("result").is_none() {
            return Err(ApiError::Server {
                code: "invalid_response".to_string(),
                message: "herdr subscribe response had neither 'result' nor 'error'".to_string(),
            });
        }
        Ok(EventStream {
            reader,
            line_buf: String::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A stream that only yields bytes up to `limit`; reads past it
    /// report `WouldBlock`, simulating the per-read timeout firing
    /// mid-line. Raising `limit` "opens the gate" for more data.
    struct DripStream {
        data: Vec<u8>,
        pos: usize,
        limit: Arc<AtomicUsize>,
    }

    impl Read for DripStream {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.pos >= self.data.len() {
                return Ok(0); // EOF
            }
            let limit = self.limit.load(Ordering::Relaxed).min(self.data.len());
            if self.pos >= limit {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "read timed out",
                ));
            }
            let end = limit.min(self.pos + buf.len());
            let n = end - self.pos;
            buf[..n].copy_from_slice(&self.data[self.pos..end]);
            self.pos = end;
            Ok(n)
        }
    }

    impl Write for DripStream {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl platform::IoStream for DripStream {
        fn set_read_timeout(&mut self, _timeout: Option<Duration>) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn drip_event_stream(data: &[u8], limit: usize) -> (EventStream, Arc<AtomicUsize>) {
        let limit = Arc::new(AtomicUsize::new(limit));
        let stream = DripStream {
            data: data.to_vec(),
            pos: 0,
            limit: limit.clone(),
        };
        let boxed: Box<dyn platform::IoStream> = Box::new(stream);
        (
            EventStream {
                reader: BufReader::new(boxed),
                line_buf: String::new(),
            },
            limit,
        )
    }

    /// Issue #87: a per-read timeout firing mid-line must not drop the
    /// partial bytes — the next `next_event` call completes the same
    /// envelope instead of parsing the tail as fresh JSON.
    #[test]
    fn event_stream_survives_mid_line_timeout() {
        let line1 = concat!(
            "{\"event\":\"pane.agent_status_changed\",",
            "\"data\":{\"pane_id\":\"w2:p1\",\"status\":\"done\"}}\n"
        );
        let line2 = "{\"event\":\"pane.exited\",\"data\":{\"pane_id\":\"w2:p1\"}}\n";
        let data = format!("{line1}{line2}");
        let split = line1.len() / 2;
        assert!(split > 0 && split < line1.len());

        let (mut events, limit) = drip_event_stream(data.as_bytes(), split);

        // First call reads half the line, then hits WouldBlock → tick.
        assert!(matches!(events.next_event(), Err(ApiError::Timeout)));
        // A second tick with no new data still returns Timeout.
        assert!(matches!(events.next_event(), Err(ApiError::Timeout)));

        // The rest of the line (and the next one) arrives.
        limit.store(data.len(), Ordering::Relaxed);
        let v = events
            .next_event()
            .expect("completed line should parse")
            .expect("an event, not EOF");
        assert_eq!(v["event"], "pane.agent_status_changed");
        assert_eq!(v["data"]["status"], "done");

        // The following line is unaffected.
        let v = events
            .next_event()
            .expect("second line should parse")
            .expect("an event, not EOF");
        assert_eq!(v["event"], "pane.exited");

        // Clean EOF after the last line.
        assert!(matches!(events.next_event(), Ok(None)));
    }

    /// Same drip, but the stream closes mid-line: the partial bytes are
    /// reported as a parse failure (recoverable upstream) rather than
    /// silently dropped or reported as clean EOF.
    #[test]
    fn event_stream_eof_mid_line_is_parse_error() {
        let data = b"{\"event\":\"pane.exited\",\"data\":{\"pane_id\":";
        let (mut events, _limit) = drip_event_stream(data, data.len());
        // limit == len: all bytes readable, then EOF with an unterminated
        // partial line.
        assert!(matches!(events.next_event(), Err(ApiError::Json(_))));
    }

    #[test]
    fn agent_status_parse_roundtrip() {
        for s in ["idle", "working", "blocked", "done", "unknown"] {
            let parsed = AgentStatus::parse(s).unwrap();
            assert_eq!(parsed.as_str(), s);
        }
        assert!(AgentStatus::parse("paused").is_none());
        assert!(AgentStatus::parse("Idle").is_none());
        assert!(AgentStatus::parse("").is_none());
    }

    #[test]
    fn subscription_wire_shape() {
        let params = EventsSubscribeParams {
            subscriptions: vec![
                Subscription::PaneAgentStatusChanged {
                    pane_id: "w2:p1".into(),
                    agent_status: Some(AgentStatus::Done),
                },
                Subscription::PaneAgentStatusChanged {
                    pane_id: "w2:p2".into(),
                    agent_status: None,
                },
                Subscription::PaneExited,
                Subscription::PaneClosed,
            ],
        };
        let v = serde_json::to_value(&params).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"subscriptions": [
                {"type": "pane.agent_status_changed", "pane_id": "w2:p1", "agent_status": "done"},
                {"type": "pane.agent_status_changed", "pane_id": "w2:p2"},
                {"type": "pane.exited"},
                {"type": "pane.closed"},
            ]})
        );
    }
}
