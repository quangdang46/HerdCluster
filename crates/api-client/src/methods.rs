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
use std::time::Duration;

use serde::{Deserialize, Serialize};

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
