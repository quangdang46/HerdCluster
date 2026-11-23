//! `provisioning` — worker-side agent-CLI presence probes and
//! consent-gated install hints (bead `m3-provisioning`, PLAN.md §3.7).
//!
//! # Hard boundary
//!
//! HerdCluster **never** handles agent credentials — no API keys, no
//! `claude login` state, nothing under `~/.claude`, `~/.codex`, etc.
//! This module only:
//!
//! 1. asks the local herdr API which agent *kinds* it supports
//!    (`server.agent_manifests`),
//! 2. checks whether a same-named CLI exists on `PATH` (a `which`-style
//!    lookup — no shell, no execution of the agent CLI itself),
//! 3. reports a plain-language `auth_hint` for kinds known to need an
//!    interactive login, and
//! 4. *offers* official install commands — running one requires
//!    `yes == true` (the caller's consent flag) and inherits stdio so
//!    the user sees the installer.
//!
//! [`probe_agent_kinds`] mirrors `resources::probe_agent_kinds` but
//! reports **every** manifest kind (installed or not) so doctor/join
//! can show what is missing, not just what works.

use std::path::PathBuf;

use serde_json::Value;
use thiserror::Error;

use crate::skills::InstallOutcome;

/// Fallback kind list when the local herdr API is unreachable.
///
/// Used only so `probe_agent_kinds` still reports PATH presence on a
/// machine where herdr isn't running yet; the live
/// `server.agent_manifests` list always wins when available. Kept to
/// well-known agent CLIs — herdr's manifest set drifts, so an
/// unreachable API means "best effort", not authoritative.
const FALLBACK_KINDS: &[&str] = &[
    "claude", "codex", "opencode", "gemini", "cursor", "aider", "copilot", "goose", "crush",
    "droid",
];

/// Login hints for kinds known to require an interactive auth step
/// after install. Detection is deliberately shallow — we never run the
/// agent CLI to check auth state (that could have side effects); the
/// hint tells the user exactly what to run.
fn auth_hint_for(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "claude" => {
            "run `claude` once and complete the login flow (or `claude login`) on this machine"
        }
        "codex" => "run `codex login` on this machine",
        "gemini" => "run `gemini` and complete the Google sign-in on this machine",
        "opencode" => "run `opencode auth login` on this machine",
        "aider" => {
            "configure API keys via `aider --api-key` or environment variables on this machine"
        }
        "copilot" => "run `copilot` and complete GitHub sign-in on this machine",
        "cursor" => "run `cursor-agent login` (or `agent login`) on this machine",
        "goose" => "run `goose configure` and complete provider setup on this machine",
        "crush" => "run `crush` and complete provider setup on this machine",
        "droid" => "run `droid` and complete sign-in on this machine",
        _ => return None,
    })
}

/// Official, well-known non-interactive install command for `kind`, or
/// `None` when no safe one-liner is known. Only commands from each
/// vendor's documented install path are listed — anything uncertain
/// returns `None` rather than guessing.
pub fn suggested_install_command(kind: &str) -> Option<String> {
    Some(match kind {
        "claude" => "npm install -g @anthropic-ai/claude-code".to_string(),
        "codex" => "npm install -g @openai/codex".to_string(),
        "opencode" => "npm install -g opencode-ai".to_string(),
        "gemini" => "npm install -g @google/gemini-cli".to_string(),
        "copilot" => "npm install -g @github/copilot".to_string(),
        // `cursor`'s official installer is `curl … | bash` — a pipeline
        // we deliberately do not run (no shell). Left as `None`.
        _ => return None,
    })
}

/// Presence + auth-hint report for one agent kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KindProbe {
    /// Agent kind as reported by `server.agent_manifests` (or the
    /// fallback list).
    pub kind: String,
    /// A same-named executable was found on `PATH`.
    pub installed: bool,
    /// Plain-language next step when the kind is installed but almost
    /// certainly needs an interactive login. `None` for kinds without a
    /// known auth requirement (or not installed — nothing to log into).
    pub auth_hint: Option<String>,
}

/// Errors from [`install_agent_cli`].
#[derive(Debug, Error)]
pub enum ProvisioningError {
    /// Spawning or waiting on the installer failed.
    #[error("installer io error for '{kind}': {source}")]
    Io {
        /// Agent kind being installed.
        kind: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
}

/// Probe every supported agent kind on this machine.
///
/// Kind list comes from the local herdr API's `server.agent_manifests`
/// (falling back to [`FALLBACK_KINDS`] when herdr isn't reachable);
/// `installed` is a `PATH` lookup per kind. Never fails and never runs
/// an agent CLI.
pub fn probe_agent_kinds() -> Vec<KindProbe> {
    supported_kinds()
        .into_iter()
        .map(|kind| {
            let installed = cli_on_path(&kind);
            KindProbe {
                auth_hint: if installed {
                    auth_hint_for(&kind).map(str::to_string)
                } else {
                    None
                },
                kind,
                installed,
            }
        })
        .collect()
}

/// JSON-able provisioning report for `join`, `doctor`, and `machines`.
///
/// Shape:
/// ```json
/// {
///   "kinds": [{"kind": "claude", "installed": true,
///              "auth_hint": "…", "install_command": "npm install -g …"}],
///   "installed": ["claude"],
///   "missing": ["codex"]
/// }
/// ```
pub fn provisioning_report() -> Value {
    let probes = probe_agent_kinds();
    let kinds: Vec<Value> = probes
        .iter()
        .map(|p| {
            serde_json::json!({
                "kind": p.kind,
                "installed": p.installed,
                "auth_hint": p.auth_hint,
                "install_command": suggested_install_command(&p.kind),
            })
        })
        .collect();
    let installed: Vec<&str> = probes
        .iter()
        .filter(|p| p.installed)
        .map(|p| p.kind.as_str())
        .collect();
    let missing: Vec<&str> = probes
        .iter()
        .filter(|p| !p.installed)
        .map(|p| p.kind.as_str())
        .collect();
    serde_json::json!({
        "kinds": kinds,
        "installed": installed,
        "missing": missing,
    })
}

/// Offer to install the CLI for `kind`.
///
/// - `yes == false` → **never runs anything**; returns an
///   [`InstallOutcome`] whose `skipped_reason` names the command that
///   *would* run (consent is the caller's job — `--yes` or a prompt).
/// - `yes == true` → runs [`suggested_install_command`] via
///   `std::process::Command` **without a shell** (argv split on
///   whitespace — every suggested command is a simple argv), inheriting
///   stdio so the user sees installer output, then re-probes `PATH`.
///
/// `InstallOutcome.path` carries the agent kind name (there is no
/// fixed install path to report); `created` is true when the CLI is on
/// `PATH` after the call. Kinds with no known installer return a
/// skipped outcome rather than an error.
pub fn install_agent_cli(kind: &str, yes: bool) -> Result<InstallOutcome, ProvisioningError> {
    let probe_path = PathBuf::from(kind);

    if cli_on_path(kind) {
        return Ok(InstallOutcome {
            path: probe_path,
            created: false,
            skipped_reason: Some(format!("'{kind}' is already on PATH")),
        });
    }

    let Some(command) = suggested_install_command(kind) else {
        return Ok(InstallOutcome {
            path: probe_path,
            created: false,
            skipped_reason: Some(format!("no known non-interactive installer for '{kind}'")),
        });
    };

    if !yes {
        return Ok(InstallOutcome {
            path: probe_path,
            created: false,
            skipped_reason: Some(format!("not run (consent required): {command}")),
        });
    }

    tracing::info!(kind, command, "running agent CLI installer");
    let status = run_argv_command(&command).map_err(|source| ProvisioningError::Io {
        kind: kind.to_string(),
        source,
    })?;

    let now_installed = cli_on_path(kind);
    Ok(InstallOutcome {
        path: probe_path,
        created: now_installed,
        skipped_reason: if status.success() {
            None
        } else {
            Some(format!(
                "installer for '{kind}' exited with {status}; check output above"
            ))
        },
    })
}

/// Run `command` as a plain argv (whitespace-split) with inherited
/// stdio — **no shell**. Suggested commands are curated to be simple
/// argv shapes (`npm install -g <pkg>`), so splitting is safe; anything
/// needing pipes/redirects is not in the suggestion table.
fn run_argv_command(command: &str) -> std::io::Result<std::process::ExitStatus> {
    let mut argv = command.split_whitespace();
    let Some(program) = argv.next() else {
        // Empty command string — treat as a successful no-op so callers
        // get a definite outcome rather than a spawn error.
        return Ok(std::process::ExitStatus::default());
    };
    std::process::Command::new(program).args(argv).status()
}

/// Agent kinds this machine's herdr supports: `server.agent_manifests`
/// when the API answers, else [`FALLBACK_KINDS`].
fn supported_kinds() -> Vec<String> {
    let client = match api_client::ApiClient::connect_default() {
        Ok(c) => c,
        Err(_) => return FALLBACK_KINDS.iter().map(|s| s.to_string()).collect(),
    };
    match client.server_agent_manifests() {
        Ok(v) => {
            let kinds = manifest_kinds(&v);
            if kinds.is_empty() {
                FALLBACK_KINDS.iter().map(|s| s.to_string()).collect()
            } else {
                kinds
            }
        }
        Err(_) => FALLBACK_KINDS.iter().map(|s| s.to_string()).collect(),
    }
}

/// Extract supported agent kinds from a `server.agent_manifests` response.
///
/// Mirrors the defensive walk in `resources::agent_kinds_via` (private
/// there): recursively collects string values under
/// `kind`/`agent_kind`/`id`/`slug` keys plus string arrays under `kinds`
/// keys. Duplicated deliberately — upstream schema drift is expected and
/// both call sites must degrade identically.
fn manifest_kinds(value: &Value) -> Vec<String> {
    use std::collections::BTreeSet;

    fn walk(v: &Value, out: &mut BTreeSet<String>) {
        match v {
            Value::Object(map) => {
                for key in ["kind", "agent_kind", "id", "slug"] {
                    if let Some(s) = map.get(key).and_then(Value::as_str) {
                        if !s.is_empty() {
                            out.insert(s.to_string());
                        }
                    }
                }
                if let Some(arr) = map.get("kinds").and_then(Value::as_array) {
                    for k in arr.iter().filter_map(Value::as_str) {
                        if !k.is_empty() {
                            out.insert(k.to_string());
                        }
                    }
                }
                for child in map.values() {
                    walk(child, out);
                }
            }
            Value::Array(items) => {
                for child in items {
                    walk(child, out);
                }
            }
            _ => {}
        }
    }

    let mut out = BTreeSet::new();
    walk(value, &mut out);
    out.into_iter().collect()
}

/// True when an executable named `kind` exists on `PATH`.
///
/// Mirrors `resources::cli_on_path` (private there): verbatim name
/// match against PATH entries, executable bit required on unix. No
/// shell involved — kind names containing path separators are rejected.
fn cli_on_path(kind: &str) -> bool {
    if kind.is_empty() || kind.contains('/') || kind.contains('\\') {
        return false;
    }
    let paths = match std::env::var_os("PATH") {
        Some(p) => p,
        None => return false,
    };
    for dir in std::env::split_paths(&paths) {
        let candidate = dir.join(kind);
        let Ok(meta) = std::fs::metadata(&candidate) else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if meta.permissions().mode() & 0o111 == 0 {
                continue;
            }
        }
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suggested_install_commands_are_official_shape() {
        assert_eq!(
            suggested_install_command("claude").as_deref(),
            Some("npm install -g @anthropic-ai/claude-code")
        );
        assert_eq!(
            suggested_install_command("codex").as_deref(),
            Some("npm install -g @openai/codex")
        );
        // Unknown kinds → None, never a guess.
        assert_eq!(suggested_install_command("definitely-not-a-kind"), None);
        assert_eq!(suggested_install_command(""), None);
        // Piped installers (e.g. cursor's `curl | bash`) are excluded —
        // installs run without a shell, so every suggestion must be a
        // plain argv with no shell metacharacters.
        for kind in FALLBACK_KINDS {
            if let Some(cmd) = suggested_install_command(kind) {
                assert!(
                    !cmd.contains('|')
                        && !cmd.contains('>')
                        && !cmd.contains('$')
                        && !cmd.contains(';')
                        && !cmd.contains('`'),
                    "suggested command for {kind} needs a shell: {cmd}"
                );
            }
        }
    }

    #[test]
    fn probe_returns_kind_for_every_supported_kind() {
        // No herdr API in tests → fallback list; every probe must carry
        // a kind name and a definite installed flag.
        let probes = probe_agent_kinds();
        assert!(!probes.is_empty());
        assert!(probes.iter().all(|p| !p.kind.is_empty()));
        // auth_hint only appears for installed kinds.
        assert!(probes.iter().all(|p| p.auth_hint.is_none() || p.installed));
    }

    #[test]
    fn provisioning_report_is_json_shaped() {
        let report = provisioning_report();
        assert!(report.get("kinds").and_then(Value::as_array).is_some());
        assert!(report.get("installed").is_some());
        assert!(report.get("missing").is_some());
    }

    #[test]
    fn install_agent_cli_without_consent_never_runs() {
        // yes=false must not spawn anything — outcome is a skipped
        // report naming the command.
        let out = install_agent_cli("claude", false).unwrap();
        match &out.skipped_reason {
            // Already installed on this machine — also fine.
            None => assert!(out.path == *"claude"),
            Some(reason) => assert!(reason.contains("claude") || reason.contains("PATH")),
        }
        // Unknown kind → skipped, not error.
        let out = install_agent_cli("no-such-kind-xyz", false).unwrap();
        assert!(out.skipped_reason.is_some());
        assert!(!out.created);
    }

    #[test]
    fn cli_on_path_rejects_unsafe_names() {
        assert!(!cli_on_path(""));
        assert!(!cli_on_path("../bin/sh"));
        assert!(!cli_on_path("a/b"));
    }
}
