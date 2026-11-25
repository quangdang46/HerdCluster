//! `autostart` — run `herdcluster worker` at login/boot
//! (bead `m3-autostart`, PLAN.md §3.10).
//!
//! Platform mechanisms:
//!
//! - **Linux / WSL2**: a systemd *user* unit
//!   `~/.config/systemd/user/herdcluster-worker.service`
//!   (`$XDG_CONFIG_HOME` aware) with `Restart=on-failure` and
//!   `WantedBy=default.target`. After writing the unit we best-effort
//!   run `systemctl --user daemon-reload` + `enable --now`; a missing
//!   `systemctl` (e.g. WSL without systemd, or a container) is reported
//!   via `tracing` but does not fail the install — the unit file is
//!   still correct for when systemd is available. WSL2 caveat from the
//!   bead: user units only start at boot when lingering is enabled
//!   (`loginctl enable-linger`) or via a Windows-side startup entry —
//!   documented, not automated.
//! - **macOS**: a launchd agent plist
//!   `~/Library/LaunchAgents/dev.herdcluster.worker.plist`
//!   (`RunAtLoad` + `KeepAlive`, stdout/stderr into
//!   `<state_dir>/logs/`), activated best-effort via
//!   `launchctl bootstrap gui/<uid>` with a legacy `launchctl load -w`
//!   fallback.
//! - **Windows / other**: [`AutostartStatus::Unsupported`]; install
//!   returns [`AutostartError::Unsupported`].

#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;

use thiserror::Error;

/// Launchd label for the worker agent.
#[cfg(target_os = "macos")]
const LAUNCHD_LABEL: &str = "dev.herdcluster.worker";
/// systemd user unit name.
#[cfg(all(unix, not(target_os = "macos")))]
const SYSTEMD_UNIT_NAME: &str = "herdcluster-worker.service";

/// Autostart installation state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutostartStatus {
    /// An autostart entry exists at `path`.
    Installed {
        /// Path of the unit file / plist.
        path: PathBuf,
    },
    /// Supported platform, no entry present.
    NotInstalled,
    /// This platform has no supported mechanism.
    Unsupported {
        /// Why autostart is unavailable.
        reason: String,
    },
}

/// Errors from [`install_autostart`] / [`uninstall_autostart`].
#[derive(Debug, Error)]
pub enum AutostartError {
    /// Filesystem error writing/removing the entry.
    #[error("autostart io error: {0}")]
    Io(#[from] std::io::Error),
    /// The platform has no supported autostart mechanism.
    #[error("autostart unsupported on this platform: {0}")]
    Unsupported(String),
}

/// Install the worker daemon as a login/boot autostart entry.
///
/// Returns the path of the written unit file / plist. Service-manager
/// activation (`systemctl --user enable --now`, `launchctl bootstrap`)
/// is best-effort: failures are logged, not fatal — the entry file is
/// the durable artifact.
pub fn install_autostart() -> Result<PathBuf, AutostartError> {
    #[cfg(unix)]
    let exe = std::env::current_exe()?;
    #[cfg(target_os = "macos")]
    {
        install_launchd(&exe)
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        install_systemd(&exe)
    }
    #[cfg(not(unix))]
    {
        Err(AutostartError::Unsupported(
            "no autostart mechanism on this platform".to_string(),
        ))
    }
}

/// Remove the autostart entry (unit file / plist) and best-effort
/// deactivate it with the service manager first.
pub fn uninstall_autostart() -> Result<(), AutostartError> {
    #[cfg(target_os = "macos")]
    {
        uninstall_launchd()
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        uninstall_systemd()
    }
    #[cfg(not(unix))]
    {
        Err(AutostartError::Unsupported(
            "no autostart mechanism on this platform".to_string(),
        ))
    }
}

/// Detect the current autostart state.
pub fn autostart_status() -> AutostartStatus {
    #[cfg(target_os = "macos")]
    {
        let path = launchd_plist_path();
        if path.exists() {
            AutostartStatus::Installed { path }
        } else {
            AutostartStatus::NotInstalled
        }
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let path = systemd_unit_path();
        if path.exists() {
            AutostartStatus::Installed { path }
        } else {
            AutostartStatus::NotInstalled
        }
    }
    #[cfg(not(unix))]
    {
        AutostartStatus::Unsupported {
            reason: "no autostart mechanism on this platform".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// systemd (Linux / WSL2)
// ---------------------------------------------------------------------------

#[cfg(all(unix, not(target_os = "macos")))]
fn systemd_unit_dir() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        if !xdg.trim().is_empty() {
            return PathBuf::from(xdg).join("systemd").join("user");
        }
    }
    dirs_home().join(".config").join("systemd").join("user")
}

#[cfg(all(unix, not(target_os = "macos")))]
fn systemd_unit_path() -> PathBuf {
    systemd_unit_dir().join(SYSTEMD_UNIT_NAME)
}

#[cfg(unix)]
fn dirs_home() -> PathBuf {
    for var in ["HOME", "USERPROFILE"] {
        if let Ok(v) = std::env::var(var) {
            if !v.trim().is_empty() {
                return PathBuf::from(v);
            }
        }
    }
    PathBuf::from(".")
}

/// Quote one path for systemd `ExecStart=`. systemd splits the command
/// line on whitespace unless a word is quoted, and expands `%`
/// specifiers, so a raw path containing spaces or `%` would corrupt the
/// unit. Per `systemd.syntax(7)`, inside `"…"` a backslash escapes the
/// next character, and a literal `%` is written `%%`.
#[cfg(all(unix, not(target_os = "macos")))]
fn systemd_quote_arg(path: &Path) -> String {
    let mut s = path.display().to_string();
    // Specifier escaping first: `%` → `%%` introduces no quotes or
    // backslashes, so later steps can't corrupt it.
    s = s.replace('%', "%%");
    // systemd quoting: `\` escapes the next char inside double quotes.
    s = s.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{s}\"")
}

/// Render the systemd user unit for `exe`.
#[cfg(all(unix, not(target_os = "macos")))]
fn systemd_unit_contents(exe: &Path) -> String {
    format!(
        "[Unit]\n\
         Description=HerdCluster worker daemon\n\
         After=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={} worker\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        systemd_quote_arg(exe)
    )
}

#[cfg(all(unix, not(target_os = "macos")))]
fn install_systemd(exe: &Path) -> Result<PathBuf, AutostartError> {
    let dir = systemd_unit_dir();
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(SYSTEMD_UNIT_NAME);
    atomic_write(&path, systemd_unit_contents(exe).as_bytes())?;
    tracing::info!(unit = %path.display(), "wrote systemd user unit");

    // Best-effort activation. WSL2 without systemd, containers, and
    // minimal distros all lack a working `systemctl --user`; the unit
    // file remains valid for when the service manager appears.
    if run_best_effort("systemctl", &["--user", "daemon-reload"]) {
        run_best_effort(
            "systemctl",
            &["--user", "enable", "--now", SYSTEMD_UNIT_NAME],
        );
    } else {
        tracing::warn!(
            "systemctl --user unavailable; unit installed at {} but not enabled \
             (on WSL2 enable lingering or add a Windows startup entry — see docs)",
            path.display()
        );
    }
    Ok(path)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn uninstall_systemd() -> Result<(), AutostartError> {
    run_best_effort(
        "systemctl",
        &["--user", "disable", "--now", SYSTEMD_UNIT_NAME],
    );
    run_best_effort("systemctl", &["--user", "daemon-reload"]);
    let path = systemd_unit_path();
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

// ---------------------------------------------------------------------------
// launchd (macOS)
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
fn launch_agents_dir() -> PathBuf {
    dirs_home().join("Library").join("LaunchAgents")
}

#[cfg(target_os = "macos")]
fn launchd_plist_path() -> PathBuf {
    launch_agents_dir().join(format!("{LAUNCHD_LABEL}.plist"))
}

/// Escape text for a plist `<string>` value. `&`, `<`, and `>` are XML
/// metacharacters; interpolating a raw path containing them produces a
/// malformed plist that launchd silently rejects.
#[cfg(target_os = "macos")]
fn plist_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Render the launchd plist for `exe`, logging to `log_dir`.
#[cfg(target_os = "macos")]
fn launchd_plist_contents(exe: &Path, log_dir: &Path) -> String {
    let out_log = log_dir.join("worker.out.log");
    let err_log = log_dir.join("worker.err.log");
    let exe = plist_escape(&exe.display().to_string());
    let out_log = plist_escape(&out_log.display().to_string());
    let err_log = plist_escape(&err_log.display().to_string());
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \
         \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
         <plist version=\"1.0\">\n\
         <dict>\n\
         \x20   <key>Label</key>\n\
         \x20   <string>{LAUNCHD_LABEL}</string>\n\
         \x20   <key>ProgramArguments</key>\n\
         \x20   <array>\n\
         \x20       <string>{exe}</string>\n\
         \x20       <string>worker</string>\n\
         \x20   </array>\n\
         \x20   <key>RunAtLoad</key>\n\
         \x20   <true/>\n\
         \x20   <key>KeepAlive</key>\n\
         \x20   <true/>\n\
         \x20   <key>StandardOutPath</key>\n\
         \x20   <string>{out_log}</string>\n\
         \x20   <key>StandardErrorPath</key>\n\
         \x20   <string>{err_log}</string>\n\
         </dict>\n\
         </plist>\n",
        exe = exe,
        out_log = out_log,
        err_log = err_log,
    )
}

#[cfg(target_os = "macos")]
fn install_launchd(exe: &Path) -> Result<PathBuf, AutostartError> {
    let dir = launch_agents_dir();
    std::fs::create_dir_all(&dir)?;

    // Worker logs live under the herdcluster state dir.
    let log_dir = match cluster_core::config::state_dir() {
        Ok(d) => d.join("logs"),
        Err(_) => dir.join("logs"),
    };
    std::fs::create_dir_all(&log_dir)?;

    let path = dir.join(format!("{LAUNCHD_LABEL}.plist"));
    atomic_write(&path, launchd_plist_contents(exe, &log_dir).as_bytes())?;
    tracing::info!(plist = %path.display(), "wrote launchd agent plist");

    // Best-effort activation: modern `launchctl bootstrap`, legacy
    // `launchctl load -w` as fallback.
    let path_str = path.to_string_lossy().to_string();
    let mut activated = false;
    if let Some(uid) = launchd_uid() {
        let domain = format!("gui/{uid}");
        activated = run_best_effort(
            "launchctl",
            &["bootstrap", domain.as_str(), path_str.as_str()],
        );
    }
    if !activated {
        run_best_effort("launchctl", &["load", "-w", path_str.as_str()]);
    }
    Ok(path)
}

#[cfg(target_os = "macos")]
fn uninstall_launchd() -> Result<(), AutostartError> {
    let path = launchd_plist_path();
    let path_str = path.to_string_lossy().to_string();
    if let Some(uid) = launchd_uid() {
        let target = format!("gui/{uid}/{LAUNCHD_LABEL}");
        run_best_effort("launchctl", &["bootout", target.as_str()]);
    }
    run_best_effort("launchctl", &["unload", "-w", path_str.as_str()]);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// The current user's uid for `gui/<uid>` launchd domains.
#[cfg(target_os = "macos")]
fn launchd_uid() -> Option<String> {
    if let Ok(v) = std::env::var("UID") {
        if !v.trim().is_empty() {
            return Some(v.trim().to_string());
        }
    }
    let out = std::process::Command::new("id").arg("-u").output().ok()?;
    if out.status.success() {
        let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !s.is_empty() {
            return Some(s);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// shared
// ---------------------------------------------------------------------------

/// Write `contents` to `path` atomically: PID-unique sibling temp file,
/// fsync, rename, then a best-effort parent-directory fsync so the rename
/// is durable. Mirrors `worker::atomic_write_600` and
/// `cluster_core::fsutil::atomic_write` (both private to their crates),
/// but without pinning `0600` — unit files and plists keep the default
/// umask permissions `std::fs::write` used to produce.
#[cfg(unix)]
fn atomic_write(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".to_string());
    let tmp = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        use std::io::Write;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    // Best-effort directory fsync so the rename is durable across crashes.
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

/// Run a service-manager command best-effort: `true` on exit status 0.
/// Missing binaries and non-zero exits only produce a `tracing::warn`.
#[cfg(unix)]
fn run_best_effort(program: &str, args: &[&str]) -> bool {
    match std::process::Command::new(program).args(args).status() {
        Ok(status) if status.success() => true,
        Ok(status) => {
            tracing::warn!(%program, ?args, %status, "autostart service-manager call failed");
            false
        }
        Err(e) => {
            tracing::warn!(%program, ?args, error = %e, "autostart service-manager unavailable");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn autostart_status_is_definite() {
        // Must always return one of the three variants — never panic.
        match autostart_status() {
            AutostartStatus::Installed { path } => {
                assert!(path.is_absolute() || path.is_relative())
            }
            AutostartStatus::NotInstalled => {}
            AutostartStatus::Unsupported { reason } => assert!(!reason.is_empty()),
        }
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_roundtrip_leaves_no_tmp() {
        let dir = std::env::temp_dir().join(format!("hc-autostart-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("unit.service");
        atomic_write(&path, b"v1").unwrap();
        atomic_write(&path, b"v2-longer").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"v2-longer");
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp files left behind: {leftovers:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn systemd_unit_contents_shape() {
        let exe = Path::new("/usr/local/bin/herdcluster");
        let unit = systemd_unit_contents(exe);
        assert!(unit.contains("[Unit]"));
        assert!(unit.contains("[Service]"));
        assert!(unit.contains("[Install]"));
        assert!(unit.contains("Restart=on-failure"));
        assert!(unit.contains("WantedBy=default.target"));
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn systemd_unit_quotes_execstart_path() {
        // The exe path is always double-quoted so spaces can't split
        // argv and `%` can't expand as a specifier.
        let exe = Path::new("/opt/my apps/herd%cluster/bin/herdcluster");
        let unit = systemd_unit_contents(exe);
        assert!(unit.contains("ExecStart=\"/opt/my apps/herd%%cluster/bin/herdcluster\" worker"));
        // No unquoted `%` may remain on the ExecStart line.
        let line = unit.lines().find(|l| l.starts_with("ExecStart=")).unwrap();
        assert!(!line.replace("%%", "").contains('%'));
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn systemd_quote_arg_escapes_quotes_and_backslashes() {
        let q = systemd_quote_arg(Path::new("/opt/a\"b\\c%d/herdcluster"));
        assert_eq!(q, "\"/opt/a\\\"b\\\\c%%d/herdcluster\"");
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn systemd_unit_path_under_xdg() {
        // Path resolution must not touch the filesystem.
        let p = systemd_unit_path();
        assert!(p.ends_with(SYSTEMD_UNIT_NAME));
        assert!(p.components().any(|c| c.as_os_str() == "user"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn launchd_plist_contents_shape() {
        let exe = Path::new("/opt/homebrew/bin/herdcluster");
        let log_dir = Path::new("/tmp/hc-logs");
        let plist = launchd_plist_contents(exe, log_dir);
        assert!(plist.contains("<key>Label</key>"));
        assert!(plist.contains(LAUNCHD_LABEL));
        assert!(plist.contains("<key>RunAtLoad</key>"));
        assert!(plist.contains("<key>KeepAlive</key>"));
        assert!(plist.contains("/opt/homebrew/bin/herdcluster"));
        assert!(plist.contains("worker"));
        assert!(plist.contains("/tmp/hc-logs/worker.out.log"));
        assert!(plist.contains("/tmp/hc-logs/worker.err.log"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn launchd_plist_escapes_xml_metacharacters() {
        // A path containing `&`, `<`, `>` must not produce a malformed
        // plist — launchd failures are warn-only, so a bad plist would
        // silently never start the worker.
        let exe = Path::new("/opt/a&b/<c>/herdcluster");
        let log_dir = Path::new("/tmp/hc<&>logs");
        let plist = launchd_plist_contents(exe, log_dir);
        assert!(plist.contains("/opt/a&amp;b/&lt;c&gt;/herdcluster"));
        assert!(plist.contains("/tmp/hc&lt;&amp;&gt;logs/worker.out.log"));
        assert!(plist.contains("/tmp/hc&lt;&amp;&gt;logs/worker.err.log"));
        // Raw metacharacters must not appear inside <string> values.
        assert!(!plist.contains("<c>"));
        assert!(!plist.contains("hc<&>"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn launchd_plist_path_under_home() {
        let p = launchd_plist_path();
        assert!(p.ends_with(format!("{LAUNCHD_LABEL}.plist")));
        assert!(p.components().any(|c| c.as_os_str() == "LaunchAgents"));
    }
}
