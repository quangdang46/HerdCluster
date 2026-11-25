//! `herdcluster update` (alias `upgrade`) — self-update from GitHub releases.
//!
//! Mirrors `install.sh`: the release publishes
//! `herdcluster-<target-triple>.tar.gz`, `checksums.txt`, and
//! `install.sh` per tag. This command queries the GitHub API for the
//! release asset list (so it can detect "already up to date" before
//! downloading anything), verifies the archive against `checksums.txt`
//! before extracting, and atomically renames the new binary over the
//! running executable.
//!
//! Confirmation policy: a prompt is shown only on an interactive TTY
//! without `--yes` (default answer: no). Non-interactive runs proceed,
//! matching `install.sh`'s non-interactive nature — pipe-friendly
//! automation never blocks.

use std::env;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

const REPO: &str = "quangdang46/herdr_cluster";
const API_BASE: &str = "https://api.github.com";
const USER_AGENT: &str = concat!("herdcluster/", env!("CARGO_PKG_VERSION"));
const HTTP_TIMEOUT: Duration = Duration::from_secs(120);

/// Cap on a single HTTP download, in bytes. The release asset is one
/// compressed Rust binary (~10–20 MiB today), and checksums.txt / the
/// release JSON are tiny — 128 MiB is generous headroom while still
/// bounding memory use against a hostile or compromised release asset.
const MAX_DOWNLOAD_BYTES: u64 = 128 * 1024 * 1024;

/// Cap on the decompressed size of the `herdcluster` entry inside the
/// release tarball. The compressed download is already capped, but a
/// gzip bomb could still expand far past it during extraction, so each
/// tar entry read is bounded independently.
const MAX_BINARY_BYTES: u64 = 128 * 1024 * 1024;

/// `herdcluster update [--version vX.Y.Z] [--yes] [--json]`.
pub fn run(version: Option<String>, yes: bool, json: bool, verbose: bool) -> i32 {
    match update(version.as_deref(), yes, verbose) {
        Ok(outcome) => {
            emit_result(&outcome, json);
            0
        }
        Err(e) => {
            if json {
                eprintln!(
                    "{}",
                    serde_json::to_string(&json!({
                        "status": "error",
                        "error": e,
                    }))
                    .unwrap_or_else(|_| "{\"status\":\"error\"}".to_string())
                );
            } else {
                eprintln!("error: {e}");
            }
            1
        }
    }
}

/// What happened, for both human and `--json` rendering.
enum Outcome {
    /// Already running the pinned/latest release.
    Current { version: String },
    /// New binary installed over the current executable.
    Updated { from: String, to: String },
}

fn update(version: Option<&str>, yes: bool, verbose: bool) -> Result<Outcome, String> {
    let target = target_triple()?;
    let asset_name = format!("herdcluster-{target}.tar.gz");
    let current = env!("CARGO_PKG_VERSION");

    let tag = match version {
        Some(v) => Some(normalize_tag(v)?),
        None => None,
    };
    crate::commands::vlog(verbose, format!("querying GitHub releases for {REPO}"));
    let release = fetch_release(tag.as_deref())?;
    let release_tag = release
        .get("tag_name")
        .and_then(Value::as_str)
        .ok_or("release response missing tag_name")?
        .to_string();
    let release_version = version_of_tag(&release_tag).to_string();

    if release_version == current {
        return Ok(Outcome::Current {
            version: release_version,
        });
    }
    // Never silently downgrade: `--version` (or a latest release older
    // than this build) must not replace a newer binary without the user
    // explicitly asking for it via install.sh.
    if cmp_version(&release_version, current)? == std::cmp::Ordering::Less {
        return Err(format!(
            "refusing to downgrade herdcluster {current} → {release_version}; \
             to install an older release on purpose, use install.sh"
        ));
    }

    let download_url = select_asset_url(&release, &asset_name)
        .ok_or_else(|| format!("release {release_tag} has no asset {asset_name}"))?
        .to_string();

    if !yes && std::io::stdin().is_terminal() && !confirm(&release_version)? {
        return Err(format!("update to {release_version} cancelled"));
    }

    crate::commands::vlog(
        verbose,
        format!("downloading {asset_name} from {download_url}"),
    );
    let archive = download(&download_url)?;

    crate::commands::vlog(verbose, "downloading checksums.txt");
    let checksums = download(&checksums_url(&release_tag))?;
    let checksums =
        String::from_utf8(checksums).map_err(|_| "checksums.txt is not valid UTF-8".to_string())?;
    let expected = checksum_for_asset(&checksums, &asset_name)
        .ok_or_else(|| format!("no checksum entry for {asset_name}"))?;
    verify_checksum(&archive, &expected)
        .map_err(|e| format!("checksum mismatch for {asset_name}: {e}"))?;

    crate::commands::vlog(verbose, "extracting herdcluster binary");
    let binary = extract_binary(&archive)?;
    install_binary(&binary, &release_version, verbose)?;

    Ok(Outcome::Updated {
        from: current.to_string(),
        to: release_version,
    })
}

// --- pure helpers (unit-tested) -------------------------------------------

/// Map the build platform to a published release target triple.
/// `os`/`arch` are `std::env::consts` values.
fn target_for(os: &str, arch: &str) -> Result<&'static str, String> {
    match (os, arch) {
        ("macos", "aarch64") => Ok("aarch64-apple-darwin"),
        ("macos", "x86_64") => Ok("x86_64-apple-darwin"),
        ("linux", "x86_64") => Ok("x86_64-unknown-linux-gnu"),
        (os, arch) => Err(format!(
            "no prebuilt herdcluster binary for {os}-{arch}; \
             build from source (cargo build --release -p herdcluster)"
        )),
    }
}

fn target_triple() -> Result<&'static str, String> {
    target_for(env::consts::OS, env::consts::ARCH)
}

/// Normalize `--version` input to a GitHub release tag (`vX.Y.Z`).
///
/// Accepts `0.1.0` or `v0.1.0` and rejects everything else — the value is
/// interpolated into the GitHub API path, so anything containing `/`,
/// `..`, whitespace, or non-numeric components must never reach
/// [`fetch_release`].
fn normalize_tag(version: &str) -> Result<String, String> {
    let bare = version.trim();
    let bare = bare.strip_prefix('v').unwrap_or(bare);
    let mut parts = bare.split('.');
    let valid = matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(a), Some(b), Some(c), None)
            if [a, b, c]
                .iter()
                .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
    );
    if !valid {
        return Err(format!(
            "invalid --version {version:?}: expected X.Y.Z or vX.Y.Z \
             (e.g. --version v0.1.1)"
        ));
    }
    Ok(format!("v{bare}"))
}

/// Parse an `X.Y.Z` version (leading `v` tolerated) into numeric parts.
fn parse_version(version: &str) -> Result<(u64, u64, u64), String> {
    let bare = version.strip_prefix('v').unwrap_or(version);
    let mut parts = bare.split('.');
    let parse = |p: Option<&str>| -> Result<u64, String> {
        p.ok_or_else(|| format!("version {version:?} is not X.Y.Z"))
            .and_then(|s| {
                s.parse::<u64>()
                    .map_err(|_| format!("version {version:?} is not X.Y.Z"))
            })
    };
    let (a, b, c) = (
        parse(parts.next())?,
        parse(parts.next())?,
        parse(parts.next())?,
    );
    if parts.next().is_some() {
        return Err(format!("version {version:?} is not X.Y.Z"));
    }
    Ok((a, b, c))
}

/// Semver-ish ordering for `X.Y.Z` versions (no pre-release handling —
/// release tags are plain `vX.Y.Z`).
fn cmp_version(a: &str, b: &str) -> Result<std::cmp::Ordering, String> {
    Ok(parse_version(a)?.cmp(&parse_version(b)?))
}

/// Strip the leading `v` from a release tag to get the crate version.
fn version_of_tag(tag: &str) -> &str {
    tag.strip_prefix('v').unwrap_or(tag)
}

/// Find the `browser_download_url` for `asset_name` in a GitHub release
/// API payload.
fn select_asset_url<'a>(release: &'a Value, asset_name: &str) -> Option<&'a str> {
    release
        .get("assets")?
        .as_array()?
        .iter()
        .find(|a| a.get("name").and_then(Value::as_str) == Some(asset_name))?
        .get("browser_download_url")?
        .as_str()
}

/// Parse `checksums.txt` (`<sha256>  <filename>` lines, as produced by
/// `sha256sum herdcluster-*.tar.gz > checksums.txt`) and return the hex
/// digest for `asset_name`.
fn checksum_for_asset(checksums: &str, asset_name: &str) -> Option<String> {
    for line in checksums.lines() {
        let mut parts = line.split_whitespace();
        let (Some(hash), Some(file)) = (parts.next(), parts.next()) else {
            continue;
        };
        // sha256sum may mark binary-mode entries with a leading `*`.
        let file = file.strip_prefix('*').unwrap_or(file);
        if file == asset_name && hash.len() == 64 {
            return Some(hash.to_ascii_lowercase());
        }
    }
    None
}

/// Verify `bytes` against a hex sha256 `expected` digest.
fn verify_checksum(bytes: &[u8], expected: &str) -> Result<(), String> {
    let actual = hex_sha256(bytes);
    if actual.eq_ignore_ascii_case(expected.trim()) {
        Ok(())
    } else {
        Err(format!("expected {expected}, got {actual}"))
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Extract the `herdcluster` binary from a `.tar.gz` archive held in
/// memory. Matches by file name so an archive layout change (e.g. a
/// wrapping directory) does not break the update.
fn extract_binary(archive: &[u8]) -> Result<Vec<u8>, String> {
    use flate2::read::GzDecoder;
    use std::io::Cursor;

    let decoder = GzDecoder::new(Cursor::new(archive));
    let mut tar = tar::Archive::new(decoder);
    for entry in tar.entries().map_err(|e| format!("tar read failed: {e}"))? {
        let entry = entry.map_err(|e| format!("tar entry failed: {e}"))?;
        let is_regular = entry.header().entry_type() == tar::EntryType::Regular;
        let named_herdcluster = entry
            .path()
            .ok()
            .and_then(|p| p.file_name().map(|n| n == "herdcluster"))
            .unwrap_or(false);
        if is_regular && named_herdcluster {
            let buf = read_capped(entry, MAX_BINARY_BYTES, "herdcluster entry in archive")?;
            if buf.is_empty() {
                return Err("herdcluster entry in archive is empty".to_string());
            }
            return Ok(buf);
        }
    }
    Err("archive did not contain a herdcluster binary".to_string())
}

// --- network + install ------------------------------------------------------

fn download(url: &str) -> Result<Vec<u8>, String> {
    let resp = ureq::get(url)
        .set("User-Agent", USER_AGENT)
        .timeout(HTTP_TIMEOUT)
        .call()
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    // Reject early when the server advertises an oversized body; the
    // take() cap in read_capped still guards chunked/lying responses.
    if let Some(len) = resp
        .header("Content-Length")
        .and_then(|v| v.parse::<u64>().ok())
    {
        if len > MAX_DOWNLOAD_BYTES {
            return Err(format!(
                "GET {url} failed: content-length {len} exceeds {} byte limit",
                MAX_DOWNLOAD_BYTES
            ));
        }
    }
    read_capped(
        resp.into_reader(),
        MAX_DOWNLOAD_BYTES,
        &format!("response from {url}"),
    )
}

/// Read `reader` to EOF, failing if it yields more than `limit` bytes.
/// Reads `limit + 1` internally so an oversized stream is detected
/// rather than silently truncated.
fn read_capped<R: Read>(reader: R, limit: u64, what: &str) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    reader
        .take(limit + 1)
        .read_to_end(&mut buf)
        .map_err(|e| format!("reading {what} failed: {e}"))?;
    if buf.len() as u64 > limit {
        return Err(format!("{what} exceeds {} byte limit", limit));
    }
    Ok(buf)
}

/// Fetch the GitHub release JSON for `tag` (`None` = latest release).
fn fetch_release(tag: Option<&str>) -> Result<Value, String> {
    let path = match tag {
        Some(t) => format!("/repos/{REPO}/releases/tags/{t}"),
        None => format!("/repos/{REPO}/releases/latest"),
    };
    let body = download(&format!("{API_BASE}{path}"))?;
    serde_json::from_slice(&body).map_err(|e| format!("release JSON parse failed: {e}"))
}

/// `browser_download_url` of `checksums.txt` for `tag`, derived from the
/// asset URL pattern `…/releases/download/<tag>/checksums.txt`.
fn checksums_url(tag: &str) -> String {
    format!("https://github.com/{REPO}/releases/download/{tag}/checksums.txt")
}

/// Ask y/N on the terminal. Only called when stdin is a TTY; anything
/// but an explicit yes answer aborts the update.
fn confirm(version: &str) -> Result<bool, String> {
    print!("Update herdcluster to {version}? [y/N] ");
    std::io::stdout()
        .flush()
        .map_err(|e| format!("failed to write prompt: {e}"))?;
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .map_err(|e| format!("failed to read confirmation: {e}"))?;
    Ok(matches!(
        line.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

/// Write `binary` to a temp file next to the running executable,
/// sanity-check it runs (`--version`), chmod +x, then atomically
/// rename it over the current exe. On Unix, renaming over a running
/// binary is safe — the old inode stays alive until the process exits,
/// so daemon children pick up the new version on next restart.
fn install_binary(binary: &[u8], version: &str, verbose: bool) -> Result<(), String> {
    let current_exe = env::current_exe().map_err(|e| format!("current_exe failed: {e}"))?;
    let dir = current_exe
        .parent()
        .ok_or("current executable has no parent directory")?;

    let (tmp, mut staged) = create_staging_file(dir)?;
    let write_result = (|| -> Result<(), String> {
        // `create_new` (O_EXCL) guarantees we own the inode: a
        // pre-planted symlink or file at the staging path fails the
        // open instead of being written through.
        staged
            .write_all(binary)
            .map_err(|e| format!("failed writing {}: {e}", tmp.display()))?;
        staged
            .sync_all()
            .map_err(|e| format!("fsync {} failed: {e}", tmp.display()))?;
        drop(staged);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&tmp)
                .map_err(|e| format!("stat {} failed: {e}", tmp.display()))?
                .permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&tmp, perms)
                .map_err(|e| format!("chmod {} failed: {e}", tmp.display()))?;
        }
        // Sanity check: the staged binary must answer --version before
        // we ever touch the live executable.
        let out = Command::new(&tmp)
            .arg("--version")
            .output()
            .map_err(|e| format!("failed to run staged binary: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "staged binary failed --version check (exit {:?})",
                out.status.code()
            ));
        }
        let reported = String::from_utf8_lossy(&out.stdout);
        crate::commands::vlog(
            verbose,
            format!("staged binary reports: {}", reported.trim()),
        );
        if !reported.contains(version) {
            // Warning only — don't block the update on a version-string
            // mismatch (e.g. dev builds), but surface it.
            eprintln!(
                "warning: staged binary reports {:?}, expected version {version}",
                reported.trim()
            );
        }
        std::fs::rename(&tmp, &current_exe).map_err(|e| {
            format!(
                "failed replacing {} with {}: {e}",
                current_exe.display(),
                tmp.display()
            )
        })?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    write_result
}

/// Create a uniquely-named staging file next to the running executable
/// and return its path plus an open handle. Uses `create_new` so an
/// existing path — including a pre-planted symlink — can never be
/// followed or clobbered; on the rare name collision we retry with a
/// fresh name (timestamp nanos + attempt counter).
fn create_staging_file(dir: &Path) -> Result<(PathBuf, File), String> {
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    for attempt in 0..16u32 {
        let tmp = dir.join(format!(".herdcluster-update-{pid}-{nanos}-{attempt}"));
        match OpenOptions::new().write(true).create_new(true).open(&tmp) {
            Ok(file) => return Ok((tmp, file)),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => continue,
            Err(e) => {
                return Err(format!("failed creating {}: {e}", tmp.display()));
            }
        }
    }
    Err("could not create a unique staging file for update".to_string())
}

fn emit_result(outcome: &Outcome, json: bool) {
    match outcome {
        Outcome::Current { version } => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string(&json!({
                        "status": "current",
                        "version": version,
                        "message": "already up to date",
                    }))
                    .unwrap_or_else(|_| "{}".to_string())
                );
            } else {
                println!("herdcluster {version}: already up to date");
            }
        }
        Outcome::Updated { from, to } => {
            if json {
                println!(
                    "{}",
                    serde_json::to_string(&json!({
                        "status": "updated",
                        "from": from,
                        "to": to,
                    }))
                    .unwrap_or_else(|_| "{}".to_string())
                );
            } else {
                println!("Updated herdcluster {from} → {to}");
                println!("restart any running herdcluster daemons to use the new version");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn target_mapping_covers_supported_platforms() {
        assert_eq!(
            target_for("macos", "aarch64").unwrap(),
            "aarch64-apple-darwin"
        );
        assert_eq!(
            target_for("macos", "x86_64").unwrap(),
            "x86_64-apple-darwin"
        );
        assert_eq!(
            target_for("linux", "x86_64").unwrap(),
            "x86_64-unknown-linux-gnu"
        );
    }

    #[test]
    fn target_mapping_rejects_unsupported_platforms() {
        for (os, arch) in [
            ("linux", "aarch64"),
            ("windows", "x86_64"),
            ("windows", "aarch64"),
            ("freebsd", "x86_64"),
        ] {
            let err = target_for(os, arch).unwrap_err();
            assert!(err.contains("no prebuilt herdcluster binary"), "{err}");
            assert!(err.contains(&format!("{os}-{arch}")), "{err}");
        }
    }

    #[test]
    fn version_tag_normalization_and_comparison() {
        assert_eq!(normalize_tag("0.1.1").unwrap(), "v0.1.1");
        assert_eq!(normalize_tag("v0.1.1").unwrap(), "v0.1.1");
        assert_eq!(version_of_tag("v0.1.1"), "0.1.1");
        assert_eq!(version_of_tag("0.1.1"), "0.1.1");
    }

    #[test]
    fn version_tag_rejects_malformed_input() {
        for bad in [
            "",
            "1",
            "1.2",
            "1.2.3.4",
            "1.2.x",
            "1.2.3-rc1",
            "../releases",
            "1.2.3/../../x",
            "v",
            "v1..2",
            "  ",
        ] {
            assert!(normalize_tag(bad).is_err(), "{bad:?} should be rejected");
        }
        // Whitespace around a valid version is tolerated.
        assert_eq!(normalize_tag("  v0.1.1\n").unwrap(), "v0.1.1");
    }

    #[test]
    fn version_comparison_orders_semver() {
        use std::cmp::Ordering::*;
        assert_eq!(cmp_version("0.1.1", "0.1.2").unwrap(), Less);
        assert_eq!(cmp_version("0.2.0", "0.10.0").unwrap(), Less);
        assert_eq!(cmp_version("1.0.0", "0.9.9").unwrap(), Greater);
        assert_eq!(cmp_version("v0.1.5", "0.1.5").unwrap(), Equal);
        assert_eq!(cmp_version("0.1.5", "v0.1.5").unwrap(), Equal);
        assert!(cmp_version("not-a-version", "0.1.0").is_err());
        assert!(cmp_version("0.1.0", "1.2.3.4").is_err());
    }

    #[test]
    fn asset_selection_from_release_json() {
        let release = json!({
            "tag_name": "v0.1.1",
            "assets": [
                {"name": "checksums.txt", "browser_download_url": "https://x/checksums.txt"},
                {"name": "herdcluster-aarch64-apple-darwin.tar.gz",
                 "browser_download_url": "https://x/mac.tgz"},
                {"name": "herdcluster-x86_64-unknown-linux-gnu.tar.gz",
                 "browser_download_url": "https://x/linux.tgz"},
            ]
        });
        assert_eq!(
            select_asset_url(&release, "herdcluster-aarch64-apple-darwin.tar.gz"),
            Some("https://x/mac.tgz")
        );
        assert_eq!(
            select_asset_url(&release, "herdcluster-x86_64-unknown-linux-gnu.tar.gz"),
            Some("https://x/linux.tgz")
        );
        assert_eq!(
            select_asset_url(&release, "herdcluster-x86_64-pc-windows-msvc.tar.gz"),
            None
        );
    }

    #[test]
    fn checksum_parsing_and_verification() {
        // sha256("hello") — a stable fixture.
        let expected = hex_sha256(b"hello");
        assert_eq!(
            expected,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );

        let checksums = format!(
            "{}  herdcluster-aarch64-apple-darwin.tar.gz\n{} *herdcluster-x86_64-apple-darwin.tar.gz\n",
            expected,
            "0".repeat(64)
        );
        assert_eq!(
            checksum_for_asset(&checksums, "herdcluster-aarch64-apple-darwin.tar.gz"),
            Some(expected.clone())
        );
        assert_eq!(
            checksum_for_asset(&checksums, "herdcluster-x86_64-apple-darwin.tar.gz"),
            Some("0".repeat(64))
        );
        assert_eq!(
            checksum_for_asset(&checksums, "herdcluster-nope.tar.gz"),
            None
        );

        assert!(verify_checksum(b"hello", &expected).is_ok());
        assert!(verify_checksum(b"goodbye", &expected).is_err());
        // Surrounding whitespace in the expected digest is tolerated.
        assert!(verify_checksum(b"hello", &format!("  {expected}\n")).is_ok());
    }

    #[test]
    fn extract_binary_finds_herdcluster_in_tar_gz() {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        {
            let mut builder = tar::Builder::new(&mut enc);
            // A decoy file plus the real binary under a nested dir —
            // file_name matching must still find it.
            let mut header = tar::Header::new_gnu();
            let data = b"#!/bin/sh\necho fake\n";
            header.set_size(data.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            builder
                .append_data(&mut header, "dist/herdcluster", &data[..])
                .unwrap();
            let mut header2 = tar::Header::new_gnu();
            header2.set_size(4);
            header2.set_mode(0o644);
            header2.set_cksum();
            builder
                .append_data(&mut header2, "README", &b"read"[..])
                .unwrap();
            builder.finish().unwrap();
        }
        let archive = enc.finish().unwrap();

        let bin = extract_binary(&archive).unwrap();
        assert_eq!(bin, b"#!/bin/sh\necho fake\n");
    }

    #[test]
    fn extract_binary_errors_when_missing() {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        {
            let mut builder = tar::Builder::new(&mut enc);
            let mut header = tar::Header::new_gnu();
            header.set_size(4);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, "other-bin", &b"data"[..])
                .unwrap();
            builder.finish().unwrap();
        }
        let archive = enc.finish().unwrap();
        assert!(extract_binary(&archive).is_err());
    }

    #[test]
    fn checksums_url_matches_install_sh_pattern() {
        assert_eq!(
            checksums_url("v0.1.1"),
            "https://github.com/quangdang46/herdr_cluster/releases/download/v0.1.1/checksums.txt"
        );
    }

    #[test]
    fn read_capped_accepts_under_limit_and_rejects_over() {
        let ok = read_capped(&b"hello"[..], 5, "test stream").unwrap();
        assert_eq!(ok, b"hello");
        // One byte over the limit is rejected, not truncated.
        let ok = read_capped(&b"hello"[..], 4, "test stream").unwrap_err();
        assert!(ok.contains("exceeds 4 byte limit"), "{ok}");
        let err = read_capped(&b"hello world"[..], 5, "test stream").unwrap_err();
        assert!(err.contains("test stream exceeds 5 byte limit"), "{err}");
    }

    #[test]
    fn staging_file_is_unique_writable_and_owned() {
        let dir = std::env::temp_dir().join(format!("hc-update-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (path1, mut f1) = create_staging_file(&dir).unwrap();
        let (path2, _f2) = create_staging_file(&dir).unwrap();
        assert_ne!(path1, path2);
        assert!(path1.starts_with(&dir));
        assert!(path1
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(".herdcluster-update-"));
        use std::io::Write;
        f1.write_all(b"payload").unwrap();
        drop(f1);
        assert_eq!(std::fs::read(&path1).unwrap(), b"payload");
        // A second open of the same path must fail — create_new owns it.
        assert!(OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path1)
            .is_err());
        let _ = std::fs::remove_file(&path1);
        let _ = std::fs::remove_file(&path2);
        let _ = std::fs::remove_dir(&dir);
    }
}
