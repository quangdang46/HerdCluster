//! Shared filesystem helpers for cluster-core's file-backed modules.
//!
//! Everything here is `pub(crate)`: identity, registry, and config use these
//! to get atomic writes, private file permissions, and hex/base64 helpers.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process;

/// Write `contents` to `path` atomically: write to a sibling temp file,
/// fsync it, rename over the destination, then best-effort fsync the parent
/// directory so the rename itself is durable.
///
/// The file is created with mode `0600` on Unix (private keys and registry
/// files must not be world-readable). If the temp file already exists its
/// permissions are tightened to `0600` as well.
pub(crate) fn atomic_write(path: &Path, contents: &[u8]) -> io::Result<()> {
    let tmp = temp_path_for(path);
    {
        let mut opts = OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&tmp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
        }
        file.write_all(contents)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    // Best-effort directory fsync so the rename is durable across crashes.
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

/// Sibling temp path for atomic writes, unique per process to avoid
/// clobbering a concurrent writer's temp file.
fn temp_path_for(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".to_string());
    path.with_file_name(format!(".{name}.{}.tmp", process::id()))
}

/// Ensure `dir` exists (and parents), creating it with mode `0700` on Unix.
/// Idempotent.
pub(crate) fn ensure_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(dir)
    }
}

/// Lowercase hex encoding (no separators).
pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Decode a hex string produced by [`hex_encode`]. Accepts upper or lower
/// case. Returns `None` on odd length or non-hex input.
pub(crate) fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let nib = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    };
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.as_chunks::<2>().0 {
        out.push((nib(pair[0])? << 4) | nib(pair[1])?);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_roundtrip() {
        let dir = std::env::temp_dir().join(format!("hc-fsutil-{}", process::id()));
        ensure_dir(&dir).unwrap();
        let path = dir.join("data.bin");
        atomic_write(&path, b"hello").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"hello");
        // Overwrite atomically.
        atomic_write(&path, b"world").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"world");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn hex_roundtrip() {
        let raw = [0xde, 0xad, 0xbe, 0xef, 0x00, 0xff];
        let s = hex_encode(&raw);
        assert_eq!(s, "deadbeef00ff");
        assert_eq!(hex_decode(&s).unwrap(), raw.to_vec());
        assert_eq!(hex_decode("abc"), None);
        assert_eq!(hex_decode("zz"), None);
        assert_eq!(hex_decode("DEADBEEF"), Some(vec![0xde, 0xad, 0xbe, 0xef]));
    }
}
