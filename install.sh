#!/usr/bin/env bash
#
# herdcluster installer — installs the herdcluster CLI from GitHub releases.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/quangdang46/herdr_cluster/main/install.sh | bash
#   ./install.sh --version            # print the installer version
#
# Environment:
#   HERDCLUSTER_VERSION          pin a release tag, e.g. v0.1.0 (default: latest)
#   HERDCLUSTER_INSTALL_DIR      install directory (default: ~/.local/bin)
#   HERDCLUSTER_REPO             owner/name override (default: quangdang46/herdr_cluster)
#
set -euo pipefail

INSTALLER_VERSION="1.0.0"
REPO="${HERDCLUSTER_REPO:-quangdang46/herdr_cluster}"
INSTALL_DIR="${HERDCLUSTER_INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${HERDCLUSTER_VERSION:-latest}"

info() { printf '==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<'EOF'
Usage: install.sh [--version]

  --version   print the installer script version and exit

Environment:
  HERDCLUSTER_VERSION       pin a release tag (e.g. v0.1.0); default: latest
  HERDCLUSTER_INSTALL_DIR   install directory (default: ~/.local/bin)
  HERDCLUSTER_REPO          GitHub repo override (default: quangdang46/herdr_cluster)
EOF
}

for arg in "$@"; do
    case "$arg" in
        --version|-V)
            echo "herdcluster install.sh ${INSTALLER_VERSION}"
            exit 0
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            die "unknown argument: $arg (try --help)"
            ;;
    esac
done

# --- detect platform -------------------------------------------------------

os="$(uname -s)"
arch="$(uname -m)"

case "$os" in
    Darwin) os="macos" ;;
    Linux)  os="linux" ;;
    *)      die "unsupported OS: $os (only macOS and Linux are supported)" ;;
esac

case "$arch" in
    arm64|aarch64) arch="aarch64" ;;
    x86_64|amd64)  arch="x86_64" ;;
    *)             die "unsupported architecture: $arch" ;;
esac

case "${os}-${arch}" in
    macos-aarch64)
        target="aarch64-apple-darwin"
        ;;
    macos-x86_64)
        target="x86_64-apple-darwin"
        ;;
    linux-x86_64)
        target="x86_64-unknown-linux-gnu"
        ;;
    linux-aarch64)
        die "no prebuilt herdcluster binary for linux-aarch64 yet; build from source (cargo build --release -p herdcluster)"
        ;;
    *)
        die "no prebuilt herdcluster binary for ${os}-${arch}"
        ;;
esac

asset="herdcluster-${target}.tar.gz"

if [ "$VERSION" != "latest" ]; then
    # VERSION is interpolated into the release download URL — restrict it
    # to X.Y.Z or vX.Y.Z (same shapes `herdcluster update --version`
    # accepts) and normalize to the v-prefixed tag GitHub uses.
    bare="${VERSION#v}"
    case "$bare" in
        *[!0-9.]*|*..*|.*|*.|*.*.*.*) die "invalid HERDCLUSTER_VERSION '${VERSION}' — expected 'latest' or vX.Y.Z" ;;
    esac
    [ "$(awk -F. '{print NF}' <<<"$bare")" = "3" ] || \
        die "invalid HERDCLUSTER_VERSION '${VERSION}' — expected 'latest' or vX.Y.Z"
    VERSION="v${bare}"
fi

if [ "$VERSION" = "latest" ]; then
    base="https://github.com/${REPO}/releases/latest/download"
else
    base="https://github.com/${REPO}/releases/download/${VERSION}"
fi

# --- download ---------------------------------------------------------------

command -v curl >/dev/null 2>&1 || die "curl is required but not installed"
command -v tar  >/dev/null 2>&1 || die "tar is required but not installed"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

info "Downloading herdcluster ${VERSION} for ${target}"
info "  ${base}/${asset}"

if ! curl -fsSL --retry 3 -o "${tmpdir}/${asset}" "${base}/${asset}"; then
    die "download failed — does release ${VERSION} exist and include ${asset}?"
fi

# --- verify checksum --------------------------------------------------------

info "Verifying checksum"
if ! curl -fsSL --retry 3 -o "${tmpdir}/checksums.txt" "${base}/checksums.txt"; then
    die "checksum file not found for release ${VERSION}; refusing to install unverified binary"
fi

expected="$(awk -v a="$asset" '$2 == a {print $1}' "${tmpdir}/checksums.txt")"
[ -n "$expected" ] || die "no checksum entry for ${asset} in checksums.txt"

if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "${tmpdir}/${asset}" | awk '{print $1}')"
else
    actual="$(shasum -a 256 "${tmpdir}/${asset}" | awk '{print $1}')"
fi

[ "$actual" = "$expected" ] || die "checksum mismatch for ${asset}: expected ${expected}, got ${actual}"

# --- install ---------------------------------------------------------------

info "Installing to ${INSTALL_DIR}/herdcluster"
mkdir -p "$INSTALL_DIR"
tar -xzf "${tmpdir}/${asset}" -C "$tmpdir"
[ -f "${tmpdir}/herdcluster" ] || die "archive did not contain a herdcluster binary"
install -m 0755 "${tmpdir}/herdcluster" "${INSTALL_DIR}/herdcluster"

# --- post-install ------------------------------------------------------------

case ":${PATH}:" in
    *":${INSTALL_DIR}:"*) ;;
    *)
        info "NOTE: ${INSTALL_DIR} is not on your PATH. Add it with:"
        info "  export PATH=\"${INSTALL_DIR}:\$PATH\""
        ;;
esac

hc_version="$("${INSTALL_DIR}/herdcluster" --version 2>/dev/null | head -n1 || echo 'unknown version')"
info "Installed ${hc_version}"

if [ -x "${INSTALL_DIR}/herdcluster" ]; then
    info "Running 'herdcluster doctor' (non-fatal)..."
    "${INSTALL_DIR}/herdcluster" doctor || \
        info "doctor reported issues — run '${INSTALL_DIR}/herdcluster doctor --fix' for details"
fi

info "Done. Next steps:"
info "  herdcluster init      # on the controller (e.g. your Mac)"
info "  herdcluster join      # on the worker (e.g. WSL2)"
info "See docs/herdcluster/INSTALL.md for the full guide."
