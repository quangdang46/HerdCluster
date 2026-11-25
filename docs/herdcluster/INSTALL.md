# Installing HerdCluster

HerdCluster ships as a single `herdcluster` binary. Releases are published on
GitHub with per-platform tarballs and a `checksums.txt`.

## Quick install (macOS / Linux)

```sh
curl -fsSL https://raw.githubusercontent.com/quangdang46/herdr_cluster/main/install.sh | bash
```

The installer detects OS/arch, downloads the matching release asset,
verifies its SHA-256 against `checksums.txt`, installs to
`~/.local/bin` (override with `HERDCLUSTER_INSTALL_DIR`), and runs
`herdcluster doctor` as a non-fatal smoke check.

Options:

```sh
HERDCLUSTER_VERSION=v0.1.0 ./install.sh   # pin a release
HERDCLUSTER_INSTALL_DIR=/usr/local/bin ./install.sh
./install.sh --version                     # installer version
```

Published targets:

| Platform            | Asset                                   |
|---------------------|------------------------------------------|
| macOS Apple Silicon | `herdcluster-aarch64-apple-darwin.tar.gz` |
| macOS Intel         | `herdcluster-x86_64-apple-darwin.tar.gz`  |
| Linux x86_64 (WSL2) | `herdcluster-x86_64-unknown-linux-gnu.tar.gz` |

Linux aarch64 has no prebuilt binary yet — build from source with
`cargo build --release -p herdcluster`.

## WSL2 worker setup

HerdCluster workers dial **out** to the controller — no inbound ports, no
SSH keys, no Tailscale required.

1. Install inside WSL2 (Ubuntu), not Windows:

   ```sh
   curl -fsSL https://raw.githubusercontent.com/quangdang46/herdr_cluster/main/install.sh | bash
   ```

2. Make sure `~/.local/bin` is on `PATH` inside WSL (the installer prints a
   hint if it isn't).

3. Pair with the controller on your Mac (same LAN):

   ```sh
   herdcluster join        # auto-discovers the controller via mDNS/broadcast
   ```

   Or point at a specific controller: `herdcluster join --controller <host>:<port>`.

4. Install boot persistence (systemd user unit):

   ```sh
   herdcluster autostart install
   herdcluster autostart status
   ```

   On WSL this writes `~/.config/systemd/user/herdcluster-worker.service`.
   Enable lingering if you want it to run without an active WSL session:
   `sudo loginctl enable-linger $USER` (requires systemd inside WSL — see
   below).

5. Verify from either side:

   ```sh
   herdcluster machines
   herdcluster doctor
   herdcluster agent list --all
   ```

### WSL2 networking caveat

Discovery uses mDNS (`_herdcluster._tcp.local.`, UDP 47820) with a UDP
broadcast fallback. If `herdcluster join` can't find the controller:

- **Mirrored networking mode** (Windows 11 22H2+): WSL shares the Windows
  host's network interfaces, which generally makes LAN discovery *easier*.
  Enable it in `%USERPROFILE%\.wslconfig`:

  ```ini
  [wsl2]
  networkingMode=mirrored
  ```

  then `wsl --shutdown` and reopen. Note mirrored mode can break some VPNs.

- **NAT mode (default)**: broadcast packets may not cross the WSL NAT. The
  UDP broadcast fallback usually still works on the same LAN; if not, pass
  `--controller <mac-ip>:47820` explicitly. You can find the Mac's LAN IP
  with `ipconfig getifaddr en0` on macOS.

- **Firewall**: the controller listens on TCP+UDP `47820`. Allow it if
  prompted (macOS: System Settings → Network → Firewall; Linux:
  `sudo ufw allow 47820`).

### herdr on the worker

The worker's herdr runtime is provisioned automatically by herdcluster —
it downloads the latest `herdr` release binary from `herdrdev/herdr`
(GitHub releases) on first run; herdcluster does **not** ship herdr itself.
You do not need to install herdr manually, though having it pre-installed is
fine (a compatible system herdr is reused; incompatible ones fall back to
the downloaded release binary).

### Agent CLIs

HerdCluster never handles agent credentials. Install and log in to agent
CLIs (Claude Code, Codex, OpenCode, …) inside WSL yourself; `herdcluster
doctor` reports which agent kinds are detected and prints install hints.

### Uninstall

```sh
herdcluster autostart uninstall   # remove the systemd/launchd entry first
rm "$(command -v herdcluster)"
```

State lives in `~/.config/herdcluster/` (Linux/WSL) or
`~/Library/Application Support/herdcluster/` (macOS) — remove it to wipe
pairing and registry state.
