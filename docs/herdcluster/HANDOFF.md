# HerdCluster — Bring-up guide (Mac controller + WSL2 worker)

Goal: run coding agents on the Windows machine's WSL2 to offload the Mac.
Everything below assumes `herdcluster` is built/installed on both machines.

## 0. Get the binary onto WSL2

Pick ONE:

**A. Release install (after first `v*` tag is published)** — inside WSL2:

```bash
curl -fsSL https://raw.githubusercontent.com/quangdang46/herdr_cluster/main/install.sh | bash
herdcluster --version
```

**B. Build from source in WSL2** (needs Rust: `curl https://sh.rustup.rs -sSf | sh`):

```bash
git clone https://github.com/quangdang46/herdr_cluster && cd HerdCluster
cargo build --release -p herdcluster
# binary: target/release/herdcluster — add to PATH or call directly
```

**C. Copy a prebuilt linux binary** (fastest if you already have one):
`scp`/`cp` the `x86_64-unknown-linux-gnu` binary into WSL2 `~/.local/bin/`.

No herdr install needed on WSL2 — `join` auto-installs the herdr release
binary into the herdcluster state dir.

## 1. Mac side (controller)

```bash
# release binary or: cargo run --release -p herdcluster --
herdcluster init            # or: herdcluster init --name macbook
```

Output gives a **pairing code** (e.g. `T7XJJNF3`) and listens on
`0.0.0.0:47820`. Keep this running — it IS the controller.

macOS may prompt to allow incoming connections on port 47820 — allow it
(System Settings → Network → Firewall, or approve the dialog).

## 2. WSL2 side (worker)

Discovery (mDNS/UDP broadcast) often does NOT cross the WSL2 NAT boundary,
so pass the Mac's LAN IP explicitly:

```bash
# find Mac IP on the Mac: ipconfig getifaddr en0
herdcluster join --controller <MAC_LAN_IP>:47820
# enter the pairing code when prompted (or: HERDCLUSTER_PAIRING_CODE=XXXX herdcluster join ...)
```

`join` will: pair → store credential → install/reuse herdr → start the
worker daemon (daemon keeps running; Ctrl-C stops it).

For persistence across WSL reboots:

```bash
herdcluster autostart install   # systemd user unit
# WSL2 note: user services need lingering — run once:
sudo loginctl enable-linger $USER
```

## 3. Verify

On the Mac (same state dir as init — i.e. normal `herdcluster`):

```bash
herdcluster machines            # mac.local / worker label should show online + CPU/MEM
herdcluster doctor              # layered checks; --fix for safe remediations
herdcluster agent list --all    # agents on BOTH machines, ids like mac.local/xxx
```

## 3b. Cluster config overrides

Per-machine caps live in `cluster.json` (`machines.<label>.max_agents`,
`.kinds_override`) and are editable without hand-editing JSON:

```bash
herdcluster config get machines.wsl2.max_agents
herdcluster config set machines.wsl2.max_agents 6
herdcluster config unset machines.wsl2.kinds_override
```

Invalid values (e.g. `max_agents > 8`, blank kinds) are rejected at write
time and flagged by `herdcluster doctor` ("machine caps" check).

## 4. Daily use

```bash
herdcluster spawn fix-auth --kind claude --machine auto --repo /home/you/project
herdcluster spawn fix-auth --kind claude --machine auto   # --repo optional
herdcluster agent prompt mac.local/fix-auth "implement JWT auth"
herdcluster agent read mac.local/fix-auth
herdcluster agent list --all
```

- `--machine auto` picks the worker with the most free RAM/slots.
- `--repo` is a path **on the machine that runs the agent** (WSL path for
  worker-spawned agents). It is optional:
  - Omit it to reuse the target machine's existing workspace when exactly
    one exists; otherwise a new workspace is created in the herdr server's
    default directory (the server user's home).
  - `--repo ~` (or `~/sub/dir`) means the target's home directory. On
    remote workers the home dir isn't discoverable via the API, so `~`
    falls back to the same default-placement behavior as omitting
    `--repo`; pass absolute remote paths for anything else.
- Agent CLIs (claude, codex…) must be installed on WSL2 — `herdcluster
  doctor` and the join output list which kinds are missing and how to
  install them. HerdCluster never handles agent logins — run e.g.
  `claude login` once inside WSL2.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `join` can't find controller | Use `--controller <mac-ip>:47820`; check Mac firewall allows 47820 |
| Paired but worker offline | `herdcluster doctor` on Mac; on WSL2 run `herdcluster worker` and read logs |
| Agents missing on worker | Install the agent CLI in WSL2 (`npm i -g …`), then `herdcluster doctor` |
| `agent list --all` missing worker rows | Worker daemon not running → `herdcluster worker` or `autostart install` |
| Revoked worker can't reconnect | `herdcluster revoke <label>` on Mac removes it; re-run `join` to re-pair |

State dirs: Mac `~/Library/Application Support/herdcluster/`,
WSL2 `~/.config/herdcluster/` (override with `HERDCLUSTER_STATE_DIR`).

## Verified so far (loopback e2e, single host)

Two `herdcluster` state dirs on one machine + real `herdr 0.9.5` verified:
pairing → TLS → worker online with CPU/MEM/kinds → `agent list --all` →
`doctor` green → `spawn --kind claude --machine auto` completes and returns
a global ID; `agent read` shows the live claude UI (`agent_status:
blocked` at the trust/login prompt — spawn treats post-launch interactive
states as ready). This surfaced and fixed several herdr-0.9.5-shape and
transport bugs (see git log around `baf0b0b`, `1a25fce`).

Still needs a real two-machine run: `m3-services-47p.10` (Mac↔WSL2
pairing), `m5-fleet-tnl.5` (spawn + prompt/read on both machines with an
authenticated agent CLI), `m7-doctor-hzs.3` (fresh-machine acceptance).
