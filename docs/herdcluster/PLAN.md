# HerdCluster — Detailed Implementation Plan (Option B: Pure Wrapper)

> Status: DRAFT v2 · 2026-11-23 — supersedes v1 (in-crate fork approach, rejected)
> Canonical source for the beads under `.beads/`. Beads must stay self-contained.

## 1. Destination statement

For a developer running AI coding agents on a Mac who also owns a Windows PC
with WSL2 Ubuntu, deliver **HerdCluster**: a standalone `herdcluster` CLI
that turns the Mac into a controller and the WSL2 machine into an autonomous
worker, so coding agents (Claude Code, Codex, OpenCode, …) can be spawned,
prompted, watched, and reviewed across **both machines from one command
surface** — with zero SSH/Tailscale/IP configuration by the user.

**Architecture decision (2026-11-23): Option B — pure wrapper.**
HerdCluster does NOT contain herdr source code and does NOT modify herdr.
It is a separate program that speaks herdr's published API (newline-delimited
JSON over a local socket) on the controller, and tunnels the same byte stream
to worker machines over an encrypted cluster connection. herdr remains the
unmodified engine on every machine; workers install the latest herdr release
binary automatically.

Primary goal: **reduce load on the Mac** by executing agent processes, builds,
and tests on the worker. Secondary goal: **more isolated environments**
(per-machine toolchains, deps, OS images).

Success evidence (MVP):
- `herdcluster init` on Mac + `herdcluster join` on WSL2 → paired cluster on
  the same LAN, verified by `herdcluster doctor` and `herdcluster machines`.
- `herdcluster spawn claude --machine wsl --repo <path>` runs `claude` inside
  WSL2 while CPU/RAM on Mac stay flat.
- `herdcluster agent list --all` shows agents from both machines with stable
  global IDs.

Non-goals (MVP): internet/NAT-traversal pairing, relay server, Windows-native
worker (WSL2 only), TUI dashboard, mobile apps, modifying herdr itself,
`herdr --machine` parity for cluster workers (cluster commands live in
`herdcluster` only).

Safety invariants:
- Pairing tokens are single-use, short-lived, and never stored long-term.
- Worker credentials are per-worker Ed25519 keypairs; revocation kills access.
- No inbound ports required on the worker (worker dials out only).
- herdr API is consumed read/write as documented; herdcluster must degrade
  gracefully on API schema drift (protocol version check + clear errors).
- Agent CLI credentials (claude login, API keys) are NEVER handled by
  HerdCluster — detection and hints only.
- Apache-2.0: this repo contains only original code; herdr is consumed as a
  binary dependency, no source copied.

## 2. Verified research findings (herdr API surface, inspected 2026-11-23)

Source inspected: upstream `herdrdev/herdr` @ master (public repo; analysis
only — no source is vendored into this repo).

### 2.1 The API contract herdcluster builds on

| Fact | Evidence (upstream paths, for reference only) |
|---|---|
| API transport = **newline-delimited JSON** over a local socket/pipe: client writes `serde_json(request) + "\n"`, reads one JSON line response | `src/api/client.rs:31,186-187` |
| Request shape: `{id, method, params}`; response: success value or `{error:{code,message}}` | `src/api/schema.rs` |
| ~100 methods: `agent.*` (list/get/read/wait/prompt/send_keys/start/rename/focus/explain), `workspace.*`, `worktree.*`, `tab.*`, `pane.*`, `server.*`, `status.server`, `api.snapshot`, `server.agent_manifests` | `src/api/schema.rs:46-125` |
| `agent.start` requires `pane_id` + `kind` + `name` (+ optional args/timeout) — a pane must exist first | `src/api/schema/agents.rs:184-197` |
| Spawn flow = compose calls: `workspace.create --cwd` → pane → `agent.start` | `src/cli/agent.rs:253-347` |
| Supported agent kinds queryable live via `server.agent_manifests`; 22 manifests ship upstream (claude, codex, opencode, gemini, cursor…) | `src/api/schema.rs:51`, `src/detect/manifests/` |
| Protocol version handshake exists (`status.server` returns protocol; clients guard mismatch) | `src/cli.rs:790-807` |
| `herdr machine add` (SSH) already auto-installs remote herdr from **release assets** per detected platform — same machinery herdcluster reuses conceptually for worker provisioning | `src/remote/attach.rs` (`SavedSshSetup`, `RemoteHerdr`, `InstallSource`) |
| `remote-api-bridge` command = connects to local herdr API socket and bridges stdio — proof that the API socket is reachable headlessly | `src/remote.rs:15-39` |
| API socket path is platform-specific (unix socket / Windows named pipe), discoverable via herdr state dir; `HERDR_SOCKET_PATH` env override exists | `src/api/`, `src/config/io.rs` |

### 2.2 What Option B deliberately does NOT do

- No herdr source in this repo (repo = cluster code only).
- No modification of herdr behavior — engine upgrades flow through official
  releases; workers always run the latest herdr.
- No `herdr --machine <cluster-worker>` — cluster workers are addressed only
  through `herdcluster` commands. `herdr machine` remains SSH-only.
- No TUI/endpoint-catalog integration — herdcluster keeps its own worker
  registry (`~/.config/herdcluster/` or platform equivalent).

### 2.3 Consequences accepted

- herdcluster must implement its own minimal API client (NDJSON — ~100
  lines) and track the API schema version it was built against.
- If upstream changes method shapes, herdcluster pins a compatible range and
  reports a clear "update herdcluster" error (compat bead in M8).
- Long-running `agent.wait` flows over the cluster stream need flow control
  — the mux must not buffer unboundedly (covered in M2 tests).

## 3. Architecture

### 3.1 System context

```
┌──────────────────────────────┐         ┌──────────────────────────────┐
│ Mac — controller             │         │ Windows PC — WSL2 worker    │
│                              │         │                              │
│ herdcluster (one binary)     │  LAN    │ herdcluster worker (same     │
│  ├─ init: cluster service    │◄───────►│      binary, worker mode)    │
│  ├─ join: (worker mode too)  │ mDNS +  │  ├─ discovery client          │
│  ├─ doctor/machines/spawn/   │ TLS 1.3 │  ├─ pairing client            │
│  │   agent commands          │ (dial-  │  ├─ connection manager         │
│  ├─ herdr API client         │  out)   │  ├─ API proxy: cluster stream │
│  │   (NDJSON, local socket)  │         │  │   ↔ local herdr API socket │
│  └─ worker registry (JSON)   │         │  ├─ resource monitor          │
│                              │         │  └─ herdr runtime supervisor  │
│ local herdr API socket ◄─────┤         │      (installs release asset, │
│ (herdr runs as today)        │         │       starts herdr server)   │
└──────────────────────────────┘         │                              │
                                         │ coding agents run HERE:      │
                                         │ claude, codex, opencode…     │
                                         └──────────────────────────────┘
```

### 3.2 Core design: tunnel the API socket, reuse nothing else

The single most important choice: **the cluster transport carries raw bytes
of the herdr API socket protocol**. On the worker, herdcluster opens a local
connection to the herdr API socket (unix socket on Linux/macOS) and pipes
bytes bidirectionally over a cluster stream. On the controller, each accepted
local client connection gets a matching cluster stream.

This means:
- herdcluster's API client is written ONCE and works identically for local
  herdr and every worker — a worker is just "another API socket reachable
  through a tunnel".
- No subprocess spawning per command; `agent.wait` long-polls are just a
  long-lived NDJSON exchange.
- herdr can be upgraded independently on every machine.

### 3.3 Crate layout (Cargo workspace — decided 2026-11-23)

HerdCluster is a **Cargo workspace of focused crates**, not a single flat
`src/`. Crate boundaries follow the architecture layers so each can be
tested and depended on independently:

```
Cargo.toml                    # [workspace] members = crates/*
crates/
├── herdcluster/              # [[bin]] — argv dispatch + CLI only (thin)
├── api-client/               # herdr NDJSON API client: socket discovery,
│                             #   request/response, errors, compat guard,
│                             #   typed method wrappers
├── cluster-core/             # pure logic, no I/O deps: identity (Ed25519),
│                             #   pairing FSM, wire protocol codec, worker
│                             #   registry, config, scheduler scoring
├── cluster-net/              # TLS 1.3 (rustls), stream mux, connection
│                             #   manager (dial-out/heartbeat/reconnect),
│                             #   LAN discovery (mDNS + UDP broadcast)
├── cluster-bridge/           # local unix socket ↔ cluster stream ↔ herdr
│                             #   API socket proxy (both directions)
├── cluster-fleet/            # global agent IDs, fan-out queries, agent ops
│                             #   routing, spawn composer, machines view
└── cluster-node/             # daemons: controller service (init), worker
                              #   daemon (join/worker), herdr runtime
                              #   supervisor (release-asset install),
                              #   resource monitor, doctor checks
```

Dependency direction (enforced, no cycles):
`herdcluster` → `cluster-node`/`cluster-fleet`/`api-client` →
`cluster-bridge`/`cluster-net`/`cluster-core`.
`cluster-core` depends on nothing internal. `api-client` is standalone
(only needs socket path + NDJSON).

Language: **Rust** (single static-ish binary per crate, no runtime deps —
right fit for non-tech install on macOS + WSL2). Workspace crates keep
dependency boundaries explicit. Deps kept minimal: rustls, mdns-sd (or raw
UDP), serde/serde_json, clap, tokio or std-threads (match simplicity needs).

### 3.4 Worker registry (replaces catalog v2)

HerdCluster owns `~/.config/herdcluster/` (platform state dir):

```jsonc
// cluster.json — created by `init`
{ "cluster_id": "…", "name": "macbook", "port": 47820,
  "protocol_version": 1, "created_at": "…" }

// workers.json — controller-side registry, updated live
{ "workers": [
  { "worker_id": "…", "label": "desktop-win", "pubkey": "…",
    "paired_at": "…", "last_seen": "…", "status": "online|offline|degraded",
    "resources": { "cpu_cores": 8, "mem_total_mb": 16384,
                   "mem_available_mb": 5120, "agent_count": 0,
                   "max_agents": 4, "kinds": ["claude","codex"] } }
] }
// credentials/ — per-worker Ed25519 keys, 0600, never exported
```

No herdr catalog involvement. `herdcluster machines` merges this registry
with optional SSH-machine info read-only (via `herdr machine list --json`
subprocess, display-only — herdcluster does not manage SSH machines).

### 3.5 Transport & security protocol

MVP scope: **same LAN only** (confirmed). Transport trait leaves room for
WAN relay later.

- **Discovery**: mDNS browse/advertise `_herdcluster._tcp.local.` via
  `mdns-sd`; UDP broadcast fallback on `47820/udp` for mDNS-filtered LANs.
  WSL2 caveat: default NAT may not forward multicast → also send directed
  broadcast via Windows host; document mirrored networking; `--discover`
  debug flag lists candidates. Manual address remains an escape hatch.
- **Pairing**: `init` generates cluster keypair + 8-char code (~40 bits,
  unambiguous alphabet, 5-min TTL, single-use, constant-time compare,
  rate-limited). `join` discovers controller → `PairHello{code, worker
  pubkey, hostname, caps}` → controller shows "Worker 'X' wants to join —
  accept?" → `PairAccept{cluster cred, CA fingerprint}` → worker stores
  credential (0600). Post-pairing auth = Ed25519-signed challenge over TLS.
- **TLS**: TLS 1.3 via `rustls`; cluster CA generated at `init`; worker pins
  CA fingerprint from pairing; mutual auth via worker keypair signature.
- **Multiplexing**: one TLS conn per worker; minimal length-prefixed mux
  (stream_id + len frames; stream 0 = control/protocol messages, 1..N = API
  byte streams). Per-stream buffer cap 1 MiB, max 64 streams/conn.
- **API bridging**: each local client conn → `ApiStreamOpen` → worker opens
  conn to local herdr API socket → `ApiStreamData` chunks ↔ `ApiStreamClose`.
  Worker needs herdr server running (supervisor ensures it).
- **Reconnect**: heartbeat 5s, degraded after 3 misses, offline after 30s;
  worker backoff 0.5s→30s jittered; controller refuses new spawns to offline
  workers; in-flight streams error as `worker_unreachable`.

### 3.6 Fleet aggregation & global IDs

- Global agent ID: `{machine-label}/{agent-name-or-terminal-id}`.
  Resolution: query local herdr API first; on miss, query workers in
  parallel (short timeout); unique hit wins; ambiguity → error listing
  candidates; offline worker → typed `worker_unreachable`.
- `agent list --all`: parallel `agent.list` to local + enabled workers,
  merge with `machine` field; offline workers → `{machine, status:
  "unreachable"}` placeholder rows; `--json` stable schema.
- `agent get/read/wait/prompt/send-keys/rename` accept global IDs;
  `wait` stays per-machine (no cross-machine wait in MVP).
- Worker status surfaces in `machines --json` for scripting.

### 3.7 Scheduler (`--machine auto`)

Worker-reported resources (heartbeat `Resources` msg every 15s + on-change):
`cpu_cores, mem_total_mb, mem_available_mb, agent_count, max_agents,
load_avg, kinds[]`. Local machine reports the same shape (herdcluster queries
local herdr API + host stats).

Policy v1:
1. Filter: online + enabled + supports kind + `agent_count < max_agents`.
2. Score: mem headroom per slot, tie-break lowest agent_count, then label.
3. Local machine competes equally.
4. Caps: default `max_agents=4`, hard ceiling 8, per-machine override in
   `cluster.json` → `machines.<label>.max_agents`.
5. No candidate → error listing per-machine rejection reasons.

### 3.8 Spawn helper

`herdcluster spawn <NAME> --kind <KIND> --machine <label|auto> --repo <path>
[--branch <b>] [--agent-arg ...] [--json]`:

1. Resolve machine → API endpoint (local socket or worker bridge).
2. `workspace.list` → find by `cwd`, else `workspace.create {cwd}`.
   (`--repo` is a path **on the target machine** — documented.)
3. Ensure dedicated pane (workspace root pane or `pane.split`; default:
   one pane per agent for clean read/wait semantics).
4. `agent.start {name, kind, pane_id}` → wait `interactive_ready`.
5. Print global ID + machine + pane; `--json` for scripting.
   Timeout default 35s. `--worktree` flag = later option.

Pre-flight: verify kind supported on target (`server.agent_manifests` or
reported `kinds[]`); verify agent CLI present (worker resource report) —
clear error naming the missing piece, never silent failure.

### 3.9 Doctor

`herdcluster doctor [--fix] [--machine <label>] [--json]` checks, in order:
cluster config → discovery → controller reachable / workers registered →
auth/session → TLS handshake → herdr server on worker → protocol version
compat → per-kind agent CLI on PATH → repo path exists → resources
(disk >2 GB, mem) → worker daemon health.

Plain-language `✓/✗/!` lines + reason + fix hint. `--fix` remediates safely
(restart daemon, guided re-pair, reload manifests, fix perms) — never
silently, `--yes` for non-interactive. `--verbose` for raw logs.

### 3.10 CLI surface

```
herdcluster init [--name <cluster-name>] [--no-skills]
herdcluster join [--controller <hint>] [--yes] [--json]
herdcluster worker                     # daemon mode (auto after join)
herdcluster doctor [--fix] [--machine <m>] [--json]
herdcluster machines [--json]
herdcluster spawn <name> --kind <k> --machine <m|auto> --repo <path>
herdcluster agent list [--all] [--json]
herdcluster agent get|read|wait|prompt|send-keys|rename <global-id> ...
herdcluster status [--all] [--json]
herdcluster skills install|status      # orchestration skill provisioning
herdcluster config get|set ...
```

## 4. Alternatives considered

- **ADR-001 Option A (in-crate fork of herdr source)** — REJECTED 2026-11-23:
  user prefers zero modification of herdr code; B gives smaller repo, no
  upstream merge burden, workers always on latest herdr release. Cost
  accepted: no `herdr --machine` parity, no TUI endpoint integration.
  Revisit only if TUI integration becomes a requirement.
- **ADR-002 Transport**: worker-dial-out TLS+TCP over LAN chosen over SSH
  (inbound+keys UX), Tailscale (external account), QUIC (dep weight),
  plain TCP (prompts traverse LAN).
- **ADR-003 Multiplexing**: minimal length-prefixed mux over one TLS conn
  over yamux (dep weight) and TCP-per-call (handshake cost on long-polls).
- **ADR-004 Language**: Rust — single binary, no runtime, matches user
  ecosystem; API client is small enough that language isn't the risk.
- **ADR-005 API strategy**: consume published NDJSON socket API + pin
  compatible schema range over subprocess-shelling `herdr` CLI (slow,
  lossy) and over vendoring herdr source (rejected, ADR-001).
- **ADR-006 Worker runtime install**: release assets (OQ-2 confirmed) —
  upstream `herdrdev/herdr` releases until HerdCluster ships its own;
  reuse the platform-detect + asset-download approach (implemented
  fresh in `cluster/runtime.rs`, not copied code).
- **ADR-007 Discovery**: mDNS primary + UDP broadcast fallback; manual
  override escape hatch only.

## 5. Milestones & bead mapping

| M | Milestone | Exit criteria |
|---|-----------|---------------|
| M0 | Plan + beads (this doc) | plan committed, beads created, no cycles |
| M1 | Cluster core: identity, pairing FSM, protocol codec, registry, config | unit tests green; no network |
| M2 | Transport: TLS, mux, conn manager, discovery, API bridge | loopback pair+stream test |
| M3 | Services: init/join/worker/CLI/machines + skill & agent provisioning | real Mac↔WSL2 pair online |
| M4 | herdr API client + method wrappers + compat guard | all needed methods work vs real herdr |
| M5 | Fleet UX: global IDs, list --all, agent ops, spawn | spawn claude/codex cross-machine e2e |
| M6 | Scheduler `--machine auto` + caps | deterministic placement tests + live demo |
| M7 | `doctor` + `--fix` | fresh WSL2 → all green unattended |
| M8 | Packaging: install script, release CI, herdr-compat tracking | curl-install both OS; tagged release |

## 6. Key risks & mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| WSL2 NAT blocks mDNS/broadcast | Med | mirrored-networking doc + UDP broadcast + Windows-host relay; `--discover` diagnostics |
| herdr API schema drift | Med | protocol-version guard, pinned compat range, M8 compat-tracking bead, clear errors |
| TLS/mux bugs = stream corruption | Med | frame checksums/seq, property tests, fault injection |
| Worker offline mid-`agent.wait` | Med | typed `worker_unreachable`; CLI guidance; resume = re-issue |
| Agent CLI missing on worker | High (expected) | provisioning bead + doctor per-kind checks + spawn pre-flight |
| Repo path semantics differ mac↔wsl | Med | `--repo` documented target-local; `--repo-map` later if needed |
| rustls/mdns dep weight | Low | pin versions ≥7 days old; licenses Apache/MIT/ISC ok |
| Long-poll flow control bugs | Med | per-stream caps + backpressure tests in M2 |

## 7. Verification strategy

- Unit: identity sign/verify, pairing FSM, frame codec, registry schema,
  scheduler scoring, global-ID resolution, NDJSON client framing.
- Contract: API client against real local herdr server (integration test
  gated on `herdr` binary presence); bridge socket indistinguishable from
  direct API socket to the client.
- Integration: loopback controller+worker over TCP+TLS on 127.0.0.1;
  fault injection (kill mid-stream, partition, replay, oversize frames).
- E2E manual: real WSL2 join → spawn claude/codex → prompt/wait/read;
  Mac load measurement before/after.
- Perf budget: bridge adds <5 ms p50 per API call on LAN; spawn end-to-end
  <35 s incl. pane+detect.

## 8. Decisions log

- **Architecture — DECIDED 2026-11-23**: Option B pure wrapper (no herdr
  source, no herdr modification). Supersedes v1 plan.
- **OQ-1 Cluster name default**: hostname (`init --name` override).
  *PROPOSED.*
- **OQ-2 Worker herdr install source — DECIDED**: release assets; upstream
  `herdrdev/herdr` releases until own releases exist (ADR-006).
- **OQ-3 Binary layout — DECIDED**: single `herdcluster` binary (contains
  controller+worker+CLI modes); herdr installed separately per machine.
- **OQ-4 Cluster service port — DECIDED**: `47820/TCP` + `47820/UDP`.

## 9. Next action

M1 entry beads: `herdcluster-m1-core-*.1` (identity) and `.4` (registry).
Beads encode M1–M8 with dependency graph; `br ready` lists entry points.
