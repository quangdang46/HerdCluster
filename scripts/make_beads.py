#!/usr/bin/env python3
"""Create HerdCluster beads (Option B: pure wrapper, Cargo workspace). Run: python3 scripts/make_beads.py"""
import json
import subprocess
import sys

BR = ["br", "create"]
DEP = ["br", "dep", "add"]

def run(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        print(f"FAIL {' '.join(cmd)}\n{r.stderr}", file=sys.stderr)
        sys.exit(1)
    return r.stdout.strip()

BEADS = []
def b(slug, title, typ="task", pri=1, parent=None, deps=(), desc=""):
    BEADS.append(dict(slug=slug, title=title, type=typ, pri=pri,
                      parent=parent, deps=list(deps), desc=desc))

CTX = """CONTEXT: HerdCluster (Option B — pure wrapper, decided 2026-11-23) is a
STANDALONE Rust Cargo WORKSPACE that clusters coding agents across machines
WITHOUT herdr source code and WITHOUT modifying herdr. Canonical plan:
docs/herdcluster/PLAN.md.

CRATE LAYOUT (workspace, dependency direction is one-way, no cycles):
  crates/herdcluster    [[bin]] argv dispatch + CLI only (thin)
  crates/api-client     herdr NDJSON API client (socket discovery, request/
                        response, errors, compat guard, typed methods)
  crates/cluster-core   pure logic: identity(Ed25519), pairing FSM, wire
                        protocol codec, worker registry, config, scheduler
  crates/cluster-net    TLS1.3(rustls), stream mux, conn manager
                        (dial-out/heartbeat/reconnect), LAN discovery
  crates/cluster-bridge local unix socket <-> cluster stream <-> herdr API
                        socket proxy
  crates/cluster-fleet  global agent IDs, fan-out, agent ops, spawn composer
  crates/cluster-node   daemons: controller(init), worker(join/worker),
                        herdr runtime supervisor, resource monitor, doctor
RULE: herdcluster -> cluster-node/fleet/api-client -> bridge/net/core.
cluster-core depends on nothing internal. api-client standalone.

ARCHITECTURE: herdcluster speaks herdr's published API — newline-delimited
JSON (NDJSON) over a local socket: write serde_json(request)+"\\n", read one
JSON line. Request = {id, method, params}; ~100 methods (agent.*, workspace.*,
pane.*, server.agent_manifests, status.server...). `agent.start` needs
pane_id+kind+name — spawn composes workspace.create -> pane -> agent.start.
Worker = herdcluster daemon that installs latest herdr RELEASE BINARY
(upstream herdrdev/herdr releases) and proxies cluster streams to the local
herdr API socket. Mac = controller (init). WSL2 = worker (join). UX target:
`init` / `join` / `doctor` with ZERO SSH/IP/key knowledge.

KEY FACTS (verified upstream, reference only — no source vendored):
- API socket path is platform-specific (unix socket / Windows named pipe),
  discoverable via herdr state dir; HERDR_SOCKET_PATH env override exists.
- Protocol version handshake via status.server; clients must guard mismatch.
- 22 agent kinds via server.agent_manifests.
- Upstream `machine add` SSH path proves release-asset auto-install pattern.

RULES: this repo contains ONLY original cluster code. Never vendor/copy herdr
source. Apache-2.0/MIT original code. Deps pinned, prefer versions >7 days old.
Worker dials OUT only (no inbound ports). Pairing tokens single-use/short-
lived. Agent CLI credentials are NEVER handled — detect + hint only.
"""

# ============ EPIC M1: cluster core library ============
b("m1-core", "M1: cluster-core crate (identity, pairing, protocol, registry, config, scheduler)", "epic", 1, None, (),
  "crates/cluster-core: pure-logic crate (no I/O deps beyond std) with\n"
  "identity, pairing FSM, wire codec, registry, config, scheduler scoring.\n"
  "Unit tests everywhere.\n" + CTX)

b("m1-identity", "Ed25519 cluster/worker identity module", "feature", 1, "m1-core", (),
  "crates/cluster-core/src/identity.rs: generate/load/store Ed25519 keypairs\n"
  "for cluster (controller) and worker identities under herdcluster state\n"
  "dir. Fingerprint = first 16 hex of SHA-256(pubkey) — short human ID in\n"
  "`machines`. Sign/verify challenge responses for post-pairing auth.\n"
  "Files 0600, atomic writes, corrupt-file detection. Unit tests: keygen\n"
  "roundtrip, tamper detection, fingerprint stability, wrong-key reject.\n" + CTX)

b("m1-pairing", "Pairing protocol state machine", "feature", 1, "m1-core", ["m1-identity"],
  "crates/cluster-core/src/pairing.rs: one-time 8-char code (Crockford-\n"
  "style alphabet: no 0/O/1/I/L, ~40 bits), 5-min TTL, single-use,\n"
  "constant-time compare, rate-limit (max 5 attempts -> 60s lockout,\n"
  "confirm timeout 120s). States: AwaitingJoin -> PendingConfirm ->\n"
  "Approved/Rejected/Expired. Controller confirm callback; worker wait\n"
  "timeout. Pure FSM — inject clock + rng. Tests: expiry, replay, lockout,\n"
  "confirm/deny, code entropy (no ambiguous chars).\n" + CTX)

b("m1-protocol", "Cluster wire protocol codec", "feature", 1, "m1-core", ["m1-identity"],
  "crates/cluster-core/src/protocol.rs: length-prefixed frames with\n"
  "protocol_version field. Messages: PairHello/PairAccept/PairReject,\n"
  "WorkerHello{pubkey,hostname,caps,resources,protocol_version},\n"
  "ApiStreamOpen{stream_id}/ApiStreamData{stream_id,bytes}/\n"
  "ApiStreamClose{stream_id,half_close}, Heartbeat{seq},\n"
  "Resources{cpu_cores,mem_total_mb,mem_available_mb,agent_count,\n"
  "max_agents,load_avg,kinds[]}, Event{kind,payload}. Stream 0 = control;\n"
  "data streams carry raw herdr API bytes, per-stream ordering guaranteed,\n"
  "half-close propagates FIN. Property tests: roundtrip, truncation/garbage,\n"
  "16MiB frame cap, version mismatch reject.\n" + CTX)

b("m1-registry", "Worker registry schema + persistence", "feature", 1, "m1-core", (),
  "crates/cluster-core/src/registry.rs + state layout: cluster.json\n"
  "{cluster_id,name,port,protocol_version,created_at}; workers.json\n"
  "{version,workers:[{worker_id,label,pubkey,paired_at,last_seen,status,\n"
  "resources{...}}]}. Schema-versioned, atomic writes (tmp+rename+fsync),\n"
  "0600 perms, corrupt-file quarantine (rename .bad, start fresh, warn).\n"
  "Status FSM online/offline/degraded with last_seen TTL. Registry is\n"
  "HerdCluster-owned — do NOT touch herdr's endpoint catalog. Tests:\n"
  "migration, corruption, concurrent update safety.\n" + CTX)

b("m1-config", "Cluster config + state dir layout", "task", 1, "m1-core", ["m1-identity"],
  "crates/cluster-core/src/config.rs: state_dir()/herdcluster/{identity.key,\n"
  "cluster.json,workers.json,credentials/,runtime/}. cluster.json: name\n"
  "(default hostname), port 47820, protocol_version=1. machines.<label>.\n"
  "{max_agents,kinds_override}. Env overrides HERDCLUSTER_PORT/\n"
  "HERDCLUSTER_STATE_DIR for tests. Platform state-dir resolution helper.\n"
  "Tests: create/load/corrupt/env-override.\n" + CTX)

b("m1-scheduler", "Scheduler scoring (pure logic)", "feature", 1, "m1-core", (),
  "crates/cluster-core/src/scheduler.rs: pure placement policy — input\n"
  "WorkerCaps{online,enabled,kinds[],agent_count,max_agents,mem_available_mb,\n"
  "label} + request{kind,machine_hint} -> Option<placement> or rejection\n"
  "reasons per machine. Filter online+kind+under-cap; score mem headroom/\n"
  "slot -> lowest agent_count -> label (deterministic). Local machine is\n"
  "just another WorkerCaps. Exhaustive unit tests over fixture sets.\n" + CTX)

# ============ EPIC M2: transport & discovery ============
b("m2-transport", "M2: cluster-net + cluster-bridge crates", "epic", 1, None,
  ["m1-core"],
  "TLS + mux + conn manager + discovery (cluster-net) and API bridge\n"
  "(cluster-bridge) tunneling herdr API socket bytes. Loopback-testable.\n" + CTX)

b("m2-tls", "TLS 1.3 layer with cluster CA", "feature", 1, "m2-transport", ["m1-identity"],
  "crates/cluster-net/src/tls.rs: rustls (Apache/MIT/ISC, pin >=7d). At\n"
  "init generate cluster CA + server cert (rcgen or rustls-pemfile —\n"
  "evaluate weight); worker pins CA fingerprint from pairing; mutual auth\n"
  "via worker Ed25519 signed challenge (custom verifier). Cert SANs=localhost\n"
  "+ worker hostname; 30-day expiry + rotation path. Tests: handshake ok/\n"
  "fail, wrong-pin reject, expired cert, wrong-key signature reject.\n" + CTX)

b("m2-mux", "Stream multiplexer over one TLS connection", "feature", 1, "m2-transport", ["m2-tls"],
  "crates/cluster-net/src/mux.rs: frames {stream_id u32, flags, len u32,\n"
  "payload}; stream 0 = control/protocol, 1..=N = API byte streams. Per-\n"
  "stream ordering guaranteed; half-close propagates FIN; no windowing MVP;\n"
  "per-stream buffer cap 1MiB, max 64 streams/conn, backpressure (stop\n"
  "reading when queue full). Tests: interleaved streams, ordering, reset,\n"
  "conn drop cleanup, slow-consumer backpressure, half-close.\n" + CTX)

b("m2-conn", "Connection manager: dial-out, heartbeat, reconnect", "feature", 1, "m2-transport", ["m2-mux"],
  "crates/cluster-net/src/conn.rs: worker resolves controller addr, dials\n"
  "TLS, auths, registers, heartbeats 5s / degraded-3-miss / offline-30s.\n"
  "Reconnect exp backoff 0.5s->30s jittered; resume without re-pairing; on\n"
  "reconnect all open streams fail typed worker_unreachable (no silent\n"
  "resume of half-dead streams). Controller: accept, auth, per-worker\n"
  "session task, conn migration handling. tokio::time pause tests.\n" + CTX)

b("m2-discovery", "LAN discovery: mDNS + UDP broadcast fallback", "feature", 1, "m2-transport", (),
  "crates/cluster-net/src/discovery.rs: advertise `_herdcluster._tcp.local.`\n"
  "at init via mdns-sd (check maturity/license) with TXT records {port,\n"
  "cluster_name, protocol_version}; browse on join. Fallback UDP broadcast\n"
  "hello/response on 47820/udp for mDNS-filtered LANs; WSL2 NAT caveat ->\n"
  "directed broadcast via Windows host; document mirrored networking.\n"
  "Multiple controllers found -> list + pick/--controller hint. `--discover`\n"
  "debug prints candidates; manual override escape hatch. Tests: two\n"
  "localhost instances discover each other.\n" + CTX)

b("m2-bridge", "cluster-bridge: local socket <-> cluster stream <-> herdr API socket", "feature", 1, "m2-transport", ["m2-conn", "m1-protocol"],
  "crates/cluster-bridge: controller binds private unix socket per worker\n"
  "profile (dir 0700, socket 0600); on accept -> ApiStreamOpen -> worker\n"
  "daemon opens LOCAL herdr API socket -> bidirectional pipe -> Close/FIN.\n"
  "Worker proxy is herdcluster-native (speaks NDJSON itself — no upstream\n"
  "remote-api-bridge subprocess). Idle timeout for abandoned streams;\n"
  "socket cleanup on exit/crash (pid-file + stale sweep). Failure reporting\n"
  "pattern like upstream SshStdioBridge.failure_rx. Contract test: NDJSON\n"
  "client through bridge socket == direct socket responses.\n" + CTX)

b("m2-fault", "Transport fault-injection tests", "task", 1, "m2-transport", ["m2-bridge"],
  "Loopback harness: kill worker mid-request, partition 30s, duplicate\n"
  "frames, oversized frame, half-close, slow consumer, conn flapping.\n"
  "Assert typed errors (no hangs), socket cleanup, reconnect resumes, no\n"
  "cross-stream bleed, stream death on reconnect. Comprehensive structured\n"
  "logs (user requires great logs for verification).\n" + CTX)

# ============ EPIC M3: services ============
b("m3-services", "M3: cluster-node + herdcluster CLI services", "epic", 1, None,
  ["m2-transport"],
  "init/join/worker services (cluster-node) + herdcluster bin CLI +\n"
  "machines + provisioning + skill install + revoke/autostart/logging.\n" + CTX)

b("m3-cli", "Workspace skeleton + herdcluster CLI", "task", 1, "m3-services", ["m1-config"],
  "Root Cargo.toml [workspace] members=crates/*; crates/herdcluster bin:\n"
  "main.rs argv dispatch -> cli.rs clap subcommands (init/join/worker/\n"
  "doctor/machines/spawn/agent/status/skills/config/revoke). Thin bin —\n"
  "all logic in library crates. --json output modes everywhere; structured\n"
  "logging via tracing (RUST_LOG + --verbose/--quiet). `herdcluster\n"
  "--version` prints protocol_version. cargo build --release --workspace\n"
  "clean; cargo test --workspace green. NO herdr dependency, NO herdr\n"
  "source anywhere in repo.\n" + CTX)

b("m3-controller", "Controller service (init)", "feature", 1, "m3-services", ["m3-cli", "m2-discovery", "m1-pairing", "m1-identity"],
  "crates/cluster-node/src/controller.rs: `herdcluster init [--name]\n"
  "[--no-skills]` — create cluster identity + cluster.json, mDNS advert +\n"
  "TCP listener 47820, pairing server (show code, AwaitingJoin), accept\n"
  "worker conns -> registry entries, bridge factory per worker. Foreground\n"
  "w/ status line; Ctrl-C clean shutdown (close listener, persist registry).\n"
  "Scripted join->register->list test.\n" + CTX)

b("m3-worker", "Worker daemon (join) + herdr runtime supervisor", "feature", 1, "m3-services", ["m3-cli", "m2-conn", "m2-discovery", "m1-pairing"],
  "crates/cluster-node/src/worker.rs + runtime.rs: `herdcluster join\n"
  "[--controller <hint>]` — discover, pair (show controller name, confirm),\n"
  "store credential, then daemon: ensure herdr server running — install\n"
  "latest herdr RELEASE BINARY for platform if missing (platform detect +\n"
  "GitHub release asset download + checksum verify into\n"
  "state_dir()/herdcluster/runtime/; upstream herdrdev/herdr releases until\n"
  "own releases exist — implement FRESH, do NOT copy upstream code), start\n"
  "server, resource monitor, connect-out loop. `herdcluster worker` =\n"
  "daemon-only mode. WSL2 mirrored-networking doc note.\n" + CTX)

b("m3-join-ux", "Pairing UX flow polish", "task", 1, "m3-services", ["m3-controller", "m3-worker"],
  "init prints cluster name + 'run herdcluster join on another machine';\n"
  "join lists discovered clusters + hostname, y/n; controller shows 'Worker\n"
  "X wants to join — accept?' with pubkey fingerprint; both print clear\n"
  "success + next steps. Non-TTY: --yes/--json. Plain language, no jargon.\n" + CTX)

b("m3-machines", "`herdcluster machines` unified listing", "feature", 1, "m3-services", ["m3-controller", "m3-worker", "m1-registry"],
  "crates/cluster-fleet machines view: table NAME TYPE OS STATUS AGENTS CPU\n"
  "MEM — cluster registry live data + optional read-only `herdr machine\n"
  "list --json` SSH rows (display only, never managed). --json stable\n"
  "schema for scripting.\n" + CTX)

b("m3-revoke", "Worker revocation + removal", "feature", 1, "m3-services", ["m3-controller", "m1-registry"],
  "`herdcluster revoke <worker>` / `herdcluster machine remove <worker>`:\n"
  "remove registry entry, delete stored credential, close live conn, kill\n"
  "open streams with typed error. Worker's next connect attempt must fail\n"
  "auth (credential no longer accepted — verify server-side check, not just\n"
  "registry absence). `machines` shows revoked state until worker re-pairs\n"
  "with fresh code. Tests: revoke->reconnect rejected; re-pair works.\n" + CTX)

b("m3-autostart", "Worker autostart on login/boot", "task", 2, "m3-services", ["m3-worker"],
  "`herdcluster worker install-autostart` (or join flag): Linux/WSL ->\n"
  "systemd user unit (linger note for WSL); macOS -> launchd plist.\n"
  "Uninstall path too. WSL2 caveat: user units need lingering or Windows\n"
  "startup script — document both, implement systemd first. Tests: unit\n"
  "file content validation; manual verify on WSL2.\n" + CTX)

b("m3-provisioning", "Agent CLI auto-provisioning on worker", "feature", 1, "m3-services", ["m3-worker"],
  "After pairing, probe each supported kind (via server.agent_manifests +\n"
  "PATH checks) on worker. Missing kinds -> offer guided auto-install where\n"
  "safe non-interactive path exists (npm i -g, official scripts) — ALWAYS\n"
  "with consent, never silent. Report per-kind: installed/auto-installed/\n"
  "missing/auth-needed. HARD BOUNDARY: agent credentials (claude login,\n"
  "API keys) are NEVER handled — detect likely-auth-needed and print exact\n"
  "login command for user. Surface in join step, machines, doctor.\n" + CTX)

b("m3-skills", "init auto-provisions orchestration skill into project", "feature", 2, "m3-services", ["m3-controller"],
  "On `herdcluster init` (and `join` with project dir), scan cwd for\n"
  ".agents/skills/herdr-orchestrator and .claude/skills/herdr-orchestrator.\n"
  "If missing, offer install of bundled copy (ship skill files in release\n"
  "assets — pattern like herdr's skills/herdr/SKILL.md). Consent-gated;\n"
  "never overwrite diverged versions without asking; `--no-skills` flag;\n"
  "doctor check 'orchestration skill present'. `herdcluster skills\n"
  "install|status` subcommands.\n" + CTX)

b("m3-e2e", "E2E: real Mac<->WSL2 pairing", "task", 0, "m3-services", ["m3-join-ux", "m3-machines"],
  "Manual e2e on real machines: fresh WSL2 Ubuntu, install herdcluster,\n"
  "`init` on mac, `join` in WSL (auto-installs herdr release), `machines`\n"
  "shows wsl online, `doctor` green. Record: WSL2 networking mode needed,\n"
  "firewall prompts, timing. Fix blockers.\n" + CTX)

# ============ EPIC M4: herdr API client ============
b("m4-api", "M4: api-client crate (NDJSON) + methods + compat", "epic", 1, None,
  ["m3-e2e"],
  "crates/api-client: minimal NDJSON client for herdr's published socket\n"
  "API + typed wrappers for needed methods + protocol-version compat guard.\n" + CTX)

b("m4-client", "NDJSON API client core", "feature", 1, "m4-api", (),
  "crates/api-client/src/lib.rs (+platform/): discover herdr API socket\n"
  "path per-OS (unix socket under herdr state dir; HERDR_SOCKET_PATH env\n"
  "override; Windows named-pipe = later bead), connect, send\n"
  "{id,method,params}\\n, read one JSON line, map to Result<value,\n"
  "ApiError{code,message}>. FIRST verify against real herdr whether the\n"
  "socket allows multiple requests per connection or is request-per-conn —\n"
  "implement accordingly (request-per-conn is acceptable MVP). Unique\n"
  "request ids; per-call timeouts (30s default, agent.wait longer).\n"
  "server_not_running -> typed 'start herdr first' error. Tests: fake\n"
  "NDJSON socket server fixture.\n" + CTX)

b("m4-methods", "Typed wrappers for needed API method subset", "feature", 1, "m4-api", ["m4-client"],
  "crates/api-client/src/methods.rs: agent.{list,get,read,wait,prompt,\n"
  "send_keys,start,rename,focus}, workspace.{list,create,get},\n"
  "pane.{list,split,get}, worktree.{list,create}, server.agent_manifests,\n"
  "status.server, api.snapshot. Serde param structs matching upstream\n"
  "snake_case schema names. Thin fns over client.request. Tests vs fake\n"
  "server fixtures + one live test gated on `herdr` binary presence.\n" + CTX)

b("m4-socketpath", "Platform API socket path discovery", "task", 1, "m4-api", ["m4-client"],
  "crates/api-client/src/platform/: resolve herdr API socket path on\n"
  "macOS/Linux (state dir convention + session name), honor\n"
  "HERDR_SOCKET_PATH/HERDR_SESSION env, Windows named-pipe stub (MVP\n"
  "WSL2-only — document). Verify against real herdr install on macOS.\n"
  "Per-platform cfg tests.\n" + CTX)

b("m4-compat", "API compat guard + drift policy", "feature", 1, "m4-api", ["m4-methods"],
  "On connect: status.server -> compare protocol/schema version vs\n"
  "compiled-in compat range -> mismatch = clear 'update herdcluster'\n"
  "error (never silent wrong-behavior). Record tested schema version; CI\n"
  "fixture of recorded responses for regression. docs/herdcluster/\n"
  "COMPAT.md policy doc.\n" + CTX)

# ============ EPIC M5: fleet UX ============
b("m5-fleet", "M5: cluster-fleet crate — global IDs, list --all, spawn", "epic", 1, None,
  ["m4-compat"],
  "crates/cluster-fleet: global agent IDs {machine}/{name}, `agent list\n"
  "--all` fan-out, agent ops routing, one-step `spawn` composer over the\n"
  "api-client.\n" + CTX)

b("m5-global-id", "Global agent ID scheme + resolution", "feature", 1, "m5-fleet", ["m4-methods"],
  "crates/cluster-fleet/src/ids.rs: ID {machine-label}/{agent-name-or-\n"
  "terminal-id}; resolution: local herdr API first -> parallel worker\n"
  "queries (short timeout) -> unique hit or ambiguity error listing\n"
  "candidates; offline -> worker_unreachable. Tests: resolution matrix\n"
  "(local hit, remote hit, ambiguous, offline, malformed ID).\n" + CTX)

b("m5-list-all", "`agent list --all` fan-out merge", "feature", 1, "m5-fleet", ["m5-global-id"],
  "crates/cluster-fleet/src/fleet.rs: parallel agent.list to local +\n"
  "enabled workers; merge rows injecting machine field; offline workers\n"
  "-> {machine,status:unreachable} placeholders; --json stable schema;\n"
  "total budget <10s; deterministic ordering (machine label, then agent).\n"
  "Tests: merge ordering, offline, timeout, empty fleet.\n" + CTX)

b("m5-agent-ops", "agent ops accept global IDs", "feature", 1, "m5-fleet", ["m5-global-id"],
  "crates/cluster-fleet agent ops: get/read/wait/prompt/send-keys/rename\n"
  "resolve global ID -> machine -> api-client endpoint (local socket or\n"
  "worker bridge). wait stays per-machine (no cross-machine wait MVP).\n"
  "Typed JSON errors: unknown_machine, ambiguous_agent, worker_unreachable.\n" + CTX)

b("m5-spawn", "`herdcluster spawn` one-step helper", "feature", 1, "m5-fleet", ["m5-agent-ops"],
  "crates/cluster-fleet/src/spawn.rs: spawn <NAME> --kind <KIND> --machine\n"
  "<label|auto> --repo <path> [--branch] [--agent-arg...] [--json]: resolve\n"
  "machine -> pre-flight kind support (server.agent_manifests) + agent CLI\n"
  "presence (worker resources) -> ensure workspace (list by cwd else\n"
  "workspace.create) -> ensure dedicated pane (pane.split or root pane —\n"
  "document choice) -> agent.start -> wait interactive_ready -> print\n"
  "global ID+machine+pane. --repo is TARGET-LOCAL path (document).\n"
  "Default timeout 35s, --json machine-readable.\n" + CTX)

b("m5-spawn-e2e", "E2E: spawn agents on both machines", "task", 0, "m5-fleet", ["m5-spawn"],
  "Real e2e: spawn claude on mac + codex on wsl; prompt, wait idle, agent\n"
  "read shows real output; verify Mac CPU/RAM delta vs worker doing work.\n"
  "Capture logs for docs.\n" + CTX)

# ============ EPIC M6: scheduler ============
b("m6-sched", "M6: scheduler --machine auto", "epic", 2, None, ["m5-list-all"],
  "Wire cluster-core scheduler scoring into placement: --machine auto\n"
  "picks worker by reported resources + kind support + per-machine caps.\n" + CTX)

b("m6-resources", "Worker resource reporting", "feature", 1, "m6-sched", ["m3-worker"],
  "crates/cluster-node/src/resources.rs: sample cpu_cores/mem_total/\n"
  "mem_available (sysinfo crate or /proc+sysctl — pick portable),\n"
  "agent_count (local API agent.list len), kinds (agent_manifests ∩ PATH),\n"
  "load_avg. Piggyback heartbeat Resources msg 15s + on-change; controller\n"
  "caches last-seen in workers.json. Local machine reports same shape.\n" + CTX)

b("m6-policy", "Placement policy + `--machine auto` wiring", "feature", 1, "m6-sched", ["m6-resources", "m5-spawn"],
  "crates/cluster-node placement: build WorkerCaps per machine (registry\n"
  "resources + config caps + local stats) -> cluster-core scheduler ->\n"
  "--machine auto resolves same path as explicit label in spawn/agent ops;\n"
  "no candidate -> error listing per-machine rejection reasons.\n" + CTX)

b("m6-caps", "Per-machine agent caps config", "task", 2, "m6-sched", ["m6-policy"],
  "cluster.json machines.<label>.{max_agents,kinds_override}; defaults\n"
  "max_agents=4 hard ceiling 8; `herdcluster config set` later — MVP edit\n"
  "JSON + doctor validates ranges. Tests: cap enforcement in scheduler,\n"
  "config roundtrip, invalid values rejected.\n" + CTX)

# ============ EPIC M7: doctor ============
b("m7-doctor", "M7: `herdcluster doctor [--fix]`", "epic", 2, None, ["m5-spawn-e2e"],
  "crates/cluster-node/src/doctor.rs: layered self-diagnostics, plain-\n"
  "language output, safe auto-fix.\n" + CTX)

b("m7-checks", "Doctor check suite", "feature", 1, "m7-doctor", ["m3-e2e"],
  "Checks in order: cluster config -> discovery -> controller reachable /\n"
  "workers registered -> auth/session -> TLS -> herdr server on worker ->\n"
  "herdr API reachable + protocol compat -> per-kind agent CLI on PATH ->\n"
  "repo path exists (if given) -> resources (disk>2GB, mem) -> worker\n"
  "daemon health -> orchestration skill present. Each: name, status,\n"
  "plain-language reason, fix hint. --machine scopes; default whole\n"
  "cluster; --json mode; exit code reflects failures.\n" + CTX)

b("m7-fix", "Doctor --fix remediations", "feature", 1, "m7-doctor", ["m7-checks"],
  "Safe auto-fix: restart worker daemon, guided re-pair on auth failure,\n"
  "reload agent manifests (via API), create state dirs, fix perms, offer\n"
  "skill install. Every fix prints what+why; --yes non-interactive; NEVER\n"
  "fix silently. Per-remediation tests.\n" + CTX)

b("m7-fresh", "Fresh-machine doctor acceptance", "task", 0, "m7-doctor", ["m7-fix"],
  "Wipe WSL2 herdcluster state -> join -> doctor --fix -> all green without\n"
  "manual steps. THE product UX test. Any step needing human intervention\n"
  "becomes a bug bead.\n" + CTX)

# ============ EPIC M8: packaging ============
b("m8-release", "M8: packaging, install, release, compat tracking", "epic", 2, None, ["m7-fresh"],
  "Installable herdcluster for macOS arm64 + Linux x64 (WSL2), release\n"
  "workflow, herdr-API compat tracking.\n" + CTX)

b("m8-bins", "Release builds: macOS arm64 + Linux x64", "task", 1, "m8-release", ["m3-cli"],
  "cargo build --release --workspace; cross-compile aarch64-apple-darwin +\n"
  "x86_64-unknown-linux-gnu (zig or cross — pick pragmatic); strip; size\n"
  "check; justfile targets; verify WSL2 run. Single binary output\n"
  "(herdcluster bin only — lib crates are deps).\n" + CTX)

b("m8-install", "Install script + WSL2 docs", "feature", 1, "m8-release", ["m8-bins"],
  "install.sh (curl|sh): detect OS/arch, download release asset, verify\n"
  "checksum, install ~/.local/bin, run `herdcluster doctor`. WSL2 guide:\n"
  "mirrored networking if discovery fails, systemd user service autostart,\n"
  "uninstall path. Docs in docs/herdcluster/.\n" + CTX)

b("m8-ci", "Release workflow + checksums", "feature", 1, "m8-release", ["m8-install"],
  ".github/workflows/release.yml: tag -> build mac+linux -> gh release +\n"
  "checksums + install script. protocol_version in WorkerHello/PairAccept;\n"
  "mismatch -> 'update herdcluster on X'. cargo test --workspace in CI.\n" + CTX)

b("m8-compat-track", "herdr API compat tracking", "task", 2, "m8-release", ["m4-compat"],
  "Track upstream herdrdev/herdr releases: watch api schema changes\n"
  "(docs/next/api/herdr-api.schema.json + Method enum), maintain compat\n"
  "range in api-client, add fixture tests per schema version, document\n"
  "policy in COMPAT.md. Alert path when newest herdr breaks compat range.\n" + CTX)

# ---------- create ----------
ids = {}
for spec in BEADS:
    cmd = BR + ["--slug", spec["slug"], "-t", spec["type"], "-p", str(spec["pri"]),
                "--silent", spec["title"], "-d", spec["desc"]]
    if spec["parent"]:
        cmd += ["--parent", ids[spec["parent"]]]
    iid = run(cmd)
    ids[spec["slug"]] = iid
    print(f"{spec['slug']:24s} -> {iid}")

for spec in BEADS:
    child = ids[spec["slug"]]
    for dep in spec["deps"]:
        run(DEP + [child, ids[dep]])
        print(f"dep: {child} blocks-on {ids[dep]}")

with open("/tmp/herdcluster-bead-ids.json", "w") as f:
    json.dump(ids, f, indent=2)
print("DONE", len(BEADS), "beads")
