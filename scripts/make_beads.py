#!/usr/bin/env python3
"""Create HerdCluster beads (Option B: pure wrapper). Run: python3 scripts/make_beads.py"""
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
STANDALONE Rust binary that clusters coding agents across machines WITHOUT
herdr source code and WITHOUT modifying herdr. Canonical plan:
docs/herdcluster/PLAN.md.

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
source. Apache-2.0 original code. Deps pinned, prefer versions >7 days old.
Worker dials OUT only (no inbound ports). Pairing tokens single-use/short-
lived. Agent CLI credentials are NEVER handled — detect + hint only.
"""

# ============ EPIC M1: cluster core library ============
b("m1-core", "M1: Cluster core library (identity, pairing, protocol, registry, config)", "epic", 1, None, (),
  "Foundation module src/cluster/ with identity, pairing state machine,\n"
  "wire protocol, worker registry, and config. Pure logic + unit tests,\n"
  "no networking yet.\n" + CTX)

b("m1-identity", "Ed25519 cluster/worker identity module", "feature", 1, "m1-core", (),
  "src/cluster/identity.rs: generate/load/store Ed25519 keypairs for cluster\n"
  "(controller) and worker identities under herdcluster state dir. Public\n"
  "key fingerprint = short human ID shown in `machines`. Sign/verify\n"
  "challenge responses for post-pairing auth. Files 0600, atomic writes.\n"
  "Unit tests: keygen roundtrip, tamper detection, fingerprint stability.\n" + CTX)

b("m1-pairing", "Pairing protocol state machine", "feature", 1, "m1-core", ["m1-identity"],
  "src/cluster/pairing.rs: one-time 8-char human code (unambiguous alphabet,\n"
  "~40 bits), 5-min TTL, single-use, constant-time compare, rate-limit\n"
  "(max 5 attempts -> 60s lockout). States: AwaitingJoin -> PendingConfirm ->\n"
  "Approved/Rejected/Expired. Controller confirm callback; worker wait\n"
  "timeout. Pure FSM — inject clock + rng. Tests: expiry, replay, lockout,\n"
  "confirm/deny.\n" + CTX)

b("m1-protocol", "Cluster wire protocol codec", "feature", 1, "m1-core", ["m1-identity"],
  "src/cluster/protocol.rs: length-prefixed frames. Messages: PairHello/\n"
  "PairAccept/PairReject, WorkerHello{pubkey,hostname,caps,resources},\n"
  "ApiStreamOpen{stream_id}/ApiStreamData{stream_id,bytes}/ApiStreamClose,\n"
  "Heartbeat{seq}, Resources{cpu_cores,mem_total_mb,mem_available_mb,\n"
  "agent_count,max_agents,load_avg,kinds[]}, Event{kind,payload}.\n"
  "Stream 0 reserved for control; data streams carry raw herdr API bytes.\n"
  "Property tests: roundtrip, truncation/garbage rejection, 16MiB frame cap.\n" + CTX)

b("m1-registry", "Worker registry schema + persistence", "feature", 1, "m1-core", (),
  "src/cluster/registry.rs + state layout under herdcluster state dir:\n"
  "cluster.json {cluster_id,name,port,protocol_version,created_at};\n"
  "workers.json {workers:[{worker_id,label,pubkey,paired_at,last_seen,\n"
  "status,resources{...}}]}; credentials/ per-worker keys 0600. Registry is\n"
  "HerdCluster-owned — do NOT touch herdr's endpoint catalog. Migration/\n"
  "corruption tests; status transitions online/offline/degraded.\n" + CTX)

b("m1-config", "Cluster config + state dir layout", "task", 1, "m1-core", ["m1-identity"],
  "src/cluster/config.rs: state_dir()/herdcluster/{identity.key,cluster.json,\n"
  "workers.json,credentials/}. cluster.json: name (default hostname), port\n"
  "47820, protocol_version=1. Env overrides HERDCLUSTER_PORT/\n"
  "HERDCLUSTER_STATE_DIR for tests. machines.<label>.{max_agents,kinds_\n"
  "override} config section. Tests: create/load/corrupt handling.\n" + CTX)

# ============ EPIC M2: transport & discovery ============
b("m2-transport", "M2: Transport, discovery, API bridge", "epic", 1, None,
  ["m1-core"],
  "TLS transport + stream mux + reconnect + LAN discovery + bridge that\n"
  "tunnels herdr API socket bytes over the cluster connection. Loopback-\n"
  "testable before real machines.\n" + CTX)

b("m2-tls", "TLS 1.3 layer with cluster CA", "feature", 1, "m2-transport", ["m1-identity"],
  "rustls (pure Rust, Apache/MIT/ISC — verify license, pin >=7d old). At\n"
  "`init` generate cluster CA + server cert; worker pins CA fingerprint\n"
  "from pairing. Mutual auth: server cert + worker Ed25519 signed challenge.\n"
  "Deps: rustls + rcgen (CA gen) — evaluate weight vs alternatives.\n"
  "Tests: handshake ok/fail, wrong-pin rejection, expiry path.\n" + CTX)

b("m2-mux", "Stream multiplexer over one TLS connection", "feature", 1, "m2-transport", ["m2-tls"],
  "src/cluster/mux.rs: frames {stream_id u32, flags, len u32, payload};\n"
  "stream 0 = control/protocol, 1..=N = API byte streams. No windowing MVP;\n"
  "per-stream buffer cap 1MiB, max 64 streams/conn, backpressure (stop\n"
  "reading when queue full). Tests: interleaved streams, reset, conn drop\n"
  "cleanup, slow-consumer backpressure.\n" + CTX)

b("m2-conn", "Connection manager: dial-out, heartbeat, reconnect", "feature", 1, "m2-transport", ["m2-mux"],
  "Worker side: resolve controller addr, dial TLS, auth, register, heartbeat\n"
  "5s / degraded-3-miss / offline-30s. Reconnect exp backoff 0.5s->30s jitter,\n"
  "resume without re-pairing. Controller: accept, auth, per-worker session\n"
  "task. tokio::time pause tests: heartbeat timeout, backoff, reconnect.\n" + CTX)

b("m2-discovery", "LAN discovery: mDNS + UDP broadcast fallback", "feature", 1, "m2-transport", (),
  "src/cluster/discovery.rs: advertise `_herdcluster._tcp.local.` at init\n"
  "(mdns-sd crate — check maturity/license); browse on join. Fallback UDP\n"
  "broadcast hello/response on 47820/udp for mDNS-filtered LANs; WSL2 NAT\n"
  "caveat -> also directed broadcast via Windows host; document mirrored\n"
  "networking. `--discover` debug flag prints candidates; manual override\n"
  "escape hatch. Tests: two localhost instances discover each other.\n" + CTX)

b("m2-bridge", "API bridge: local socket <-> cluster stream <-> herdr API socket", "feature", 1, "m2-transport", ["m2-conn", "m1-protocol"],
  "src/cluster/bridge.rs: controller binds private unix socket per worker\n"
  "profile; on accept -> ApiStreamOpen on worker conn -> worker daemon opens\n"
  "LOCAL herdr API socket -> bidirectional byte pipe -> ApiStreamClose.\n"
  "Worker side implements its own proxy (herdcluster speaks NDJSON natively\n"
  "— no remote-api-bridge subprocess needed). Failure reporting like\n"
  "upstream SshStdioBridge.failure_rx pattern. Contract test: NDJSON client\n"
  "through bridge socket gets identical responses to direct socket.\n" + CTX)

b("m2-fault", "Transport fault-injection tests", "task", 1, "m2-transport", ["m2-bridge"],
  "Loopback harness: kill worker mid-request, partition 30s, duplicate\n"
  "frames, oversized frame, half-close, slow consumer. Assert typed errors\n"
  "(no hangs), socket cleanup, reconnect resumes, no cross-stream bleed.\n"
  "Comprehensive logging (user requires great logs for verification).\n" + CTX)

# ============ EPIC M3: services ============
b("m3-services", "M3: init/join services + herdcluster CLI", "epic", 1, None,
  ["m2-transport"],
  "`herdcluster init` controller + `herdcluster join`/`worker` daemon as\n"
  "real commands; unified `machines`; agent-CLI and skill provisioning.\n" + CTX)

b("m3-cli", "herdcluster standalone crate + CLI skeleton", "task", 1, "m3-services", ["m1-config"],
  "Cargo project at repo root: bin `herdcluster`, src/main.rs argv dispatch,\n"
  "src/cli.rs clap subcommands (init/join/worker/doctor/machines/spawn/\n"
  "agent/status/skills/config). Standalone crate — NO herdr dependency, NO\n"
  "herdr source. --json output modes from the start. `herdcluster --version\n"
  "protocol_version`. Build must pass `cargo build --release` clean.\n" + CTX)

b("m3-controller", "Controller service (init)", "feature", 1, "m3-services", ["m3-cli", "m2-discovery", "m1-pairing", "m1-identity"],
  "`herdcluster init [--name] [--no-skills]`: create cluster identity +\n"
  "cluster.json, mDNS advert + TCP listener 47820, pairing server (show\n"
  "code, AwaitingJoin), accept worker conns -> registry entries, bridge\n"
  "factory per worker. Foreground w/ status line; Ctrl-C clean shutdown.\n"
  "Scripted join->register->list test.\n" + CTX)

b("m3-worker", "Worker daemon (join) + herdr runtime supervisor", "feature", 1, "m3-services", ["m3-cli", "m2-conn", "m2-discovery", "m1-pairing"],
  "`herdcluster join [--controller <hint>]`: discover, pair (show controller\n"
  "name, confirm), store credential, then daemon mode: ensure herdr server\n"
  "running — install latest herdr RELEASE BINARY for platform if missing\n"
  "(platform detect + GitHub release asset download + checksum verify,\n"
  "state dir herdcluster/runtime/; upstream herdrdev/herdr releases until\n"
  "own releases exist — implement fresh, do NOT copy upstream code), start\n"
  "server, resource monitor (cpu/mem/agent_count/kinds via local API + host\n"
  "stats), connect-out loop. Autostart later (systemd user/launchd bead).\n"
  "WSL2 mirrored-networking doc note.\n" + CTX)

b("m3-join-ux", "Pairing UX flow polish", "task", 1, "m3-services", ["m3-controller", "m3-worker"],
  "init prints cluster name + 'run herdcluster join on another machine';\n"
  "join lists discovered clusters + hostname, y/n; controller shows 'Worker\n"
  "X wants to join — accept?' with pubkey fingerprint; both print clear\n"
  "success + next steps. Non-TTY: --yes/--json. Plain language, no jargon.\n" + CTX)

b("m3-machines", "`herdcluster machines` unified listing", "feature", 1, "m3-services", ["m3-controller", "m3-worker", "m1-registry"],
  "Table NAME TYPE OS STATUS AGENTS CPU MEM — cluster registry live data +\n"
  "optional read-only `herdr machine list --json` SSH rows (display only,\n"
  "never managed). --json stable schema for scripting.\n" + CTX)

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
  "doctor check 'orchestration skill present'. herdcluster skills\n"
  "install|status subcommands.\n" + CTX)

b("m3-e2e", "E2E: real Mac<->WSL2 pairing", "task", 0, "m3-services", ["m3-join-ux", "m3-machines"],
  "Manual e2e on real machines: fresh WSL2 Ubuntu, install herdcluster,\n"
  "`init` on mac, `join` in WSL (auto-installs herdr release), `machines`\n"
  "shows wsl online, `doctor` green. Record: WSL2 networking mode needed,\n"
  "firewall prompts, timing. Fix blockers.\n" + CTX)

# ============ EPIC M4: herdr API client ============
b("m4-api", "M4: herdr API client (NDJSON) + method wrappers + compat", "epic", 1, None,
  ["m3-e2e"],
  "Minimal NDJSON client for herdr's published socket API + typed wrappers\n"
  "for the method subset herdcluster needs + protocol-version compat guard.\n" + CTX)

b("m4-client", "NDJSON API client core", "feature", 1, "m4-api", (),
  "src/api_client.rs: discover herdr API socket path per-OS (unix socket\n"
  "under herdr state dir; HERDR_SOCKET_PATH env override; Windows named\n"
  "pipe = later bead), connect, send {id,method,params}\\n, read one JSON\n"
  "line, map to Result<value, ApiError{code,message}>. Timeouts per call\n"
  "(default 30s, agent.wait longer). server_not_running detection -> typed\n"
  "error 'start herdr first'. Unit tests with fake socket server (spawn\n"
  "test NDJSON responder).\n" + CTX)

b("m4-methods", "Typed wrappers for needed API method subset", "feature", 1, "m4-api", ["m4-client"],
  "src/api_methods.rs: agent.{list,get,read,wait,prompt,send_keys,start,\n"
  "rename,focus}, workspace.{list,create,get}, pane.{list,split,get},\n"
  "worktree.{list,create}, server.agent_manifests, status.server,\n"
  "api.snapshot. Params as serde structs matching upstream schema names\n"
  "(snake_case). Each wrapper = thin fn over client.request. Tests vs fake\n"
  "server fixtures; one live test gated on `herdr` binary presence.\n" + CTX)

b("m4-socketpath", "Platform API socket path discovery", "task", 1, "m4-api", ["m4-client"],
  "src/platform/: resolve herdr API socket path on macOS/Linux (state dir\n"
  "convention + session name), honor HERDR_SOCKET_PATH/HERDR_SESSION env,\n"
  "Windows named-pipe path stub (MVP WSL2 only — document). Verify against\n"
  "real herdr install on macOS. Tests per platform cfg.\n" + CTX)

b("m4-compat", "API compat guard + drift policy", "feature", 1, "m4-api", ["m4-methods"],
  "On connect: status.server -> compare protocol/schema version against\n"
  "compiled-in compat range -> mismatch = clear 'update herdcluster' error\n"
  "(never silent wrong-behavior). Record which schema version tested; CI\n"
  "fixture of recorded responses for regression. Document compat policy in\n"
  "docs/herdcluster/COMPAT.md.\n" + CTX)

# ============ EPIC M5: fleet UX ============
b("m5-fleet", "M5: Fleet UX — global IDs, list --all, spawn", "epic", 1, None,
  ["m4-compat"],
  "Global agent IDs {machine}/{name}, `agent list --all` fan-out, one-step\n"
  "`spawn` helper composing workspace+pane+agent.start over the API client.\n" + CTX)

b("m5-global-id", "Global agent ID scheme + resolution", "feature", 1, "m5-fleet", ["m4-methods"],
  "ID {machine-label}/{agent-name-or-terminal-id}; resolution: local herdr\n"
  "API first -> parallel worker queries (short timeout) -> unique hit or\n"
  "ambiguity error listing candidates; offline -> worker_unreachable.\n"
  "Tests: resolution matrix (local hit, remote hit, ambiguous, offline).\n" + CTX)

b("m5-list-all", "`agent list --all` fan-out merge", "feature", 1, "m5-fleet", ["m5-global-id"],
  "Parallel agent.list to local + enabled workers; merge rows injecting\n"
  "machine field; offline workers -> {machine,status:unreachable}; --json\n"
  "stable schema; total budget <10s. Tests: merge ordering, offline,\n"
  "timeout.\n" + CTX)

b("m5-agent-ops", "agent ops accept global IDs", "feature", 1, "m5-fleet", ["m5-global-id"],
  "herdcluster agent {get,read,wait,prompt,send-keys,rename} resolve global\n"
  "ID -> machine -> API client (local or worker bridge). wait stays\n"
  "per-machine. Typed JSON errors for unknown machine/ambiguous/offline.\n" + CTX)

b("m5-spawn", "`herdcluster spawn` one-step helper", "feature", 1, "m5-fleet", ["m5-agent-ops"],
  "spawn <NAME> --kind <KIND> --machine <label|auto> --repo <path>\n"
  "[--branch] [--agent-arg...] [--json]: resolve machine -> pre-flight kind\n"
  "support + agent CLI presence -> ensure workspace (list by cwd else\n"
  "workspace.create) -> ensure dedicated pane -> agent.start -> wait\n"
  "interactive_ready -> print global ID+machine+pane. --repo is TARGET-\n"
  "LOCAL path (document). Default timeout 35s.\n" + CTX)

b("m5-spawn-e2e", "E2E: spawn agents on both machines", "task", 0, "m5-fleet", ["m5-spawn"],
  "Real e2e: spawn claude on mac + codex on wsl; prompt, wait idle, agent\n"
  "read shows real output; verify Mac CPU/RAM delta vs worker doing work.\n"
  "Capture logs for docs.\n" + CTX)

# ============ EPIC M6: scheduler ============
b("m6-sched", "M6: Scheduler (--machine auto)", "epic", 2, None, ["m5-list-all"],
  "Capacity-aware placement via worker-reported resources + kind support +\n"
  "per-machine caps. Local machine competes equally.\n" + CTX)

b("m6-resources", "Worker resource reporting", "feature", 1, "m6-sched", ["m3-worker"],
  "Worker daemon samples cpu_cores/mem_total/mem_available (portable:\n"
  "sysinfo crate or /proc+sysctl), agent_count (local API), kinds (agent\n"
  "manifests ∩ PATH), load_avg. Piggyback heartbeat Resources msg 15s +\n"
  "on-change; controller caches last-seen in workers.json.\n" + CTX)

b("m6-policy", "Placement policy + `--machine auto`", "feature", 1, "m6-sched", ["m6-resources", "m5-spawn"],
  "src/cluster/scheduler.rs: filter online+enabled+kind-supported+under-cap;\n"
  "score mem headroom/slot -> lowest agent_count -> label; deterministic;\n"
  "--machine auto resolves same path as explicit label; no candidate ->\n"
  "error listing per-machine rejection reasons. Unit tests over fixtures.\n" + CTX)

b("m6-caps", "Per-machine agent caps config", "task", 2, "m6-sched", ["m6-policy"],
  "cluster.json machines.<label>.{max_agents,kinds_override}; defaults\n"
  "max_agents=4 hard ceiling 8; `herdcluster config set` later — MVP edit\n"
  "JSON + doctor validates. Tests: cap enforcement, config roundtrip.\n" + CTX)

# ============ EPIC M7: doctor ============
b("m7-doctor", "M7: `herdcluster doctor [--fix]`", "epic", 2, None, ["m5-spawn-e2e"],
  "Layered self-diagnostics, plain-language output, safe auto-fix.\n" + CTX)

b("m7-checks", "Doctor check suite", "feature", 1, "m7-doctor", ["m3-e2e"],
  "Checks: cluster config -> discovery -> controller reachable / workers\n"
  "registered -> auth/session -> TLS -> herdr server on worker -> herdr API\n"
  "reachable + protocol compat -> per-kind agent CLI on PATH -> repo path\n"
  "exists -> resources (disk>2GB, mem) -> worker daemon health -> orch\n"
  "skill present. Each: name, status, plain-language reason, fix hint.\n"
  "--machine scopes; default whole cluster; --json mode.\n" + CTX)

b("m7-fix", "Doctor --fix remediations", "feature", 1, "m7-doctor", ["m7-checks"],
  "Safe auto-fix: restart worker daemon, guided re-pair on auth failure,\n"
  "reload agent manifests, create state dirs, fix catalog perms, offer\n"
  "skill install. Every fix prints what+why; --yes non-interactive; NEVER\n"
  "fix silently. Tests per remediation.\n" + CTX)

b("m7-fresh", "Fresh-machine doctor acceptance", "task", 0, "m7-doctor", ["m7-fix"],
  "Wipe WSL2 herdcluster state -> join -> doctor --fix -> all green without\n"
  "manual steps. THE product UX test. Any step needing human intervention\n"
  "becomes a bug bead.\n" + CTX)

# ============ EPIC M8: packaging ============
b("m8-release", "M8: Packaging, install, release, compat tracking", "epic", 2, None, ["m7-fresh"],
  "Installable herdcluster for macOS arm64 + Linux x64 (WSL2), release\n"
  "workflow, herdr-API compat tracking.\n" + CTX)

b("m8-bins", "Release builds: macOS arm64 + Linux x64", "task", 1, "m8-release", ["m3-cli"],
  "cargo build --release for aarch64-apple-darwin + x86_64-unknown-linux-gnu;\n"
  "strip; size check; justfile targets; verify WSL2 run. Single static-ish\n"
  "binary, no runtime deps.\n" + CTX)

b("m8-install", "Install script + WSL2 docs", "feature", 1, "m8-release", ["m8-bins"],
  "install.sh (curl|sh): detect OS/arch, download release asset, verify\n"
  "checksum, install ~/.local/bin, run `herdcluster doctor`. WSL2 guide:\n"
  "mirrored networking if discovery fails, systemd user service autostart.\n"
  "Docs in docs/herdcluster/.\n" + CTX)

b("m8-ci", "Release workflow + checksums", "feature", 1, "m8-release", ["m8-install"],
  ".github/workflows/release.yml: tag -> build mac+linux -> gh release +\n"
  "checksums + install script. Version handshake: protocol_version in\n"
  "WorkerHello/PairAccept; mismatch -> 'update herdcluster on X'.\n" + CTX)

b("m8-compat-track", "herdr API compat tracking", "task", 2, "m8-release", ["m4-compat"],
  "Track upstream herdrdev/herdr releases: watch api schema changes\n"
  "(docs/next/api/herdr-api.schema.json + Method enum), maintain compat\n"
  "range in herdcluster, add fixture tests per schema version, document\n"
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
