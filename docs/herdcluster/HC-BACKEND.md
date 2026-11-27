# HC-BACKEND — assessing herdr-core/`hc` as a second bridge backend

> Bead: `herdcluster-hc-backend-assessment-yzq` · 2026-11-27 · assessment only,
> no code changed.
>
> Question: can herdcluster workers run `hc connect --json` (herdr-core
> JSON-RPC over stdio) instead of the herdr NDJSON socket, with the bridge
> piping `local socket ↔ cluster stream ↔ hc connect stdio`, while
> cluster-net / pairing / scheduler / fleet stay unchanged?
>
> Ground truth below was verified live against `hc 0.1.17` on this machine
> (`hc schema` + `hc connect --json` probes) and cross-checked against the
> sister project's migration research
> (`herdr_cross_session_messaging/docs/hc-migration/{00-rfc,01-gap-analysis,02-spike-notes}.md`,
> `crates/hc-spike/src/client.rs`).

## TL;DR / recommendation

**Defer — do not build the adapter now; keep the herdr backend and treat
`hc machine add` (over ssh/`--via`) as the answer for ad-hoc remote
terminals.** The transport half of the idea is nearly free (both protocols
are newline-delimited JSON, and `cluster-bridge` already moves opaque
bytes), but the *semantic* half is not: hc has no workspace/tab/pane model,
no agent-kind manifests, no `agent_blocked`/`interactive_ready` semantics,
and agent status depends entirely on programs self-reporting OSC 7501 —
coverage for claude/codex/opencode/devin is unverified. Every fleet
feature that makes herdcluster more than `hc machine ls` (spawn composer,
`agent wait --until done`, watch, doctor per-kind checks, scheduler
`kinds[]`, keepawake) would need re-implementation or honest degradation.
Revisit if/when hc ships (a) 7501 coverage across the agent CLIs herdcluster
supports, and (b) stable `blocked`/readiness semantics — or if herdr's API
surface stagnates while hc's matures.

## 1. Current coupling inventory — what the bridge actually tunnels

`cluster-bridge` contains **zero protocol logic**: it pipes raw bytes
between a unix socket and a mux `StreamHandle` (`pipe.rs`), in both
directions, with a shared idle deadline. The protocol knowledge lives
entirely in `api-client` + `cluster-fleet`.

### 1.1 Transport facts the design relies on

- herdr API = NDJSON, **one request per connection**: `{id,method,params}\n`
  → one `{id,result|error}` line → server closes (`api-client/src/lib.rs`).
- Every `ApiClient::request()` = new unix conn → new bridge socket accept →
  new mux stream → new herdr socket conn. No reuse anywhere.
- Two long-lived exceptions, both of which hold a connection open:
  `agent.wait`-style calls (single response after a long server-side wait)
  and `events.subscribe` (ack line, then a stream of bare
  `{"event","data"}` envelopes — `api-client/src/methods.rs:675-868`).
- Bridge failure contract: pre-pipe failures are answered with a synthetic
  NDJSON error envelope (`worker_unreachable`, `herdr_unavailable` —
  `cluster-bridge/src/lib.rs:120`). Anything downstream of the pipe is the
  endpoints' problem.

### 1.2 herdr methods herdcluster actually calls

From `api-client/src/methods.rs` and call sites in `cluster-fleet`,
`cluster-node`, `herdcluster`:

| Layer | Methods |
|---|---|
| Compat/health | `ping` (protocol version guard), `session.snapshot`, `server.agent_manifests` |
| Fleet reads | `agent.list`, `agent.get`, `agent.read`, `agent.wait`, `agent.explain` |
| Agent ops | `agent.prompt`, `agent.send_keys`, `agent.rename`, `agent.focus`, `pane.close` |
| Spawn composer | `workspace.list/create/get/close`, `pane.list/split/get/read/current`, `agent.start` |
| Worktrees | `worktree.list`, `worktree.create` (one call site each — repo-path verification) |
| Events | `events.subscribe` with `pane.agent_status_changed`, `pane.exited`, `pane.closed` |

Consumers that depend on *semantics*, not just bytes:

- `cluster-fleet/src/spawn.rs` — the spawn composer is deeply herdr-shaped:
  find-or-create workspace by `cwd` → verify the workspace's real repo path
  via `workspace.get`/`pane.list` (guards against herdr silently landing in
  the wrong directory, issue #81) → `pane.split` → `agent.start` → wait for
  `interactive_ready`. It parses `workspace_created`/`pane_info` response
  envelopes for ids.
- `cluster-fleet/src/watch.rs` + `ops.rs` — consume `agent.*` status words
  (`idle|working|blocked|done|unknown`) and pane-scoped events; `agent
  wait --until` maps onto `agent.wait` `until` params.
- `cluster-node/src/resources.rs` — worker `Resources` reports
  `agent_count` (from `agent.list`) and `kinds[]` (from
  `server.agent_manifests`); the scheduler filters on both.
- `cluster-node/src/keepawake.rs`, `doctor.rs`, `provisioning.rs` —
  agent-count-driven sleep inhibition, per-kind CLI presence checks, and
  herdr release-asset install on the worker.

**Bottom line:** the bridge is backend-agnostic; everything above it is
herdr-API-shaped. "hc backend" therefore means either (a) a translation
shim that re-presents the herdr API on top of hc, or (b) a second
controller-side client + per-machine backend dispatch in the fleet layer.
A pure byte-pipe to `hc connect --json` stdio is *not* sufficient on its
own — the byte formats are compatible (NDJSON both ways) but the envelope
vocabularies are not (`{id,method,params}` vs JSON-RPC 2.0
`{jsonrpc,id,method,params}`, different method names, different result
shapes, different event envelopes).

## 2. hc ground truth (verified live, hc 0.1.17)

Transport: `hc connect --json` = persistent JSON-RPC 2.0 over stdio,
one message per line. `hello` returns `{generation:1, version, core{id,name},
methods[31], features[framed,attach,events,status,shell,resize_notifications],
framed:false}` and is **required before `events.subscribe` notifications
flow**. Method set: `hello`, `server.status|stop|restart`,
`terminal.run|list|get|kill|rm|keep|label|read|attach|send|wait|keys|
interrupt|put`, `machine.exec|list|add|connect|rename|rm`, `file.*`,
`doc.*`, `events.subscribe`, `rpc.discover`.

Verified behaviors:

- **`events.subscribe` hijacks the connection** — after the
  `{seq, terminals[]}` snapshot the conn "carries only notifications";
  subsequent RPCs time out (reproduced). Needs a dedicated `hc connect`
  process alongside the RPC one(s).
- **`terminal.run`** `{argv[], cwd, labels{}}` → `Terminal` with stable
  `address` (`t7`); remote form `machine/t7` is native.
- **`Terminal` model is flat**: `address, argv, cwd, labels, name,
  parent, pid, state(live|finished), status, status_ms, turn,
  foreground{name,argv,pid,cwd}, exit_code, reason, keep, screen`.
  `status` = OSC **7501** self-reported record `{state:
  idle|working|done|blocked|error, kind, msg, progress, app, title}`;
  `reported:false`/`status:null` when the program never reports.
  `turn` increments each time status leaves `working`.
- **`terminal.send`** `{terminal, text, if?(idle|working), force?, wait?}`
  → `{started: bool|null, status, turn, wait?}`. `started` = went working
  within a few seconds; `null` when the program doesn't report status.
  **No `agent_blocked`-style rejection** — a blocked agent's TUI receives
  the text unless gated by `if`/`force`.
- **`terminal.wait`** reasons: `status|exit|output|timeout|not_started|
  message|children`; result `{ended, reason, status, exit_code, signal,
  turn, children[]}`.
- **`terminal.read`** `{terminal, lines}` → `{text, complete, how:
  screen|history|stitched, why?}`.
- **`terminal.keys`/`interrupt`/`label`/`kill`/`rm`/`put`/`attach`** —
  full lifecycle; `interrupt` reports `{key, outcome:
  stopped|exited|still_working|unknown}`.
- **Events** arrive as `{"method":"event","params":{type, terminal,
  data, seq}}`; observed `type`s: `terminal` (create/update, data=Terminal),
  `foreground`, `finished` (`{exit_code, reason, signal}`), `doc`, plus
  `status`/`heartbeat`(15s)/`missed` per schema description. Snapshot+stream
  in one call with `seq` resume — arguably better than herdr's
  subscribe-then-filter model.
- **`machine.*`**: `machine.add` registers ssh targets or `--via` commands;
  `machine.list`/`connect`/`exec` give remote exec; `terminal.*` and
  `events.subscribe` span connected machines natively.
- `server.status` gives core id/version/pid/uptime/holders — the compat
  analogue of herdr's `ping`.

## 3. Method-mapping table (herdr → hc) for herdcluster's actual surface

| herdr method | hc equivalent | Verdict |
|---|---|---|
| `ping` | `hello` + `server.status` | NATIVE (different handshake) |
| `session.snapshot` | `events.subscribe` snapshot `{seq,terminals[]}` or `terminal.list` | WORKAROUND (no single snapshot RPC; subscribe snapshot is better) |
| `server.agent_manifests` | `templates.list` + heuristics | **GAP** — no manifest detection; "kind" is `foreground.name`/`argv`/`status.app` guesswork |
| `agent.list` | `terminal.list` (+labels filter) | WORKAROUND — must synthesize agent records, filter to agent-ish terminals |
| `agent.get` | `terminal.get` | WORKAROUND — field rename + status model remap |
| `agent.prompt` | `terminal.send` (+pre-poll `terminal.get`) | **GAP** — no typed blocked/not-ready rejection; `if` gate is `idle|working` only |
| `agent.wait` | `terminal.wait` | NATIVE-ish — reasons differ; `until:{done,idle}` maps to status-word waits |
| `agent.read` | `terminal.read` | NATIVE — no `detection`/`ansi` source variants |
| `agent.send_keys` | `terminal.keys` (+`interrupt`) | NATIVE |
| `agent.rename` | `terminal.label` / `name` at `run` | PARTIAL — rename-after-start unclear (labels yes, name maybe run-only) |
| `agent.focus`, `agent.explain` | — | MISSING — no focus concept; explain ≈ nothing |
| `pane.close` | `terminal.kill` + `terminal.rm` | WORKAROUND — two calls, and `rm` on a live terminal errors |
| `workspace.*` (list/create/get/close) | — | **MISSING** — flat model; spawn composer must be rewritten around `cwd`+labels |
| `pane.*` (list/split/get/read/current) | — | **MISSING** — terminal *is* the pane; `terminal.read` covers reads |
| `worktree.*` | — | MISSING — would need `git worktree` via `machine.exec`/local exec |
| `events.subscribe` (`pane.agent_status_changed` etc.) | `events.subscribe` (dedicated conn) | WORKAROUND — flat terminal events; map `status`/`finished`/`foreground` types; must translate herdr `{"event","data"}` envelopes if the controller still speaks herdr |
| `agent.start` (kind+pane) | `terminal.run {argv:[agent-cli…]}` | WORKAROUND — kind→argv resolution moves into herdcluster (templates or a kind table) |

## 4. Proposed designs

### Design A — worker-edge translation shim (herdr API facade over hc)

Worker bridge keeps piping raw NDJSON bytes, but instead of connecting to
the herdr unix socket it spawns a shim (`hc-bridge`, new module/crate)
that speaks herdr NDJSON on its stdio and internally drives one or two
persistent `hc connect --json` processes (one RPC, one dedicated to
`events.subscribe` — the hijack rule).

- **Controller side: zero changes.** `api-client`, `cluster-fleet`,
  `cluster-node`, CLI all keep speaking herdr NDJSON; a worker is still
  "another API socket".
- The shim must implement ~15 methods and *synthesize* the missing model:
  fake `workspace_id`/`pane_id`/`agent` records over hc terminals (e.g.
  one workspace per cwd, pane_id = `hc:<address>`), map `agent.start` →
  `terminal.run` with a herdcluster-owned kind→argv table, translate
  `events.subscribe` herdr envelopes from hc notifications, and fabricate
  `interactive_ready`/`agent_blocked` from `status.state`/`reported`.
- **Cost:** the shim is essentially a herdr-API compatibility layer —
  the hardest part of the whole idea, and it accumulates semantic debt
  (every herdcluster feature that reads workspace/pane structure gets a
  progressively faker answer). Estimated **3–6 weeks** of careful work +
  ongoing drift-tracking against both APIs.
- **Risk:** synthesized semantics will *look* right and be subtly wrong
  (e.g. `agent_blocked` can't be detected for non-7501 agents; spawn's
  repo-cwd verification has no `workspace.get` to check against).

### Design B — hc-native backend trait (dual backend, like the pool RFC)

Introduce a backend trait at the `api-client`/fleet seam: local machines
keep `HerdrBackend` (today's `ApiClient`), hc workers get `HcBackend`
(JSON-RPC client over the same bridge socket — the tunnel is byte-agnostic,
so `cluster-bridge` needs only a worker-side mode that pipes
stream ↔ `hc connect` stdio instead of the unix socket; `pipe.rs` generalizes
to `AsyncRead+AsyncWrite`). Fleet code branches per `MachineEndpoint`
backend for spawn/watch/ops; scheduler/resources report hc-shaped data.

- **Honest semantics** — no fake workspace/pane layer; spawn becomes
  `terminal.run` + labels; global IDs become `machine/t-address`.
- **Cost:** touches every fleet call site (spawn composer rewrite, watch
  event remap, ops status remap, doctor/resources/provisioning for hc,
  compat guard → `hello`/`server.status`). Estimated **4–8 weeks**,
  and it permanently forks the fleet logic into two dialects.
- The sister project's RFC (`herdr_cross_session_messaging` 00-rfc)
  already designed exactly this trait (`AgentRuntime`) for the messaging
  pool — reusable design, not reusable code (different workspace).

### Design C — status quo: `hc machine add`

hc already does cross-machine terminals over ssh or `--via` commands,
with `machine/t7` addressing, remote `machine.exec`, and events spanning
machines. For "run an agent on the Windows box" this covers the raw
capability today with **zero herdcluster code**.

- **What it lacks vs herdcluster:** no mDNS/pairing-code onboarding (needs
  ssh keys or a via-command per worker — the exact UX herdcluster was
  built to remove), no worker registry/scheduler/`--machine auto`, no
  caps, no keepawake, no `.herdcluster` spawn tracking, no `agent wait
  --until done` fleet semantics, no doctor. It is a terminal multiplexer
  feature, not a fleet controller.
- Verdict: complementary, not competitive. If the user's real need is
  "fire a coding agent at a remote box", `hc machine add` already wins;
  herdcluster's value is the controller/fleet layer on top.

## 5. What stays / what moves (Design B view)

| Component | hc backend impact |
|---|---|
| `cluster-core` (identity, pairing, codec, registry, scheduler scoring) | **Unchanged** — backend-agnostic already |
| `cluster-net` (TLS, mux, discovery, conn mgr) | **Unchanged** |
| `cluster-bridge` | Small: worker side gains "pipe to child stdio" mode (spawn `hc connect --json` per stream, or a small process pool); `pipe.rs` generalizes from `UnixStream` to `AsyncRead+AsyncWrite` |
| `api-client` | Gains `HcClient` (JSON-RPC over the bridge socket path) + backend trait; herdr client untouched |
| `cluster-fleet` | **Heaviest change** — spawn composer, watch, ops, ids all branch on backend |
| `cluster-node` | Worker provisioning installs `hc` (not herdr) on hc-mode workers; resource monitor maps `terminal.list`; keepawake/doctor per-backend |
| `herdcluster` CLI | Mostly unchanged; `--kind` semantics differ per backend (manifest vs template/argv table) |

## 6. Risks

1. **Agent status semantics (highest).** herdr detects agent state via
   manifests + screen scraping; hc relies on OSC 7501 self-reporting.
   `status:null, reported:false` is the common case for plain shells —
   whether claude/codex/opencode/devin actually emit 7501 records is an
   **open question** (also open in the sister project's RFC §8). Without
   it: `agent wait --until done` degrades to exit/`turn` heuristics,
   `blocked` is invisible, spawn readiness (`interactive_ready`) has no
   signal, and `agent_pane_busy`-style prompt gating disappears.
2. **7501 coverage per CLI** — must be verified empirically per `--kind`
   before any adapter ships; this is the gating unknown.
3. **Event model flat-vs-hierarchy.** hc events are terminal-scoped with
   `seq` resume (simpler, arguably better) but there is no
   workspace/tab/pane lifecycle — `watch`/`clean` semantics that assume
   pane ids and workspace grouping need re-derivation from `cwd`+labels.
4. **`events.subscribe` hijack** — a dedicated `hc connect` per worker
   (or per subscription) is mandatory; process lifecycle must be managed
   (respawn on core restart — `server.status` holders change on update).
5. **Process-spawn-per-stream cost.** herdr's one-conn-per-request maps to
   one `hc connect` spawn per API call if done naively (~tens of ms each,
   plus core connection setup). Needs a persistent-process pool keyed by
   stream role — new machinery in the worker bridge.
6. **`terminal.send` has no blocked rejection** — mail/prompts typed into
   a blocked TUI. Mitigation = pre-send `terminal.get` poll + `if` gate,
   same workaround the pool RFC chose; still heuristic for non-7501 CLIs.
7. **API drift on two axes** — hc is 0.1.x (generation 1, `framed` mode
   unexplored); herdr API drift tracking (compat bead) would need a
   sibling for hc. Dual maintenance.
8. **`machine/t7` collision with herdcluster global IDs** — hc's own
   machine registry and herdcluster's `label/agent` IDs are different
   namespaces; an hc-backend worker must not confuse them (hc machines
   are *terminals'* peers, herdcluster machines are *API* endpoints).

## 7. Effort estimate

| Piece | Size |
|---|---|
| `hc connect` JSON-RPC client (sync or tokio) | S — spike code exists (`hc-spike/src/client.rs`, ~300 lines, proven) |
| Worker bridge stdio-pipe mode + process pool | M |
| Design A shim (herdr facade over hc) | **L/XL** — 3–6 wks, high semantic-debt risk |
| Design B backend trait + fleet dual-dialect | **XL** — 4–8 wks |
| 7501 coverage verification per agent CLI | M — empirical, gates everything |
| Doctor/provisioning/scheduler/resources for hc | M |

## 8. Recommendation

**Defer the adapter; ship nothing now.** Rationale:

- The transport insight is correct and cheap, but the value of an hc
  backend is gated on **7501 status coverage** and **blocked/readiness
  semantics** that hc does not yet guarantee — the two things herdcluster's
  fleet UX (`wait --until`, watch, spawn readiness, scheduler kinds)
  depends on most.
- `hc machine add` already covers the "run a terminal on a remote box"
  use case for users willing to configure ssh/`--via`; herdcluster's
  differentiators (pairing, scheduler, registry, keepawake, fleet CLI)
  have no hc equivalent to fall back on.
- If hc matures (7501 coverage proven across claude/codex/opencode/devin,
  stable status semantics, framed mode for multiplexing), **Design B**
  (backend trait, honest semantics) is the right shape — the pool RFC's
  `AgentRuntime` trait is a proven template. Design A (fake herdr facade)
  should be avoided: it maximizes short-term diff size while minimizing
  long-term correctness.

Trigger to revisit: a verified 7501 coverage matrix for the supported
agent kinds + a decision that herdr-core (not herdr TUI) is the strategic
worker runtime.

## 9. Empirical OSC-7501 coverage (verified 2026-11-27, hc 0.1.17)

The gating unknown from §6.1/§8 was tested live: agent CLIs launched via
`hc run --bg <cli>` in `/tmp`, status read via `hc get` after ~25s boot.

| CLI | `reported` | `status.state` observed | Verdict |
|---|---|---|---|
| claude (2.1.332) | **true** | `blocked` + `kind:"permission"` + `msg:"trust this folder"` on the workspace-trust prompt; cleared after approval | **7501 works**, incl. blocked semantics |
| opencode | false | `status:null` | no 7501 observed |
| devin (v3000.15.14) | false | `status:null` | no 7501 observed |
| codex | — | not installed on this machine | untested |

Additional transport notes from the probe:

- `hc run --bg <cli>` accepts a bare CLI name and reports
  `{"address":"tN","agent":<cli>,...}` — agent detection is argv-based.
- `hc send` returned `{"started":false,"status":null}` for claude at its
  session-picker screen — `started` correctly reports "no working
  transition observed" when status reporting is absent/stale.
- `hc keys <t> down enter` drove the claude trust dialog end-to-end —
  `terminal.keys` covers interactive prompts.

**Impact on recommendation:** coverage is 1/3 for the kinds herdcluster
actually spawns (claude yes; opencode/devin no). The "defer" call stands
and is now evidence-backed: an hc backend today would have working
`blocked`/`wait` semantics for claude only, with silent degradation for
opencode/devin — exactly the "looks right, subtly wrong" failure mode
Design A was rejected for. Revisit trigger unchanged, now measurable:
7501 (or equivalent) coverage ≥ the kinds in `server.agent_manifests`.
