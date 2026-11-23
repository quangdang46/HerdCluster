# Agent lifecycle

Manage peer main agents **only** through Herdr panes and the documented
CLI / Socket API. Disable native sub-agent / dedicated-thread primitives
from the host harness for the whole run.

## Preflight

```bash
test "${HERDR_ENV:-}" = 1 || { echo "not inside Herdr"; exit 1; }
printf '%s\n' "$HERDR_WORKSPACE_ID" "$HERDR_TAB_ID" "$HERDR_PANE_ID"
herdr pane current --current
herdr agent list
herdr api snapshot   # optional bootstrap; not a subscription
```

Parse every `pane_id` / `workspace_id` / `tab_id` from JSON. Never invent IDs.
Prefer `--current` or explicit IDs over the UI-focused pane of another client.

## Forbidden

| Forbidden | Why | Use instead |
| --- | --- | --- |
| Harness `Agent` / sub-agent / thread spawn | creates hidden workers outside Herdr | `herdr agent start` or `pane split` + `pane run` |
| Waiting only on `idle` for background peers | completion often reports `done` first | wait `done` **or** `idle`; see Monitor |
| Closing panes you did not create | can kill user work | only close orchestrator-owned panes |
| `herdr server stop` | ends the whole session | never from orchestration unless user orders it |
| Telling a peer "you are orchestrated by root" | breaks the peer illusion | peers stay user-facing |

## Spawn patterns

### A. Named agent helper (preferred when you want agent identity)

`agent start` takes an **existing pane**, not a cwd. Create the pane (or the
worktree, which opens one) first, then start the agent in it:

```bash
# 1. the pane must already be at its interactive shell prompt
herdr worktree create --cwd "$PWD" --branch "orch/<plan>/<slug>" \
  --label "<slug>" --no-focus --json      # parse result.root_pane.pane_id
#    (or: herdr pane split --current --direction right --no-focus)

# 2. start the agent in that pane by KIND
herdr agent start <name> --kind <kind> --pane <pane_id> --timeout 60000
```

Verified on the installed binary (2026-11-14):

```
Usage: herdr agent start <NAME> --kind <KIND> --pane <ID> [OPTIONS] [-- [AGENT_ARG]...]
```

- `--kind` is required and normalises the executable (`devin`, `claude`,
  `codex`, `gemini`, `opencode`, … — see `herdr agent start --help` for the
  full list). Pass extra argv after `--`.
- There is **no** `--cwd`, `--workspace`, `--tab`, `--split` or `--env` on
  `agent start`. The cwd comes from the pane. Getting this wrong yields
  `unknown option: --cwd`.
- `--permission-mode dangerous` and similar are agent argv, so they go after
  `--`: `herdr agent start X --kind devin --pane w4:p1 -- devin --permission-mode dangerous`
  (or omit them and rely on `--kind`'s canonical executable).
- Success returns `agent_status` plus `interactive_ready: true`. If the pane
  is not at a shell prompt, the start fails.

Do **not** inject orchestration-flavored env vars into peer panes — peers
should not be labeled as orchestrated workers. Keep role only in root's
roster/log.

Do **not** pass the task as argv. Open the interactive TUI, confirm it is
ready, then submit the brief (see *Submitting a brief* below).

### B. Low-level pane split + run

```bash
herdr pane layout --pane "$HERDR_PANE_ID"
herdr pane split --current --direction right --no-focus --cwd <worktree-path>
# parse result.pane.pane_id
herdr pane rename <pane_id> "<role>-<short-task>"
herdr pane run <pane_id> "claude"   # or codex / pi / ...
```

Geometry rule (from stock Herdr skill): wide pane → split `right`;
tall/narrow → split `down`. Avoid stacking same-direction splits until panes
are unusable.

### After launch — hand the brief

**Preferred: `agent prompt`.** It is the purpose-built submission call, has
`--wait`/`--until`, and returns a real error instead of silently doing
nothing:

```bash
herdr agent prompt <pane_id> "$(cat path/to/brief.md)" --wait --until working
```

Errors you can act on: `agent_blocked` (agent is waiting on permission UI —
do not auto-approve), `agent_prompt_stalled` (accepted but never reached
`working`/`blocked` within 5 s), `timeout` (your `--timeout` expired first).

**Fallback: `pane run` + explicit Enter.**

```bash
herdr pane get <pane_id>          # confirm agent_status
herdr pane run <pane_id> "$(cat path/to/brief.md)"
herdr pane send-keys <pane_id> enter
```

Measured 2026-11-14 on the installed binary (`devin` v3000.12.57): despite
`herdr pane run --help` saying "sends text and Enter in one call", a
multi-line brief arrived in the TUI composer as `[Pasted text #1 +78 lines]`
and **was not submitted** — the agent stayed `idle` until an explicit
`send-keys enter`. Reports of this differ by agent CLI and by how the text is
pasted, so treat the Enter as required and verify with `pane get`:

- still `idle` after submission → the text was typed but not submitted; send
  `enter`.
- `working` → accepted.
- text never appeared → it went somewhere else; `pane read` before retrying
  so you do not paste a second copy.

Never assume submission succeeded. `pane get` is the only confirmation.

Log every spawn to `data/orchestrator/log.jsonl` and append the roster row:

```json
{"ts":"...","event":"spawn","pane_id":"w1:p3","role":"implementer","name":"impl-auth","cwd":"/repo/.worktrees/impl-auth","model":"strong"}
```

## Peer illusion

Peers receive a normal user-facing brief. They must **not** be told:

- that root exists as an orchestrator
- that another pane is controlling them
- that their input is synthetic

From the peer's point of view, the human typed the brief. Root is invisible
infrastructure. See `roles/peer.md`.

## Monitor (required — not the root wait)

### Why a separate monitor

Race that freezes runs:

1. Root calls `herdr agent wait <pane> --until idle`
2. Peer finishes while its tab is backgrounded → Herdr reports **`done`**
3. Root is still waiting for `idle` → timeout or hang
4. Meanwhile the pane may be closed / recycled → wait never recovers cleanly

`done` and `idle` both appear in Herdr's public state set. The stock Herdr
skill treats them as related "not working" states with different attention
semantics; background completions often surface as `done`. **Confirm on your
binary** with `pane get` samples — policy below is defensive, not a claim of
undocumented server internals.

### Monitor rules

- Run a **monitor loop outside the root's blocking wait**.
  Implementation options that stay on documented surface:
  - periodic poll: `herdr pane get <pane_id>` / `herdr agent get <target>`
  - one-shot waits with short timeouts alternating targets
  - raw `events.subscribe` on `pane.agent_status_changed` when building a
    long-lived client
- Treat terminal completion as: status ∈ `{done, idle}` **after** having
  observed `working` (or after a settled prompt if the task was trivial).
- On `blocked`: surface to the user / root decision path; do not auto-type
  secrets or approvals unless the user explicitly authorized that class.
- On `unknown`: `herdr pane read ...` + `herdr agent explain <target> --json`
  before assuming failure.
- Never rely on a single long `wait ... --status idle` for background peers.

### Poll skeleton (CLI-only)

```bash
# pseudo — implement in the root shell / monitor pane
while true; do
  herdr pane get "$PANE"   # parse agent_status
  # log sample → data/orchestrator/log.jsonl
  case "$status" in
    working) : ;;
    blocked) handle_blocked ;;
    done|idle)
      if seen_working || brief_already_submitted; then
        harvest_and_reconcile
        break
      fi
      ;;
    unknown) diagnose ;;
  esac
  sleep 2
done
```

Optional wait with timeout as a secondary signal (never the only one):

```bash
# NOTE: there is no top-level `herdr wait`. The wait verbs live under
# `herdr agent wait` and `herdr pane wait-output`.
herdr agent wait "$PANE" --until done --timeout 5000 || true
herdr agent wait "$PANE" --until idle --timeout 5000 || true
```

### Harvest output

```bash
herdr pane read <pane_id> --source recent-unwrapped --lines 200
# or
herdr agent read <target> --source recent-unwrapped --lines 200
```

Sources: `visible`, `recent`, `recent-unwrapped` (preferred for transcripts),
`detection` (bottom buffer used by agent detection — not for user review).

## Restart / reconcile

| Situation | Action |
| --- | --- |
| Peer stuck `working` past budget | read output; if hung, send a clarifying follow-up via `pane run`; if still dead, close **only if orchestrator-owned** and respawn |
| Peer `blocked` on permission UI | notify user; do not guess |
| Pane process exited (`pane.exited` event / process-info empty) | log, remove from roster active set, respawn if work unfinished |
| Wrong cwd / wrong worktree | do not "fix in place"; spawn a new peer on the correct worktree |
| Server restart | Herdr restores layout; processes may be gone. Re-check `session.snapshot` / `agent list`. Native agent resume only if integrations reported session refs (see session-state docs). Re-bind roster to live pane IDs. |

## Kill / cleanup

Only for panes listed in `data/orchestrator/roster.jsonl` as owned:

```bash
herdr pane close <pane_id>
```

Worktree cleanup is separate (`parallel-worktrees.md`):

```bash
herdr worktree remove --workspace <child_workspace_id>   # optional --force if dirty
```

`workspace close` closes Herdr state only; it does **not** delete the git
checkout. Prefer `worktree remove` when the checkout should go away.

Log every close/remove.

## Event-driven attention broker (alternative to polling)

The polling monitor above works for any Herdr-compatible agent, but it
wastes turns on empty checks. A **Herdr plugin** can replace polling with
event-driven completion signals.

### Pattern: Attention Broker

A plugin (e.g. `local.attention-broker`) subscribes to
`pane.agent_status_changed` and wakes Root once when a peer reaches `idle`,
`done`, or `blocked`. See `herdr-plugins/attention-broker/` for a complete
reference implementation.

Key design:

- **No polling.** Events arrive on agent status transitions only.
- **Persistent queue before delivery.** Events are written to a state file
  before `pane run` attempts delivery. If Root is busy (e.g. composing a
  reply), the next turn will drain the queue — nothing is lost.
- **Deduplication.** A configurable `dedupe_window_ms` (default 5 s) prevents
  duplicate wake-ups for the same event.
- **Retry on Root idle/done.** If Root was not idle when the event arrived
  (`pane run` to a busy Codex session is silently dropped), the queue is
  preserved and retried when Root next reports `idle` or `done`.
- **Seat-scoped state.** Runtime state is keyed by the Herdr socket path, so
  linked rooms cannot cross-deliver events.

To link an attention broker into a named room session:

```bash
herdr --session <name> plugin link <path-to-plugin-dir>
```

Optional config (written to the plugin's config dir, discovered via
`herdr --session <name> plugin config-dir local.attention-broker`):

```json
{
  "root_name": "Root",
  "dedupe_window_ms": 5000
}
```

### When to use event-driven vs polling

| Method | Best for | Trade-off |
| --- | --- | --- |
| Polling (`pane get` loop) | Short-lived orchestration, generic agents | Wasted cycles; simple to debug |
| Event-driven (plugin) | Long-running rooms, many peers | Plugin install; Root must be idle at delivery |
| Hybrid | Production rooms | Plugin delivers events; poll monitor as fallback timeout |

## Agent state mapping (by backend)

Different agent CLIs report different session states to Herdr. The Herdr
integration plugin translates CLIs' internal states into the semantic states
that `agent_status` reports. Know your backend to interpret status correctly.

### OpenCode / Codex (state from `session.status`)

Mapped in the Herdr integration (see `herdr-agent-state.js`):

```
session.status.type:
  idle       → "idle"
  active     → "working"
  busy       → "working"
  pending    → "working"
  running    → "working"
  streaming  → "working"
  retry      → "working"
```

| Herdr event → State | Trigger |
| --- | --- |
| `session.created` | fresh session; report with `session_start_source: "new"` |
| `session.idle` | session went idle |
| `permission.asked` → blocked | subagent blocked on user input |
| `permission.replied` → working | user resolved |
| `question.asked` → blocked | blocked on a question |
| `question.replied` → working | answered |
| `session.error` → blocked | error surfaced |
| `session.deleted` | dropped (no report) |
| `session.compacted` → working | context compacted |

Child sessions (subagent/task tool sessions with a `parentID`) are filtered
so they cannot clobber the pane's root-agent state — only
`permission.asked`/`replied` surfaces as `blocked`/`working` from subagents.

### Claude Code

State mapping follows Herdr's standard agent protocol. Key transitions:

- idle → agent waiting at a prompt
- working → Claude is generating or running tools
- blocked → permission dialog or question pending user input
- done → task completed, output available

### Other agents (generic)

If `session.status` is not available, the Herdr integration reports agent
liveness through terminal-level heuristics. Expect less precise state.

### Roster invariants

- one root pane id recorded
- every active peer has: `pane_id`, `role`, `cwd`/`worktree`, `status`, `brief_ref`
- closed pane IDs are never reused by Herdr — drop them; do not recycle
- after `pane.move` across workspaces, public pane id changes — re-read response