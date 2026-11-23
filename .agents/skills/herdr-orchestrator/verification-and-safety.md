# Verification and safety

Transparent orchestration: every pane action and state sample is logged.
Heavy tests/evidence require locks so agents do not stomp each other.

## Transparency log

Append-only file:

```text
data/orchestrator/log.jsonl
```

Log at least:

| Event | Fields |
| --- | --- |
| `spawn` | pane_id, role, cwd, cmd/argv, name |
| `delegate` | pane_id, plan, brief ref |
| `state` | pane_id, agent_status, source (`pane get` / wait) |
| `read` | pane_id, source, lines |
| `wait` | pane_id, kind (`agent-status`/`output`), target, timeout, outcome |
| `lock_acquire` / `lock_release` | resource, owner pane_id |
| `close` / `remove` | pane_id / workspace_id |
| `error` | context, message |

No silent setup. If it happened, it is in the log.

Roster mirror:

```text
data/orchestrator/roster.jsonl
```

### Log rotation (required for long runs)

`log.jsonl` must not grow without bound into root context.

- Soft cap: ~2 MiB or ~10_000 lines (whichever first).
- On cap: rename to `data/orchestrator/log-archive/log-YYYYMMDD-HHMMSS.jsonl`
  then start a fresh `log.jsonl` with one `{"event":"log_rotate","from":"..."}`
  line.
- Root reads only the active `log.jsonl` plus, if needed, the newest archive
  tail — never the full archive history in one shot.
- Evidence files under `data/orchestrator/evidence/` are separate; prune by
  plan completion, not by stuffing them into the live log.

## Locks for heavy evidence

### Who may lock

**Only root** acquires and releases resource locks.

Peers / implementers / reviewers never write lock files themselves. If a peer
needs a heavy suite, it says so in chat; root acquires, authorizes the run
(or runs it), then releases. This avoids TOCTOU races between two peer
panes both trying "check then write".

### When a lock is mandatory

- full test suite / integration suite
- e2e, browser, or device runs
- shared local services (db on fixed port, dev server on fixed port)
- any job that mutates shared databases, queues, or docker compose
- release builds that contend on the same artifact dir

Light unit tests inside an isolated worktree may skip locks if they truly
do not share ports/state. When unsure, lock.

### Lock mechanism (skill-level, not a Herdr API)

Herdr does not ship a distributed lock API in the public docs. Implement
locks as project files coordinated by **root only**, using pane state for
liveness of the *job owner* (the pane allowed to run the suite):

```text
data/orchestrator/locks/<resource>.lock
```

Suggested contents:

```json
{
  "resource": "just-test",
  "acquired_by_root_pane_id": "w1:p1",
  "job_owner_pane_id": "w1:p4",
  "job_owner_name": "impl-auth",
  "acquired_at": "...",
  "expires_at": "...",
  "note": "just check"
}
```

Acquire algorithm (**root serializes all lock ops**):

1. Root is the only writer of lock files — no parallel acquirers by policy.
2. `herdr pane get` on `job_owner_pane_id` if reclaiming / checking progress.
3. If lock file missing or expired **and** previous job owner pane gone →
   take lock.
4. If lock held for a live job → wait (poll + backoff). Do not start a second
   heavy job on that resource.
5. Write lock file; log `lock_acquire`.
6. Authorize / run the suite.
7. Release lock; log `lock_release`.

Optional: dedicate a small **lock-monitor** pane (ordinary shell, not a
coding agent) that only prints lock table + agent statuses for humans.

### Use Herdr state as liveness, not as the lock itself

```bash
herdr pane get <job_owner_pane_id>
herdr agent wait <job_owner_pane_id> --until done --timeout 5000 || true
```

If the job owner is `working` on the locked resource, wait. If that pane
is gone but lock remains, root reclaims after logging.

## Test / evidence discipline

- Prefer project recipes (`just test`, `just check`) when the repo defines
  them.
- Capture command + exit code + tail of output into
  `data/orchestrator/evidence/<run-id>.txt` (or similar).
- Root does not "declare green" without evidence files or a peer's logged
  command result.
- Flaky suspicion → re-run under the same lock; do not parallelize the same
  suite to "go faster".

## Review safety (read-only)

Reviewer peers are **read-only on the product tree and shared services**.

Allowed without root lock:

- read files, `git diff`, static explain, search
- write review notes under `data/plans/...` only

Not allowed for reviewers by default:

- product file edits, commit, push, merge
- any test or command that mutates shared DBs, ports, docker, or fixtures
- claiming "I ran the full suite" unless root ran it under lock and attached
  evidence

If confidence truly needs a heavy suite: reviewer **requests** it; **root**
acquires the lock and either runs the suite or temporarily authorizes a
non-reviewer job owner. The reviewer role itself stays non-mutating.

See `roles/reviewer.md`.

## Wait safety

- Always inspect current state before waiting (`pane get` / `pane read`).
- Prefer short timed waits + monitor loop over one huge blocking wait
  (see `agent-lifecycle.md` idle/done race).
- `pane wait-output --match` for ordinary commands; `agent wait --until` for
  agents. There is no top-level `herdr wait`.
- Timeout → diagnose, do not invent success.

## Focus / UI safety

- Background orchestration uses `--no-focus`.
- Do not steal the human's focused pane.
- Do not close user-owned workspaces/tabs/panes.
- Never `herdr server stop` unless the user explicitly commands full stop.

## Blocked / permission UI

When status is `blocked`:

1. log it
2. `pane read` / `agent read` to see the prompt
3. notify the user
4. do not auto-approve destructive permissions without explicit standing orders

## Session restore awareness

From session-state docs: detach keeps processes; server restart does not
keep arbitrary processes; native agent resume only for integrations that
reported session refs. After any reconnect:

```bash
herdr api snapshot
herdr agent list
```

Rebuild roster bindings to live IDs before continuing waits.