# Herdr Orchestrator

Coordinate **many peer main agents** through Herdr panes.
Each peer is a full agent process in its own terminal. Root only steers via
the Herdr CLI / Socket API. There is no sub-agent tree.

## When to use this skill

Use when the user wants:

- multiple independent coding agents working in parallel
- peer review, adversarial design, or multi-owner implementation
- isolation via git worktrees per agent
- orchestration that survives detach (Herdr server keeps panes alive)

Do **not** use when:

- a single agent can finish the task in one context
- `HERDR_ENV=1` is not set (you are outside Herdr)
- the user only asked for ordinary Herdr pane control (use stock Herdr skill)

## What this skill is not

- Not a reinstall of Herdr's bundled `SKILL.md` agent skill.
- Not a plugin, marketplace package, or server extension.
- Not permission to invent APIs. Only documented Herdr control surface.

## Topology

| Role | Count | Writes project? | File |
| --- | --- | --- | --- |
| `root` | exactly 1 | no (read + orchestrate only) | `roles/root.md` |
| `peer` | N | yes, inside assigned worktree | `roles/peer.md` |
| `implementer` | 0..N peer subtype | yes, assigned files only | `roles/implementer.md` |
| `reviewer` | 0..N peer subtype | no (read-only review) | `roles/reviewer.md` |

Scout / weak-model work is a **mode** of a peer, not a fifth role.
See `model-routing-and-context.md`.

## Control surface (documented only)

Prefer CLI wrappers (they talk to the same local socket API):

| Job | CLI |
| --- | --- |
| Bootstrap view | `herdr api snapshot` / `session.snapshot` |
| List agents | `herdr agent list` |
| Start named agent | `herdr agent start <name> ... -- <argv...>` |
| Split pane | `herdr pane split ... --direction right\|down --no-focus` |
| Launch in pane | `herdr pane run <pane_id> "..."` |
| Inspect pane | `herdr pane get <pane_id>` |
| Read output | `herdr pane read <pane_id> --source recent-unwrapped --lines N` |
| Wait agent state | `herdr agent wait <pane_id> --until <idle\|working\|blocked\|done\|unknown> [--timeout MS]` |
| Wait output | `herdr pane wait-output <pane_id> --match <text> [--timeout MS]` |
| Worktree | `herdr worktree create\|open\|list\|remove ...` |
| Events (raw) | `events.subscribe`, `events.wait` |

Agent semantic states (from Herdr docs): `blocked`, `working`, `done`, `idle`,
`unknown`.

### `idle` vs `done` (operational rule)

Public docs list both states. The stock Herdr agent skill describes them as
related completion/waiting states with different **attention** treatment
(`done` often means finished but not yet seen; `idle` often means waiting or
finished and already seen / opened at a prompt). **Verify against your
installed binary and current docs** if behavior looks off — do not treat this
paragraph as stronger than live `herdr pane get` samples.

Operational rule for this skill (defensive):

- Treat **either** `done` or `idle` as "not actively working" when reconciling.
- Never wait **only** on `idle` for a background peer — it may report `done`
  first and freeze an idle-only waiter.
- Always pair waits with a monitor poll of `pane get` / `agent get`.

## Progressive file map

1. Confirm gate + decide topology → this file + `roles/root.md`
2. Spawn peers → `agent-lifecycle.md` + `parallel-worktrees.md`
3. Assign work → `delegation.md` + role file for the peer type
4. Choose models / keep context small → `model-routing-and-context.md`
5. Before tests or merges → `verification-and-safety.md`
6. When something feels wrong → `ANTI_PATTERN.md`

## Runtime data layout (project-local)

Create under the project root (gitignored if needed):

```text
data/
  orchestrator/
    roster.jsonl          # pane_id, role, worktree, model, started_at
    log.jsonl             # every spawn/read/wait/state sample
    locks/                # resource locks (tests, shared services)
  plans/
    <plan-name>.md        # named large plans
```

All orchestration actions log a line to `data/orchestrator/log.jsonl`.
No blackbox steps.

## Success criteria for a run

- one root only
- every peer on its own worktree (or explicit shared-read reviewer path)
- no native sub-agent spawns
- every heavy test/evidence run holds a lock first
- every pane lifecycle event is logged
- peers never receive "the answer is X, confirm" pre-solved prompts

## Paseo transport (alternative room backend)

When `HERDR_ENV` is not set and `PASEO_AGENT_ID` is available, the room
transport is **Paseo** — another control plane for managing engineering
seats. Paseo is not "Herdr but different flags"; it is a distinct system
with its own vocabulary and lifecycle model.

### When to use

- The session has injected Paseo tools (`paseo_*` functions available).
- `HERDR_ENV=1` is false but a Paseo-managed session is detected.
- The user explicitly says "Paseo" or a room setup flow identifies Paseo.

### Hard rule

Exactly **one room transport** for engineering seats. Do not run a hidden
Herdr department behind a Paseo Root or a hidden Paseo fleet behind a
Herdr Root.

### What Paseo is and is not

Paseo is a **control plane** for provider discovery, structured sessions,
lifecycle notifications, activity/context metadata, permissions, and
optional chat transport. It is **not** the room's operating philosophy.
Do not load or imitate Paseo's advisor, committee, handoff, loop, or base
orchestration workflows as a substitute for this Root doctrine.

### Seat topology

Every engineering collaborator created by Root in Paseo is a **detached
sibling** in the current workspace:

```json
{
  "relationship": { "kind": "detached" },
  "workspace": { "kind": "current" },
  "notifyOnFinish": true
}
```

Never create a Paseo `subagent`. Parent/child ownership, cascade archive,
and function-like synthesis are not the room topology.

### Operational commands

| Job | Paseo |
| --- | --- |
| List seats | `list_agents` |
| Inspect one seat | `get_agent_status` |
| Read recent trajectory | `get_agent_activity` (bounded, only what a decision needs) |
| Send work | `send_agent_prompt` |
| Cancel active turn | `cancel_agent` (not archive) |
| Archive (safe only) | `archive_agent` |
| Kill permanently | `kill_agent` |

### Notifications, not polling

Seats created with `notifyOnFinish: true` fire finish/error/permission
notifications automatically. Do not poll after creating or prompting a seat;
continue other leadership work until a notification, agreed checkpoint, or
safety reassessment becomes due. A notification reports lifecycle, not
acceptance. Read the stable handback and judge it.

### Chat rooms (optional)

Paseo chat rooms are optional decision surfaces, not a status bus. Use a
direct prompt for ordinary one-to-one ownership and consultation. A seat
may post and mention Root for a cross-boundary decision gate or a
discussion that genuinely benefits multiple existing participants. Do not
broadcast routine progress, recruit seats through chat, or let chat create
a second command hierarchy.

### Provider profiles

Discover provider, model, thinking option, mode, and feature IDs — do not
guess them. Provider profiles select runtime and credentials; provider
modes may select a behavioral agent profile (e.g. OpenCode writable owner
or independent reviewer). The project room protocol may narrow the allowed
providers, models, modes, or review rhythm.

## Canonical docs to re-check when unsure

- https://herdr.dev/docs/socket-api/
- https://herdr.dev/docs/cli-reference/
- https://herdr.dev/docs/session-state/
- https://herdr.dev/docs/agents/
- https://herdr.dev/docs/concepts/
- https://herdr.dev/docs/agent-skill/ (reference only; do not install as this skill)