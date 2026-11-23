# Delegation

How root hands work to peer main agents without turning them into
obedient sub-agents.

## Core rules

1. **No pre-solving.** Root must not compute the answer, then ask the peer
   to "confirm" or "implement exactly this solution". Open questions first;
   challenge only after the peer presents a position.
2. **Peers are peers.** Treat them as agents with their own judgment, not
   interns executing a script. Give goals, constraints, and evidence paths —
   not keystroke-level recipes unless the user demanded that level.
3. **One root.** Only the root profile may assign, reassign, or reconcile
   department-wide work.
4. **Named large plans.** Any plan larger than a single PR-sized change
   gets a stable name and a file under `data/plans/<plan-name>.md`.

## Brief shape (send via `pane run`)

A brief is user-voice text. No mention of orchestration, root, or Herdr
control. Suggested sections:

```markdown
# Task: <short title>

## Goal
<what done looks like — observable outcome>

## Context
- repo / area: ...
- constraints: ...
- out of scope: ...

## Open questions
- <question the peer should answer before coding>
- <trade-off the peer owns>

## Evidence expected
- commands to run / artifacts to leave
- files that may change (if known)

## Plan name (if any)
data/plans/<plan-name>.md
```

For design / research peers, stop after **Open questions** and wait for
their write-up before authorizing implementation.

## Anti pre-solve checklist

Before sending a brief, root must answer **no** to all of:

- [ ] Did I already pick the architecture and only want rubber-stamp?
- [ ] Did I embed the exact patch / diff the peer must produce?
- [ ] Did I phrase the task as true/false against my hidden answer?
- [ ] Did I forbid the peer from disagreeing?

If any is yes, rewrite: state the problem, constraints, and success checks;
leave the approach open.

## Challenge protocol (after peer responds)

1. Read peer output with `pane read` / `agent read`.
2. Extract claims, not vibes.
3. Challenge with **new** questions or counter-examples — still open form.
4. Do not say "wrong, do X" unless the peer's approach is unsafe or the user
   already fixed the decision.
5. If escalation is needed, spawn a second peer with the first peer's
   artifact as context (still no pre-solved verdict from root).

## Plan files

```text
data/plans/<plan-name>.md
```

Minimum plan file contents:

- title + date
- problem statement
- options considered (filled by peers over time)
- decision log (who decided what, when)
- work packages with owners (peer names / pane labels)
- verification plan (links to locks / tests)
- status: `draft | active | blocked | done`

Root creates the shell; peers fill substance. Root does not pre-fill the
chosen option.

## Assignment patterns

| Pattern | When | How |
| --- | --- | --- |
| Solo peer | one coherent task | one brief → one pane |
| Parallel peers | independent packages | N briefs, N worktrees, monitor all |
| Implementer + reviewer | code + audit | implementer writes; reviewer read-only on same diff/worktree or main |
| Adversarial pair | high-risk design | two peers, same open question, independent; root synthesizes later |
| Scout then strong | large unknown surface | weak model maps; strong model decides (see model-routing) |

## Reassignment

- Prefer follow-up `pane run` on the same peer when context is still warm.
- Respawn on a fresh peer when the first peer is stuck, contaminated by a
  bad plan, or on the wrong worktree.
- Never silently overwrite another peer's worktree files from root.

## What root may do vs must not do

| Root may | Root must not |
| --- | --- |
| Frame goals and constraints | Pre-write the peer's solution |
| Supply facts, logs, failing tests | Hide the real goal and quiz the peer |
| Name plans and packages | Micromanage every file edit |
| Challenge after hearing the peer | Force agreement with root's private answer |
| Stop unsafe work | Gaslight the peer about user intent |

## Logging

Every delegation event → `data/orchestrator/log.jsonl`:

```json
{"ts":"...","event":"delegate","to":"w1:p4","plan":"auth-refresh","brief_sha":"...","mode":"open-question"}
{"ts":"...","event":"challenge","to":"w1:p4","note":"asked for failure-mode analysis"}
{"ts":"...","event":"accept","from":"w1:p4","artifact":"data/plans/auth-refresh.md"}
```