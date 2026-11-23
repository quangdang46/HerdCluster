# Anti-patterns

Each entry: **Symptoms** / **Why it hurts** / **Fix**.

---

## 1. Pre-solve pattern

**Symptoms**

- Briefs say "implement the following design" with the full solution already chosen.
- Root asks true/false quizzes against a hidden answer.
- Peer is punished for disagreeing with root's private plan.

**Why it hurts**

- Wastes the peer's judgment; turns a main agent into a stenographer.
- Encodes root's blind spots into every package.
- Hides disagreement until production.

**Fix**

- Open questions first; challenge after the peer presents a position.
- Follow `delegation.md` anti pre-solve checklist.
- Name plans in `data/plans/` and let peers fill options.

---

## 2. Balloon pattern

**Symptoms**

- Huge features land on an untested or unclear foundation.
- New locks, heuristics, and special cases pile up to paper over core bugs.
- "Just one more adapter" keeps the demo alive while invariants rot.

**Why it hurts**

- Cost compounds; later peers inherit a swamp.
- False confidence from UI progress without structural truth.

**Fix**

- Require a named plan with foundation checks before parallel feature work.
- Strong reviewer on architecture before N implementers fan out.
- Scout may map, but must not greenlight a balloon.

---

## 3. Brake pattern

**Symptoms**

- Merge proceeds without locks, tests, or evidence.
- "We'll add safety later" on auth, data, or protocol paths.
- Root accepts peer output that never ran the project's check recipe.

**Why it hurts**

- Shipping without brakes makes every later fix emergency work.
- Multi-agent speed multiplies breakage.

**Fix**

- Mandatory locks for heavy suites (`verification-and-safety.md`).
- No accept without evidence log.
- Reviewer is read-only but has veto language on high-impact paths.

---

## 4. Blind subordinate pattern

**Symptoms**

- Peer briefs forbid questions or alternatives.
- Root treats peers as sub-agents of the host harness.
- Peers are told "you are worker #3 of the swarm".

**Why it hurts**

- Destroys peer initiative and the user-illusion contract.
- Reintroduces main→sub-agent coupling this skill exists to avoid.

**Fix**

- Peer illusion: they think they talk to the user (`agent-lifecycle.md`).
- Delegate goals and constraints, not obedience scripts.
- Disable native sub-agent spawns entirely.

---

## 5. Idle-state freeze race

**Symptoms**

- Root hangs on `agent wait --until idle`.
- Peer already finished with `done` in a background tab.
- Orchestration freezes until a huge timeout; pane may already be closed.

**Why it hurts**

- Indefinite stalls; false "peer still working" reports.
- Cleanup races corrupt the roster.

**Fix**

- Separate monitor loop; treat `done` **or** `idle` as completion.
- Short timed waits + `pane get` polls.
- See monitor section in `agent-lifecycle.md`.

---

## 6. Blackbox setup

**Symptoms**

- Panes appear with no log lines.
- Nobody can answer which worktree a peer used.
- Failures cannot be reconstructed after detach/reattach.

**Why it hurts**

- Undebuggable multi-agent systems are worse than one agent.
- Violates the transparency rule of this skill.

**Fix**

- Every spawn/wait/read/lock/close → `data/orchestrator/log.jsonl`.
- Maintain `roster.jsonl`.
- Prefer CLI JSON responses as the source of truth for IDs.

---

## 7. Shared-worktree stampede

**Symptoms**

- Two implementers started with the same `--cwd` or branch.
- Random merge conflicts; flaky tests from dual writers.

**Why it hurts**

- Silent overwrite; lost work; unreproducible diffs.

**Fix**

- One writer per `worktree create` branch (`parallel-worktrees.md`).
- Partition paths in the plan before spawn.

---

## 8. Scout-as-judge

**Symptoms**

- Weak model maps the repo and also decides schema/security.
- Draft scout notes get merged as final architecture.

**Why it hurts**

- High-impact wrong answers at low confidence.

**Fix**

- Scout mode is draft-only; escalate to strong peer/root
  (`model-routing-and-context.md`).

---

## 9. Focus theft

**Symptoms**

- Peer spawns use default focus and yank the human's UI around.
- Root closes random panes to "clean up".

**Why it hurts**

- Breaks human trust; may destroy live user work.

**Fix**

- `--no-focus` for background work.
- Close only roster-owned panes.
- Never `server stop` without explicit user order.

---

## 10. Inventory of one root violated

**Symptoms**

- Multiple panes both believe they are root.
- Competing assignments to the same peer.

**Why it hurts**

- Split-brain department; double locks; contradictory briefs.

**Fix**

- Exactly one root profile per department/project run.
- Record root `pane_id` in roster; refuse second root spawn.