# herdr-orchestrator

Peer-to-peer multi-agent orchestration over Herdr's Socket API / CLI.
Not the stock Herdr agent skill. Not a main→sub-agent hierarchy.

## Hard gate

```bash
test "${HERDR_ENV:-}" = 1
```

If this fails but `PASEO_AGENT_ID` is set, the room transport is **Paseo**.
Read `HERDR_ORCHESTRATOR.md` → *Paseo transport* section instead.

If neither is set: stop. You are not inside a managed room. Do not invent
control paths or call the focused session from outside the room transport.

## Transport selection logic

1. `HERDR_ENV=1` → Herdr. All role and lifecycle docs apply as written.
2. `PASEO_AGENT_ID` set → Paseo. Read the Paseo section in
   `HERDR_ORCHESTRATOR.md`. Same Root doctrine; different commands and
   lifecycle model.
3. Neither → not in a multi-agent room. Stop.

## Progressive disclosure — read only what the current step needs

| When | Read |
| --- | --- |
| First load / decide whether this skill applies | `HERDR_ORCHESTRATOR.md` |
| Paseo transport (when `PASEO_AGENT_ID` is set) | `HERDR_ORCHESTRATOR.md` → *Paseo transport* |
| Spawn, kill, restart, reconcile panes | `agent-lifecycle.md` |
| Event-driven attention (plugin) vs polling | `agent-lifecycle.md` → *Event-driven attention broker* |
| Interpret an agent's status correctly | `agent-lifecycle.md` → *Agent state mapping (by backend)* |
| Hand work to a peer without pre-solving | `delegation.md` |
| Choose model strength / manage context | `model-routing-and-context.md` |
| Parallel coding without file collisions | `parallel-worktrees.md` |
| Locks, tests, evidence, review safety | `verification-and-safety.md` |
| Catch yourself doing the wrong thing | `ANTI_PATTERN.md` |
| Act as the single orchestrator | `roles/root.md` |
| Act as a peer main agent | `roles/peer.md` |
| Act as an implementer peer | `roles/implementer.md` |
| Act as a read-only reviewer peer | `roles/reviewer.md` |

`AGENTS.md` is the **single** progressive map. `SKILL.md` only points here.

Root is the only profile that may coordinate the whole department.
Peers never load root procedures unless they are promoted.

## Authority stack (do not invert)

1. Live Herdr binary in `PATH` (`herdr --help`, command groups, JSON responses).
2. Public docs: https://herdr.dev/docs/ (socket API, CLI, session state, agents).
3. This skill: orchestration policy on top of (1) and (2).

Never invent socket methods, CLI flags, agent states, or IDs.
Parse IDs from JSON. Prefer CLI wrappers; use raw socket methods only when
you need request/response control or event subscriptions.

## Forbidden harness primitives

Disable completely while this skill is active:

- native "spawn sub-agent" / "dedicated thread" / in-process swarm tools
- any main→sub-agent control channel that bypasses Herdr panes

Lifecycle is only Herdr: split/start pane → send task → wait/read state →
reconcile. See `agent-lifecycle.md`.

## One-line model

```
user (1)  ──herdr CLI/socket──►  peer panes (N main agents)
                 ▲
                 │
            monitor loop
         (not the root wait)
```

Peers believe they are talking to the user. They do not know they are
orchestrated through Herdr.