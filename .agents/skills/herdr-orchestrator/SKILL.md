---
name: herdr-orchestrator
description: >-
  Peer-to-peer multi-agent orchestration through Herdr panes (Socket API / CLI).
  Use when coordinating many main agents in parallel, worktree-isolated
  implementers, read-only reviewers, or department-style coding without native
  sub-agents. Requires HERDR_ENV=1. Not the stock Herdr agent skill.
---

# herdr-orchestrator

Read **`AGENTS.md` first** — it is the **only** progressive-disclosure map.
Do not maintain a second map here. Do not collapse this skill into one prompt.

## Hard gate

```bash
test "${HERDR_ENV:-}" = 1
```

If unset: check `PASEO_AGENT_ID`. If that is set, the room transport is
Paseo — see `HERDR_ORCHESTRATOR.md` → *Paseo transport*. If neither is
set, stop.

## Non-negotiables

- No native sub-agent / dedicated-thread spawns — only Herdr panes.
- Exactly one root.
- No pre-solving peers.
- Monitor treats `done` **or** `idle` as completion (avoid idle-only freeze).
- Peers do not know they are orchestrated.
- Log every orchestration action to `data/orchestrator/log.jsonl`.
- Only documented Herdr CLI / Socket API (see https://herdr.dev/docs/).
- Only root acquires/releases resource locks (peers never self-lock).
