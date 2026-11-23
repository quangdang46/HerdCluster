# Model routing and context

Only **four roles** exist: `root`, `peer`, `implementer`, `reviewer`.
Each has one config file under `roles/`. Do not invent extra role names.
Do not paste a full system prompt on every spawn — load the role file once
into the brief preamble or agent instructions for that pane.

## Role × model matrix

| Role | Default strength | Allowed to conclude high-impact calls? |
| --- | --- | --- |
| `root` | strong | yes (orchestration + final synthesis) |
| `peer` | strong by default | yes within assigned scope |
| `implementer` | strong for risky code; medium for local edits | yes for local code; escalate schema/security |
| `reviewer` | strong preferred | yes for review verdicts; still read-only on tree |

### Scout mode (not a fifth role)

A **scout** is a `peer` or `implementer` intentionally run on a weaker /
cheaper model for mapping only.

Scout **may**:

- list files, summarize modules, draft inventories
- produce *draft* artifacts clearly marked `DRAFT — scout`
- propose candidate approaches without ranking them as final

Scout **must not**:

- declare final true/false on schema, architecture, security, or migrations
- approve merges
- edit production paths without a strong peer reviewing first
- be the only opinion on irreversible decisions

Escalate scout output to a strong `peer` / `root` before acting on it.

## What counts as high-impact (always escalate off scout)

- database / API schema changes
- auth, crypto, permissions, multi-tenant boundaries
- public protocol / wire format
- data deletion or destructive migrations
- security-sensitive config
- anything the user labeled release-risk

## Context management

### Root

- Keep a slim roster + plan index, not full peer transcripts.
- Read peer tails with `pane read --source recent-unwrapped --lines N`
  (start ~80–200 lines; increase only if incomplete).
- Prefer artifacts on disk (`data/plans/...`, PR diffs, test logs) over
  re-injecting entire chats into the next brief.
- One root conversation for the department — do not fork root.

### Peers

- Each peer starts clean in its pane/worktree.
- Brief carries only the package it owns + pointers to plan files.
- Do not dump other peers' full transcripts into a peer brief.
- When context is bloated: ask the peer (via follow-up) to write a summary
  artifact, then continue from that file — or respawn with the summary.

### Reviewer

- Input = diff / file list / plan section, not the implementer's whole chat.
- Output = structured findings only (see `roles/reviewer.md`).

## Spawn-time config (no prompt spam)

Per role file, keep:

- mission boundary
- write permissions
- escalation rules
- output format

Root's spawn path:

1. create worktree / pane (`agent-lifecycle.md`, `parallel-worktrees.md`)
2. start agent interactive binary
3. `pane run` a user-voice brief: goal, constraints, owned paths, evidence
   expected — **no** pointers to `roles/*.md`, no "you are implementer #2",
   no orchestrator vocabulary
4. do **not** restate this skill into the peer

Peers should never receive root procedures or skill control-plane files.
If a project keeps human task cards, they must be free of herdr/orchestrator
jargon; otherwise inline the constraints in the brief only.

## Routing heuristics

```text
unknown large surface     → scout peer (weak) → strong peer decides
single-package code       → implementer (strong/medium)
security / protocol       → strong peer + strong reviewer
parallel independent pkgs → N implementers, one reviewer later
user-facing product call  → strong peer; root synthesizes options for user
```

## Reasoning effort

Override effort per-agent when the question warrants it:

```bash
herdr agent start <seat-name> --cwd <project> --split right -- \
  codex -p peer -c 'model_reasoning_effort="<medium|high|xhigh|max>"'
```

| Effort | When |
| --- | --- |
| `medium` | Settled, locally bounded work; straightforward edits |
| `high` | Default for independent technical judgment and meaningful behavior |
| `xhigh` / `max` | High stakes, deep architecture, protocol/security decisions |

Treat effort as reasoning budget. Escalate when decision depth and stakes
justify the latency. Default to `high` unless the question is mechanically
trivial.

## Logging model choice

```json
{"ts":"...","event":"route","pane_id":"w1:p5","role":"peer","mode":"scout","model":"weak","reason":"map src/detect only"}
{"ts":"...","event":"escalate","from":"w1:p5","to":"w1:p6","reason":"schema decision"}
```