# herdcluster

Control a fleet of coding agents spread across machines (Mac controller +
remote workers such as WSL2/Windows/Linux/Mac minis). herdcluster is a thin
wrapper over each machine's local `herdr` — it never touches agent
credentials and never runs commands on workers directly; everything goes
through herdr's NDJSON API.

## Mental model

- Every machine runs `herdr` (the engine). herdcluster only routes commands.
- Global agent IDs look like `machine/agent-name` (e.g. `macbook/probe2`,
  `desktop-win/fix-auth`). Bare names are rejected — always qualify.
- `local` is the machine herdcluster runs on; other labels come from
  `herdcluster machines`.
- `--machine auto` lets the scheduler pick the worker with the most free
  RAM/agent slots for the requested kind.
- `.herdcluster/` in a project dir tracks spawned agents (`agents.json`)
  and per-machine repo paths (`config.json`); `spawn` records automatically
  and `clean` tears them down.
- Daemons (controller/worker) hold a keep-awake assertion while any local
  agent is `working` — machines won't sleep mid-task (`keepawake` config,
  default on).

## Commands agents should use

```bash
herdcluster machines                 # fleet table: name/type/status/agents/cpu/mem
herdcluster machines --json          # stable schema for scripting

herdcluster agent list --all         # every agent on every machine, merged
herdcluster agent list --all --json  # kind, status, pane, workspace fields

herdcluster agent get <id>           # details
herdcluster agent read <id>          # terminal output (visible source)
herdcluster agent wait <id> [--until done,idle] [--timeout 30m] [--stuck-after 10m]
                                     # fleet-aware: works for remote ids too;
                                     # exit 0 on status match, 1 on timeout
herdcluster agent watch <id> [--json]  # stream status transitions (JSONL events)
herdcluster agent prompt <id> "..."  # send a prompt
herdcluster agent send-keys <id> ... # raw keys (space-separated names)
herdcluster agent rename <id> <new>

herdcluster spawn <name> --kind <kind> --machine <label|auto> [--repo <path>] [--branch b] [--json] [-- agent args]
                                     # one-step: workspace+pane+agent.start,
                                     # prints global id when ready;
                                     # --repo optional: uses .herdcluster
                                     # machine mapping, else home dir

herdcluster clean [--machine <label>] [--dry-run] [--yes] [--close-workspaces]
                                     # tear down agents recorded in
                                     # .herdcluster/agents.json (pane.close,
                                     # optional workspace.close)

herdcluster status [--all] [--json]  # cluster overview
herdcluster doctor [--fix] [--machine <label>] [--repo <path>] [--json]
herdcluster skills status|install    # bundled skills: herdr-orchestrator + herdcluster
herdcluster config get|set|unset     # cluster.json keys incl. keepawake, caps
herdcluster keepawake status [--json]  # sleep-inhibit state + working agents
herdcluster update|upgrade [--version vX.Y.Z] [--yes] [--json]
                                     # self-update from GitHub releases
                                     # (sha256-verified, atomic replace)
herdcluster revoke <label>           # remove a worker (re-pair to restore)
herdcluster autostart status|install|uninstall  # worker daemon persistence
```

## Spawn workflow (preferred for delegating work)

1. `herdcluster machines` — pick a target or use `--machine auto`.
2. `herdcluster spawn <name> --kind claude --machine auto --repo <path>` —
   `--repo` is a path **on the machine that will run the agent**. Omit it to
   use the `.herdcluster/config.json` mapping for that machine, reuse the
   machine's only workspace, or create one in the server's default (home)
   directory; `--repo ~` means the target's home dir.
3. Use the printed global id (`<machine>/<name>`) for all follow-ups.
4. `herdcluster agent prompt <id> "<task>"`, then `agent wait --until done`
   or `agent watch` to track completion. `--stuck-after` warns when an agent
   sits in `working`/`blocked` too long.

Supported `--kind` values come from herdr's agent manifests (claude, codex,
devin, opencode, …) — `herdcluster doctor` reports which kinds are installed
per machine.

## Pairing / ops notes

- `herdcluster init` prints a ready-to-paste `join --controller <lan-ip>:<port>`
  line for the other machine; pairing code TTL is 5 minutes and burns only
  on approval — denied/timed-out attempts keep the code valid.
- Worker machines dial out only; pairing is one-time (`herdcluster join`),
  credential persists across reboots (`autostart install` recommended).
- After `herdcluster update`, restart running daemons (`init`/`worker`) to
  pick up the new binary.

## Rules

- Never handle agent credentials — if a kind needs login, tell the user to
  run e.g. `claude login` on that machine.
- Errors are JSON `{"error":{"code","message"}}` under `--json`; codes like
  `worker_unreachable`, `agent_not_found`, `no_placement`,
  `agent_pane_busy`, `invalid_pairing_code` are stable.
- Exit codes: 0 ok, 1 runtime error, 2 usage error.
