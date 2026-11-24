# herdcluster

Control a fleet of coding agents spread across machines (Mac controller +
remote workers such as WSL2). herdcluster is a thin wrapper over each
machine's local `herdr` — it never touches agent credentials and never runs
commands on workers directly; everything goes through herdr's NDJSON API.

## Mental model

- Every machine runs `herdr` (the engine). herdcluster only routes commands.
- Global agent IDs look like `machine/agent-name` (e.g. `macbook/probe2`,
  `desktop-win/fix-auth`). Bare names are rejected — always qualify.
- `local` is the machine herdcluster runs on; other labels come from
  `herdcluster machines`.
- `--machine auto` lets the scheduler pick the worker with the most free
  RAM/agent slots for the requested kind.

## Commands agents should use

```bash
herdcluster machines                 # fleet table: name/type/status/agents/cpu/mem
herdcluster machines --json          # stable schema for scripting

herdcluster agent list --all         # every agent on every machine, merged
herdcluster agent list --all --json

herdcluster agent get <id>           # details
herdcluster agent read <id>          # terminal output (visible source)
herdcluster agent wait <id>          # block until state change (per-machine)
herdcluster agent prompt <id> "..."  # send a prompt
herdcluster agent send-keys <id> ... # raw keys
herdcluster agent rename <id> <new>

herdcluster spawn <name> --kind <kind> --machine <label|auto> [--repo <path>] [--branch b] [--json] [-- agent args]
                                     # one-step: workspace+pane+agent.start,
                                     # prints global id when ready (~35s);
                                     # --repo optional ("~" = home dir)

herdcluster status [--all] [--json]  # cluster overview
herdcluster doctor [--fix] [--machine <label>] [--json]  # health checks
herdcluster skills status|install    # orchestration skill provisioning
herdcluster config get|set|unset     # machines.<label>.{max_agents,kinds_override}
herdcluster revoke <label>           # remove a worker (re-pair to restore)
herdcluster autostart status|install|uninstall  # worker daemon persistence
```

## Spawn workflow (preferred for delegating work)

1. `herdcluster machines` — pick a target or use `--machine auto`.
2. `herdcluster spawn <name> --kind claude --machine auto --repo <path>` —
   `--repo` is a path **on the machine that will run the agent**. Omit it
   to reuse the machine's only workspace or create one in the server's
   default (home) directory; `--repo ~` means the target's home dir.
3. Use the printed global id (`<machine>/<name>`) for all follow-ups.
4. `herdcluster agent prompt <id> "<task>"` then `agent wait`/`agent read`.

Supported `--kind` values come from herdr's agent manifests (claude, codex,
opencode, …) — `herdcluster doctor` reports which kinds are installed per
machine.

## Rules

- Never handle agent credentials — if a kind needs login, tell the user to
  run e.g. `claude login` on that machine.
- Worker machines dial out only; pairing is one-time (`herdcluster join`).
- Errors are JSON `{"error":{"code","message"}}` under `--json`; codes like
  `worker_unreachable`, `agent_not_found`, `no_placement` are stable.
- Exit codes: 0 ok, 1 runtime error, 2 usage/not-implemented.
