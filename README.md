# HerdCluster

Distributed coding-agent cluster: one Mac controller + remote workers
(WSL2 Ubuntu first, Windows/Linux later), orchestrating coding agents
(Claude Code, Codex, OpenCode, …) across machines from a single CLI.

HerdCluster is a **standalone wrapper** — it does not contain or modify
[herdr](https://github.com/herdrdev/herdr) source code. It speaks herdr's
published API (newline-delimited JSON over a local socket) locally, and
tunnels the same byte stream to workers over an encrypted cluster
connection. Workers auto-install the latest herdr release binary.

## Status

Early development — see [docs/herdcluster/PLAN.md](docs/herdcluster/PLAN.md)
for the architecture and [`.beads/`](.beads/) for the work breakdown.

## UX target

```bash
# Mac (controller)
herdcluster init

# WSL2 (worker) — no SSH, no IP, no keys
herdcluster join

# Both machines, one agent fleet
herdcluster machines
herdcluster spawn coder1 --kind claude --machine auto --repo ~/Projects/api
herdcluster agent list --all
herdcluster agent prompt desktop-win/coder1 "implement auth module"
```

## License

MIT
