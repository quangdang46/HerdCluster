# HerdCluster

Guidance for agents working in this repository.

## What this project is

HerdCluster is a **standalone Rust Cargo workspace** that clusters coding
agents across machines. It is a pure wrapper around
[herdr](https://github.com/herdrdev/herdr): it does **not** contain herdr
source code and does **not** modify herdr. It speaks herdr's published API
(newline-delimited JSON over a local socket) on the controller, and tunnels
the same byte stream to worker machines over an encrypted cluster
connection.

Canonical plan: `docs/herdcluster/PLAN.md`. Work items: `.beads/`
(manage with `br`).

## Hard rules

- **Never vendor or copy herdr source code** into this repo. Consume herdr
  only as an installed binary + its published NDJSON socket API.
- **Never handle agent credentials** (Claude/Codex/OpenCode logins, API
  keys). Detect availability and print hints only.
- Worker machines **dial out only** — no inbound ports required.
- Pairing tokens are single-use and short-lived; worker identity keys are
  long-lived and separate.
- Original code only; MIT license. External deps must be pinned to versions
  published at least 7 days ago.

## Layout

Cargo workspace under `crates/` — dependency direction is one-way, no
cycles (`herdcluster` → `cluster-node`/`cluster-fleet`/`api-client` →
`cluster-bridge`/`cluster-net`/`cluster-core`; `cluster-core` depends on
nothing internal; `api-client` is standalone):

- `crates/herdcluster` — `[[bin]]` argv dispatch + CLI only (thin)
- `crates/api-client` — herdr NDJSON API client, typed methods, compat guard
- `crates/cluster-core` — pure logic: identity, pairing FSM, wire codec,
  registry, config, scheduler scoring
- `crates/cluster-net` — TLS, stream mux, connection manager, LAN discovery
- `crates/cluster-bridge` — local socket ↔ cluster stream ↔ herdr API socket
- `crates/cluster-fleet` — global agent IDs, fan-out, agent ops, spawn
- `crates/cluster-node` — daemons: controller, worker, herdr runtime
  supervisor, resource monitor, doctor

## Conventions

- `cargo build --workspace` / `cargo test --workspace` must stay green.
- Keep `cluster-core` free of I/O dependencies — pure, unit-testable logic.
- State lives under the herdcluster state dir (`~/.config/herdcluster/` or
  platform equivalent), never inside herdr's state.
- Beads are the source of truth for planned work; keep bead descriptions
  self-contained (regenerate via `scripts/make_beads.py` only when
  intentionally restructuring).
