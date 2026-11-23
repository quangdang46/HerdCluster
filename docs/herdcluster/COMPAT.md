# herdr API compatibility policy

HerdCluster is a pure wrapper: it never modifies herdr and never vendors its
source. Every machine runs an unmodified `herdr` binary, and herdcluster
speaks herdr's published socket API — newline-delimited JSON
(`{id, method, params}` request, single JSON line response) over a local
socket.

## Pinned range

The compat guard lives in `crates/api-client/src/compat.rs`:

| Constant | Value | Meaning |
|---|---|---|
| `MIN_SUPPORTED_PROTOCOL` | `22` | oldest herdr API protocol accepted |
| `MAX_SUPPORTED_PROTOCOL` | `22` | newest herdr API protocol accepted |
| `TESTED_HERDR_VERSION` | `"0.9.5"` | herdr release the range was validated against |

The `ping` handshake returns `{version, protocol, capabilities}`. A server
whose `protocol` falls outside the pinned range produces
`ApiError::IncompatibleProtocol` with a clear "update herdcluster" style
error — never silent wrong behavior. Method/response shapes that drifted
between herdr revisions are tracked in that module's docs (e.g. `ping` +
`session.snapshot` in 0.9.5 vs. older `status.server`/`api.snapshot`).

## How to bump the compat range

1. Pick the new herdr release to support (check `herdrdev/herdr` releases).
2. Run `herdr api schema` on that version and diff
   `docs/next/api/herdr-api.schema.json` + the `Method` enum against what
   `crates/api-client/src/methods.rs` calls.
3. Validate the changed states live: use the manifest hot-reload / throwaway
   session approach — exercise idle/working/blocked transitions and confirm
   `herdcluster doctor`, `machines`, `agent list`, `spawn`, `prompt`.
4. Update `MIN_SUPPORTED_PROTOCOL` / `MAX_SUPPORTED_PROTOCOL` /
   `TESTED_HERDR_VERSION` in `compat.rs`.
5. Add/adjust fixture tests in `crates/api-client` covering the new
   protocol handshake and any changed method shapes (fixtures per schema
   version — keep old ones so regressions on the old range are caught).
6. Update this file's table and note the herdr version in the commit.

Widen the range only after validation; prefer keeping `MIN`/`MAX` tight.
If upstream removes or reinterprets a method herdcluster depends on, that
is a breaking change — gate it behind a new minimum protocol rather than
silently degrading.

## Cluster wire protocol

Independent of the herdr API, herdcluster's own worker↔controller
connection carries `protocol_version` in `WorkerHello`/`PairAccept`
(`crates/cluster-net`). A mismatch fails fast with
"protocol version N unsupported" — the fix is to update herdcluster on the
reported machine. Bump this only with a matching wire-codec change and
mixed-version rollout notes.

## Upstream tracking

- Releases: <https://github.com/herdrdev/herdr/releases>
- API schema: `docs/next/api/herdr-api.schema.json` in the herdr repo,
  inspectable live via `herdr api schema` on an installed herdr.
- Watch for: new `protocol` values in `ping`, schema/method additions or
  renames, and changes to `session.snapshot`/`agent.*` shapes.

## Alert path

When a user hits `IncompatibleProtocol`, the error names the supported
range and the server's reported version/protocol. The remediation is
always "update herdcluster" (or downgrade/upgrade herdr) — there is no
auto-negotiation beyond the handshake check.
