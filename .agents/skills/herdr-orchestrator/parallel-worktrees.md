# Parallel worktrees

Each writing peer/implementer gets an isolated git checkout. Root stays
read-oriented on the primary project workspace and does not write product
code there.

## Herdr worktree API (documented)

```bash
herdr worktree list [--workspace ID | --cwd PATH] [--json]
herdr worktree create [--workspace ID | --cwd PATH] [--branch NAME] [--base REF] [--path PATH] [--label TEXT] [--focus] [--no-focus] [--json]
herdr worktree open [--workspace ID | --cwd PATH] (--path PATH | --branch NAME) [--label TEXT] [--focus] [--no-focus] [--json]
herdr worktree remove --workspace ID [--force] [--json]
```

Raw socket methods: `worktree.list`, `worktree.create`, `worktree.open`,
`worktree.remove`.

Facts from docs:

- A Herdr worktree is a normal workspace **with git checkout provenance**.
- `worktree create` creates the checkout, opens a workspace, groups it with
  the parent repo workspace.
- If `--branch` exists locally → checkout that branch; else create from
  `--base` or `HEAD`.
- Default path (no `--path`): under `<worktrees.directory>/<repo>/<branch-slug>`.
- `workspace close` closes Herdr state only.
- `worktree remove` runs `git worktree remove`, **never deletes the branch**,
  needs `--force` when git refuses a dirty checkout.
- Socket `cwd` / `path` values must be absolute (CLI may expand relative).

## Policy

| Actor | Checkout | Writes |
| --- | --- | --- |
| root | primary project workspace | orchestration data only (`data/…`), no product code |
| peer / implementer | own `worktree create` | yes, inside that checkout |
| reviewer | primary or implementer tree **read-only** | no code writes |

## Create one worktree per writer

```bash
# from root, parent repo workspace id = $HERDR_WORKSPACE_ID or discovered id
herdr worktree create \
  --workspace "$HERDR_WORKSPACE_ID" \
  --branch "orch/<plan>/<peer-slug>" \
  --base HEAD \
  --label "<peer-slug>" \
  --no-focus \
  --json
```

Parse returned `workspace`, `tab`, `root_pane`, `worktree` records.
`worktree create` already opened a workspace whose root pane has the worktree
as its cwd, so start the agent **in that pane** — the cwd comes from the pane,
not from an `agent start` flag.

```bash
# root_pane.pane_id came from the worktree create response
herdr agent start <name> --kind claude --pane <root_pane.pane_id> --timeout 60000
```

Verified 2026-11-14: `agent start` has **no** `--cwd` / `--workspace` /
`--split` flags (it errors `unknown option: --cwd`). It requires `--kind` and
an existing `--pane`. Verify the cwd afterwards with `herdr pane get <id>` —
it should report the worktree path as both `cwd` and `foreground_cwd`.

Log branch, path, workspace id, pane id.

## Avoid two agents on the same files / branch

### Hard rules

1. **One writer per branch.** Never attach two implementers to the same
   worktree/branch.
2. **Partition file ownership in the plan** before parallel spawn.
   Record paths in `data/plans/<plan>.md` work packages.
3. **No drive-by edits** outside the package path set without root reassign.
4. **Reviewers do not share a writable branch** with an implementer. If they
   must see WIP, open the implementer's worktree read-only (or review a
   committed diff / `git diff` output) without starting a second writer cwd
   on that branch.

### Soft coordination

- Prefer package boundaries that match directory ownership
  (`src/foo/**` vs `src/bar/**`).
- If two packages must touch one file: serialize — finish A, merge or
  rebase B onto updated base, then B proceeds.
- Root detects collision risk by comparing planned path sets before spawn;
  if overlap ≠ empty, do not parallelize.

### Detection after the fact

```bash
# in each worktree
git status --short
git diff --name-only
```

If two peers modified the same path: stop the later one, reassign, or
rebase explicitly. Do not "hope merge works".

## Integration with panes

- Prefer one Herdr **child workspace per worktree** (what `worktree create`
  already opens) rather than cramming all peers into root's tab.
- Keep root's tab for roster/monitor; use `--no-focus` so peer workspaces
  do not steal the human's UI.
- When harvesting, read the peer pane; when comparing trees, use git in
  each cwd.

## Cleanup

After accept/merge (user-driven):

```bash
herdr worktree remove --workspace <child_workspace_id>
# --force only if dirty and user approved discard
```

Branch remains unless a human/peer deletes it through normal git. Do not
assume Herdr deletes branches — docs say it does not.

## Root non-write rule

Root may:

- read primary tree
- write `data/orchestrator/**` and `data/plans/**`
- run read-only git / search

Root may not:

- implement product changes on the primary checkout "to help"
- commit from primary on behalf of a peer
- hot-fix files inside a peer worktree while that peer is live