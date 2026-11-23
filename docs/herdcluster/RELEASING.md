# Cutting a HerdCluster release

HerdCluster releases ship **the `herdcluster` CLI itself** — the
controller/worker/orchestration binary. They do **not** ship herdr: the
worker runtime downloads `herdr` binaries from upstream
`herdrdev/herdr` GitHub releases at runtime (see
`crates/cluster-node/src/runtime.rs`, `DEFAULT_RELEASE_BASE`), independent
of herdcluster's own versioning. Updating herdcluster never requires a
herdr release, and vice versa.

## Versioning

- The release version is the `version` in `[workspace.package]` of the
  root `Cargo.toml` (all crates inherit it via `version.workspace = true`).
- Tag format: `vX.Y.Z` (semver-ish; pre-releases like `v0.2.0-rc.1` also
  match the `v*` trigger).
- The git tag **must equal** the crate version, so `herdcluster --version`
  output matches the release name.

## Steps

1. Bump `[workspace.package].version` in `Cargo.toml`; run `just check`
   (or `cargo fmt --all -- --check && cargo clippy --workspace --all-targets
   -- -D warnings && cargo test --workspace`).
2. Commit, then tag and push:

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. `.github/workflows/release.yml` runs:
   - **build matrix** — native release builds per target:
     `aarch64-apple-darwin` + `x86_64-apple-darwin` (macos-14) and
     `x86_64-unknown-linux-gnu` (ubuntu-22.04), each producing
     `herdcluster-<triple>.tar.gz`.
   - **release job** — merges artifacts, writes `checksums.txt`
     (SHA-256), and creates the GitHub release with `install.sh` included.

4. Users install with:

   ```sh
   curl -fsSL https://raw.githubusercontent.com/quangdang46/HerdCluster/main/install.sh | bash
   ```

   or pin a version: `HERDCLUSTER_VERSION=v0.1.0 ./install.sh`.

## Asset contract

`install.sh` hard-codes the asset naming — keep them in sync:

- `herdcluster-<rust-target-triple>.tar.gz`, containing a single
  `herdcluster` executable at the archive root.
- `checksums.txt` with `<sha256>  <asset-name>` lines covering every
  tarball.
- `install.sh` itself uploaded with each release (so pinned-version URLs
  work: `releases/download/<tag>/install.sh`).

Adding a new target = add a matrix entry in `release.yml` **and** a
`uname` mapping in `install.sh` (and update the target table in
`INSTALL.md`).

## Cross-compilation note

Linux-gnu is built natively on `ubuntu-22.04` rather than cross-compiled
from macOS: the dependency tree needs a C toolchain/linker for the target
(`x86_64-linux-gnu-gcc` or `cargo-zigbuild`), which CI runners provide for
free natively. To reproduce a Linux binary locally on macOS use
`cargo install cargo-zigbuild && cargo zigbuild --release -p herdcluster
--target x86_64-unknown-linux-gnu`.
