# HerdCluster task runner — https://github.com/casey/just
# Fallback: every recipe is a plain cargo invocation; run them manually if
# `just` is not installed.

# Build the debug workspace
build:
    cargo build --workspace

# Run the workspace test suite
test:
    cargo test --workspace

# What CI runs: format check + clippy (deny warnings) + tests
check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

# Release build for the host target -> target/release/herdcluster
release:
    cargo build --release -p herdcluster

# Release binaries for every published target.
# NOTE: cross-compiling x86_64-unknown-linux-gnu from macOS needs a C
# toolchain/linker (e.g. `cargo install cargo-zigbuild` + `cargo zigbuild`,
# or a Linux container). CI builds each target natively instead — see
# .github/workflows/release.yml and docs/herdcluster/RELEASING.md.
release-all:
    cargo build --release -p herdcluster --target aarch64-apple-darwin
    cargo build --release -p herdcluster --target x86_64-apple-darwin
    cargo build --release -p herdcluster --target x86_64-unknown-linux-gnu

# Install the CLI into ~/.cargo/bin from this checkout
install-local:
    cargo install --path crates/herdcluster --locked
