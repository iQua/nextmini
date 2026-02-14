---
title: "Build and Test Guide"
description: "Complete local workflow for building, running, and testing Nextmini."
---

This page is the canonical development workflow for Nextmini. Start here when setting up a fresh environment, validating a change before a pull request, or debugging a failing test.

## Prepare your environment

Work from the repository root (`nextmini/`) unless you are intentionally iterating on a single crate.

For Rust development, install a current Rust toolchain and ensure `cargo` is available on your path.

For Python-related paths (`python-api/`, `examples/`, `tools/`, and `docs/`), use CPython `3.13.x`.

If you are planning to run controller integration tests, make sure Postgres is available locally.

## Fast compile loop while iterating

Use `cargo check` for the fastest compile feedback:

```bash
cargo check
```

When you need a full workspace compile, run:

```bash
cargo build --workspace
```

## Quality gates before committing changes

Run formatting and linting before opening or updating a PR:

```bash
cargo fmt --all
cargo clippy --workspace -- -D warnings
```

## Build Python bindings (`nextmini_py`)

If your change touches the Python API or any Python-driven examples, build the extension module:

```bash
pip install maturin
maturin develop --release -m python-api/Cargo.toml
```

For wheel-based workflows, use:

```bash
maturin build --release -m python-api/Cargo.toml
```

## Run the system locally

Use separate terminals for controller and dataplane:

```bash
RUST_LOG=info cargo run -p controller
```

```bash
cargo run -p dataplane
```

`RUST_LOG=info` is recommended so runtime state transitions are visible while debugging.

## Run tests

If controller tests are part of your run, start Postgres first:

```bash
bash utils/start-database.sh
```

Then run workspace tests:

```bash
cargo test --workspace
```

For the full development suite used in this repo, run:

```bash
cargo nextest run --no-default-features --features python-extension --features dev-tests
```

If the Python extension is required for your environment, set `PYO3_PYTHON` to a Python 3.13 binary before running test commands. On macOS, a common example is:

```bash
export PYO3_PYTHON=/opt/homebrew/opt/python@3.13/bin/python3.13
```

## Validate docs changes

When documentation is part of your change:

```bash
cd docs
bun dev
```

In a separate run, execute:

```bash
bun run types:check
```

## Additional notes

Use [Testing Multicast and Membership Churn](/docs/devel/multicast) for end-to-end multicast validation with the Docker harness.
