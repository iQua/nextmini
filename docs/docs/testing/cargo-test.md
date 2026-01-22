# Cargo test guide

This page documents the common Rust test workflows for the Nextmini workspace.

## Prerequisites

- Rust toolchain (edition 2024)
- `cargo nextest` (install via `cargo install cargo-nextest`)
- Python 3.13 (for `nextmini_py` dev tests)
- Postgres (for controller integration tests that read/write DB state)

Start a local Postgres for controller tests:

```bash
./start-database.sh
```

## Fast edit loop

```bash
cargo check --workspace
```

## Unit/integration tests (workspace)

The repository uses `cargo nextest` for test runs.

1) Verify Python 3.13:

```bash
which python3.13
```

2) Run tests pointing PyO3 at that interpreter:

```bash
PYO3_PYTHON=/path/to/python3.13 \
cargo nextest run --no-default-features --features python-extension --features dev-tests
```

Notes:

- `--features python-extension` enables dataplane code paths needed by the Python bindings.
- `--features dev-tests` enables PyO3 auto-initialize helpers for Rust-side tests in `python-api/`.

## Formatting and linting

```bash
cargo fmt --all
cargo clippy --workspace -- -D warnings
```

## Generate Rust API docs

```bash
cargo doc --workspace --no-deps
```

To publish rustdoc inside the MkDocs site, see: [Rust API (rustdoc)](../design/rust-api.md).
