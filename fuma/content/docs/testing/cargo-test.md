# Cargo Test Guide

Run tests from the repository root (`nextmini/`).

## Workspace test command

```bash
PYO3_PYTHON=/path/to/python3.13 \
cargo nextest run --no-default-features --features python-extension --features dev-tests
```

Notes:

- `PYO3_PYTHON` should point to a CPython 3.13 interpreter.
- Controller integration tests that require Postgres need the database started first (for example `./utils/start-database.sh`).

## Python API crate-only tests

If you only want `nextmini_py` crate tests:

```bash
PYO3_PYTHON=/path/to/python3.13 \
cargo nextest run -p nextmini_py --no-default-features --features dev-tests
```
