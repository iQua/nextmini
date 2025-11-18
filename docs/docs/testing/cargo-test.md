Run all workspace tests with a single command:

Prerequisite: Python 3.13 on PATH.

1) Verify Python 3.13:

```bash
which python3.13
```
Note the path (e.g. `/opt/homebrew/bin/python3.13`).

2) Run tests pointing PyO3 at that interpreter:

```bash
PYO3_PYTHON=<Change to Your PATH: /opt/homebrew/bin/python3.13> \
cargo nextest run --no-default-features --features python-extension --features dev-tests
```
