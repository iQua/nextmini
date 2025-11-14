# Cargo Test Cheat Sheet

## Workspace

Run all workspace tests with a single command:

```bash
cargo nextest run --no-default-features --features reliable --features dev-tests
```

## nextmini_py

```bash
# runs tests without the extension-module feature
env PYO3_PYTHON=/opt/homebrew/bin/python3.13 \
    cargo nextest run -p nextmini_py --no-default-features --features dev-tests
```

## Controller

```bash
cargo nextest run -p controller
```
