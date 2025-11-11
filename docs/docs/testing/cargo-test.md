# Cargo Test Cheat Sheet

## Workspace

```bash
cargo test --workspace --exclude nextmini_py
cargo nextest run --workspace --exclude nextmini_py
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
