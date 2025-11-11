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
./start-database.sh

cargo nextest run -p controller

# the below test needs start the db
cargo test -p controller receivers_join_leave_independent_groups -- --nocapture
```

## Stop the database (optional)

```bash
docker stop nextmini-database
```
