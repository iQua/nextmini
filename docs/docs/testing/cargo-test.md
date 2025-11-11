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

# fast sweep (skip the DB-heavy tests)
cargo nextest run -p controller --filter-expr 'not test(db::tests::receivers_join_leave_independent_groups) and not test(db::tests::multicast_missing_member_route_delivers_locally)'

# run each DB-dependent test separately
cargo nextest run -p controller db::tests::receivers_join_leave_independent_groups
cargo nextest run -p controller db::tests::multicast_missing_member_route_delivers_locally
```

## Stop the database (optional)

```bash
docker stop nextmini-database
```
