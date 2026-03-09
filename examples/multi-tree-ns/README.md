# Multi-Tree Namespace Example

```bash
. "$HOME/.cargo/env"
export CARGO_TARGET_DIR="$PWD/target-mtns"
cargo build -p controller --release
```

```bash
PYTHON_BIN="$HOME/nextmini/.venv-mtns/bin/python" \
NEXTMINI_CONTROLLER_BIN="$PWD/target-mtns/release/controller" \
./examples/multi-tree-ns/run.sh \
  --mode plain \
  --source 1 \
  --receivers 4,5 \
  --tree 1-2,2-4,2-5 \
  --file /tmp/input.bin
```

```bash
PYTHON_BIN="$HOME/nextmini/.venv-mtns/bin/python" \
NEXTMINI_CONTROLLER_BIN="$PWD/target-mtns/release/controller" \
./examples/multi-tree-ns/run.sh \
  --mode fec \
  --source 1 \
  --receivers 4,5 \
  --tree 1-2,2-4,2-5 \
  --tree 1-3,3-4,3-5 \
  --file /tmp/input.bin
```
