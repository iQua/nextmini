# ns-flow: namespace mode + many lossless flows

This example runs the dataplane in Linux network namespaces on a single host, using a ring topology and a large number of controller-managed lossless flows.

Defaults (generated configs):

- Nodes: `136` (namespace mode)
- Topology: ring
- Flows: `180` identical flows from `src=1` to `dst=136`
- Transport: `lossless_unicast`
- Routes: explicit single-hop routes `[1, 136]` and `[136, 1]`

## Regenerate configs

```bash
python3 examples/ns-flow/generate.py --n-nodes 136 --n-flows 180 --src 1 --dst 136
```

This generates (gitignored):

- `examples/ns-flow/controller-config.toml`
- `examples/ns-flow/config.toml`

## Run

Recommended (tmux + sysctl tuning):

```bash
./examples/ns-flow/run.sh
```

`run.sh` waits for the controller to seed routes + flows in Postgres before starting the dataplane.

Manual (two terminals):

1) Apply sysctl tuning:

```bash
./examples/ns-flow/run.sh --sysctl-only
```

2) Terminal A (controller + DB):

```bash
cd examples/ns-flow
python3 ./generate.py
docker compose up --build
```

3) Terminal B (dataplane, Linux only):

```bash
cd /path/to/nextmini
cargo build -p nextmini --release
sudo -E RUST_LOG=info ./target/release/nextmini --config-path examples/ns-flow/config.toml
```

## Verify all flows finished

```bash
docker exec postgres psql -U pgusr -d nextmini -c "SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE is_finished) AS finished FROM flows;"
```

## Cleanup

```bash
./examples/ns-flow/cleanup.sh
```
