# LP Result for Multicast Integration with Nextmini

## How it works

```
LP Solver → Tree Edges → Nextmini DB → pg_notify → Controller → Dataplane
```

## Usage

# Step 1: Start Nextmini

```bash
cd examples/routes
docker compose build; docker compose up
```

# Step 2: Run LP Solver
```bash
cd examples/lp-multicast

# Dry run (solve LP, show results)
uv run main.py --topo topo.toml --dry-run

# Inject into Nextmini (requires running controller)
uv run main.py --topo topo.toml --clear
```

Note: This integration is for topology construction. Haven't integrated all the square experiments yet.