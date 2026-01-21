# Ring all-reduce launcher (ring-emu)

`ring-emu` is a small Python SSH launcher for running a ring all-reduce binary across multiple nodes.

It is used by several SBA/Fly.io examples (for example, `examples/*/ring-emu/`).

## Run

From a container/host where `uv` is available:

```bash
uv run launch_ring.py \
  --ring ring.txt \
  --bin /var/nextmini/ringallreduce \
  --remote-dir /var/nextmini/ring-emu \
  --len 1048576 \
  --reps 10 \
  --verify
```

## Notes

- Use `--no-copy` if the binary and `ring.txt` are already present on the remote side.
- `ring.txt` format: one `ip:port` per line, in ring order.

For the full set of flags, run:

```bash
uv run launch_ring.py --help
```

