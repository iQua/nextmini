# Batch sync & run (rsync + SSH)

When iterating on a multi-machine deployment, it is common to:

1. Update code/config locally
2. Copy the repo to many remote machines
3. Run the same command on every machine (build, restart, collect logs, …)

Nextmini includes a small helper that does this in parallel using `rsync` and `ssh`:

- Script: `tools/multidc/multidc.py`

## Prerequisites

- Local machine has `rsync` and `ssh`
- Python 3.13
- You can SSH to every host **without** password prompts (recommended)
- Host inventory (choose one):
  - Bare-metal hosts file: `examples/bare-metal/dataplane/hosts.txt`
  - TOML inventory (recommended for multi-role clusters): `examples/rl/multidc/inventory.toml`

## Host list format

Each line is:

```text
node_id|username@host|public_ip
```

Example:

```text
1|<ssh_user>@<node1_public_ip_address>|<node1_public_ip_address>
2|<ssh_user>@<node2_public_ip_address>|<node2_public_ip_address>
```

## Inventory format (TOML)

For multi-role clusters (e.g., controller + trainer + workers across VMs), it is often easier to keep a single
`inventory.toml` with SSH defaults and per-node overrides.

Start from: `examples/rl/multidc/inventory.example.toml` and save as `examples/rl/multidc/inventory.toml`.

Supported fields:

- `[ssh]`: `user`, `port`, `identity_file`
- `[paths]`: `remote_repo_dir` (used by `sync`)
- `[controller]`: `host` (and optional `user`, `port`, `identity_file`)
- `[[nodes]]`: `host` (and optional `role`, `node_id`, `user`, `port`, `identity_file`)

Notes:

- `tools/multidc/multidc.py` treats `[controller]` and every `[[nodes]]` entry as an SSH target and ignores other fields.
- If multiple entries point at the same SSH target, they are de-duplicated.

## Sync the repo to all hosts

This mirrors your local `nextmini/` checkout into a remote directory on every host:

```bash
python3 tools/multidc/multidc.py sync \
  --inventory examples/rl/multidc/inventory.toml \
  --jobs 16 \
  --batch-ssh
```

Notes:

- `sync` excludes `target/`, `.git/`, virtualenv caches, and generated docs output.
- Add `--delete` only if you want the remote directory to be a strict mirror of local.

## Run a command on all hosts

Run a remote command in parallel:

```bash
python3 tools/multidc/multidc.py run \
  --inventory examples/rl/multidc/inventory.toml \
  --batch-ssh \
  --cmd 'cd ~/nextmini && ./target/release/nextmini --help'
```

Tip: you can still use `--hosts-file examples/bare-metal/dataplane/hosts.txt` if you don't have a TOML inventory.

## One command (sync + run)

If you want a single command that syncs the repo and then runs a command on every host:

```bash
python3 tools/multidc/multidc.py sync-run \
  --inventory examples/rl/multidc/inventory.toml \
  --batch-ssh \
  --cmd 'cd ~/nextmini && ./target/release/nextmini --help'
```

## When not to use this

If you only need to deploy the `nextmini` binary + configs, prefer the dedicated scripts in:

- `examples/bare-metal/controller/`
- `examples/bare-metal/dataplane/`
