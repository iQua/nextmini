# Batch sync & run (rsync + SSH)

When iterating on a multi-machine deployment, it is common to:

1. Update code/config locally
2. Copy the repo to many remote machines
3. Run the same command on every machine (build, restart, collect logs, …)

Nextmini includes a small helper that does this in parallel using `rsync` and `ssh`:

- Script: `tools/multidc/multidc.py`

## Prerequisites

- Local machine has `rsync` and `ssh`
- You can SSH to every host **without** password prompts (recommended)
- A host list file in the bare-metal format: `examples/bare-metal/dataplane/hosts.txt`

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

## Sync the repo to all hosts

This mirrors your local `nextmini/` checkout into a remote directory on every host:

```bash
python3 tools/multidc/multidc.py sync \
  --hosts-file examples/bare-metal/dataplane/hosts.txt \
  --remote-repo-dir ~/nextmini \
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
  --hosts-file examples/bare-metal/dataplane/hosts.txt \
  --batch-ssh \
  --cmd 'cd ~/nextmini && ./target/release/nextmini --help'
```

## When not to use this

If you only need to deploy the `nextmini` binary + configs, prefer the dedicated scripts in:

- `examples/bare-metal/controller/`
- `examples/bare-metal/dataplane/`
