# Ring All-Reduce Launcher

A Python-based SSH launcher for distributed ring all-reduce operations across multiple nodes.

## Usage

### Basic Usage

Launch a ring all-reduce operation across 2 nodes:

```bash
uv run launch_ring.py \
  --ring ring.txt \
  --bin /var/nextmini/ringallreduce \
  --no-copy \
  --remote-dir /var/nextmini \
  --len 1048576 \
  --init rank \
  --reps 10 \
  --verify
```

### Command Line Options

- `--ring <FILE>`: Path to ring file with IP:port per line (bind addresses)
- `--bin <PATH>`: Local path to compiled ringallreduce binary
- `--remote-dir <DIR>`: Remote working directory on each host (default: ~/ringallreduce_run)
- `--ssh-hosts <FILE>`: Optional file with SSH targets (one per line, user@host) in rank order
- `--ssh-port <PORT>`: SSH port (default: 22)
- `--len <N>`: Tensor length in elements (default: 1024)
- `--init <PATTERN>`: Initialization pattern: rank, ones, or random (default: rank)
- `--reps <N>`: Number of repetitions (default: 1)
- `--verify`: Enable verification after all-reduce
- `--no-copy`: Skip copying binary/ring file (assume already present remotely)
- `--remote-ring-name <NAME>`: Filename for ring file on remote side (default: ring.txt)
- `--strict-host-key-checking`: Enable StrictHostKeyChecking (off by default)

### Ring File Format

The ring file should contain one IP:port per line:

```
10.0.0.1:9000
10.0.0.2:9000
```

Comments (lines starting with `#`) and empty lines are ignored.

## Examples

### Small test with verification

```bash
uv run launch_ring.py \
  --ring ring.txt \
  --bin ringallreduce \
  --no-copy \
  --remote-dir /var/nextmini \
  --len 1048 \
  --init rank \
  --reps 5 \
  --verify
```

### Large tensor without verification

```bash
uv run launch_ring.py \
  --ring ring.txt \
  --bin ringallreduce \
  --no-copy \
  --remote-dir /var/nextmini \
  --len 10485760 \
  --init ones \
  --reps 100
```

### Using different SSH hosts

If your ring bind addresses differ from SSH endpoints:

```bash
uv run launch_ring.py \
  --ring ring_bind.txt \
  --ssh-hosts ssh_hosts.txt \
  --bin ringallreduce \
  --remote-dir /var/nextmini \
  --len 1048576 \
  --verify
```

## Troubleshooting

### Port already in use

If you see "Address in use" errors, kill existing processes:

```bash
pkill -9 ringallreduce
ssh 10.0.0.2 "pkill -9 ringallreduce"
```

### SSH connection issues

Ensure SSH keys are properly configured and nodes are reachable:

```bash
ssh-keyscan -H 10.0.0.1 >> ~/.ssh/known_hosts
ssh-keyscan -H 10.0.0.2 >> ~/.ssh/known_hosts
```

### Binary not found

Make sure the binary exists and is executable:

```bash
ls -la /var/nextmini/ringallreduce
chmod +x /var/nextmini/ringallreduce
```
