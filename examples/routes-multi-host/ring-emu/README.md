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
  --remote-dir /var/nextmini/ring-emu \
  --len 1048576 \
  --init rank \
  --reps 10 \
  --verify
```

**Note**
No need of `--no-copy` if you want to copy the binary and ring file to remote nodes. Then no need to mount ring.txt in docker-swarm.yml.

### Command Line Options

- `--ring <FILE>`: Path to ring file with IP:port per line (bind addresses)
- `--bin <PATH>`: Local path to compiled ringallreduce binary
- `--remote-dir <DIR>`: Remote working directory where ring.txt is located (use `/var/nextmini/ring-emu` in Docker containers)
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

```txt
10.0.0.1:9000
10.0.0.2:9000
```

## Directory Structure

```txt
/var/nextmini/
├── pyproject.toml              # PyTorch dependencies (for training scripts)
├── ringallreduce               # Ring all-reduce binary
├── lenet5.py, gpt2.py, etc.    # Training scripts
└── ring-emu/                   # Ring launcher (isolated environment)
    ├── pyproject.toml          # Minimal config, no external dependencies
    ├── launch_ring.py          # Launcher script
    ├── ring.txt                # Ring topology file
    └── README.md               # This file
```

The `ring-emu` directory has its own `pyproject.toml` with no external dependencies, ensuring clean execution without warnings from parent project dependencies.

## Examples

All examples assume you're in the `ring-emu` directory:

```bash
cd /var/nextmini/ring-emu
```

### Small test with verification

```bash
uv run launch_ring.py \
  --ring ring.txt \
  --bin /var/nextmini/ringallreduce \
  --no-copy \
  --remote-dir /var/nextmini/ring-emu \
  --len 1048 \
  --init rank \
  --reps 5 \
  --verify
```

### Large tensor without verification

```bash
uv run launch_ring.py \
  --ring ring.txt \
  --bin /var/nextmini/ringallreduce \
  --no-copy \
  --remote-dir /var/nextmini/ring-emu \
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
  --bin /var/nextmini/ringallreduce \
  --remote-dir /var/nextmini/ring-emu \
  --len 1048576 \
  --verify
```

### Quick one-liner for testing

```bash
cd /var/nextmini/ring-emu && uv run launch_ring.py --ring ring.txt --bin /var/nextmini/ringallreduce --no-copy --remote-dir /var/nextmini/ring-emu --len 1048 --init rank --reps 5 --verify
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

### Ring file not found

If you see "failed to read ring file" errors, ensure you're using the correct `--remote-dir`:

```bash
# Correct: points to ring-emu directory where ring.txt is located
--remote-dir /var/nextmini/ring-emu

# Incorrect: ring.txt is not in /var/nextmini directly
--remote-dir /var/nextmini
```
