# Bare Metal Deployment

Deploy Nextmini Controller and Dataplane Nodes as native binaries across multiple physical machines.

This guide demonstrates **native binary deployment** on bare metal servers or virtual machines. Unlike Docker-based examples where components run in containers, **both the controller and dataplane nodes run directly on the host OS** as native processes. PostgreSQL runs in a local Docker container to simplify database management.

It uses the included deployment scripts:

- `examples/bare-metal/controller/deploy.sh` starts the controller (and the local Postgres container if needed).
- `examples/bare-metal/dataplane/deploy.sh` bundles the `nextmini` binary + certs + `node.toml`, uploads the bundle to all hosts **in parallel** (background `scp`), and starts each node over SSH.
- If you need to sync a full checkout to many machines (iterative development), see [Batch sync & run (rsync + SSH)](batch-sync.md).

## Quick Start

Before you start, remember to generate your own SSH key.

**Generate SSH Key:**

```bash
# Generate a new RSA SSH key pair (if you don't have one).
ssh-keygen -t rsa -b 4096 -f ~/nextmini/examples/bare-metal/dataplane/ssh/id_rsa -N ""
```

The `-N ""` flag creates a key without a passphrase. If you prefer to use a passphrase for added security, omit this flag.

### Step 1: Build the Rust Binaries

```bash
cd nextmini
cargo run -p cert-gen
cargo build --release -p controller
cargo build --release -p nextmini
```

### Optional Step: Start Database (One-time)

```bash
./start-database.sh
```

### Step 2: Deploy Controller

```bash
cd examples/bare-metal/controller
chmod +x ./deploy.sh
./deploy.sh
```

### Step 3: Setup SSH (One-time)

This could be set up on any physical machine.

```bash
# Install keychain firstly.
sudo apt-get install keychain

# Load SSH key.
eval $(keychain --eval ~/nextmini/examples/bare-metal/dataplane/ssh/id_rsa)
```

Or add to ~/.bashrc for permanent setup:

```bash
echo 'eval $(keychain --eval --quiet ~/nextmini/examples/bare-metal/dataplane/ssh/id_rsa)' >> ~/.bashrc
source ~/.bashrc
```

Enter passphrase once, it will persist across terminal sessions.

### Step 4: Configure

For a complete list of configuration options, see the [Configuration Reference](../design/config-reference.md).

Edit `examples/bare-metal/controller/config.toml`:

```toml
[topology]
type = "full_mesh"
full_mesh_config = { n_nodes = 2 }
```

Edit `examples/bare-metal/dataplane/hosts.txt`:

Each line contains three pipe-separated fields: `node_id|username@host|public_ip_address`

- Column 1: Unique node ID
- Column 2: SSH connection string (`username@host`, supports both IP addresses and DNS hostnames)
- Column 3: Public IP address for network configuration

```text
1|<ssh_user>@<node1_public_ip_address>|<node1_public_ip_address>
2|<ssh_user>@<node2_public_ip_address>|<node2_public_ip_address>
```

Edit `examples/bare-metal/dataplane/node.toml`:

Specify the Controller's WebSocket address (IP address with port 3000).

```toml
controller_addr = "ws://<controller_public_ip_address>:3000"
```

### Step 5: Deploy Nodes

```bash
cd examples/bare-metal/dataplane
chmod +x ./deploy.sh
./deploy.sh
```

Notes:

- `deploy.sh` runs non-interactively by default. Ensure:
  - SSH access to every host in `hosts.txt` is passwordless (agent key loaded), and
  - `sudo` on the remote hosts does not block on a password prompt.
- If you need a password prompt, run `./deploy.sh -i` to deploy serially with an interactive SSH TTY.

### Step 6: Manage Services

**Stop nodes:**

```bash
cd examples/bare-metal/dataplane
chmod +x ./stop.sh
./stop.sh
```

**Stop controller:**

```bash
cd examples/bare-metal/controller
chmod +x ./stop.sh
./stop.sh
```
