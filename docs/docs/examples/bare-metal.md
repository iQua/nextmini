# Bare Metal Deployment

Deploy Nextmini Controller and Dataplane Nodes as native binaries across multiple physical machines.

This guide demonstrates **native binary deployment** on bare metal servers or virtual machines using automated Python deployment scripts. Unlike Docker-based examples where components run in containers, **both the Controller and Dataplane nodes run directly on the host operating system** as native processes, leveraging TUN interfaces for networking. This deployment model requires **sudo privileges** for network configuration. PostgreSQL runs in a Docker container to simplify database management.

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
./utils/start-database.sh
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

Edit `dataplane/hosts.txt`:

Each line contains three pipe-separated fields: `node_id|username@host|public_ip_address`

- Column 1: Unique node ID
- Column 2: SSH connection string (`username@host`, supports both IP addresses and DNS hostnames)
- Column 3: Public IP address for network configuration

```text
1|root@157.180.84.40|157.180.84.40
2|ubuntu@206.12.91.229|206.12.91.229
```

Edit `dataplane/node.toml`:

Specify the Controller's WebSocket address (IP address with port 3000).

```toml
controller_addr = "ws://206.12.89.244:3000"
```

### Step 5: Deploy Nodes

```bash
cd examples/bare-metal/dataplane
chmod +x ./deploy.sh
./deploy.sh
```

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
