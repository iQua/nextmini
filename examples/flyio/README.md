# Fly.io Deployment

Deploy Nextmini to Fly.io using optimized Docker images with cargo-chef and Alpine Linux.

## Prerequisites

### 1. Install flyctl

```bash
# Install flyctl
curl -L https://fly.io/install.sh | sh

# Add to PATH (copy the output from installer or manually add)
export FLYCTL_INSTALL="/home/$USER/.fly"
export PATH="$FLYCTL_INSTALL/bin:$PATH"

# Permanently add to shell profile
echo 'export FLYCTL_INSTALL="/home/$USER/.fly"' >> ~/.bashrc
echo 'export PATH="$FLYCTL_INSTALL/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# Verify installation
flyctl version
```

### 2. Login to Fly.io

```bash
# Login (opens browser for authentication)
flyctl auth login

# Verify you're logged in
flyctl auth whoami
```

## Quick Deployment

```bash
# Deploy everything
cd nextmini/examples/flyio
./deploy.sh
```

The script will:
1. Create PostgreSQL database
2. Deploy Controller
3. Deploy Dataplane nodes (default: 2)

## Manual Deployment

### 1. Create PostgreSQL

```bash
flyctl postgres create --name nextmini-db --region iad
```

### 2. Deploy Controller

```bash
cd nextmini
flyctl apps create nextmini-controller
flyctl postgres attach nextmini-db -a nextmini-controller
flyctl deploy \
  --config examples/flyio/fly.controller.toml \
  --dockerfile examples/flyio/Dockerfile.controller \
  --app nextmini-controller \
  --ha=false
```

### 3. Deploy Nodes

```bash
# Node 1
flyctl apps create nextmini-node-1
flyctl secrets set \
  NODE_ID=1 \
  CONTROLLER_ADDR=ws://nextmini-controller.internal:3000 \
  -a nextmini-node-1

# Deploy (from repo root)
cd nextmini
flyctl deploy \
  --config examples/flyio/fly.dataplane.toml \
  --dockerfile examples/flyio/Dockerfile.dataplane \
  --app nextmini-node-1 \
  --ha=false

# Repeat for node 2, 3, etc.
```

## Status & Logs

```bash
# View status
fly status -a nextmini-controller
fly status -a nextmini-node-1

# View logs
./logs.sh all
# or
fly logs -a nextmini-controller
fly logs -a nextmini-node-1

# SSH into machine
fly ssh console -a nextmini-controller
```

## Scaling

```bash
./scale.sh up 2      # Scale to 2 machines per app
./scale.sh down      # Scale to 0 (suspend)
./scale.sh status    # View current status
```

## Troubleshooting

### App Crashed or Stopped

```bash
# Check status
fly status -a nextmini-controller

# View logs
fly logs -a nextmini-controller --tail 100

# Restart machine
fly machines list -a nextmini-controller
fly machines start <machine-id> -a nextmini-controller

# Delete and rebuild
flyctl apps destroy nextmini-controller -y
./deploy.sh
```

### Nodes Can't Connect to Controller

```bash
# 1. Check Controller is running
fly status -a nextmini-controller
# Should be "started", not "stopped"

# 2. Check Node environment variables
fly secrets list -a nextmini-node-1
# Should have NODE_ID and CONTROLLER_ADDR

# 3. Reset secrets
fly secrets set \
  NODE_ID=1 \
  CONTROLLER_ADDR=ws://nextmini-controller.internal:3000 \
  -a nextmini-node-1

# 4. Test connectivity
fly ssh console -a nextmini-node-1
ping nextmini-controller.internal
```

### Database Issues

```bash
# Re-attach database
flyctl postgres attach nextmini-db -a nextmini-controller

# Connect to database
flyctl postgres connect -a nextmini-db

# View database logs
fly logs -a nextmini-db
```

### Build Fails

- **Edition 2024 error**: Use `rust:alpine` (supports latest Rust)
- **Out of memory**: `flyctl scale vm performance-2x`
- **GLIBC error**: Ensure using Alpine base image (musl libc)

## Cleanup

### Delete All Apps

```bash
./cleanup.sh
# or manually:
flyctl apps destroy nextmini-controller -y
flyctl apps destroy nextmini-node-1 -y
flyctl apps destroy nextmini-node-2 -y
flyctl apps destroy nextmini-db -y
```

## References

- [Fly.io Rust Docs](https://fly.io/docs/languages-and-frameworks/rust/)
- [Cargo Chef Optimization](https://fly.io/docs/rust/the-basics/cargo-chef/)
- [Bare Metal Deployment](../bare-metal/)
