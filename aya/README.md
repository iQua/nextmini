# Run the simple eBPF load balancer test

## Prerequisites

```bash
# Install Rust components
rustup toolchain install nightly --component rust-src
cargo install bpf-linker
export PATH="$HOME/.cargo/bin:$PATH"
```

## How to Run

### 1. Build the eBPF Program

```bash
cd nextmini/aya
cargo build --release
```

### 2. Start the Test Environment

```bash
# Start all containers (eBPF proxy, load balancer, 3 backends, test client)
docker compose up -d

# Check containers are running
docker ps --filter "name=backend\|frontend\|ebpf\|test-client"
```

### 3. Run the Test

```bash
# Execute the socket redirection test
docker exec test-client /scripts/test-redirection.sh
```

Expected output shows load balancing across backends:
```
Request 1: Backend Server 1
Request 2: Backend Server 2
Request 3: Backend Server 3
Request 4: Backend Server 1  # Round-robin working
```

### 4. Monitor eBPF Activity

```bash
# View eBPF socket redirection logs
docker logs ebpf-socket-proxy

# Look for these success indicators:
# [INFO] sock_ops: connection socket added to map
# [INFO] sk_msg: standard redirect successful
```

### 5. Manual Testing (Optional)

```bash
# Test load balancer from host
curl http://localhost:9080

# Test individual backends
docker exec test-client curl http://172.20.0.11:8080  # Backend 1
docker exec test-client curl http://172.20.0.12:8080  # Backend 2
docker exec test-client curl http://172.20.0.13:8080  # Backend 3
```

## Clean Up

```bash
docker compose down
```

## Architecture

- **Test Client** (172.20.0.20) → **Load Balancer** (172.20.0.5:80) → **3 Backend Servers** (172.20.0.11-13:8080)
- **eBPF Proxy** (172.20.0.10) monitors and redirects all socket traffic
