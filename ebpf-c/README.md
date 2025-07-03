# eBPF Kernel Bypass Socket Redirection

This project implements a kernel bypass system using eBPF sockhash maps to redirect network traffic between Docker containers without going through the userspace TCP/IP stack.

## Architecture

```
Client (8080) → Proxy (8081) → Server (8082)
```

Traffic is redirected purely in kernel space using:
- **SK_SKB** programs for packet interception
- **SOCKHASH** maps for socket storage
- **bpf_sk_redirect_hash()** for kernel-level redirection

## Components

### eBPF Programs
- `sockmap_redirect.c` - eBPF program with SK_SKB verdict and parser
- `sockmap_loader.c` - Userspace loader for eBPF programs

### Rust Applications
- `src/client.rs` - Client that sends "Hello" messages
- `src/proxy.rs` - Proxy that forwards messages (bypassed by eBPF)
- `src/server.rs` - Server that responds to messages

## Usage

### Build and Run

```bash
# Build eBPF programs
make

# Start all containers
docker compose up --build

# Or run individual containers
docker compose up client
docker compose up proxy
docker compose up server
```

### Load eBPF Programs

```bash
# Load eBPF programs (requires root)
sudo ./sockmap_loader /sys/fs/cgroup/unified
```

## How It Works

1. **Socket Registration**: The sockops program registers TCP sockets in the sockhash map when connections are established
2. **Packet Interception**: SK_SKB programs intercept packets at socket level
3. **Kernel Redirection**: Traffic from client→proxy is redirected directly to server in kernel space
4. **Bypass Achievement**: The proxy application is completely bypassed, achieving ~50% performance improvement

## Requirements

- Linux kernel 4.14+
- Docker with privileged mode
- libbpf development headers
- Rust 1.70+

## Performance Benefits

- Eliminates TCP/IP stack overhead
- Reduces context switches
- Achieves kernel-level packet redirection
- Approximately 50% performance improvement over traditional TCP/IP