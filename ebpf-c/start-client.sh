#!/bin/bash

echo "Starting client container..."

# Load BPF and networking modules
modprobe bpf 2>/dev/null || true
modprobe sock_diag 2>/dev/null || true
modprobe tcp_diag 2>/dev/null || true

# Enable BPF system calls (set to 0 to enable)
echo 0 > /proc/sys/kernel/unprivileged_bpf_disabled 2>/dev/null || true

# Set BPF memory limits (ignore if not supported)
echo 2147483647 > /proc/sys/net/core/bpf_jit_limit 2>/dev/null || echo "BPF JIT limit not available"

# Mount bpffs if not mounted
mount -t bpf bpf /sys/fs/bpf 2>/dev/null || true

# Mount debugfs for BPF
mount -t debugfs debugfs /sys/kernel/debug 2>/dev/null || true

# Try to load eBPF program in background (don't block startup)
{
    echo "Attempting to load eBPF program..."
    ./sockmap_loader /sys/fs/cgroup && echo "eBPF loaded successfully" || echo "eBPF load failed, running without kernel bypass"
} &

# Start client application immediately
echo "Starting Rust client application..."
./target/release/client