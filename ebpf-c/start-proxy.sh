#!/bin/bash

# Load BPF and networking modules
modprobe bpf 2>/dev/null || true
modprobe sock_diag 2>/dev/null || true
modprobe tcp_diag 2>/dev/null || true

# Enable BPF system calls (set to 0 to enable)
echo 0 > /proc/sys/kernel/unprivileged_bpf_disabled 2>/dev/null || true

# Set BPF memory limits
echo 2147483647 > /proc/sys/net/core/bpf_jit_limit 2>/dev/null || true

# Mount bpffs if not mounted
mount -t bpf bpf /sys/fs/bpf 2>/dev/null || true

# Mount debugfs for BPF
mount -t debugfs debugfs /sys/kernel/debug 2>/dev/null || true

# Test basic BPF functionality first
echo "Testing basic BPF functionality..."
./test_loader || echo "Basic BPF test failed"

# Try to load eBPF program in background
echo "Loading sockmap eBPF program..."
./sockmap_loader /sys/fs/cgroup &
LOADER_PID=$!

# Wait a moment for eBPF to load
sleep 3

# Start proxy application
./target/release/proxy