#!/usr/bin/env python3
"""
Cleanup ringallreduce processes on all nodes before running
Usage: uv run cleanup_ring.py
"""

import subprocess
from pathlib import Path


def run_cmd(cmd):
    """Run command silently."""
    subprocess.run(cmd, shell=True, capture_output=True)


def main():
    script_dir = Path(__file__).parent.resolve()
    ssh_key = script_dir.parent / "dataplane" / "ssh" / "id_rsa"
    ssh_hosts_file = script_dir / "ssh_hosts.txt"

    if not ssh_hosts_file.exists():
        print("Error: ssh_hosts.txt not found")
        return

    with open(ssh_hosts_file, "r") as f:
        hosts = [
            line.strip() for line in f if line.strip() and not line.startswith("#")
        ]

    print("Cleaning up ringallreduce processes on all nodes...")

    for host in hosts:
        print(f"  Cleaning {host}...", end=" ")
        cmd = f"ssh -i {ssh_key} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {host} 'pkill -9 ringallreduce 2>/dev/null; echo OK' 2>/dev/null"
        run_cmd(cmd)
        print("✓")

    print("Cleanup complete!")


if __name__ == "__main__":
    main()
