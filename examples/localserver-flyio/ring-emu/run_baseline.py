#!/usr/bin/env python3
"""
Automatically cleanup old ringallreduce processes, then run baseline test.
Avoids "Address in use" errors on port 9000.

Usage:
    uv run run_with_cleanup.py --num-nodes 5 --len 1048576 --reps 20
"""

import argparse
import asyncio
import sys


async def cleanup_node(app_name: str):
    """Cleanup ringallreduce process on one node."""
    cmd = ["flyctl", "ssh", "console", "-a", app_name, "-C", "killall -9 ringallreduce 2>/dev/null || true"]
    
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()


async def cleanup_all_nodes(num_nodes: int):
    """Cleanup ringallreduce processes on all baseline nodes."""
    print("Step 1/3: Cleaning up old ringallreduce processes...")
    
    tasks = []
    for i in range(1, num_nodes + 1):
        app_name = f"baseline-ring-{i}"
        tasks.append(cleanup_node(app_name))
    
    # Execute all cleanup tasks concurrently
    await asyncio.gather(*tasks, return_exceptions=True)
    
    print("Waiting for ports to be released...")
    await asyncio.sleep(3)
    print("Cleanup complete")


async def trigger_node(app_name: str, rank: int, length: int, reps: int, init: str, verify: bool):
    """Trigger one node via SSH."""
    # Build ringallreduce command directly
    verify_flag = "--verify" if verify else ""
    ringallreduce_cmd = (
        f"/usr/local/bin/ringallreduce "
        f"--ring /tmp/ring-baseline/ring.txt "
        f"--rank {rank} "
        f"--len {length} "
        f"--init {init} "
        f"--reps {reps} "
        f"{verify_flag}"
    )
    
    cmd = [
        "flyctl", "ssh", "console", "-a", app_name, "-C", ringallreduce_cmd
    ]
    
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT
    )
    
    # Stream output
    async for line in proc.stdout:
        decoded = line.decode().rstrip()
        print(f"[rank={rank}@{app_name}] {decoded}")
    
    await proc.wait()


async def run_test(num_nodes: int, length: int, reps: int, init: str, verify: bool):
    """Run ring-allreduce test on all nodes."""
    print()
    print("  Step 2/3: Starting ring-allreduce test...")
    print(f"   Nodes: {num_nodes}")
    print(f"   Tensor: {length} elements ({length * 4 / 1024 / 1024:.2f} MB)")
    print(f"   Init: {init}")
    print(f"   Reps: {reps}")
    print(f"   Verify: {verify}")
    print()
    
    # Trigger all nodes simultaneously (parallel SSH)
    tasks = []
    for i in range(num_nodes):
        app_name = f"baseline-ring-{i + 1}"
        task = trigger_node(app_name, i, length, reps, init, verify)
        tasks.append(task)
    
    await asyncio.gather(*tasks)
    
    print()
    print("  Step 3/3: Test completed!")


async def main():
    parser = argparse.ArgumentParser(
        description="Run baseline ring-allreduce with automatic cleanup"
    )
    parser.add_argument("--num-nodes", type=int, default=10, help="Number of nodes")
    parser.add_argument("--len", type=int, default=2621440, help="Tensor length")
    parser.add_argument("--reps", type=int, default=10, help="Repetitions")
    parser.add_argument("--init", type=str, default="ones", choices=["rank", "ones", "random"])
    parser.add_argument("--verify", action="store_true", help="Enable verification")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("  Baseline Ring All-Reduce - Auto Cleanup + Test")
    print("=" * 60)
    print()
    
    # Step 1: Cleanup old processes
    await cleanup_all_nodes(args.num_nodes)
    
    # Step 2 & 3: Run test
    await run_test(args.num_nodes, args.len, args.reps, args.init, args.verify)
    
    print()
    print("=" * 60)
    print("All done!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

