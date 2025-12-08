#!/usr/bin/env python3
"""
Automatically cleanup old processes, then run ring-allreduce test.
Avoids "Address in use" errors.
"""

import asyncio
import sys
import subprocess


async def cleanup_remote_nodes(apps: list[str]):
    """Cleanup ringallreduce processes on all remote nodes."""
    print("🧹 Step 1/3: Cleaning up old processes on remote nodes...")

    tasks = []
    for app in apps:
        cmd = ["flyctl", "ssh", "console", "-a", app, "-C", "killall ringallreduce"]
        proc = asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        tasks.append(proc)

    # Execute all cleanup tasks concurrently
    procs = await asyncio.gather(*tasks)
    await asyncio.gather(*[p.wait() for p in procs], return_exceptions=True)

    print("⏳ Waiting for ports to be released...")
    await asyncio.sleep(3)


async def run_test(args: list[str]):
    """Run ring-allreduce test."""
    print()
    print("🚀 Step 2/3: Starting ring-allreduce test...")
    print()

    # Run test script
    cmd = ["uv", "run", "launch_ring_flyio_nossh_setup.py"] + args
    proc = await asyncio.create_subprocess_exec(*cmd)
    return_code = await proc.wait()

    print()
    if return_code == 0:
        print("✅ Step 3/3: Test completed successfully!")
    else:
        print(f"❌ Step 3/3: Test failed (exit code: {return_code})")

    return return_code


async def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: uv run run_with_cleanup.py --apps node1 node2 ... [other args]")
        print()
        print("Example:")
        print("  uv run run_with_cleanup.py \\")
        print("    --apps nextmini-node-1 nextmini-node-2 ... nextmini-node-10 \\")
        print("    --len 1024 --reps 3 --verify")
        sys.exit(1)

    # Parse --apps parameter to get node list
    args = sys.argv[1:]
    apps = []

    try:
        apps_idx = args.index("--apps")
        # Find all app names (until next -- parameter or end)
        i = apps_idx + 1
        while i < len(args) and not args[i].startswith("--"):
            apps.append(args[i])
            i += 1
    except (ValueError, IndexError):
        print("Error: --apps parameter is required")
        sys.exit(1)

    if not apps:
        print("Error: --apps parameter must have at least one node name")
        sys.exit(1)

    # Cleanup remote nodes
    await cleanup_remote_nodes(apps)

    # Run test (pass all arguments)
    return_code = await run_test(args)

    sys.exit(return_code)


if __name__ == "__main__":
    asyncio.run(main())
