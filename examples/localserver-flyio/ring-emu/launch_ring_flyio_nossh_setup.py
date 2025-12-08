#!/usr/bin/env python3
"""
Simplified launcher for ring-allreduce on Fly.io nodes.
Assumes ring.txt and directories are already prepared in the Docker image.
This eliminates multiple SSH connections for file copying.
"""

import argparse
import asyncio
import sys


async def run_rank(app_name, rank, args):
    """
    Run ringallreduce on a single app via SSH.
    Only ONE SSH connection per node.
    """
    verify_flag = "--verify" if args.verify else ""
    ring_path = f"{args.remote_dir}/ring.txt"

    # Build the command to run (single line, already in the image)
    cmdline = (
        f"cd {args.remote_dir} && "
        f"{args.bin} --ring {ring_path} "
        f"--rank {rank} --len {args.length} --init {args.init} "
        f"--reps {args.reps} {verify_flag}"
    )

    # Single SSH connection with sh -c
    flyctl_cmd = [
        "flyctl",
        "ssh",
        "console",
        "-a",
        app_name,
        "-C",
        f'sh -c "{cmdline}"',
    ]

    # Create subprocess
    proc = await asyncio.create_subprocess_exec(
        *flyctl_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    # Stream output with rank prefix
    async for line in proc.stdout:
        line_str = line.decode("utf-8", errors="replace").rstrip()
        print(f"[rank={rank}@{app_name}] {line_str}", flush=True)

    await proc.wait()
    return proc.returncode


async def main():
    parser = argparse.ArgumentParser(
        description="Launch ring-allreduce on Fly.io nodes (optimized, no file copy)"
    )
    parser.add_argument(
        "--apps",
        nargs="+",
        required=True,
        help="List of Fly app names (in rank order)",
    )
    parser.add_argument(
        "--ring",
        default="ring.txt",
        help="Path to local ring.txt (for reference only, already in image)",
    )
    parser.add_argument(
        "--remote-dir",
        default="/tmp/ring-test",
        help="Remote directory (already prepared in image)",
    )
    parser.add_argument(
        "--bin",
        default="/usr/local/bin/ringallreduce",
        help="Path to ringallreduce binary (already in image)",
    )
    parser.add_argument("--len", dest="length", type=int, default=1024)
    parser.add_argument("--init", default="ones", help="ones|rank|random")
    parser.add_argument("--reps", type=int, default=1)
    parser.add_argument("--verify", action="store_true")

    args = parser.parse_args()

    print("[launcher] Optimized mode: No file copying needed!")
    print(f"[launcher] world size = {len(args.apps)}")
    print(f"[launcher] apps = {args.apps}")
    print(f"[launcher] remote dir = {args.remote_dir}")
    print(f"[launcher] binary = {args.bin}")
    print()
    print("[launcher] Starting all ranks (1 SSH connection per node)...")
    print()

    # Print all commands
    for rank, app_name in enumerate(args.apps):
        verify_flag = "--verify" if args.verify else ""
        cmdline = (
            f"cd {args.remote_dir} && "
            f"{args.bin} --ring {args.remote_dir}/ring.txt "
            f"--rank {rank} --len {args.length} --init {args.init} "
            f"--reps {args.reps} {verify_flag}"
        )
        print(f"[launcher] rank={rank} app={app_name}: {cmdline}")

    print()
    print("[launcher] All ranks launched. Streaming output...")
    print()

    # Launch all ranks concurrently
    tasks = [run_rank(app_name, rank, args) for rank, app_name in enumerate(args.apps)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Check for errors
    failed = []
    for rank, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"\n[launcher] ERROR: rank {rank} failed with exception: {result}")
            failed.append(rank)
        elif result != 0:
            print(f"\n[launcher] ERROR: rank {rank} exited with code {result}")
            failed.append(rank)

    print()
    if failed:
        print(f"[launcher] FAILED ranks: {failed}")
        sys.exit(1)
    else:
        print("[launcher] All ranks completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
