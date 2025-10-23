#!/usr/bin/env python3
"""
Launch ring-allreduce on Fly.io nodes via flyctl ssh.

This is a Fly.io-specific variant that uses 'flyctl ssh console -a <app-name>'
instead of direct SSH connections.

Example:
  uv run launch_ring_flyio.py \
    --apps nextmini-node-1 nextmini-node-2 \
    --ring ring.txt \
    --len 1048576 --verify
"""

import argparse
import asyncio
import pathlib
from typing import List


def read_nonempty_lines(path: pathlib.Path) -> List[str]:
    """Read non-empty, non-comment lines from a file."""
    lines = []
    for raw in path.read_text().splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        lines.append(s)
    if not lines:
        raise SystemExit(f"ERROR: {path} has no usable lines.")
    return lines


async def stream_rank(prefix: str, cmd: List[str]):
    """Run command and stream combined stdout/stderr line by line."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        assert proc.stdout is not None
        async for raw in proc.stdout:
            line = raw.decode("utf-8", errors="replace").rstrip("\n")
            print(f"[{prefix}] {line}")
    except asyncio.CancelledError:
        proc.terminate()
        raise
    finally:
        await proc.wait()
        code = proc.returncode
        print(f"[{prefix}] flyctl ssh exited with code {code}")


async def main():
    ap = argparse.ArgumentParser(
        description="Launch ringallreduce on Fly.io via flyctl ssh."
    )
    ap.add_argument(
        "--apps",
        nargs="+",
        required=True,
        help="Fly.io app names in rank order (e.g., nextmini-node-1 nextmini-node-2).",
    )
    ap.add_argument(
        "--ring",
        type=pathlib.Path,
        required=True,
        help="Path to ring file with IP:port per line (bind addresses).",
    )
    ap.add_argument(
        "--bin",
        type=str,
        default="/usr/local/bin/ringallreduce",
        help="Path to ringallreduce binary on remote machines (default: /usr/local/bin/ringallreduce).",
    )
    ap.add_argument(
        "--remote-dir",
        type=str,
        default="/tmp/ring-test",
        help="Remote working directory (default: /tmp/ring-test).",
    )
    ap.add_argument(
        "--len",
        type=int,
        default=1024,
        dest="length",
        help="Tensor length (elements).",
    )
    ap.add_argument(
        "--init",
        type=str,
        default="rank",
        choices=["rank", "ones", "random"],
        help="Initialization pattern.",
    )
    ap.add_argument("--reps", type=int, default=1, help="Repetitions.")
    ap.add_argument(
        "--verify",
        action="store_true",
        help="Enable verification after all-reduce.",
    )
    args = ap.parse_args()

    ring_lines = read_nonempty_lines(args.ring)
    world = len(ring_lines)

    if len(args.apps) != world:
        raise SystemExit(
            f"ERROR: number of apps ({len(args.apps)}) != ring size ({world})."
        )

    print(f"[launcher] world size = {world}")
    print(f"[launcher] apps = {args.apps}")
    print(f"[launcher] remote dir = {args.remote_dir}")
    print(f"[launcher] binary = {args.bin}")
    print()

    # Create ring.txt content to upload
    ring_content = "\n".join(ring_lines)

    # For each app, we'll:
    # 1. Create remote directory
    # 2. Upload ring.txt
    # 3. Run ringallreduce

    print("[launcher] Setting up remote directories and ring files...")
    for rank, app_name in enumerate(args.apps):
        # Create directory
        mkdir_cmd = [
            "flyctl",
            "ssh",
            "console",
            "-a",
            app_name,
            "-C",
            f"mkdir -p {args.remote_dir}",
        ]
        proc = await asyncio.create_subprocess_exec(*mkdir_cmd)
        await proc.wait()

        # Write ring.txt line by line to avoid shell escaping issues
        ring_path = f"{args.remote_dir}/ring.txt"

        # First, truncate/create empty file
        truncate_cmd = [
            "flyctl",
            "ssh",
            "console",
            "-a",
            app_name,
            "-C",
            f'sh -c "> {ring_path}"',
        ]
        proc = await asyncio.create_subprocess_exec(*truncate_cmd)
        await proc.wait()

        # Then append each line
        for line in ring_lines:
            append_cmd = [
                "flyctl",
                "ssh",
                "console",
                "-a",
                app_name,
                "-C",
                f"sh -c \"echo '{line}' >> {ring_path}\"",
            ]
            proc = await asyncio.create_subprocess_exec(*append_cmd)
            await proc.wait()

        print(f"[launcher] Setup complete for {app_name} (rank {rank})")

    print()
    print("[launcher] Starting all ranks...")
    print()

    # Launch all ranks concurrently
    runners = []
    for rank, app_name in enumerate(args.apps):
        verify_flag = "--verify" if args.verify else ""
        ring_path = f"{args.remote_dir}/ring.txt"

        # Build the command to run
        cmdline = (
            f"cd {args.remote_dir} && "
            f"{args.bin} --ring {ring_path} "
            f"--rank {rank} --len {args.length} --init {args.init} "
            f"--reps {args.reps} {verify_flag}"
        )

        print(f"[launcher] rank={rank} app={app_name}: {cmdline}")

        # Use flyctl ssh console with bash -lc to ensure shell features (cd && ...)
        flyctl_cmd = [
            "flyctl",
            "ssh",
            "console",
            "-a",
            app_name,
            "-C",
            f'sh -c "{cmdline}"',
        ]

        prefix = f"rank={rank}@{app_name}"
        runners.append(stream_rank(prefix, flyctl_cmd))

    print()
    print("[launcher] All ranks launched. Streaming output...")
    print()

    try:
        await asyncio.gather(*runners)
    except KeyboardInterrupt:
        print("\n[launcher] Caught Ctrl-C; terminating sessions...")
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
        try:
            await asyncio.gather(
                *[
                    t
                    for t in asyncio.all_tasks()
                    if t is not asyncio.current_task()
                ],
                return_exceptions=True,
            )
        except Exception:
            pass
        print("[launcher] Done.")
        return


if __name__ == "__main__":
    asyncio.run(main())
