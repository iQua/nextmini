#!/usr/bin/env python3
# Launch a multi-node ring-allreduce (Rust TCP emulator) via SSH.
# - Copies the binary and ring file to each host (using scp)
# - Starts one rank per host (using ssh)
# - Streams stdout/stderr from all ranks with rank-prefixed lines
#
# Example:
#   cargo build --release
#   python3 launch_ring.py \
#     --ring ./hosts.txt \
#     --bin ./target/release/ringallreduce \
#     --len 1000000 --init rank --verify \
#     --remote-dir ~/ringallreduce_run
#
# If SSH endpoints differ from the ring bind IPs, provide:
#   --ssh-hosts ./ssh_hosts.txt     # one host (or user@host) per line, in rank order

import argparse
import asyncio
import os
import pathlib
import shlex
import sys
from typing import List, Tuple


def read_nonempty_lines(path: pathlib.Path) -> List[str]:
    lines = []
    for raw in path.read_text().splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        lines.append(s)
    if not lines:
        raise SystemExit(f"ERROR: {path} has no usable lines.")
    return lines


def parse_ring_hosts(ring_lines: List[str]) -> List[Tuple[str, int]]:
    # Lines are "IP:port" (bind addresses for the Rust program)
    out = []
    for i, line in enumerate(ring_lines):
        if ":" not in line:
            raise SystemExit(
                f"ERROR: ring line {i + 1!r} must be IP:port, got {line!r}"
            )
        host, port = line.rsplit(":", 1)
        try:
            p = int(port)
        except ValueError:
            raise SystemExit(f"ERROR: invalid port on ring line {i + 1}: {line!r}")
        out.append((host, p))
    return out


def derive_ssh_targets(
    ring_hosts: List[Tuple[str, int]], ssh_hosts_file: pathlib.Path | None
) -> List[str]:
    if ssh_hosts_file:
        hosts = read_nonempty_lines(ssh_hosts_file)
        return hosts
    # Default: SSH to the host part of each ring address
    return [h for (h, _) in ring_hosts]


async def run_cmd(cmd: List[str], *, stdin=None, capture=False, pipe_stderr=False):
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=stdin,
        stdout=asyncio.subprocess.PIPE if capture else None,
        stderr=asyncio.subprocess.STDOUT
        if pipe_stderr
        else (asyncio.subprocess.PIPE if capture else None),
    )
    if capture:
        out, _ = await proc.communicate()
        return proc.returncode, out.decode("utf-8", errors="replace")
    else:
        rc = await proc.wait()
        return rc, None


async def scp_to_host(local: pathlib.Path, host: str, remote_path: str, ssh_port: int):
    cmd = ["scp", "-P", str(ssh_port), "-q", str(local), f"{host}:{remote_path}"]
    rc, _ = await run_cmd(cmd, capture=False)
    if rc != 0:
        raise RuntimeError(f"SCP to {host} failed for {local}")


async def ssh_mkdir(host: str, remote_dir: str, ssh_port: int):
    cmd = ["ssh", "-p", str(ssh_port), host, "mkdir", "-p", remote_dir]
    rc, _ = await run_cmd(cmd, capture=False)
    if rc != 0:
        raise RuntimeError(f"ssh mkdir on {host} failed.")


async def ssh_chmod(host: str, remote_bin: str, ssh_port: int):
    cmd = ["ssh", "-p", str(ssh_port), host, "chmod", "+x", remote_bin]
    rc, _ = await run_cmd(cmd, capture=False)
    if rc != 0:
        raise RuntimeError(f"ssh chmod on {host} failed.")


async def stream_rank(prefix: str, cmd: List[str]):
    # Run `ssh ... <command>` and stream combined stdout/stderr line by line
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
        # If cancelled (e.g., Ctrl-C), terminate remote session
        proc.terminate()
        raise
    finally:
        await proc.wait()
        code = proc.returncode
        print(f"[{prefix}] ssh exited with code {code}")


async def main():
    ap = argparse.ArgumentParser(description="Launch ringallreduce ranks over SSH.")
    ap.add_argument(
        "--ring",
        type=pathlib.Path,
        required=True,
        help="Path to ring file with IP:port per line (bind addresses).",
    )
    ap.add_argument(
        "--bin",
        type=pathlib.Path,
        required=True,
        help="Local path to compiled ringallreduce binary (cargo build --release).",
    )
    ap.add_argument(
        "--remote-dir",
        type=str,
        default="~/ringallreduce_run",
        help="Remote working directory on each host (default: %(default)s).",
    )
    ap.add_argument(
        "--ssh-hosts",
        type=pathlib.Path,
        default=None,
        help="Optional: file with SSH targets (one per line, user@host), in rank order.",
    )
    ap.add_argument(
        "--ssh-port",
        type=int,
        default=22,
        help="SSH port (uniform across hosts; default: 22).",
    )
    ap.add_argument(
        "--len", type=int, default=1024, dest="length", help="Tensor length (elements)."
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
        "--verify", action="store_true", help="Enable verification after all-reduce."
    )
    ap.add_argument(
        "--no-copy",
        action="store_true",
        help="Skip copying binary/ring file (assume already present remotely).",
    )
    ap.add_argument(
        "--remote-ring-name",
        type=str,
        default="ring.txt",
        help="Filename to use for the ring file on the remote side.",
    )
    ap.add_argument(
        "--strict-host-key-checking",
        action="store_true",
        help="Enable StrictHostKeyChecking (off by default).",
    )
    args = ap.parse_args()

    ring_lines = read_nonempty_lines(args.ring)
    ring_hosts = parse_ring_hosts(ring_lines)
    ssh_targets = derive_ssh_targets(ring_hosts, args.ssh_hosts)
    world = len(ring_hosts)
    if len(ssh_targets) != world:
        raise SystemExit(
            f"ERROR: number of SSH targets ({len(ssh_targets)}) != world size ({world})."
        )

    # Expand ~ in remote_dir
    remote_dir = args.remote_dir

    print(f"[launcher] world size = {world}")
    print(f"[launcher] remote dir  = {remote_dir}")
    print(f"[launcher] ssh port    = {args.ssh_port}")
    if args.ssh_hosts:
        print(f"[launcher] using SSH hosts file: {args.ssh_hosts}")
    else:
        print(f"[launcher] SSH hosts derived from ring IPs.")

    # Normalize paths used on remote
    remote_dir_clean = remote_dir.rstrip("/")
    remote_ring_path = f"{remote_dir_clean}/{args.remote_ring_name}"
    remote_bin_path = f"{remote_dir_clean}/ringallreduce"

    # 1) Prepare remote directories and copy artifacts
    if not args.no_copy:
        # Ensure binary exists and is executable
        if not args.bin.exists():
            raise SystemExit(f"ERROR: binary not found at {args.bin}. Build first.")
        # Create temp copy of ring file to ensure exactly what is sent
        tasks = []
        for host in ssh_targets:
            tasks.append(ssh_mkdir(host, remote_dir, args.ssh_port))
        await asyncio.gather(*tasks)

        tasks = []
        for host in ssh_targets:
            tasks.append(scp_to_host(args.ring, host, remote_ring_path, args.ssh_port))
        await asyncio.gather(*tasks)

        tasks = []
        for host in ssh_targets:
            tasks.append(scp_to_host(args.bin, host, remote_bin_path, args.ssh_port))
        await asyncio.gather(*tasks)

        tasks = []
        for host in ssh_targets:
            tasks.append(ssh_chmod(host, remote_bin_path, args.ssh_port))
        await asyncio.gather(*tasks)
        print("[launcher] Copied binary and ring file to all hosts.")
    else:
        print("[launcher] Skipping copy; assuming artifacts already present remotely.")

    # 2) Launch all ranks concurrently and stream logs
    runners = []
    for rank, host in enumerate(ssh_targets):
        verify_flag = "--verify" if args.verify else ""
        # Build the command to run on the remote host
        # Use cd to ensure we're in the right directory, then run the binary
        cmdline = (
            f"cd {remote_dir_clean} && {remote_bin_path} --ring {remote_ring_path} "
            f"--rank {rank} --len {args.length} --init {args.init} --reps {args.reps} {verify_flag}"
        )

        # Debug print of the exact command per host
        print(f"[launcher] cmd rank={rank} host={host}: {cmdline}")

        ssh_cmd = [
            "ssh",
            "-p",
            str(args.ssh_port),
        ]
        if not args.strict_host_key_checking:
            ssh_cmd += [
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
            ]
        # Remove the bash -lc wrapper - just pass the command directly to ssh
        ssh_cmd += [host, cmdline]

        prefix = f"rank={rank}@{host}"
        runners.append(stream_rank(prefix, ssh_cmd))

    print("[launcher] Starting all ranks ...")
    try:
        await asyncio.gather(*runners)
    except KeyboardInterrupt:
        print("\n[launcher] Caught Ctrl-C; terminating ssh sessions ...")
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
        # Let cancellations propagate
        try:
            await asyncio.gather(
                *[t for t in asyncio.all_tasks() if t is not asyncio.current_task()],
                return_exceptions=True,
            )
        except Exception:
            pass
        print("[launcher] Done.")
        return


if __name__ == "__main__":
    if sys.platform == "win32":
        # On Windows, ProactorEventLoop is default; ensure asyncio subprocess works
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
