#!/usr/bin/env python3
"""
Launch ring all-reduce benchmark on physical network (bypassing Nextmini TUN).

This script tests raw network performance between machines using their physical IPs,
not TUN interfaces. Useful for baseline performance measurements.

Example:
    uv run launch_physical.py \
      --hosts 206.12.95.232 206.12.91.229 \
      --port 9000 \
      --len 1048576 \
      --init rank \
      --reps 10 \
      --verify
"""

import argparse
import asyncio
import pathlib
import sys
import tempfile
from typing import List


async def run_cmd(cmd: List[str], *, capture=False):
    """Run a command and optionally capture output."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE if capture else None,
        stderr=asyncio.subprocess.STDOUT if capture else (asyncio.subprocess.PIPE if capture else None),
    )
    if capture:
        out, _ = await proc.communicate()
        return proc.returncode, out.decode("utf-8", errors="replace")
    else:
        rc = await proc.wait()
        return rc, None


async def scp_to_host(local: pathlib.Path, host: str, remote_path: str, ssh_port: int, user: str):
    """Copy file to remote host via SCP."""
    target = f"{user}@{host}" if user else host
    # Expand ~ on remote side by wrapping in quotes
    cmd = [
        "scp", "-P", str(ssh_port),
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        str(local), f"{target}:{remote_path}"
    ]
    rc, _ = await run_cmd(cmd, capture=False)
    if rc != 0:
        raise RuntimeError(f"SCP to {host} failed for {local}")


async def ssh_exec(host: str, command: str, ssh_port: int, user: str):
    """Execute command on remote host via SSH."""
    target = f"{user}@{host}" if user else host
    cmd = [
        "ssh", "-p", str(ssh_port),
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        target, command
    ]
    rc, _ = await run_cmd(cmd, capture=False)
    if rc != 0:
        raise RuntimeError(f"SSH command on {host} failed: {command}")


async def stream_rank(prefix: str, cmd: List[str]):
    """Run SSH command and stream output with prefix."""
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
        print(f"[{prefix}] ssh exited with code {code}")


async def main():
    ap = argparse.ArgumentParser(
        description="Launch ring all-reduce on physical network (no TUN).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    ap.add_argument("--hosts", nargs="+", required=True,
                    help="Physical IP addresses for binding (e.g., 192.168.196.164 192.168.196.80).")
    ap.add_argument("--ssh-hosts", nargs="+", default=None,
                    help="SSH connection addresses (e.g., 206.12.95.232 206.12.91.229). Defaults to --hosts.")
    ap.add_argument("--port", type=int, default=9000,
                    help="Port for ring communication (default: 9000).")
    ap.add_argument("--user", type=str, default="ubuntu",
                    help="SSH username (default: ubuntu).")
    ap.add_argument("--ssh-port", type=int, default=22,
                    help="SSH port (default: 22).")
    ap.add_argument("--remote-dir", type=str, default="~/ring-test",
                    help="Remote working directory (default: ~/ring-test).")
    ap.add_argument("--len", type=int, default=104857, dest="length",
                    help="Tensor length in elements (default: 104857).")
    ap.add_argument("--init", type=str, default="rank",
                    choices=["rank", "ones", "random"],
                    help="Initialization pattern (default: rank).")
    ap.add_argument("--reps", type=int, default=1,
                    help="Number of repetitions (default: 1).")
    ap.add_argument("--verify", action="store_true",
                    help="Enable verification after all-reduce.")
    ap.add_argument("--no-copy", action="store_true",
                    help="Skip copying binary/ring file (assume already present).")
    
    args = ap.parse_args()

    bind_hosts = args.hosts  # IPs for binding (internal IPs)
    ssh_hosts = args.ssh_hosts if args.ssh_hosts else bind_hosts  # IPs for SSH (external IPs)
    world = len(bind_hosts)
    
    if world < 2:
        raise SystemExit("ERROR: Need at least 2 hosts for ring all-reduce.")
    
    if len(ssh_hosts) != world:
        raise SystemExit(f"ERROR: --ssh-hosts count ({len(ssh_hosts)}) must match --hosts count ({world}).")

    print(f"[launcher] World size: {world}")
    print(f"[launcher] Bind hosts: {', '.join(bind_hosts)}")
    if args.ssh_hosts:
        print(f"[launcher] SSH hosts: {', '.join(ssh_hosts)}")
    print(f"[launcher] Port: {args.port}")
    print(f"[launcher] Remote dir: {args.remote_dir}")
    
    # Find the binary
    bin_path = pathlib.Path.home() / "nextmini/examples/bare-metal/ring-emu/target/release/ringallreduce-routes"
    if not bin_path.exists():
        raise SystemExit(f"ERROR: Binary not found at {bin_path}. Build first:\n"
                        f"  cd ~/nextmini/examples/bare-metal/ring-emu\n"
                        f"  cargo build --release")

    # Create temporary ring file with bind IPs (internal IPs)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        ring_file = pathlib.Path(f.name)
        for host in bind_hosts:
            f.write(f"{host}:{args.port}\n")
    
    print(f"[launcher] Generated ring file: {ring_file}")
    with open(ring_file) as f:
        print(f.read())

    # Expand ~ to absolute path for remote operations
    if args.remote_dir.startswith("~"):
        remote_dir = f"/home/{args.user}{args.remote_dir[1:]}"
    else:
        remote_dir = args.remote_dir
    
    remote_bin = f"{remote_dir}/ringallreduce"
    remote_ring = f"{remote_dir}/ring.txt"

    # Setup remote directories and copy files
    if not args.no_copy:
        print("[launcher] Setting up remote directories...")
        tasks = []
        for host in ssh_hosts:
            tasks.append(ssh_exec(host, f"mkdir -p {remote_dir}", args.ssh_port, args.user))
        await asyncio.gather(*tasks)

        print("[launcher] Copying ring file to all hosts...")
        tasks = []
        for host in ssh_hosts:
            tasks.append(scp_to_host(ring_file, host, remote_ring, args.ssh_port, args.user))
        await asyncio.gather(*tasks)

        print("[launcher] Copying binary to all hosts...")
        tasks = []
        for host in ssh_hosts:
            tasks.append(scp_to_host(bin_path, host, remote_bin, args.ssh_port, args.user))
        await asyncio.gather(*tasks)

        print("[launcher] Setting execute permissions...")
        tasks = []
        for host in ssh_hosts:
            tasks.append(ssh_exec(host, f"chmod +x {remote_bin}", args.ssh_port, args.user))
        await asyncio.gather(*tasks)
        
        print("[launcher] Setup complete.")
    else:
        print("[launcher] Skipping copy (--no-copy).")

    # Launch all ranks
    runners = []
    for rank, ssh_host in enumerate(ssh_hosts):
        verify_flag = "--verify" if args.verify else ""
        cmdline = (
            f"cd {remote_dir} && {remote_bin} "
            f"--ring {remote_ring} "
            f"--rank {rank} "
            f"--len {args.length} "
            f"--init {args.init} "
            f"--reps {args.reps} "
            f"{verify_flag}"
        )

        print(f"[launcher] cmd rank={rank} host={args.user}@{ssh_host}: {cmdline}")

        target = f"{args.user}@{ssh_host}" if args.user else ssh_host
        ssh_cmd = [
            "ssh", "-p", str(args.ssh_port),
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            target, cmdline
        ]

        prefix = f"rank={rank}@{args.user}@{ssh_host}"
        runners.append(stream_rank(prefix, ssh_cmd))

    print("[launcher] Starting all ranks ...")
    try:
        await asyncio.gather(*runners)
    except KeyboardInterrupt:
        print("\n[launcher] Caught Ctrl-C; terminating ...")
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
        try:
            await asyncio.gather(
                *[t for t in asyncio.all_tasks() if t is not asyncio.current_task()],
                return_exceptions=True
            )
        except Exception:
            pass
        print("[launcher] Done.")
    finally:
        # Clean up temp ring file
        ring_file.unlink(missing_ok=True)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())

