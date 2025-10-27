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


async def scp_to_host(local: pathlib.Path, host: str, remote_path: str, ssh_port: int, user: str, ssh_key=None):
    """Copy file to remote host via SCP, expanding ~ on remote."""
    target = host if '@' in host else f"{user}@{host}"
    
    # Remote parent directory inside user's home
    remote_parent = pathlib.Path(remote_path).parent.as_posix()

    # Use tar over ssh: create parent dir, then extract into it so the filename matches remote_path
    ssh_opts = f"-p {ssh_port} {'-i ' + str(ssh_key) if ssh_key else ''} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    tar_cmd = (
        f"tar cf - -C {local.parent} {local.name} | "
        f"ssh {ssh_opts} {target} \"mkdir -p ~/{remote_parent} && tar xf - -C ~/{remote_parent}\""
    )
    
    proc = await asyncio.create_subprocess_shell(
        tar_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    
    if proc.returncode != 0:
        print(f"SCP to {host} failed. Stderr: {stderr.decode()}")
        raise RuntimeError(f"SCP to {host} failed for {local}")


async def ssh_exec(host: str, command: str, ssh_port: int, user: str, ssh_key=None):
    """Execute command on remote host via SSH."""
    target = host if '@' in host else f"{user}@{host}"
    cmd = ["ssh", "-p", str(ssh_port)]
    if ssh_key:
        cmd += ["-i", str(ssh_key)]
    cmd += [
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
    ap.add_argument("--hosts", nargs="+", default=None,
                    help="Physical IP addresses for binding (e.g., 192.168.196.164 192.168.196.80).")
    ap.add_argument("--ssh-hosts", nargs="+", default=None,
                    help="SSH connection addresses (e.g., 206.12.95.232 206.12.91.229). Defaults to --hosts.")
    ap.add_argument("--ssh-hosts-file", type=pathlib.Path, default=None,
                    help="File with SSH hosts (user@host format, one per line).")
    ap.add_argument("--port", type=int, default=9000,
                    help="Port for ring communication (default: 9000).")
    ap.add_argument("--user", type=str, default="ubuntu",
                    help="SSH username (default: ubuntu).")
    ap.add_argument("--ssh-port", type=int, default=22,
                    help="SSH port (default: 22).")
    ap.add_argument("--ssh-key", type=pathlib.Path, default=None,
                    help="SSH private key file (e.g., dataplane/ssh/id_rsa).")
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

    if args.ssh_hosts_file:
        if not args.ssh_hosts_file.exists():
            raise SystemExit(f"ERROR: --ssh-hosts-file not found: {args.ssh_hosts_file}")
        with open(args.ssh_hosts_file) as f:
            ssh_hosts = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        if not args.hosts:
            bind_hosts = []
            for host in ssh_hosts:
                bind_hosts.append(host.split('@')[1] if '@' in host else host)
        else:
            bind_hosts = args.hosts
    elif args.hosts:
        bind_hosts = args.hosts
        ssh_hosts = args.ssh_hosts if args.ssh_hosts else bind_hosts
    else:
        raise SystemExit("ERROR: Must provide either --hosts or --ssh-hosts-file")

    world = len(bind_hosts)
    
    if world < 2:
        raise SystemExit("ERROR: Need at least 2 hosts for ring all-reduce.")
    
    if len(ssh_hosts) != world:
        raise SystemExit(f"ERROR: --ssh-hosts count ({len(ssh_hosts)}) must match --hosts count ({world}).")

    print(f"[launcher] World size: {world}")
    print(f"[launcher] Bind hosts: {', '.join(bind_hosts)}")
    if args.ssh_hosts:
        print(f"[launcher] SSH hosts: {', '.join(ssh_hosts)}")

    # Resolve remote working directory to an absolute path because scp does not
    # expand tildes on the remote side (it bypasses the user shell).
    if args.remote_dir.startswith("~"):
        user_home = "/root" if args.user == "root" else f"/home/{args.user}"
        remote_dir = args.remote_dir.replace("~", user_home, 1)
    else:
        remote_dir = args.remote_dir

    print(f"[launcher] Port: {args.port}")
    print(f"[launcher] Remote dir: {remote_dir}")
    
    # Find the binary
    bin_path = pathlib.Path.home() / "nextmini/examples/bare-metal/ring-emu/target/release/ringallreduce-routes"
    if not bin_path.exists():
        raise SystemExit(f"ERROR: Binary not found at {bin_path}. Build first:\n"
                        f"  cd ~/nextmini/examples/bare-metal/ring-emu\n"
                        f"  cargo build --release")

    # Create temporary ring file with bind IPs (internal IPs)
    # Use a fixed name 'ring.txt' so tar extraction preserves the filename
    temp_dir = pathlib.Path(tempfile.mkdtemp())
    ring_file = temp_dir / "ring.txt"
    with open(ring_file, 'w') as f:
        for host in bind_hosts:
            f.write(f"{host}:{args.port}\n")
    
    print(f"[launcher] Generated ring file: {ring_file}")
    with open(ring_file) as f:
        print(f.read())

    remote_dir = args.remote_dir.lstrip('~/')
    remote_bin = f"{remote_dir}/ringallreduce-routes"
    remote_ring = f"{remote_dir}/ring.txt"

    # Setup remote directories and copy files
    if not args.no_copy:
        print("[launcher] Setting up remote directories...")
        tasks = []
        for host in ssh_hosts:
            tasks.append(ssh_exec(host, f"mkdir -p ~/{remote_dir}", args.ssh_port, args.user, args.ssh_key))
        await asyncio.gather(*tasks)

        print("[launcher] Copying ring file to all hosts...")
        tasks = []
        for host in ssh_hosts:
            tasks.append(scp_to_host(ring_file, host, remote_ring, args.ssh_port, args.user, args.ssh_key))
        await asyncio.gather(*tasks)

        print("[launcher] Copying binary to all hosts...")
        tasks = []
        for host in ssh_hosts:
            tasks.append(scp_to_host(bin_path, host, remote_bin, args.ssh_port, args.user, args.ssh_key))
        await asyncio.gather(*tasks)

        print("[launcher] Setting execute permissions...")
        tasks = []
        for host in ssh_hosts:
            tasks.append(ssh_exec(host, f"chmod +x ~/{remote_bin}", args.ssh_port, args.user, args.ssh_key))
        await asyncio.gather(*tasks)
        
        print("[launcher] Setup complete.")
    else:
        print("[launcher] Skipping copy (--no-copy).")

    # Launch all ranks
    runners = []
    for rank, ssh_host in enumerate(ssh_hosts):
        verify_flag = "--verify" if args.verify else ""
        cmdline = (
            f"cd ~/{remote_dir} && ./{remote_bin.split('/')[-1]} "
            f"--ring ring.txt "
            f"--rank {rank} "
            f"--len {args.length} "
            f"--init {args.init} "
            f"--reps {args.reps} "
            f"{verify_flag}"
        )

        print(f"[launcher] cmd rank={rank} host={ssh_host}: {cmdline}")

        target = ssh_host if '@' in ssh_host else f"{args.user}@{ssh_host}"
        ssh_cmd = ["ssh", "-p", str(args.ssh_port)]
        if args.ssh_key:
            ssh_cmd += ["-i", str(args.ssh_key)]
        ssh_cmd += [
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            target, cmdline
        ]

        prefix = f"rank={rank}@{ssh_host}"
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
        # Clean up temp directory
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())

