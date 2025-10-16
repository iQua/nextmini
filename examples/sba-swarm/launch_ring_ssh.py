#!/usr/bin/env python3
"""
Launch ring-emu ranks across multiple hosts via SSH in parallel.
Each rank listens on a port and connects to the next rank in a ring topology.
"""
import argparse
import os
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple


def parse_hosts(arg: str) -> List[str]:
    """Parse hosts from comma-separated string or file (one host per line)."""
    if os.path.isfile(arg):
        with open(arg) as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]
    return [h.strip() for h in arg.split(",") if h.strip()]


def ssh_exec(host: str, remote_cmd: str, ssh_opts: str = "", dry_run: bool = False) -> Tuple[int, str, str]:
    """Execute a command on a remote host via SSH."""
    ssh_parts = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes"]
    if ssh_opts:
        ssh_parts += shlex.split(ssh_opts)
    ssh_parts += [host, remote_cmd]
    
    if dry_run:
        print(f"[DRY-RUN] {' '.join(shlex.quote(p) for p in ssh_parts)}")
        return 0, "", ""
    
    proc = subprocess.run(ssh_parts, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return proc.returncode, proc.stdout, proc.stderr


def build_start_cmd(binary: str, rank: int, world_size: int, listen_port: int, 
                    next_host: str, next_port: int, numel: int, chunks: int, 
                    iters: int, verbose: bool, log_dir: str = "/var/nextmini") -> str:
    """Build the command to start ring-emu on a remote host."""
    args = [
        shlex.quote(binary),
        "--rank", str(rank),
        "--world_size", str(world_size),
        "--listen", f"0.0.0.0:{listen_port}",
        "--next", f"{next_host}:{next_port}",
        "--numel", str(numel),
        "--chunks", str(chunks),
        "--iters", str(iters),
    ]
    if verbose:
        args.append("--verbose")
    
    # Run in background and redirect output to log file
    log_file = f"{log_dir}/ring-emu-rank{rank}.log"
    cmd = f"nohup {' '.join(args)} > {log_file} 2>&1 & echo $!"
    return cmd


def build_stop_cmd() -> str:
    """Build the command to stop all ring-emu processes."""
    return "pkill -f ring-emu || true"


def stop_all_ranks(hosts: List[str], ssh_opts: str, parallel: int, dry_run: bool):
    """Stop ring-emu processes on all hosts in parallel."""
    print(f"Stopping ring-emu on {len(hosts)} hosts...")
    stop_cmd = build_stop_cmd()
    
    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = {executor.submit(ssh_exec, host, stop_cmd, ssh_opts, dry_run): host 
                   for host in hosts}
        
        for future in as_completed(futures):
            host = futures[future]
            rc, out, err = future.result()
            status = "OK" if rc == 0 else f"FAILED (rc={rc})"
            print(f"  [{host}] {status}")
    
    print("Done.")


def start_all_ranks(hosts: List[str], binary: str, base_port: int, numel: int, 
                   chunks: int, iters: int, verbose: bool, ssh_opts: str, 
                   parallel: int, dry_run: bool, log_dir: str):
    """Start ring-emu ranks on all hosts in parallel."""
    world_size = len(hosts)
    print(f"Starting ring all-reduce with {world_size} ranks...")
    print(f"  numel={numel}, chunks={chunks}, iters={iters}")
    print(f"  Ring topology: {' -> '.join(hosts)} -> (back to {hosts[0]})")
    
    commands = []
    for rank, host in enumerate(hosts):
        listen_port = base_port + rank
        next_rank = (rank + 1) % world_size
        next_host = hosts[next_rank]
        next_port = base_port + next_rank
        
        cmd = build_start_cmd(
            binary, rank, world_size, listen_port, next_host, next_port,
            numel, chunks, iters, verbose, log_dir
        )
        commands.append((rank, host, cmd))
    
    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = {executor.submit(ssh_exec, host, cmd, ssh_opts, dry_run): (rank, host) 
                   for rank, host, cmd in commands}
        
        for future in as_completed(futures):
            rank, host = futures[future]
            rc, out, err = future.result()
            
            if rc == 0:
                pid = out.strip() if out.strip() else "?"
                print(f"  [rank {rank} @ {host}] Started (PID={pid})")
            else:
                print(f"  [rank {rank} @ {host}] FAILED (rc={rc})")
                if err:
                    print(f"    stderr: {err.strip()}")
    
    print(f"\nRing all-reduce started. Logs at: {log_dir}/ring-emu-rank{{0..{world_size-1}}}.log")
    print("To stop: run with 'stop' mode")


def main():
    parser = argparse.ArgumentParser(
        description="Launch ring-emu ranks across SSH hosts (based on SSH launcher pattern)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("mode", choices=["start", "stop"], 
                       help="Start or stop ring-emu processes")
    parser.add_argument("hosts", 
                       help="Comma-separated hosts (e.g., '10.0.0.1,10.0.0.2') or path to hosts file")
    parser.add_argument("--binary", default="/var/nextmini/ring-emu", 
                       help="Path to ring-emu binary on remote hosts")
    parser.add_argument("--base-port", type=int, default=9001, 
                       help="Base port; rank i listens on base-port + i")
    parser.add_argument("--numel", type=int, default=65536, 
                       help="Number of f32 elements in tensor")
    parser.add_argument("--chunks", type=int, default=2, 
                       help="Number of chunks (recommend: equal to world_size)")
    parser.add_argument("--iters", type=int, default=10, 
                       help="Number of all-reduce iterations")
    parser.add_argument("--verbose", action="store_true", 
                       help="Print tensor summaries after each iteration")
    parser.add_argument("--log-dir", default="/var/nextmini", 
                       help="Directory for log files on remote hosts")
    parser.add_argument("--ssh-opts", default="", 
                       help="Additional SSH options (e.g., '-i /path/to/key')")
    parser.add_argument("--parallel", type=int, default=8, 
                       help="Number of parallel SSH connections")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Print commands without executing")
    
    args = parser.parse_args()
    
    # Parse hosts
    hosts = parse_hosts(args.hosts)
    if not hosts:
        print("ERROR: No hosts provided", file=sys.stderr)
        sys.exit(1)
    
    if len(hosts) < 2:
        print("ERROR: Need at least 2 hosts for ring topology", file=sys.stderr)
        sys.exit(1)
    
    # Execute mode
    if args.mode == "stop":
        stop_all_ranks(hosts, args.ssh_opts, args.parallel, args.dry_run)
    else:  # start
        start_all_ranks(
            hosts, args.binary, args.base_port, args.numel, args.chunks, 
            args.iters, args.verbose, args.ssh_opts, args.parallel, 
            args.dry_run, args.log_dir
        )


if __name__ == "__main__":
    main()


