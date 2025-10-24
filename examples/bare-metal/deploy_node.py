#!/usr/bin/env python3
"""
Deploy Nextmini Dataplane Node
Usage: uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def run_command(cmd, cwd=None, check=True, shell=False):
    """Run a command and return the result."""
    print(f"Running: {cmd if isinstance(cmd, str) else ' '.join(cmd)}")
    try:
        if shell:
            result = subprocess.run(cmd, shell=True, cwd=cwd, check=check,
                                   capture_output=True, text=True)
        else:
            result = subprocess.run(cmd, cwd=cwd, check=check,
                                   capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        return result
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        if e.stderr:
            print(f"Error output: {e.stderr}")
        if check:
            sys.exit(1)
        return e


def check_file_exists(filepath, error_msg):
    """Check if a file exists, exit with error message if not."""
    if not filepath.exists():
        print(f"Error: {error_msg}")
        print(f"File not found: {filepath}")
        sys.exit(1)


def setup_ssh_on_node():
    """Setup SSH public key on the current machine (run this on node machines)."""
    script_dir = Path(__file__).parent.resolve()
    ssh_dir = script_dir / "ssh"
    public_key_file = ssh_dir / "id_rsa.pub"
    
    if not public_key_file.exists():
        print("Warning: SSH public key not found, skipping SSH setup")
        return
    
    with open(public_key_file, 'r') as f:
        public_key = f.read().strip()
    
    print("\n" + "=" * 60)
    print("Setting up SSH access for ring all-reduce...")
    print("=" * 60)
    print("\nRun this command on THIS node machine:")
    print("-" * 60)
    print(f"""mkdir -p ~/.ssh && chmod 700 ~/.ssh && \\
echo '{public_key}' >> ~/.ssh/authorized_keys && \\
chmod 600 ~/.ssh/authorized_keys && \\
sort -u ~/.ssh/authorized_keys -o ~/.ssh/authorized_keys
""")
    print("-" * 60)
    print("This allows the controller to SSH into this node for testing.")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description='Deploy Nextmini Dataplane Node',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Deploy node 1 to controller at 206.12.89.244
  uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1
  
  # Deploy with custom port (if running multiple nodes on same host)
  uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1 --port 8080
  
  # Deploy with custom network interface
  uv run deploy_node.py --controller-ip 206.12.89.244 --node-id 1 --interface ens3
        """
    )
    
    parser.add_argument(
        '--controller-ip',
        required=True,
        help='IP address of the controller (e.g., 206.12.89.244)'
    )
    
    parser.add_argument(
        '--node-id',
        type=int,
        required=True,
        help='Node ID (unique identifier for this node)'
    )
    
    parser.add_argument(
        '--controller-port',
        type=int,
        default=3000,
        help='Controller port (default: 3000)'
    )
    
    parser.add_argument(
        '--port',
        type=str,
        default='8080',
        help='Public network port for this node (default: 8080). Use different ports if multiple nodes on same host.'
    )
    
    parser.add_argument(
        '--interface',
        default='ens3',
        help='Network interface name (default: ens3). Use "ip addr" to find yours.'
    )
    
    parser.add_argument(
        '--network-name',
        default='net1',
        help='Private network name (default: net1)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"Deploying Nextmini Dataplane Node {args.node_id}")
    print("=" * 60)
    print(f"Controller: ws://{args.controller_ip}:{args.controller_port}")
    print(f"Node ID: {args.node_id}")
    print(f"Port: {args.port}")
    print(f"Interface: {args.interface}")
    print("=" * 60)
    
    # Determine paths
    script_dir = Path(__file__).parent.resolve()
    repo_root = script_dir.parent.parent
    
    # Step 1: Check if binary exists
    print("\nStep 1: Checking for nextmini binary...")
    nextmini_bin = repo_root / "target" / "release" / "nextmini"
    check_file_exists(
        nextmini_bin,
        "nextmini binary not found. Please build it first:\n"
        f"  cd {repo_root} && cargo build --release -p nextmini"
    )
    
    # Check certificates
    cert_file = repo_root / "server_cert.pem"
    key_file = repo_root / "server_key.pem"
    check_file_exists(
        cert_file,
        "TLS certificates not found. Please generate them first:\n"
        f"  cd {repo_root} && cargo run -p cert-gen"
    )
    
    # Step 2: Setup deployment directory
    print("\nStep 2: Setting up deployment directory...")
    deploy_dir = script_dir / f"node{args.node_id}-deploy"
    deploy_dir.mkdir(exist_ok=True)
    
    # Copy files
    import shutil
    shutil.copy(nextmini_bin, deploy_dir / "nextmini")
    shutil.copy(cert_file, deploy_dir / "server_cert.pem")
    shutil.copy(key_file, deploy_dir / "server_key.pem")
    
    print(f"Files copied to: {deploy_dir}")
    
    # Step 3: Generate configuration
    print(f"\nStep 3: Generating configuration for Node {args.node_id}...")
    
    config_content = f"""private_network_interface = "{args.interface}"
private_network_name = "{args.network_name}"
ip_version = "ipv4"
num_tun_queues = 4
num_packet_processors = 4
channel_capacity = 4000
queue_capacity = 3000
feature = "concurrent"

controller_addr = "ws://{args.controller_ip}:{args.controller_port}"
node_id = {args.node_id}
public_network_port = "{args.port}"
"""
    
    config_file = deploy_dir / f"node{args.node_id}.toml"
    with open(config_file, "w") as f:
        f.write(config_content)
    
    print(f"\nGenerated config file: {config_file}")
    print("Config contents:")
    print("-" * 40)
    print(config_content)
    print("-" * 40)
    
    # Step 4: Start node
    print(f"\nStep 4: Starting Node {args.node_id}...")
    
    env = os.environ.copy()
    env['RUST_LOG'] = 'info'
    
    controller_addr = f"ws://{args.controller_ip}:{args.controller_port}"
    
    with open(deploy_dir / f"node{args.node_id}.log", "w") as log_file:
        # Need sudo for TUN device access
        cmd = [
            "sudo", "-E",
            str(deploy_dir / "nextmini"),
            "--config-path", str(config_file),
            controller_addr
        ]
        
        process = subprocess.Popen(
            cmd,
            cwd=deploy_dir,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
    
    node_pid = process.pid
    
    # Wait a bit for startup
    time.sleep(2)
    
    print("\n" + "=" * 60)
    print(f"Node {args.node_id} Deployment Complete")
    print("=" * 60)
    print(f"Node PID: {node_pid}")
    print(f"Config: {config_file}")
    print(f"Logs: {deploy_dir / f'node{args.node_id}.log'}")
    print(f"\nTo view logs: tail -f {deploy_dir / f'node{args.node_id}.log'}")
    print(f"To stop: sudo kill {node_pid}")
    print("=" * 60)
    
    # Show SSH setup instructions
    setup_ssh_on_node()


if __name__ == "__main__":
    main()

