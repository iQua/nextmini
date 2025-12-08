#!/usr/bin/env python3
"""
Usage:
```bash
cd ~/nextmini/examples/bare-metal/vm-fly
./deploy-flyio-relay.py --controller-ip 206.12.89.244 --node-ids 2 4 6 8
```
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run_command(cmd, check=True, capture_output=False):
    """Run a shell command."""
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=check, capture_output=capture_output, text=True)
    return result


def check_flyctl():
    """Check if flyctl is installed."""
    result = subprocess.run(
        ["flyctl", "version"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        print("❌ flyctl is not installed!")
        print("   Install: https://fly.io/docs/hands-on/install-flyctl/")
        sys.exit(1)


def check_authenticated():
    """Check if user is authenticated with Fly.io."""
    result = subprocess.run(
        ["flyctl", "auth", "whoami"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        print("❌ Not authenticated with Fly.io!")
        print("   Run: flyctl auth login")
        sys.exit(1)


def get_app_ipv4(app_name):
    """Get allocated IPv4 address for a Fly.io app."""
    result = subprocess.run(
        ["flyctl", "ips", "list", "-a", app_name, "--json"],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        print(f"DEBUG: flyctl ips list failed: {result.stderr}")
        return None

    try:
        import json

        ips = json.loads(result.stdout)
        print(f"DEBUG: Got {len(ips)} IPs: {ips}")
        for ip_info in ips:
            ip_type = ip_info.get("Type") or ip_info.get("type")
            ip_addr = (
                ip_info.get("Address") or ip_info.get("address") or ip_info.get("IP")
            )
            print(f"DEBUG: Checking IP - type={ip_type}, addr={ip_addr}")
            if ip_type in ["v4", "shared_v4"]:
                return ip_addr
    except Exception as e:
        print(f"Failed to parse IP list: {e}")
        print(f"DEBUG: stdout was: {result.stdout}")

    return None


def create_node_config(
    template_path, output_path, controller_ip, node_id, public_ipv4=None
):
    """Create node config from template."""
    # Set both private and public network addr to the Fly.io allocated IPv4
    addr_config = (
        f'private_network_addr = "{public_ipv4}"\npublic_network_addr = "{public_ipv4}"'
        if public_ipv4
        else ""
    )

    template = f"""ip_version = "ipv4"
{addr_config}
num_tun_queues = 1
num_packet_processors = 4
channel_capacity = 4000
queue_capacity = 3000
feature = "concurrent"

controller_addr = "ws://{controller_ip}:3000"
node_id = {node_id}
public_network_port = "8080"
"""

    with open(output_path, "w") as f:
        f.write(template)


# CAUTION: Here we use UDP instead of TCP as we are using QUIC protocol.
# TODO: Haven't tested if TCP here really works.
# And the [[vm]] section can be used to set the VM size.
def create_fly_config(output_path, app_name, config_path):
    """Create fly.toml from template."""
    content = f"""app = "{app_name}"
primary_region = "iad"

[build]

[env]
  RUST_LOG = "info"

[[vm]]
  memory = "1gb"
  cpus = 1

[[files]]
  guest_path = "/app/config.toml"
  local_path = "{config_path}"

[[services]]
  protocol = "udp"
  internal_port = 8080

  [[services.ports]]
    port = 8080
"""

    with open(output_path, "w") as f:
        f.write(content)


def deploy_node(repo_root, script_dir, controller_ip, node_id, vm_size=None):
    """Deploy a single Fly.io relay node."""
    app_name = f"nextmini-relay-{node_id}"

    print()
    print("=" * 60)
    print(f"Deploying Relay Node {node_id}")
    print("=" * 60)
    print()

    # Check if app exists
    result = subprocess.run(
        ["flyctl", "status", "-a", app_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )

    if result.returncode != 0:
        print(f"Creating new app: {app_name}")
        run_command(["flyctl", "apps", "create", app_name, "--org", "personal"])
    else:
        print(f"App exists: {app_name}")

    # Get or allocate IPv4 address
    print(f"Checking IPv4 address for {app_name}...")
    ipv4 = get_app_ipv4(app_name)

    if not ipv4:
        print(f"No IPv4 found, allocating shared IPv4 (free)...")
        run_command(["flyctl", "ips", "allocate-v4", "--shared", "-a", app_name])
        # Wait a moment for allocation
        import time

        time.sleep(2)
        ipv4 = get_app_ipv4(app_name)

    if ipv4:
        print(f"Using IPv4 address: {ipv4}")
    else:
        print(f"Warning: Could not get IPv4 address, will use auto-detection")

    # Create temporary config files with real IPv4
    tmp_node_config = f"/tmp/relay-config-{node_id}.toml"
    tmp_fly_config = f"/tmp/fly.relay-{node_id}.toml"

    create_node_config(
        None,  # No template needed, we create content directly
        tmp_node_config,
        controller_ip,
        node_id,
        ipv4,  # Use real IPv4
    )

    abs_node_config = os.path.abspath(tmp_node_config)

    create_fly_config(
        tmp_fly_config,
        app_name,
        abs_node_config,
    )

    # Use vm-fly specific Dockerfile (includes TLS certificates for QUIC)
    dockerfile = script_dir / "Dockerfile.relay"

    if not dockerfile.exists():
        print(f"❌ Dockerfile not found: {dockerfile}")
        sys.exit(1)

    print(f"Using Dockerfile: {dockerfile}")

    # Deploy
    print(f"Deploying {app_name}...")
    os.chdir(str(repo_root))

    abs_fly_config = os.path.abspath(tmp_fly_config)
    abs_dockerfile = os.path.abspath(str(dockerfile))

    deploy_cmd = [
        "flyctl",
        "deploy",
        "--config",
        abs_fly_config,
        "--dockerfile",
        abs_dockerfile,
        "--build-arg",
        "CARGO_PROFILE=release",
        "--app",
        app_name,
        "--ha=false",
        "--yes",
    ]

    if vm_size:
        deploy_cmd.extend(["--vm-size", vm_size])

    run_command(deploy_cmd)

    # Clean up temp files
    os.remove(tmp_node_config)
    os.remove(tmp_fly_config)

    print(f"Relay node {node_id} deployed successfully!")
    return app_name


def main():
    """Main deployment function."""
    parser = argparse.ArgumentParser(
        description="Deploy Fly.io relay nodes with custom node IDs"
    )
    parser.add_argument(
        "--controller-ip",
        required=True,
        help="Public IP address of the controller",
    )
    parser.add_argument(
        "--node-ids",
        type=int,
        nargs="+",
        default=[2, 4, 6, 8],
        help="Node IDs to deploy (default: 2 4 6 8)",
    )
    parser.add_argument(
        "--vm-size",
        default=None,
        help="Fly.io VM size (default: shared-cpu-1x)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Nextmini Fly.io Relay Nodes Deployment")
    print("=" * 60)
    print()
    print(f"Controller IP:  {args.controller_ip}")
    print(f"Relay Node IDs: {args.node_ids}")
    print(f"VM Size:        {args.vm_size or 'shared-cpu-1x (default)'}")
    print()

    # Pre-flight checks
    check_flyctl()
    check_authenticated()

    # Get paths
    script_dir = Path(__file__).parent.absolute()
    repo_root = script_dir.parent.parent.parent

    # Deploy nodes
    deployed_apps = []
    for node_id in args.node_ids:
        try:
            app_name = deploy_node(
                repo_root, script_dir, args.controller_ip, node_id, args.vm_size
            )
            deployed_apps.append((node_id, app_name))
        except Exception as e:
            print(f"❌ Failed to deploy node {node_id}: {e}")
            sys.exit(1)

    # Summary
    print()
    print("=" * 60)
    print("All relay nodes deployed successfully!")
    print("=" * 60)
    print()
    print("Deployed Applications:")
    for node_id, app in deployed_apps:
        print(f"   Node {node_id}: {app}")
    print()
    print("Next Steps:")
    print("   1. Verify nodes connect to controller:")
    print("      docker logs -f nextmini-controller")
    print("   2. Check relay node logs:")
    for node_id, app in deployed_apps:
        print(f"      flyctl logs -a {app}")
    print()
    print("Cleanup:")
    for _, app in deployed_apps:
        print(f"   flyctl apps destroy {app} --yes")
    print()


if __name__ == "__main__":
    main()
