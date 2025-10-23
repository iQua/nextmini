#!/usr/bin/env python3
"""
Deploy Nextmini Dataplane nodes to Fly.io.

This script deploys dataplane nodes to Fly.io that will connect back
to the local controller instance.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run_command(cmd, check=True, capture_output=False):
    """Run a shell command."""
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(
        cmd, check=check, capture_output=capture_output, text=True
    )
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
        print("flyctl is not installed!")
        print(
            "   Install it from: https://fly.io/docs/hands-on/install-flyctl/"
        )
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
        print("Not authenticated with Fly.io!")
        print("   Run: flyctl auth login")
        sys.exit(1)


def create_node_config(template_path, output_path, public_ip, node_id):
    """Create node config from template."""
    with open(template_path, "r") as f:
        content = f.read()

    content = content.replace("{{PUBLIC_IP}}", public_ip)
    content = content.replace("{{NODE_ID}}", str(node_id))

    with open(output_path, "w") as f:
        f.write(content)


def create_fly_config(template_path, output_path, app_name, config_path):
    """Create fly.toml from template."""
    with open(template_path, "r") as f:
        content = f.read()

    content = content.replace("{{APP_NAME}}", app_name)
    content = content.replace("{{CONFIG_PATH}}", config_path)

    # Remove [build] section to avoid conflict with --dockerfile argument
    lines = content.split("\n")
    filtered_lines = []
    in_build_section = False

    for line in lines:
        if line.strip().startswith("[build]"):
            in_build_section = True
            continue
        elif in_build_section and line.strip().startswith("["):
            in_build_section = False

        if not in_build_section:
            filtered_lines.append(line)

    content = "\n".join(filtered_lines)

    with open(output_path, "w") as f:
        f.write(content)


def deploy_node(repo_root, script_dir, public_ip, node_id, vm_size=None):
    """Deploy a single dataplane node."""
    app_name = f"nextmini-node-{node_id}"

    print()
    print("=" * 60)
    print(f"Deploying Node {node_id}")
    print("=" * 60)
    print()

    # Create temporary config files
    tmp_node_config = f"/tmp/node-config-{node_id}.toml"
    tmp_fly_config = f"/tmp/fly.node-{node_id}.toml"

    create_node_config(
        script_dir / "node-config.toml.template",
        tmp_node_config,
        public_ip,
        node_id,
    )

    # Use absolute path for node config in fly.toml
    abs_node_config = os.path.abspath(tmp_node_config)

    create_fly_config(
        script_dir / "fly.dataplane.toml.template",
        tmp_fly_config,
        app_name,
        abs_node_config,
    )

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

    # Deploy
    print(f"Deploying {app_name}...")
    # Change to repo root for build context
    os.chdir(str(repo_root))

    # Use absolute paths for config and dockerfile
    abs_fly_config = os.path.abspath(tmp_fly_config)
    abs_dockerfile = os.path.abspath(str(script_dir / "Dockerfile.dataplane"))

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
        "--no-public-ips",
        "--yes",
    ]
    
    # Add vm-size if specified
    if vm_size:
        deploy_cmd.extend(["--vm-size", vm_size])
    
    run_command(deploy_cmd)

    # Clean up temp files
    os.remove(tmp_node_config)
    os.remove(tmp_fly_config)

    print(f"Node {node_id} deployed successfully!")
    return app_name


def main():
    """Main deployment function."""
    parser = argparse.ArgumentParser(
        description="Deploy Nextmini dataplane nodes to Fly.io"
    )
    parser.add_argument(
        "--public-ip",
        required=True,
        help="Public IP address of the controller server",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=2,
        help="Number of nodes to deploy (default: 2)",
    )
    parser.add_argument(
        "--region", default="iad", help="Fly.io region (default: iad)"
    )
    parser.add_argument(
        "--vm-size",
        default=None,
        help="Fly.io VM size (e.g., shared-cpu-1x, performance-2x, default: shared-cpu-1x)",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Nextmini Dataplane - Fly.io Deployment")
    print("=" * 60)
    print()
    print(f"Controller IP:  {args.public_ip}")
    print(f"Number of Nodes: {args.nodes}")
    print(f"Region:         {args.region}")
    print(f"VM Size:        {args.vm_size or 'shared-cpu-1x (default)'}")
    print()

    # Pre-flight checks
    check_flyctl()
    check_authenticated()

    # Get paths
    script_dir = Path(__file__).parent.absolute()
    repo_root = script_dir.parent.parent

    # Deploy nodes
    deployed_apps = []
    for i in range(1, args.nodes + 1):
        try:
            app_name = deploy_node(repo_root, script_dir, args.public_ip, i, args.vm_size)
            deployed_apps.append(app_name)
        except Exception as e:
            print(f"Failed to deploy node {i}: {e}")
            sys.exit(1)

    # Summary
    print()
    print("=" * 60)
    print("✅ All nodes deployed successfully!")
    print("=" * 60)
    print()
    print("Deployed Applications:")
    for app in deployed_apps:
        print(f"   - {app}")
    print()
    print("Next Steps:")
    print("   1. Check controller logs: docker compose logs -f controller")
    print(f"   2. Check node logs: flyctl logs -a {deployed_apps[0]}")
    print("   3. Monitor status: flyctl status -a <app-name>")
    print("   4. SSH into a node:")
    for app in deployed_apps:
        print(f"      flyctl ssh console -a {app}")
    print()
    print("Cleanup:")
    for app in deployed_apps:
        print(f"   flyctl apps destroy {app}")
    print()


if __name__ == "__main__":
    main()
