#!/usr/bin/env python3
"""
Deploy baseline ring-emu nodes to Fly.io for performance comparison.

This deploys ONLY ringallreduce (no nextmini dataplane) to measure
bare-metal performance on Fly.io native networking.

Usage:
    python3 deploy_baseline.py --nodes 7 --region sjc

After deployment:
    python3 launch_ring_baseline.py --num-nodes 7
"""

import argparse
import subprocess
import sys
import pathlib
import time


def run(cmd, check=True, cwd=None):
    """Run a command and return result."""
    print(f"  → {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"❌ Command failed: {' '.join(cmd)}")
        print(f"   stdout: {result.stdout}")
        print(f"   stderr: {result.stderr}")
        sys.exit(1)
    return result


def check_flyctl():
    """Verify flyctl is installed and authenticated."""
    result = run(["flyctl", "version"], check=False)
    if result.returncode != 0:
        print("flyctl not found. Install: https://fly.io/docs/hands-on/install-flyctl/")
        sys.exit(1)

    result = run(["flyctl", "auth", "whoami"], check=False)
    if result.returncode != 0:
        print("Not logged in to Fly.io. Run: flyctl auth login")
        sys.exit(1)

    print("flyctl authenticated")


def deploy_node(
    node_num: int,
    region: str,
    memory: str,
    cpu_kind: str,
    cpus: int,
    script_dir: pathlib.Path,
):
    """Deploy a single baseline node."""
    app_name = f"baseline-ring-{node_num}"

    print(f"\n  Deploying {app_name}...")

    # Create fly.toml from template
    template = (script_dir / "fly.baseline.toml.template").read_text()
    fly_toml = template.replace("{APP_NAME}", app_name).replace(
        "{NODE_NUM}", str(node_num)
    )
    fly_toml = fly_toml.replace(
        "primary_region = 'sjc'", f"primary_region = '{region}'"
    )
    fly_toml = fly_toml.replace("{MEMORY}", memory)
    fly_toml = fly_toml.replace("{CPU_KIND}", cpu_kind)
    fly_toml = fly_toml.replace("{CPUS}", str(cpus))

    fly_toml_path = script_dir / f"fly.baseline-{node_num}.toml"
    fly_toml_path.write_text(fly_toml)

    # Check if app exists
    result = run(["flyctl", "apps", "list"], check=True)
    app_exists = app_name in result.stdout

    if not app_exists:
        print(f"  Creating new app {app_name}...")
        run(["flyctl", "apps", "create", app_name, "--org", "personal"], check=True)
    else:
        print(f"  App {app_name} already exists")

    # Deploy
    print(f"  Deploying to {app_name}...")
    # Use the parent directory for build context so Dockerfile.baseline can access ring-emu/
    parent_dir = script_dir.parent

    # Copy Dockerfile.baseline to parent so it can access ring-emu/
    dockerfile_content = (script_dir / "Dockerfile.baseline").read_text()
    temp_dockerfile = parent_dir / "Dockerfile.baseline"
    temp_dockerfile.write_text(dockerfile_content)

    try:
        run(
            [
                "flyctl",
                "deploy",
                "--config",
                str(fly_toml_path),
                "--dockerfile",
                "Dockerfile.baseline",
                "--app",
                app_name,
                "--ha=false",  # Single instance
            ],
            cwd=parent_dir,
            check=True,
        )
    finally:
        # Clean up temp dockerfile
        if temp_dockerfile.exists():
            temp_dockerfile.unlink()

    print(f"{app_name} deployed")

    return app_name


def main():
    parser = argparse.ArgumentParser(
        description="Deploy baseline ring-emu nodes to Fly.io"
    )
    parser.add_argument(
        "--nodes", type=int, default=7, help="Number of nodes to deploy (default: 7)"
    )
    parser.add_argument(
        "--region",
        type=str,
        default="iad",
        help="Fly.io region (default: iad = Ashburn)",
    )
    parser.add_argument(
        "--memory",
        type=str,
        default="256mb",
        help="Memory size (default: 256mb, options: 512mb, 1gb, 2gb)",
    )
    parser.add_argument(
        "--cpu-kind",
        type=str,
        default="shared",
        help="CPU kind (default: shared, options: shared, performance)",
    )
    parser.add_argument(
        "--cpus", type=int, default=1, help="Number of vCPUs (default: 1)"
    )
    parser.add_argument(
        "--start", type=int, default=1, help="Starting node number (default: 1)"
    )

    args = parser.parse_args()

    script_dir = pathlib.Path(__file__).parent.absolute()

    print("  Deploying Baseline Ring-Emu Nodes to Fly.io")
    print(f"   Nodes: {args.nodes}")
    print(f"   Region: {args.region}")
    print(f"   CPU: {args.cpu_kind}-{args.cpus}x")
    print(f"   Memory: {args.memory}")
    print(f"   Starting at: baseline-ring-{args.start}")
    print()

    check_flyctl()

    deployed_apps = []
    for i in range(args.start, args.start + args.nodes):
        app_name = deploy_node(
            i, args.region, args.memory, args.cpu_kind, args.cpus, script_dir
        )
        deployed_apps.append(app_name)
        time.sleep(2)  # Brief pause between deployments

    print("\n" + "=" * 60)
    print("All baseline nodes deployed!")
    print("=" * 60)
    print(f"\nDeployed apps: {', '.join(deployed_apps)}")
    print(f"\n Next steps:")
    print(f"   1. Wait ~30 seconds for nodes to be ready")
    print(f"   2. Run baseline test:")
    print(f"      cd {script_dir}")
    print(f"      python3 launch_ring_baseline.py --num-nodes {args.nodes}")
    print()


if __name__ == "__main__":
    main()
