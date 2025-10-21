#!/usr/bin/env python3
"""
Deploy Nextmini Controller
Usage: uv run deploy_controller.py
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


def main():
    parser = argparse.ArgumentParser(
        description='Deploy Nextmini Controller',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run deploy_controller.py
  uv run deploy_controller.py --db-password mypassword
        """
    )
    
    parser.add_argument(
        '--db-password',
        default='pgpwrd',
        help='PostgreSQL database password (default: pgpwrd)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=3000,
        help='Controller port (default: 3000)'
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Deploying Nextmini Controller")
    print("=" * 60)
    
    # Determine paths
    script_dir = Path(__file__).parent.resolve()
    repo_root = script_dir.parent.parent
    
    # Step 1: Start PostgreSQL database
    print("\nStep 1: Starting PostgreSQL database...")
    start_db_script = repo_root / "start-database.sh"
    
    if not start_db_script.exists():
        print(f"Error: start-database.sh not found at {start_db_script}")
        sys.exit(1)
    
    # Make it executable
    os.chmod(start_db_script, 0o755)
    
    # Start database
    run_command(str(start_db_script), cwd=repo_root)
    
    # Step 2: Wait for database
    print("\nStep 2: Waiting for database to be ready...")
    time.sleep(5)
    
    # Step 3: Setup deployment directory
    print("\nStep 3: Setting up deployment directory...")
    deploy_dir = script_dir / "controller-deploy"
    deploy_dir.mkdir(exist_ok=True)
    
    # Check if binaries exist
    controller_bin = repo_root / "target" / "release" / "controller"
    check_file_exists(
        controller_bin,
        "Controller binary not found. Please build it first:\n"
        f"  cd {repo_root} && cargo build --release -p controller"
    )
    
    # Check if certificates exist
    cert_file = repo_root / "server_cert.pem"
    key_file = repo_root / "server_key.pem"
    check_file_exists(
        cert_file,
        "TLS certificates not found. Please generate them first:\n"
        f"  cd {repo_root} && cargo run -p cert-gen"
    )
    
    # Copy files
    import shutil
    shutil.copy(controller_bin, deploy_dir / "controller")
    shutil.copy(cert_file, deploy_dir / "server_cert.pem")
    shutil.copy(key_file, deploy_dir / "server_key.pem")
    shutil.copy(script_dir / "controller-config.toml", deploy_dir / "config.toml")
    
    print(f"Files copied to: {deploy_dir}")
    
    # Step 4: Start controller
    print("\nStep 4: Starting Controller...")
    
    env = os.environ.copy()
    env['RUST_LOG'] = 'info'
    
    with open(deploy_dir / "controller.log", "w") as log_file:
        process = subprocess.Popen(
            [str(deploy_dir / "controller")],
            cwd=deploy_dir,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
    
    controller_pid = process.pid
    
    # Wait a bit and check if it's still running
    time.sleep(2)
    poll = process.poll()
    if poll is not None:
        print(f"Error: Controller exited with code {poll}")
        print("\nLast 20 lines of log:")
        with open(deploy_dir / "controller.log", "r") as f:
            lines = f.readlines()
            for line in lines[-20:]:
                print(line.rstrip())
        sys.exit(1)
    
    # Get host IP
    result = run_command(["hostname", "-I"], check=False)
    host_ip = result.stdout.split()[0] if result.stdout else "unknown"
    
    print("\n" + "=" * 60)
    print("Controller Deployment Complete")
    print("=" * 60)
    print(f"Controller PID: {controller_pid}")
    print(f"Controller listening on: 0.0.0.0:{args.port}")
    print(f"Logs: {deploy_dir / 'controller.log'}")
    print(f"\nTo view logs: tail -f {deploy_dir / 'controller.log'}")
    print(f"To stop: kill {controller_pid}")
    print("\n" + "=" * 60)
    print("Get this host's IP address for dataplane nodes:")
    print(f"  IP: {host_ip}")
    print(f"  Full address: ws://{host_ip}:{args.port}")
    print("=" * 60)


if __name__ == "__main__":
    main()


