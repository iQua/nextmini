#!/usr/bin/env python3
"""
Start Controller and PostgreSQL locally using Docker Compose.

This script manages the local controller instance that will receive
connections from Fly.io dataplane nodes.
"""

import socket
import subprocess
import sys
import time

import requests


def get_public_ip():
    """Get the public IP address of this machine."""
    try:
        response = requests.get("https://ifconfig.me/ip", timeout=5)
        return response.text.strip()
    except Exception as e:
        print(f"Warning: Could not detect public IP: {e}")
        print("Assuming localhost for testing...")
        return "127.0.0.1"


def check_port_available(port):
    """Check if a port is available."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("0.0.0.0", port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def docker_compose(action, *args):
    """Run docker-compose command."""
    cmd = ["docker", "compose"] + [action] + list(args)
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)
    return result.returncode == 0


def main():
    """Main function to start the controller stack."""
    print("=" * 60)
    print("Nextmini Controller - Local Deployment")
    print("=" * 60)
    print()

    # Get public IP
    public_ip = get_public_ip()
    print(f"Public IP: {public_ip}")
    print(f"Controller will be accessible at: ws://{public_ip}:3000")
    print()

    # Check if port 3000 is available
    if not check_port_available(3000):
        print("Port 3000 is already in use!")
        print(
            "Stop the conflicting service or change the port in docker-compose.yml."
        )
        sys.exit(1)

    # Check if Docker is running
    result = subprocess.run(
        ["docker", "info"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        print("Docker is not running...")
        print("Please start Docker and try again.")
        sys.exit(1)

    # Start services
    print("Starting PostgreSQL and Controller...")
    print()
    if not docker_compose("up", "-d", "--build"):
        print("Failed to start services.")
        sys.exit(1)

    print()
    print("Waiting for services to be healthy...")
    time.sleep(5)

    # Check service status
    print()
    print("Service Status:")
    docker_compose("ps")

    print()
    print("=" * 60)
    print("   Controller is running!")
    print("=" * 60)
    print()
    print(f"Controller WebSocket: ws://{public_ip}:3000")
    print("PostgreSQL:           localhost:5432")
    print()
    print("Next Steps:")
    print(f"1. Run: uv run deploy-flyio.py --public-ip {public_ip} --nodes 2")
    print("2. Check logs: docker compose logs -f controller")
    print("3. Stop services: docker compose down")
    print()


if __name__ == "__main__":
    main()
