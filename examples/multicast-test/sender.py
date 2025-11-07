#!/usr/bin/env python3
"""
Multicast Sender - sends data to a multicast group (nodes 2, 3, 4)
Uses the NEW SIMPLIFIED multicast API
"""

import sys
import time
import argparse
from pathlib import Path

import torch

# Add current directory to path for imports
_EXAMPLE_DIR = Path(__file__).resolve().parent
if str(_EXAMPLE_DIR) not in sys.path:
    sys.path.insert(0, str(_EXAMPLE_DIR))

try:
    from nextmini_py import Dataplane, FrozenBuffer
except ImportError as e:
    print(f"ERROR: Failed to import: {e}")
    print("\nMake sure nextmini_py is installed:")
    print("  maturin build --release -m python-api/Cargo.toml")
    print("  pip install target/wheels/nextmini_py-*.whl")
    sys.exit(1)

# Configuration constants
GROUP_TEST = 520  # User-specified group ID (just like node_id)
SRC_PORT = 4000
DST_PORT = 5000


def serialize_simple(data: bytes) -> bytes:
    """Simple serialization - just return the data."""
    return data


class MulticastSender:
    """Sends test data to a multicast group."""
    
    def __init__(self, config_path: str):
        """
        Initialize sender node.
        
        Args:
            config_path: Path to node configuration file
        """
        print(f"[Sender] Initializing Multicast Sender")
        print(f"[Sender] Config: {config_path}")
        print(f"[Sender] Group ID: {GROUP_TEST}")
        
        # Initialize Dataplane
        self.dataplane = Dataplane(config_path)
        
        # Wait for routes to be established
        print(f"[Sender] Waiting 3 seconds for routes...")
        time.sleep(3)
        
        # Create multicast group (NEW SIMPLIFIED API!)
        print(f"[Sender] Creating multicast group {GROUP_TEST}...")
        self.dataplane.create_group(GROUP_TEST, "test-group")
        
        print(f"[Sender] Group created! Ready to send.")
    
    def send_message(self, message: str, iteration: int):
        """Send a test message to the multicast group."""
        # Prepare data
        data = f"[Iteration {iteration}] {message}".encode('utf-8')
        frozen = FrozenBuffer(data)
        
        # Send to multicast group (NEW SIMPLIFIED API!)
        # No need to know group_ip - it's calculated automatically!
        self.dataplane.send_to_group(
            group_id=GROUP_TEST,  # ← Just use group_id!
            frozen=frozen,
            src_port=SRC_PORT,
            dst_port=DST_PORT
        )
        
        print(f"[Sender] Sent: {message} (iteration {iteration})")
    
    def run_test(self, iterations: int = 10, interval: float = 2.0):
        """Run the sending test."""
        print(f"\n[Sender] Starting test: {iterations} iterations, {interval}s interval")
        print("=" * 60)
        
        for i in range(iterations):
            message = f"Test message #{i}"
            self.send_message(message, i)
            
            if i < iterations - 1:
                time.sleep(interval)
        
        print("=" * 60)
        print(f"[Sender] Test completed! Sent {iterations} messages.")


def main():
    parser = argparse.ArgumentParser(description="Multicast Sender Test")
    parser.add_argument(
        "--config",
        type=str,
        default="node-config.toml",
        help="Path to node configuration file"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=10,
        help="Number of test iterations"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Interval between messages (seconds)"
    )
    
    args = parser.parse_args()
    
    try:
        sender = MulticastSender(args.config)
        sender.run_test(args.iterations, args.interval)
    except KeyboardInterrupt:
        print("\n[Sender] Interrupted by user")
    except Exception as e:
        print(f"\n[Sender] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

