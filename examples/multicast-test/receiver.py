#!/usr/bin/env python3
"""
Multicast Receiver - receives data from a multicast group
Uses the NEW SIMPLIFIED multicast API
"""

import sys
import time
import argparse
from pathlib import Path

# Add current directory to path for imports
_EXAMPLE_DIR = Path(__file__).resolve().parent
if str(_EXAMPLE_DIR) not in sys.path:
    sys.path.insert(0, str(_EXAMPLE_DIR))

try:
    from nextmini_py import Dataplane
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


def _extract_tcp_payload(packet_buffer: bytes) -> bytes:
    """Extract TCP payload from an IPv4/TCP packet."""
    
    if len(packet_buffer) < 20:
        raise ValueError("Packet too short to contain IPv4 header")
    
    ip_header_len = (packet_buffer[0] & 0x0F) * 4
    if ip_header_len < 20 or len(packet_buffer) < ip_header_len + 20:
        raise ValueError("Invalid IPv4 header length in packet")
    
    tcp_header_offset = ip_header_len
    tcp_data_offset = (packet_buffer[tcp_header_offset + 12] >> 4) & 0x0F
    tcp_header_len = tcp_data_offset * 4
    if tcp_header_len < 20:
        raise ValueError("Invalid TCP header length in packet")
    
    payload_offset = ip_header_len + tcp_header_len
    if payload_offset > len(packet_buffer):
        raise ValueError("Malformed packet: payload offset exceeds packet length")
    
    return packet_buffer[payload_offset:]


class MulticastReceiver:
    """Receives test data from a multicast group."""
    
    def __init__(self, config_path: str, sender_node_id: int):
        """
        Initialize receiver node.
        
        Args:
            config_path: Path to node configuration file
            sender_node_id: Sender's node ID
        """
        print(f"[Receiver] Initializing Multicast Receiver")
        print(f"[Receiver] Config: {config_path}")
        print(f"[Receiver] Group ID: {GROUP_TEST}")
        print(f"[Receiver] Sender node ID: {sender_node_id}")
        
        # Initialize Dataplane
        self.dataplane = Dataplane(config_path)
        self.sender_node_id = sender_node_id
        
        # Wait for routes to be established
        print(f"[Receiver] Waiting 3 seconds for routes...")
        time.sleep(3)
        
        # Join multicast group (NEW SIMPLIFIED API!)
        print(f"[Receiver] Joining multicast group {GROUP_TEST}...")
        self.dataplane.join_group(GROUP_TEST)
        
        # Wait for group membership to propagate
        print(f"[Receiver] Waiting 2 seconds for group membership...")
        time.sleep(2)
        
        # Register receiver for multicast packets (NEW SIMPLIFIED API!)
        # No need to know group_ip - it's calculated automatically!
        print(f"[Receiver] Registering receiver...")
        self.receiver = self.dataplane.register_group_receiver(
            group_id=GROUP_TEST,        # ← Just use group_id!
            src_node_id=sender_node_id,
            src_port=SRC_PORT,
            dst_port=DST_PORT
        )
        
        print(f"[Receiver] Ready to receive!")
    
    def receive_message(self, timeout_ms: int = 30000) -> str | None:
        """Receive a test message from the multicast group."""
        packet_data = self.receiver.recv(timeout_ms=timeout_ms)
        
        if packet_data is None:
            return None
        
        try:
            # Extract TCP payload
            payload = _extract_tcp_payload(packet_data)
            message = payload.decode('utf-8')
            return message
        except Exception as e:
            print(f"[Receiver] Error extracting payload: {e}")
            return None
    
    def run_test(self, max_iterations: int = 10):
        """Run the receiving test."""
        print(f"\n[Receiver] Starting test: expecting {max_iterations} messages")
        print("=" * 60)
        
        received_count = 0
        
        for i in range(max_iterations):
            print(f"[Receiver] Waiting for message {i}...")
            message = self.receive_message(timeout_ms=10000)
            
            if message:
                print(f"[Receiver] Received: {message}")
                received_count += 1
            else:
                print(f"[Receiver] Timeout waiting for message {i}")
        
        print("=" * 60)
        print(f"[Receiver] Test completed!")
        print(f"[Receiver] Received {received_count}/{max_iterations} messages")
        
        return received_count


def main():
    parser = argparse.ArgumentParser(description="Multicast Receiver Test")
    parser.add_argument(
        "--config",
        type=str,
        default="node-config.toml",
        help="Path to node configuration file"
    )
    parser.add_argument(
        "--sender-node-id",
        type=int,
        default=1,
        help="Sender's node ID"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=10,
        help="Number of messages to expect"
    )
    
    args = parser.parse_args()
    
    try:
        receiver = MulticastReceiver(args.config, args.sender_node_id)
        received = receiver.run_test(args.iterations)
        
        # Exit with error if not all messages received
        if received < args.iterations:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n[Receiver] Interrupted by user")
    except Exception as e:
        print(f"\n[Receiver] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

