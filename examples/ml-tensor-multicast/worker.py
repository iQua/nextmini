#!/usr/bin/env python3
"""
ML Worker for multicast branch (using nextmini_py.Dataplane API)

Simulates a distributed ML worker node that:
1. Receives tensors from the trainer
2. Processes them (simulated computation)
3. Sends results back to the trainer

Uses multicast branch's Dataplane API with virtual IP overlay.
"""

import sys
import time
import argparse
from pathlib import Path

import torch

# Ensure the example directory is in the path for local imports
_EXAMPLE_DIR = Path(__file__).resolve().parent
if str(_EXAMPLE_DIR) not in sys.path:
    sys.path.insert(0, str(_EXAMPLE_DIR))

try:
    from torch_serializer import serialize_tensor, deserialize_tensor
    import nextmini_py as nm
except ImportError as e:
    print(f"ERROR: Failed to import required modules: {e}")
    print("\nMake sure to:")
    print("1. Build nextmini_py: maturin build --release -m python-api/Cargo.toml")
    print("2. Install: pip install target/wheels/nextmini_py-*.whl")
    print("3. Install PyTorch: pip install torch")
    sys.exit(1)


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


class MLWorker:
    """Simulates an ML worker using multicast branch's Dataplane API."""
    
    def __init__(
        self,
        config_path: str,
        trainer_node_id: int,
        *,
        src_port: int = 5000,
        dst_port: int = 4000,
    ):
        """
        Initialize worker node.
        
        Args:
            config_path: Path to Nextmini node configuration
            trainer_node_id: Trainer's node ID (not IP address)
            src_port: Source port for responses
            dst_port: Destination port (trainer's listening port)
        """
        print(f"[Worker] Initializing ML Worker")
        print(f"[Worker] Config: {config_path}")
        print(f"[Worker] Trainer node ID: {trainer_node_id}")
        print(f"[Worker] Ports: {src_port} → {dst_port}")
        
        # Initialize Dataplane (multicast API)
        self.dataplane = nm.Dataplane(config_path)
        self.trainer_node_id = trainer_node_id
        self.src_port = src_port
        self.dst_port = dst_port
        
        # Register receiver for tensors from trainer
        self.receiver = self.dataplane.register_receiver_from_node(
            src_node_id=trainer_node_id,
            src_port=dst_port,  # Reverse: trainer's dst becomes our src
            dst_port=src_port,  # Reverse: trainer's src becomes our dst
        )
        
        print(f"[Worker] Ready to receive tensors!")
    
    def recv_tensor(self, timeout_ms: int = 30000) -> torch.Tensor | None:
        """
        Receive tensor from trainer.
        
        Args:
            timeout_ms: Receive timeout in milliseconds
            
        Returns:
            Received tensor, or None if timeout
        """
        packet = self.receiver.recv(timeout_ms=timeout_ms)
        if packet is None:
            return None
        
        # Extract TCP payload from the full packet
        payload = _extract_tcp_payload(packet)
        
        # Deserialize tensor
        tensor = deserialize_tensor(payload)
        return tensor
    
    def process_tensor(self, tensor: torch.Tensor) -> torch.Tensor:
        """
        Simulate processing/computation on received tensor.
        
        In a real distributed training scenario, this might be:
        - Applying optimizer updates
        - Computing forward/backward pass
        - Aggregating gradients
        
        For this simulation, we simply add 1.0 to all elements.
        
        Args:
            tensor: Input tensor
            
        Returns:
            Processed tensor
        """
        # Simulate some computation time
        time.sleep(0.001)  # 1ms
        
        # Simple operation: increment all values
        result = tensor + 1.0
        
        return result
    
    def send_result(self, result: torch.Tensor) -> int:
        """
        Send processed result back to trainer.
        
        Args:
            result: Processed tensor to send
            
        Returns:
            Number of bytes sent
        """
        # Serialize result
        data = serialize_tensor(result)
        data_len = len(data)
        
        # Send via Dataplane (multicast API)
        self.dataplane.send_to_node(
            dst_node_id=self.trainer_node_id,
            payload=memoryview(data),
            src_port=self.src_port,
            dst_port=self.dst_port,
        )
        
        return data_len
    
    def run_worker_loop(self, max_iterations: int = None):
        """
        Main worker loop - receives tensors and processes them.
        
        Args:
            max_iterations: Maximum number of iterations (None = infinite)
        """
        print(f"\n[Worker] Starting worker loop")
        print(f"[Worker] Max iterations: {max_iterations if max_iterations else 'infinite'}")
        print(f"[Worker] Waiting for tensors from trainer...")
        print("-" * 60)
        
        iteration = 0
        
        while True:
            if max_iterations and iteration >= max_iterations:
                print(f"\n[Worker] Reached max iterations ({max_iterations})")
                break
            
            # Receive tensor from trainer
            print(f"\n[Worker] Iteration {iteration + 1}")
            print(f"  Waiting for tensor...")
            
            tensor = self.recv_tensor(timeout_ms=30000)
            
            if tensor is None:
                print(f"  ⏱️  Timeout - no tensor received")
                if iteration == 0:
                    print(f"  Waiting for trainer to start...")
                    continue
                else:
                    print(f"  Assuming training complete")
                    break
            
            recv_time = time.time()
            
            print(f"  Received tensor: shape={tensor.shape}, dtype={tensor.dtype}")
            print(f"  Tensor stats: min={tensor.min():.4f}, max={tensor.max():.4f}, mean={tensor.mean():.4f}")
            
            # Process tensor
            print(f"  Processing tensor...")
            result = self.process_tensor(tensor)
            
            print(f"  Result: shape={result.shape}, dtype={result.dtype}")
            
            # Send result back
            sent_bytes = self.send_result(result)
            
            send_time = time.time()
            processing_ms = (send_time - recv_time) * 1000
            
            print(f"  Sent result back to trainer: {sent_bytes:,} bytes")
            print(f"  ✅ Processing time: {processing_ms:.2f}ms")
            
            iteration += 1
        
        # Print summary
        print("\n" + "=" * 60)
        print(f"[Worker] Worker loop complete!")
        print(f"  Total tensors processed: {iteration}")
        print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="ML Worker (multicast branch)")
    parser.add_argument(
        "--config",
        type=str,
        default="node2-config.toml",
        help="Path to node configuration file"
    )
    parser.add_argument(
        "--trainer-node-id",
        type=int,
        required=True,
        help="Trainer node ID (not IP address)"
    )
    parser.add_argument(
        "--src-port",
        type=int,
        default=5000,
        help="Source TCP port for responses"
    )
    parser.add_argument(
        "--dst-port",
        type=int,
        default=4000,
        help="Destination TCP port (trainer's listening port)"
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Maximum number of iterations (default: infinite)"
    )
    
    args = parser.parse_args()
    
    try:
        worker = MLWorker(
            args.config,
            args.trainer_node_id,
            src_port=args.src_port,
            dst_port=args.dst_port,
        )
        worker.run_worker_loop(max_iterations=args.max_iterations)
        # Don't exit - keep nextmini node alive  
        print("\n[Worker] Processing complete. Dataplane node remains active.")
        print("[Worker] Press Ctrl+C to exit.")
        # Keep the process alive
        import signal
        signal.pause()
    except KeyboardInterrupt:
        print("\n[Worker] Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n[Worker] ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

