#!/usr/bin/env python3
"""
ML Trainer for multicast branch (using nextmini_py.Dataplane API)

Simulates a distributed ML training node that:
1. Generates tensors (simulating gradients from training)
2. Sends them to a worker node via Nextmini (using node_id, not IP)
3. Receives processed results back
4. Measures round-trip latency

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
    from nextmini_py import Dataplane, FrozenBuffer
except ImportError as e:
    print(f"ERROR: Failed to import required modules: {e}")
    print("\nMake sure to:")
    print("1. Build nextmini_py: maturin build --release -m python-api/Cargo.toml")
    print("2. Install: pip install target/wheels/nextmini_py-*.whl")
    print("3. Install PyTorch: pip install torch")
    sys.exit(1)


# Note: Manual TCP header parsing is no longer needed.
# The dataplane now delivers PayloadDelivery objects with headers already stripped.


class MLTrainer:
    """Simulates an ML trainer using multicast branch's Dataplane API."""
    
    def __init__(
        self,
        config_path: str,
        worker_node_id: int,
        *,
        src_port: int = 4000,
        dst_port: int = 5000,
    ):
        """
        Initialize trainer node.
        
        Args:
            config_path: Path to Nextmini node configuration
            worker_node_id: Worker's node ID (not IP address)
            src_port: Source port for communication
            dst_port: Destination port for communication
        """
        print(f"[Trainer] Initializing ML Trainer")
        print(f"[Trainer] Config: {config_path}")
        print(f"[Trainer] Worker node ID: {worker_node_id}")
        print(f"[Trainer] Ports: {src_port} → {dst_port}")
        
        # Initialize Dataplane (multicast API)
        self.dataplane = Dataplane(config_path)
        self.worker_node_id = worker_node_id
        self.src_port = src_port
        self.dst_port = dst_port
        
        # Register receiver for responses from worker
        self.receiver = self.dataplane.register_receiver_from_node(
            src_node_id=worker_node_id,
            src_port=dst_port,  # Reverse: worker's dst becomes our src
            dst_port=src_port,  # Reverse: worker's src becomes our dst
        )
        
        # Wait for routes to be fully established in dataplane
        print(f"[Trainer] Waiting for routes to be fully established...")
        time.sleep(10)  # Give dataplane time to install and activate routes
        
        print(f"[Trainer] Ready to send tensors!")
    
    def generate_gradient(self, size: tuple, iteration: int) -> torch.Tensor:
        """
        Simulate gradient generation from training.
        
        Args:
            size: Tensor size (e.g., (1000, 1000))
            iteration: Training iteration number
            
        Returns:
            Random tensor simulating a gradient
        """
        torch.manual_seed(iteration)
        tensor = torch.randn(*size)
        return tensor
    
    def send_tensor(self, tensor: torch.Tensor) -> int:
        """
        Serialize and send tensor to worker.
        
        Args:
            tensor: PyTorch tensor to send
            
        Returns:
            Number of bytes sent
        """
        # Serialize tensor using torch.save
        data = serialize_tensor(tensor)
        data_len = len(data)
        
        # Wrap in FrozenBuffer for efficient transmission
        frozen = FrozenBuffer(data)
        
        # Send via Dataplane (multicast API uses node_id, not IP)
        self.dataplane.send_to_node(
            dst_node_id=self.worker_node_id,
            frozen=frozen,
            src_port=self.src_port,
            dst_port=self.dst_port,
        )
        
        return data_len
    
    def recv_result(self, timeout_ms: int = 5000) -> torch.Tensor | None:
        """
        Receive processed result from worker.
        
        Args:
            timeout_ms: Receive timeout in milliseconds
            
        Returns:
            Received tensor, or None if timeout
        """
        delivery = self.receiver.recv(timeout_ms=timeout_ms)
        if delivery is None:
            return None

        # Get TCP payload (headers already stripped by dataplane)
        payload = delivery.payload

        # Deserialize tensor
        tensor = deserialize_tensor(payload)
        return tensor
    
    def run_training_loop(self, tensor_size: tuple, iterations: int, verify: bool = True):
        """
        Main training loop.
        
        Args:
            tensor_size: Size of tensors to generate
            iterations: Number of training iterations
            verify: Whether to verify received tensors
        """
        print(f"\n[Trainer] Starting training loop")
        print(f"[Trainer] Tensor size: {tensor_size}")
        print(f"[Trainer] Iterations: {iterations}")
        print(f"[Trainer] Verification: {verify}")
        print("-" * 60)
        
        latencies = []
        
        for i in range(iterations):
            # Generate gradient
            gradient = self.generate_gradient(tensor_size, i)
            
            # Send to worker
            t0 = time.time()
            sent_bytes = self.send_tensor(gradient)
            
            print(f"\n[Trainer] Iteration {i+1}/{iterations}")
            print(f"  Generated tensor: shape={gradient.shape}, dtype={gradient.dtype}")
            print(f"  Serialized size: {sent_bytes:,} bytes")
            print(f"  Sent to worker node {self.worker_node_id}")
            
            # Wait for result
            print(f"  Waiting for result...")
            result = self.recv_result(timeout_ms=5000)
            
            if result is None:
                print(f"  ❌ Timeout waiting for result from worker")
                continue
            
            t1 = time.time()
            latency_ms = (t1 - t0) * 1000
            latencies.append(latency_ms)
            
            print(f"  Received result: shape={result.shape}, dtype={result.dtype}")
            print(f"  ✅ Round-trip latency: {latency_ms:.2f}ms")
            
            # Verify result (worker should have incremented all values by 1.0)
            if verify:
                expected = gradient + 1.0
                if torch.allclose(result, expected, rtol=1e-5, atol=1e-5):
                    print(f"  ✅ Verification passed")
                else:
                    max_diff = torch.max(torch.abs(result - expected)).item()
                    print(f"  ❌ Verification failed! Max diff: {max_diff}")
        
        # Print summary
        print("\n" + "=" * 60)
        print(f"[Trainer] Training complete!")
        print(f"  Total iterations: {iterations}")
        print(f"  Successful: {len(latencies)}")
        
        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            
            print(f"  Average latency: {avg_latency:.2f}ms")
            print(f"  Min latency: {min_latency:.2f}ms")
            print(f"  Max latency: {max_latency:.2f}ms")
            
            # Calculate throughput
            tensor_bytes = tensor_size[0] * tensor_size[1] * 4  # float32
            throughput_mbps = (tensor_bytes * 8 / 1_000_000) / (avg_latency / 1000)
            print(f"  Throughput: {throughput_mbps:.2f} Mbps")
        
        print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="ML Trainer (multicast branch)")
    parser.add_argument(
        "--config",
        type=str,
        default="node1-config.toml",
        help="Path to node configuration file"
    )
    parser.add_argument(
        "--worker-node-id",
        type=int,
        required=True,
        help="Worker node ID (not IP address)"
    )
    parser.add_argument(
        "--src-port",
        type=int,
        default=4000,
        help="Source TCP port"
    )
    parser.add_argument(
        "--dst-port",
        type=int,
        default=5000,
        help="Destination TCP port"
    )
    parser.add_argument(
        "--tensor-size",
        type=int,
        nargs=2,
        default=[100, 100],
        metavar=("ROWS", "COLS"),
        help="Tensor dimensions (rows cols)"
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of training iterations"
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Disable result verification"
    )
    
    args = parser.parse_args()
    
    try:
        trainer = MLTrainer(
            args.config,
            args.worker_node_id,
            src_port=args.src_port,
            dst_port=args.dst_port,
        )
        trainer.run_training_loop(
            tensor_size=tuple(args.tensor_size),
            iterations=args.iterations,
            verify=not args.no_verify
        )
        # Don't exit - keep nextmini node alive
        print("\n[Trainer] Training complete. Dataplane node remains active.")
        print("[Trainer] Press Ctrl+C to exit.")
        # Keep the process alive
        import signal
        signal.pause()
    except KeyboardInterrupt:
        print("\n[Trainer] Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n[Trainer] ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

