#!/usr/bin/env python3
"""
Publisher script for the async PacketReceiver example.

Launches a Nextmini dataplane in-process via nextmini_py and streams JSON
telemetry to the destination node. Pair this with async_receiver.py to see
`send_to_node` interoperate with `recv_async`.

Even though this script is synchronous, generous logging helps developers
correlate each emitted sample with the async consumer to confirm the event
loop stays responsive.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path
from typing import Optional

try:
    from nextmini_py import Dataplane, FrozenBuffer
except ImportError as exc:  # pragma: no cover - surfaced immediately for users
    raise SystemExit(
        "nextmini_py is not installed. Build it via `maturin build --release -m python-api/Cargo.toml` "
        "and install the resulting wheel before running this example."
    ) from exc


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send JSON telemetry via nextmini_py for the async receiver example."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).with_name("node-sender.toml"),
        help="Path to the sender node config (default: node-sender.toml).",
    )
    parser.add_argument(
        "--dst-node-id",
        type=int,
        default=2,
        help="Numeric node ID of the receiver (default: 2).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=20,
        help="Number of telemetry samples to send (default: 20).",
    )
    parser.add_argument(
        "--sleep-ms",
        type=float,
        default=250.0,
        help="Delay between packets in milliseconds (default: 250).",
    )
    parser.add_argument(
        "--src-port",
        type=int,
        default=4000,
        help="Source TCP port override (default: 4000).",
    )
    parser.add_argument(
        "--dst-port",
        type=int,
        default=5000,
        help="Destination TCP port override (default: 5000).",
    )
    return parser.parse_args(argv)


def make_payload(step: int) -> tuple[bytes, float]:
    """
    Synthesize a deterministic telemetry record. Using JSON keeps the
    example dependency-free while still demonstrating structured payloads.
    """
    # Simulate a training loss curve that decays over time to make the output interesting.
    # These values mirror the "scalar telemetry" test-case in docs/testing/python_api_validation.md.
    loss = 1.5 * math.exp(-0.05 * step)
    record = {
        "step": step,
        "loss": loss,
        "timestamp": time.time(),
    }
    return json.dumps(record).encode("utf-8"), loss


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    dataplane = Dataplane(str(args.config))

    interval = max(args.sleep_ms / 1000.0, 0.0)
    print(
        f"[sender] Streaming {args.count} records to node {args.dst_node_id} "
        f"(src_port={args.src_port}, dst_port={args.dst_port})"
    )

    for step in range(args.count):
        payload, loss = make_payload(step)
        frozen = FrozenBuffer(payload)
        dataplane.send_to_node(
            args.dst_node_id,
            frozen,
            src_port=args.src_port,
            dst_port=args.dst_port,
        )
        print(f"[sender] step={step:03d} loss={loss:.4f}")
        if interval:
            time.sleep(interval)

    print("[sender] Done. Dataplane will keep running until the process exits.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
