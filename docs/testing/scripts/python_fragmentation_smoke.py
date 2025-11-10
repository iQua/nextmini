#!/usr/bin/env python3
"""Smoke test helper for the python fragmentation pipeline.

This script is intentionally lightweight so it can run from the docs/ tree:

1. Boots a dataplane instance via nextmini_py.Dataplane.
2. Sends an arbitrary payload to the requested destination node.
3. Optionally registers a receiver so you can verify reconstructed
   payloads on the destination.

It is meant for manual runs during development rather than automated CI.
The dataplane config *must* already have python_fragmentation_enabled=true
and the other python_fragmentation_* limits sized appropriately.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

try:
    import nextmini_py as nm
except ImportError as exc:  # pragma: no cover - utility script
    sys.stderr.write(
        "nextmini_py is not installed in this environment (did you run maturin build/develop?).\n"
    )
    raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual fragmentation smoke test")
    parser.add_argument("--config", required=True, help="Path to the node config TOML")
    parser.add_argument(
        "--dst-node-id",
        type=int,
        required=True,
        help="Numeric node id to target with send_to_node",
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=2 * 1024 * 1024,
        help="Size of the synthetic payload to send (defaults to 2 MiB)",
    )
    parser.add_argument(
        "--recv-from-node",
        type=int,
        default=None,
        help=(
            "Optional source node id to register a PacketReceiver for. Use this on the node that should receive the "
            "fragments so the script can verify the reconstructed buffer length."
        ),
    )
    parser.add_argument(
        "--recv-timeout-ms",
        type=int,
        default=5_000,
        help="Maximum time to wait for the reconstructed payload when --recv-from-node is set",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg_path = Path(args.config)
    if not cfg_path.is_file():
        sys.stderr.write(f"Config file {cfg_path} does not exist.\n")
        return 1

    print(f"[*] Booting dataplane with config {cfg_path} ...", flush=True)
    dp = nm.Dataplane(str(cfg_path))

    receiver = None
    if args.recv_from_node is not None:
        print(
            f"[*] Registering receiver for flow sourced from node {args.recv_from_node} ...",
            flush=True,
        )
        receiver = dp.register_receiver_from_node(
            src_node_id=args.recv_from_node,
            payload_only=True,
        )

    payload = os.urandom(args.payload_bytes)
    frozen = nm.FrozenBuffer(payload)

    print(
        f"[*] Sending {len(payload)} bytes to node {args.dst_node_id} (fragmentation flag should be enabled).",
        flush=True,
    )
    start = time.perf_counter()
    dp.send_to_node(args.dst_node_id, frozen)
    elapsed = time.perf_counter() - start
    print(f"[+] send_to_node completed in {elapsed:.3f}s")

    if receiver is None:
        print("[i] Receiver not requested; exiting after send.")
        return 0

    print(
        f"[*] Waiting up to {args.recv_timeout_ms} ms for the reconstructed payload ...",
        flush=True,
    )
    delivery = receiver.recv(timeout_ms=args.recv_timeout_ms)
    if not delivery:
        sys.stderr.write(
            "[-] Timed out waiting for the reconstructed payload. Ensure the destination node is running the "
            "python interface and that python_fragmentation is enabled.\n"
        )
        return 2

    if hasattr(delivery, "payload"):
        reconstructed = delivery.payload
        received_len = len(reconstructed)
        message_id = getattr(delivery, "message_id", None)
        total_len = getattr(delivery, "total_len", None)
        payload_format = getattr(delivery, "payload_format", "payload")
    else:
        reconstructed = delivery
        received_len = len(reconstructed)
        message_id = None
        total_len = None
        payload_format = "raw_packet"

    if received_len != len(payload):
        sys.stderr.write(
            f"[-] Payload length mismatch (sent {len(payload)} bytes, received {received_len} bytes).\n"
        )
        return 3

    meta = f"message_id={message_id} total_len={total_len} format={payload_format}"
    print(
        f"[+] Successfully received {received_len} bytes ({meta}). Fragmentation pipeline looks healthy!\n"
        "[i] If you enabled python_fragmentation_trace_flow_events, inspect controller logs for any "
        "PythonFragmentEvents warnings to confirm there were no drops."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
