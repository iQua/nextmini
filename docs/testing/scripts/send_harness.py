#!/usr/bin/env python3
"""
Simple sender harness for exercising the Nextmini Python dataplane API.

The script bootstraps an in-process dataplane and repeatedly invokes
`Dataplane.send_to_node` (or batched sends) using configurable payload sizes.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Iterable, Optional

try:
    import nextmini_py as nm
except ImportError as exc:  # pragma: no cover - surfaced at launch time
    raise SystemExit(
        "nextmini_py is not installed. Build/install the wheel before running the harness."
    ) from exc


def _positive_int(value: str) -> int:
    intval = int(value)
    if intval <= 0:
        raise argparse.ArgumentTypeError("value must be > 0")
    return intval


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inject payloads into the dataplane via nextmini_py."
    )
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="Local dataplane config TOML (same format used by the Rust binary).",
    )
    parser.add_argument(
        "--dst-node-id",
        type=_positive_int,
        required=True,
        help="Destination node ID.",
    )
    parser.add_argument(
        "--count",
        type=_positive_int,
        default=1,
        help="Number of payloads to send.",
    )
    parser.add_argument(
        "--batch",
        type=_positive_int,
        default=1,
        help="Bundle this many payloads per `send_batch_to_node` call.",
    )
    parser.add_argument(
        "--size",
        type=_positive_int,
        default=1024,
        help="Payload size in bytes (ignored when --payload-file is provided).",
    )
    parser.add_argument(
        "--pattern",
        choices=("zeros", "ones", "random"),
        default="random",
        help="Payload pattern to generate when no payload file is supplied.",
    )
    parser.add_argument(
        "--payload-file",
        type=Path,
        default=None,
        help="File whose contents should be sent for each payload.",
    )
    parser.add_argument(
        "--src-node-id",
        type=_positive_int,
        default=None,
        help="Optional source node ID to use when computing the flow ID (defaults to config).",
    )
    parser.add_argument(
        "--src-port",
        type=_positive_int,
        default=None,
        help="Optional TCP source port override.",
    )
    parser.add_argument(
        "--dst-port",
        type=_positive_int,
        default=None,
        help="Optional TCP destination port override.",
    )
    parser.add_argument(
        "--sleep-ms",
        type=_positive_int,
        default=0,
        help="Sleep between payloads (milliseconds).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-payload logging.",
    )
    return parser.parse_args(argv)


def make_payload(args: argparse.Namespace) -> bytes:
    if args.payload_file:
        return args.payload_file.read_bytes()

    if args.pattern == "random":
        return os.urandom(args.size)

    fill_byte = 0x00 if args.pattern == "zeros" else 0x01
    return bytes([fill_byte]) * args.size


def emit_batch(dataplane: nm.Dataplane, dst_node: int, payloads: Iterable[memoryview], args: argparse.Namespace) -> None:
    dataplane.send_batch_to_node(
        dst_node,
        list(payloads),
        src_port=args.src_port,
        dst_port=args.dst_port,
    )


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    payload = make_payload(args)

    dataplane = nm.Dataplane(str(args.config))
    flow_id = None
    if args.src_node_id is not None:
        flow_id = dataplane.flow_id_from_nodes(
            src_node_id=args.src_node_id,
            dst_node_id=args.dst_node_id,
            src_port=args.src_port,
            dst_port=args.dst_port,
        )

    batch_payload = [memoryview(payload) for _ in range(args.batch)]
    start = time.time()
    delay = args.sleep_ms / 1000 if args.sleep_ms else 0.0

    total_sent = 0
    for idx in range(args.count):
        if args.batch == 1:
            dataplane.send_to_node(
                args.dst_node_id,
                memoryview(payload),
                src_port=args.src_port,
                dst_port=args.dst_port,
            )
        else:
            emit_batch(dataplane, args.dst_node_id, batch_payload, args)

        total_sent += args.batch
        if not args.quiet:
            log_msg = f"[{idx + 1}/{args.count}] sent {args.batch} payload(s) ({len(payload)} bytes each)"
            if flow_id is not None:
                log_msg += f" flow_id={flow_id}"
            print(log_msg, flush=True)
        if delay:
            time.sleep(delay)

    elapsed = max(time.time() - start, 1e-6)
    total_bytes = total_sent * len(payload)
    print(
        f"Sent {total_sent} payload(s) totaling {total_bytes} bytes in {elapsed:.3f}s "
        f"({total_bytes * 8 / elapsed / 1e6:.2f} Mbps).",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
