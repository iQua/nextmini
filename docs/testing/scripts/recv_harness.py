#!/usr/bin/env python3
"""
Async receiver harness for the Nextmini Python dataplane API.

This helper spins up an in-process dataplane via `nextmini_py.Dataplane`,
registers a `PacketReceiver`, and writes inbound payloads to disk (optional)
while reporting basic throughput statistics.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

try:
    import nextmini_py as nm
except ImportError as exc:  # pragma: no cover - surfaced at launch time
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with `maturin build` and "
        "install it into the active virtualenv before running the harness."
    ) from exc


def _positive_int(value: str) -> int:
    intval = int(value)
    if intval <= 0:
        raise argparse.ArgumentTypeError("value must be > 0")
    return intval


@dataclass
class PacketRecord:
    index: int
    size: int
    received_at: float
    output_path: Optional[str]


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Receive payloads via nextmini_py and optionally persist them to disk."
    )
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="Local dataplane config TOML (same format used by the Rust binary).",
    )
    parser.add_argument(
        "--src-node-id",
        type=_positive_int,
        required=True,
        help="Node ID of the expected sender.",
    )
    parser.add_argument(
        "--src-port",
        type=_positive_int,
        default=None,
        help="Optional TCP source port override (defaults to config user-space client port).",
    )
    parser.add_argument(
        "--dst-port",
        type=_positive_int,
        default=None,
        help="Optional TCP destination port override (defaults to config user-space server port).",
    )
    parser.add_argument(
        "--timeout-ms",
        type=_positive_int,
        default=5000,
        help="Timeout for awaiting each payload (milliseconds).",
    )
    parser.add_argument(
        "--expected",
        type=_positive_int,
        default=None,
        help="Stop after this many payloads (defaults to run-until-timeout).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to write payload_<index>.bin files for each received payload.",
    )
    parser.add_argument(
        "--summary-json",
        type=Path,
        default=None,
        help="Optional path to write a JSON summary (counts, bytes, timestamps).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-payload logging (only final summary will be printed).",
    )
    return parser.parse_args(argv)


async def receive_loop(args: argparse.Namespace) -> dict:
    start = time.time()
    dataplane = nm.Dataplane(str(args.config))
    receiver = dataplane.register_receiver_from_node(
        src_node_id=args.src_node_id,
        src_port=args.src_port,
        dst_port=args.dst_port,
    )

    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)

    received: list[PacketRecord] = []
    terminating = False
    timeout = args.timeout_ms / 1000

    async def _recv_once() -> Optional[bytes]:
        try:
            delivery = await asyncio.wait_for(receiver.recv_async(), timeout=timeout)
            # Extract payload from PayloadDelivery object
            return delivery.payload if delivery else None
        except asyncio.TimeoutError:
            return None

    while not terminating:
        payload = await _recv_once()
        if payload is None:
            if args.expected is None:
                # Treat timeout as end-of-stream only if no payloads were seen yet.
                if not received:
                    continue
                terminating = True
                break
            terminating = True
            break

        output_path = None
        if args.output_dir:
            output_path = args.output_dir / f"payload_{len(received):05d}.bin"
            output_path.write_bytes(payload)

        record = PacketRecord(
            index=len(received),
            size=len(payload),
            received_at=time.time(),
            output_path=str(output_path) if output_path else None,
        )
        received.append(record)

        if not args.quiet:
            elapsed = record.received_at - start
            print(
                f"[{elapsed:8.3f}s] received {record.size} bytes "
                f"(total packets={len(received)})",
                flush=True,
            )

        if args.expected is not None and len(received) >= args.expected:
            terminating = True

    duration = max(time.time() - start, 1e-6)
    total_bytes = sum(rec.size for rec in received)
    summary = {
        "packets": len(received),
        "total_bytes": total_bytes,
        "duration_seconds": duration,
        "throughput_bps": total_bytes * 8 / duration,
        "records": [asdict(rec) for rec in received],
        "config_path": str(args.config),
        "src_node_id": args.src_node_id,
        "src_port": args.src_port,
        "dst_port": args.dst_port,
        "timeout_ms": args.timeout_ms,
    }

    print(
        f"Received {summary['packets']} packets "
        f"({summary['total_bytes']} bytes) in {duration:.3f}s "
        f"({summary['throughput_bps'] / 1e6:.2f} Mbps).",
        flush=True,
    )

    if args.summary_json:
        args.summary_json.parent.mkdir(parents=True, exist_ok=True)
        args.summary_json.write_text(json.dumps(summary, indent=2))

    return summary


def install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, loop.stop)
        except NotImplementedError:  # pragma: no cover - Windows
            signal.signal(sig, lambda _sig, _frame: loop.stop())


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    install_signal_handlers(loop)

    try:
        summary = loop.run_until_complete(receive_loop(args))
    finally:
        loop.close()

    return 0 if summary["packets"] > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
