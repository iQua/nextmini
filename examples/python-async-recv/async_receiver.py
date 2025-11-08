#!/usr/bin/env python3
"""
Async consumer for nextmini_py PacketReceiver.

This example awaits `recv_async()` inside `asyncio.run()` so developers
can see how to integrate the Python dataplane bindings with cooperative
event loops. It directly exercises Validation Case #4 ("Async recv") from
`docs/testing/python_api_validation.md`, providing a runnable proof that the
Python bindings interoperate with asyncio.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import statistics
import time
from pathlib import Path
from typing import Optional

try:
    from nextmini_py import Dataplane
except ImportError as exc:  # pragma: no cover - fast failure when deps missing
    raise SystemExit(
        "nextmini_py is not installed. Build it via `maturin build --release -m python-api/Cargo.toml` "
        "and install the resulting wheel before running this example."
    ) from exc


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Await PacketReceiver.recv_async() to consume telemetry frames."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).with_name("node-receiver.toml"),
        help="Path to the receiver node config (default: node-receiver.toml).",
    )
    parser.add_argument(
        "--src-node-id",
        type=int,
        default=1,
        help="Node ID of the sender producing telemetry (default: 1).",
    )
    parser.add_argument(
        "--expected",
        type=int,
        default=20,
        help="Number of payloads to consume before exiting (default: 20).",
    )
    parser.add_argument(
        "--timeout-ms",
        type=float,
        default=2_000.0,
        help="Timeout applied to each await (default: 2000 ms).",
    )
    parser.add_argument(
        "--src-port",
        type=int,
        default=4000,
        help="Expected TCP source port for the flow (default: 4000).",
    )
    parser.add_argument(
        "--dst-port",
        type=int,
        default=5000,
        help="Receiver-side TCP port (default: 5000).",
    )
    return parser.parse_args(argv)


async def heartbeat(stop_event: asyncio.Event) -> None:
    """Emit a short heartbeat so it is obvious the loop stays responsive."""
    while not stop_event.is_set():
        await asyncio.sleep(1.0)
        print("[receiver] ...still waiting for telemetry (async loop alive)")


async def consume(args: argparse.Namespace) -> None:
    dataplane = Dataplane(str(args.config))
    receiver = dataplane.register_receiver_from_node(
        src_node_id=args.src_node_id,
        src_port=args.src_port,
        dst_port=args.dst_port,
    )

    timeout = max(args.timeout_ms / 1000.0, 0.001)
    received: list[float] = []
    stop_event = asyncio.Event()
    # Keep a secondary coroutine alive so we can visually confirm the asyncio scheduler continues
    # to run other tasks while `recv_async` waits for dataplane packets.
    hb_task = asyncio.create_task(heartbeat(stop_event))

    print(
        f"[receiver] Awaiting recv_async() for up to {args.expected} payload(s) "
        f"(src_node={args.src_node_id}, flow {args.src_port}->{args.dst_port})"
    )
    start = time.time()

    try:
        while len(received) < args.expected:
            try:
                payload = await asyncio.wait_for(receiver.recv_async(), timeout=timeout)
            except asyncio.TimeoutError:
                # Deliberately log timeouts to prove the coroutine wakes up and the heartbeat keeps
                # running even when no packets arrive (critical for async correctness testing).
                print("[receiver] timeout waiting for payload; loop is still responsive")
                continue

            if payload is None:
                # Nothing ready yet, keep polling.
                await asyncio.sleep(0)
                continue

            record = json.loads(payload.decode("utf-8"))
            received.append(record["loss"])
            print(
                f"[receiver] step={record['step']:03d} "
                f"loss={record['loss']:.4f} "
                f"delay={(time.time() - record['timestamp']):.3f}s"
            )
    finally:
        stop_event.set()
        hb_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await hb_task

    duration = time.time() - start
    mean_loss = statistics.mean(received) if received else float("nan")
    # This summary acts as the observable result for Validation Case #4: if the async path
    # misbehaves, we either never hit this block or the counts/latencies will expose it.
    print(
        f"[receiver] completed {len(received)} payload(s) in {duration:.2f}s "
        f"(mean loss={mean_loss:.4f})"
    )


async def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    await consume(args)


if __name__ == "__main__":
    asyncio.run(main())
