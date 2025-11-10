#!/usr/bin/env python3
"""Multicast docker helper with built-in reliability/flow control.

This version removes the Postgres dependency by keeping all coordination
between the source and receivers inside the dataplane:

* The source fragments tensors, tags each chunk with a compact header, and
  maintains a bounded in-flight window entirely in memory.
* Receivers send READY / ACK / REPAIR control messages directly to the source
  via the PyO3 bindings, so the sender knows when every chunk has landed and
  when to retransmit.
* Group metadata (IDs, tensor sizes) is exchanged through the shared
  artifact directory so containers stay in sync without touching the DB.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import struct
import sys
import time
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set, Tuple

try:
    import nextmini_py as nm
    from nextmini_py import FrozenBuffer
except ImportError as exc:  # pragma: no cover - surfaced at launch time
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with `maturin build`."
    ) from exc

METADATA_FILE = "tensor-metadata.json"
GROUP_INFO_FILE = "group-info.json"
CONTROL_STRUCT = struct.Struct("!BII")
CTRL_READY = 1
CTRL_ACK = 2
CTRL_REPAIR = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reliable multicast demo")
    parser.add_argument("--role", choices=("source", "receiver"), required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--group-label", required=True)
    parser.add_argument("--chunk-size", type=int, default=32768)
    parser.add_argument("--payload-count", type=int, default=8)
    parser.add_argument("--expected-bytes", type=int, default=None)
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--receive-timeout-ms", type=int, default=5000)
    parser.add_argument("--source-node-id", type=int, default=1)
    parser.add_argument("--node-id", type=int, default=None)
    parser.add_argument("--src-port", type=int, default=None)
    parser.add_argument("--dst-port", type=int, default=None)
    parser.add_argument(
        "--receiver-ids",
        type=str,
        default="",
        help="Comma-separated list of receiver node IDs (source role only).",
    )
    parser.add_argument(
        "--tensor-path",
        type=Path,
        default=None,
        help="Optional tensor path to stream; defaults to generated tensor.",
    )
    parser.add_argument(
        "--checksum-path",
        type=Path,
        default=None,
        help="Override auto-derived checksum location in artifact dir.",
    )
    parser.add_argument("--generate-tensor", action="store_true")
    parser.add_argument("--artifact-dir", type=Path, default=Path("/artifacts"))
    parser.add_argument("--sink-path", type=Path, default=None)
    parser.add_argument("--verify-checksum", action="store_true")
    parser.add_argument("--flow-window", type=int, default=256)
    parser.add_argument("--flow-poll-ms", type=int, default=100)
    parser.add_argument("--group-timeout", type=int, default=90)
    parser.add_argument("--member-timeout", type=int, default=60)
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def parse_receiver_ids(value: str) -> List[int]:
    if not value:
        return []
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def log(message: str, quiet: bool = False) -> None:
    if quiet:
        return
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


def chunk_count(total_bytes: int, chunk_size: int) -> int:
    if total_bytes <= 0:
        return 0
    return math.ceil(total_bytes / chunk_size)


def encode_chunk(chunk_index: int, chunk: bytes) -> bytes:
    header = struct.pack("!QI", chunk_index, len(chunk))
    return header + chunk


def decode_chunk(payload: bytes) -> Tuple[int, bytes]:
    if len(payload) < 12:
        raise ValueError("payload shorter than chunk header")
    chunk_index, length = struct.unpack("!QI", payload[:12])
    start = 12
    end = start + length
    if end > len(payload):
        raise ValueError(
            f"chunk {chunk_index} truncated (expected {length}, saw {len(payload) - 12})"
        )
    return int(chunk_index), payload[start:end]


def control_payload(kind: int, node_id: int, chunk_index: int = 0) -> bytes:
    return CONTROL_STRUCT.pack(kind, node_id, chunk_index)


def decode_control(payload: bytes) -> Tuple[int, int, int]:
    if len(payload) < CONTROL_STRUCT.size:
        raise ValueError("control payload too short")
    return CONTROL_STRUCT.unpack(payload[: CONTROL_STRUCT.size])


def stream_tensor_chunks(path: Path, chunk_size: int) -> Iterator[bytes]:
    if chunk_size <= 0:
        raise ValueError("chunk-size must be positive")
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            yield chunk


def resolve_checksum_path(args: argparse.Namespace) -> Path:
    if args.checksum_path:
        return args.checksum_path
    return args.artifact_dir / f"{args.group_label}.sha256"


def metadata_path(args: argparse.Namespace) -> Path:
    return args.artifact_dir / METADATA_FILE


def write_tensor_metadata(args: argparse.Namespace, tensor_path: Path, size: int) -> None:
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    payload = {"path": str(tensor_path), "bytes": size}
    metadata_path(args).write_text(json.dumps(payload))


def load_tensor_metadata_if_needed(args: argparse.Namespace) -> None:
    if args.tensor_path is not None and args.expected_bytes is not None:
        return
    path = metadata_path(args)
    deadline = time.monotonic() + args.group_timeout
    while time.monotonic() < deadline:
        if path.exists():
            data = json.loads(path.read_text())
            if args.tensor_path is None:
                args.tensor_path = Path(data["path"])
            if args.expected_bytes is None:
                args.expected_bytes = int(data["bytes"])
            return
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for tensor metadata at {path}.")


def group_info_path(args: argparse.Namespace) -> Path:
    return args.artifact_dir / GROUP_INFO_FILE


def write_group_info(
    args: argparse.Namespace, *, group_id: int, group_ip: str, receiver_ids: List[int]
) -> None:
    payload = {
        "label": args.group_label,
        "group_id": group_id,
        "group_ip": group_ip,
        "receiver_ids": receiver_ids,
    }
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    group_info_path(args).write_text(json.dumps(payload))


def wait_for_group_info(args: argparse.Namespace, timeout: int) -> Tuple[int, str]:
    path = group_info_path(args)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            data = json.loads(path.read_text())
            return int(data["group_id"]), data["group_ip"]
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for group info file at {path}.")


def send_control_message(
    dataplane: nm.Dataplane,
    dst_node_id: int,
    *,
    local_node_id: int,
    kind: int,
    chunk_index: int = 0,
    src_port: Optional[int],
    dst_port: Optional[int],
) -> None:
    payload = control_payload(kind, local_node_id, chunk_index)
    dataplane.send_to_node(
        dst_node_id,
        FrozenBuffer(payload),
        src_port=src_port,
        dst_port=dst_port,
    )


def generate_tensor_if_needed(args: argparse.Namespace) -> None:
    if not args.generate_tensor:
        return
    if args.tensor_path is None:
        tensor_dir = Path("/workspace/tensors")
        tensor_dir.mkdir(parents=True, exist_ok=True)
        args.tensor_path = tensor_dir / "tensor-auto-1g.pt"
    import torch  # Imported lazily to keep startup light

    log(f"Generating ~1GB tensor at {args.tensor_path}...", args.quiet)
    torch.manual_seed(42)
    tensor = torch.randn(256, 1024, 1024, dtype=torch.float32).contiguous().cpu()
    args.tensor_path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(tensor, args.tensor_path)
    size = args.tensor_path.stat().st_size
    log(f"Generated tensor ({size} bytes).", args.quiet)
    args.expected_bytes = size
    write_tensor_metadata(args, args.tensor_path, size)


def poll_control_receivers(
    receivers: Dict[int, "nm.PacketReceiver"], timeout_ms: int
) -> List[Tuple[int, int, int]]:
    events = []
    for ctrl in receivers.values():
        delivery = ctrl.recv(timeout_ms=timeout_ms)
        while delivery is not None:
            try:
                events.append(decode_control(delivery.payload))
            except ValueError as exc:
                log(f"WARN: failed to decode control payload: {exc}", quiet=False)
            delivery = ctrl.recv(timeout_ms=0)
    return events


def run_source(args: argparse.Namespace) -> None:
    receiver_ids = parse_receiver_ids(args.receiver_ids)
    if not receiver_ids:
        raise SystemExit("Source role requires --receiver-ids=<id1,id2,...>.")

    dataplane = nm.Dataplane(str(args.config))
    log(f"Creating multicast group '{args.group_label}'...", args.quiet)
    dataplane.create_group(args.group_label)
    group_id, group_ip, _ = dataplane.group_is_ready(
        timeout_ms=max(args.group_timeout, 1) * 1000
    )
    write_group_info(args, group_id=group_id, group_ip=group_ip, receiver_ids=receiver_ids)
    log(f"Controller assigned group {group_id} ({group_ip}).", args.quiet)

    tensor_mode = args.tensor_path is not None
    total_bytes = args.expected_bytes
    if tensor_mode:
        if not args.tensor_path.exists():
            raise SystemExit(f"Tensor file {args.tensor_path} does not exist")
        file_size = args.tensor_path.stat().st_size
        total_bytes = total_bytes or file_size
        if total_bytes <= 0:
            raise SystemExit("Tensor file is empty; nothing to transmit.")
        payload_goal = chunk_count(total_bytes, args.chunk_size)
        write_tensor_metadata(args, args.tensor_path, total_bytes)
    else:
        payload_goal = args.payload_count

    control_receivers = {
        node_id: dataplane.register_receiver_from_node(
            node_id,
            src_port=args.src_port,
            dst_port=args.dst_port,
            payload_only=True,
        )
        for node_id in receiver_ids
    }

    ready_nodes: Set[int] = set()
    inflight: Dict[int, Set[int]] = {}
    chunk_cache: Dict[int, bytes] = {}
    resend_queue: Set[int] = set()
    chunk_window = max(1, args.flow_window)
    poll_interval = max(1, args.flow_poll_ms)
    delay = args.sleep_ms / 1000 if args.sleep_ms else 0.0
    sha = hashlib.sha256() if tensor_mode and args.verify_checksum else None
    bytes_sent = 0

    def flush_control(timeout_ms: int) -> None:
        for kind, node_id, chunk_index in poll_control_receivers(control_receivers, timeout_ms):
            if kind == CTRL_READY:
                ready_nodes.add(node_id)
            elif kind == CTRL_ACK:
                entry = inflight.get(chunk_index)
                if entry is not None:
                    entry.add(node_id)
                    if len(entry) == len(receiver_ids):
                        inflight.pop(chunk_index, None)
                        chunk_cache.pop(chunk_index, None)
            elif kind == CTRL_REPAIR:
                if chunk_index in chunk_cache:
                    resend_queue.add(chunk_index)

    def wait_for_window() -> None:
        while len(inflight) >= chunk_window:
            flush_control(poll_interval)
            process_resends()

    def process_resends() -> None:
        while resend_queue:
            chunk_index = resend_queue.pop()
            payload = chunk_cache.get(chunk_index)
            if payload is None:
                continue
            dataplane.send_to_ip(
                group_ip,
                FrozenBuffer(payload),
                src_port=args.src_port,
                dst_port=args.dst_port,
            )
            log(f"Resent chunk {chunk_index} via multicast.", args.quiet)

    def send_chunk(chunk_index: int, raw_chunk: bytes) -> None:
        nonlocal bytes_sent
        wait_for_window()
        encoded = encode_chunk(chunk_index, raw_chunk)
        dataplane.send_to_ip(
            group_ip,
            FrozenBuffer(encoded),
            src_port=args.src_port,
            dst_port=args.dst_port,
        )
        inflight[chunk_index] = set()
        chunk_cache[chunk_index] = encoded
        bytes_sent += len(raw_chunk)
        if sha:
            sha.update(raw_chunk)
        log(
            f"[{chunk_index}/{payload_goal}] sent {len(raw_chunk)} bytes to {group_ip}",
            args.quiet,
        )
        if delay:
            time.sleep(delay)
        flush_control(0)
        process_resends()

    log("Waiting for receivers to report ready state...", args.quiet)
    while ready_nodes != set(receiver_ids):
        flush_control(poll_interval)
        time.sleep(0.05)

    if tensor_mode:
        for chunk_index, chunk in enumerate(
            stream_tensor_chunks(args.tensor_path, args.chunk_size), start=1
        ):
            if chunk_index > payload_goal:
                break
            send_chunk(chunk_index, chunk)
        if payload_goal == 0:
            log("WARN: tensor payload goal is zero.", args.quiet)
    else:
        for chunk_index in range(1, payload_goal + 1):
            send_chunk(chunk_index, os.urandom(args.chunk_size))

    while inflight:
        flush_control(poll_interval)
        process_resends()
        time.sleep(0.05)

    if sha:
        checksum_path = resolve_checksum_path(args)
        checksum_path.parent.mkdir(parents=True, exist_ok=True)
        checksum_path.write_text(sha.hexdigest() + "\n")
        log(f"Wrote checksum to {checksum_path}", args.quiet)
    if tensor_mode:
        log(f"Source streamed {bytes_sent} bytes from {args.tensor_path}", args.quiet)
    log("Source finished sending multicast payloads.", args.quiet)


def run_receiver(args: argparse.Namespace) -> None:
    if args.node_id is None:
        raise SystemExit("Receiver role requires --node-id.")

    load_tensor_metadata_if_needed(args)
    group_id, group_ip = wait_for_group_info(args, args.group_timeout)
    dataplane = nm.Dataplane(str(args.config))
    log(f"Joining multicast group id={group_id} ({group_ip})...", args.quiet)
    dataplane.join_group(group_id)

    log("Waiting for local membership confirmation...", args.quiet)
    if not dataplane.wait_for_local_membership(
        group_id, timeout_ms=args.member_timeout * 1000
    ):
        raise TimeoutError("Timed out waiting for LocalMemberJoined event.")

    routes = dataplane.wait_for_routes_installed(
        group_id,
        src_node_id=args.source_node_id,
        timeout_ms=args.member_timeout * 1000,
    )
    if routes is None:
        raise TimeoutError("Timed out waiting for GroupRoutesInstalled event.")

    log("Routes installed; notifying source that receiver is ready.", args.quiet)
    send_control_message(
        dataplane,
        args.source_node_id,
        local_node_id=args.node_id,
        kind=CTRL_READY,
        src_port=args.src_port,
        dst_port=args.dst_port,
    )

    expected_chunks = args.payload_count
    if args.expected_bytes:
        expected_chunks = chunk_count(args.expected_bytes, args.chunk_size)
        if expected_chunks == 0:
            raise SystemExit("expected-bytes must be positive when provided.")

    receiver = dataplane.register_receiver_for_group(
        src_node_id=args.source_node_id,
        group_ip=group_ip,
        src_port=args.src_port,
        dst_port=args.dst_port,
        payload_only=True,
    )

    sink_path = args.sink_path
    if sink_path is None and args.artifact_dir:
        suffix = args.node_id if args.node_id is not None else "receiver"
        sink_path = args.artifact_dir / f"receiver-{suffix}.bin"
    sink_file = None
    if sink_path:
        sink_path.parent.mkdir(parents=True, exist_ok=True)
        sink_file = sink_path.open("wb")
    sha = hashlib.sha256() if args.verify_checksum else None

    pending_chunks: Dict[int, bytes] = {}
    expected_chunk = 1
    total_bytes = 0

    def flush_ready_chunks() -> None:
        nonlocal expected_chunk, total_bytes
        while expected_chunk in pending_chunks:
            chunk_bytes = pending_chunks.pop(expected_chunk)
            if sink_file:
                sink_file.write(chunk_bytes)
            if sha:
                sha.update(chunk_bytes)
            total_bytes += len(chunk_bytes)
            log(
                f"[{expected_chunk}/{expected_chunks}] committed {len(chunk_bytes)} bytes.",
                args.quiet,
            )
            expected_chunk += 1

    while expected_chunk <= expected_chunks:
        delivery = receiver.recv(timeout_ms=args.receive_timeout_ms)
        if delivery is None:
            send_control_message(
                dataplane,
                args.source_node_id,
                local_node_id=args.node_id,
                kind=CTRL_REPAIR,
                chunk_index=expected_chunk,
                src_port=args.src_port,
                dst_port=args.dst_port,
            )
            log(
                f"Timeout waiting for chunk {expected_chunk}; requested repair.",
                args.quiet,
            )
            continue

        try:
            chunk_index, chunk_bytes = decode_chunk(delivery.payload)
        except ValueError as exc:
            log(f"WARN: receiver failed to decode chunk: {exc}", args.quiet)
            continue

        send_control_message(
            dataplane,
            args.source_node_id,
            local_node_id=args.node_id,
            kind=CTRL_ACK,
            chunk_index=chunk_index,
            src_port=args.src_port,
            dst_port=args.dst_port,
        )

        if chunk_index < expected_chunk:
            continue
        if chunk_index > expected_chunks:
            log(
                f"WARN: received chunk {chunk_index} beyond target {expected_chunks}; skipping.",
                args.quiet,
            )
            continue

        pending_chunks[chunk_index] = chunk_bytes
        flush_ready_chunks()

    if sink_file:
        sink_file.flush()
        sink_file.close()
        log(f"Wrote reconstructed tensor to {sink_path} ({total_bytes} bytes)", args.quiet)

    if sha:
        checksum_path = resolve_checksum_path(args)
        if not checksum_path.exists():
            raise TimeoutError(f"Checksum file {checksum_path} missing.")
        expected_digest = checksum_path.read_text().strip()
        digest = sha.hexdigest()
        if digest != expected_digest:
            raise RuntimeError(
                f"Checksum mismatch: expected {expected_digest}, received {digest}."
            )
        log("Checksum verified successfully.", args.quiet)

    log("Receiver observed all expected multicast payloads.", args.quiet)


def main() -> int:
    args = parse_args()
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be positive.")
    if args.role == "source" and args.tensor_path is None:
        args.generate_tensor = True
    if args.artifact_dir:
        args.artifact_dir.mkdir(parents=True, exist_ok=True)

    try:
        if args.role == "source":
            generate_tensor_if_needed(args)
            run_source(args)
        else:
            run_receiver(args)
    except TimeoutError as exc:
        log(f"ERROR: {exc}", quiet=False)
        return 1
    except Exception as exc:  # pragma: no cover
        log(f"ERROR: {exc}", quiet=False)
        return 1

    log("Task completed. Keeping container alive (Ctrl+C to exit)...", quiet=False)
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log("Received interrupt signal, exiting.", quiet=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
