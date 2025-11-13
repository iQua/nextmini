#!/usr/bin/env python3
"""Multicast docker helper backed by Rust-side reliability primitives."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import List, Tuple

try:
    import nextmini_py as nm
except ImportError as exc:  # pragma: no cover - surfaced at launch time
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with `maturin build`."
    ) from exc


METADATA_FILE = "tensor-metadata.json"
GROUP_INFO_FILE = "group-info.json"


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_text(json.dumps(payload))
    tmp.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reliable multicast demo")
    parser.add_argument("--role", choices=("source", "receiver"), required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--group-label", required=True)
    parser.add_argument("--chunk-size", type=int, default=4096)
    parser.add_argument("--flow-window", type=int, default=256)
    parser.add_argument("--flow-poll-ms", type=int, default=100)
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--receive-timeout-ms", type=int, default=5000)
    parser.add_argument("--group-timeout", type=int, default=90)
    parser.add_argument("--member-timeout", type=int, default=60)
    parser.add_argument(
        "--payload-count",
        type=int,
        default=None,
        help="Compatibility shim; when provided, expected-bytes defaults to payload-count * chunk-size.",
    )
    parser.add_argument("--expected-bytes", type=int, default=None)
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
    parser.add_argument("--generate-tensor", action="store_true")
    parser.add_argument("--artifact-dir", type=Path, default=Path("/artifacts"))
    parser.add_argument("--sink-path", type=Path, default=None)
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


def metadata_path(args: argparse.Namespace) -> Path:
    return args.artifact_dir / METADATA_FILE


def write_tensor_metadata(
    args: argparse.Namespace, tensor_path: Path, size: int
) -> None:
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    payload = {"path": str(tensor_path), "bytes": size}
    metadata_path(args).write_text(json.dumps(payload))
    log(
        f"Recorded tensor metadata path={tensor_path} bytes={size} at {metadata_path(args)}",
        args.quiet,
    )


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
            log(
                f"Loaded tensor metadata path={args.tensor_path} bytes={args.expected_bytes}",
                args.quiet,
            )
            return
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for tensor metadata at {path}.")


def group_info_path(args: argparse.Namespace) -> Path:
    return args.artifact_dir / GROUP_INFO_FILE


def write_group_info(
    args: argparse.Namespace,
    *,
    group_id: int,
    group_ip: str,
    receiver_ids: List[int],
) -> None:
    payload = {
        "label": args.group_label,
        "group_id": group_id,
        "group_ip": group_ip,
        "receiver_ids": receiver_ids,
        "source_node_id": args.source_node_id,
    }
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(group_info_path(args), payload)


def wait_for_group_info(
    args: argparse.Namespace, timeout: int
) -> Tuple[int, str]:
    path = group_info_path(args)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            data = json.loads(path.read_text())
            return int(data["group_id"]), data["group_ip"]
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for group info file at {path}.")


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
    # Generate ~1GB tensor: 256 * 1024 * 1024 floats * 4 bytes/float ≈ 1GB
    tensor = (
        torch.randn(256, 1024, 1024, dtype=torch.float32).contiguous().cpu()
    )

    # For 1MB testing, uncomment the following lines and comment out the 1GB version above:
    # args.tensor_path = tensor_dir / "tensor-auto-1m.pt"
    # log(f"Generating ~1MB tensor at {args.tensor_path}...", args.quiet)
    # # Generate ~1MB tensor: 256 * 1024 floats * 4 bytes/float ≈ 1MB
    # tensor = torch.randn(256, 1024, dtype=torch.float32).contiguous().cpu()

    args.tensor_path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(tensor, args.tensor_path)
    size = args.tensor_path.stat().st_size
    log(f"Generated tensor ({size} bytes).", args.quiet)
    args.expected_bytes = size
    write_tensor_metadata(args, args.tensor_path, size)


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
    write_group_info(
        args,
        group_id=group_id,
        group_ip=group_ip,
        receiver_ids=receiver_ids,
    )
    log(f"Controller assigned group {group_id} ({group_ip}).", args.quiet)
    log(
        f"Receiver IDs={receiver_ids} chunk_size={args.chunk_size} "
        f"flow_window={args.flow_window} flow_poll_ms={args.flow_poll_ms}",
        args.quiet,
    )

    if args.tensor_path is None:
        raise SystemExit(
            "Source role requires a tensor file; set --tensor-path or --generate-tensor."
        )
    if not args.tensor_path.exists():
        raise SystemExit(f"Tensor file {args.tensor_path} does not exist")

    total_bytes = args.tensor_path.stat().st_size
    if total_bytes <= 0:
        raise SystemExit("Tensor file is empty; nothing to transmit.")
    args.expected_bytes = total_bytes
    write_tensor_metadata(args, args.tensor_path, total_bytes)

    # New reliable path (feature=reliable): use thin shim; engines may be stubbed until writer lands
    sid = dataplane.send_file(
        group_ip,
        receiver_ids,
        str(args.tensor_path),
        chunk_size=args.chunk_size,
        src_port=args.src_port,
        dst_port=args.dst_port,
        ack_policy="all",
    )
    log(f"Started reliable send session sid={sid}", args.quiet)
    if hasattr(dataplane, "reliable_wait"):
        ok = dataplane.reliable_wait(sid, timeout_ms=args.group_timeout * 1000)
        log(f"Send completion: {ok}", args.quiet)
    else:
        log(
            "Dataplane lacks reliable_wait; send completion signal unavailable.",
            args.quiet,
        )
    log("Source issued reliable send request.", args.quiet)


def run_receiver(args: argparse.Namespace) -> None:
    if args.node_id is None:
        raise SystemExit("Receiver role requires --node-id.")

    load_tensor_metadata_if_needed(args)
    if args.expected_bytes is None and args.payload_count:
        args.expected_bytes = args.payload_count * args.chunk_size
    if args.expected_bytes is None or args.expected_bytes <= 0:
        raise SystemExit("expected-bytes must be known for reliable reception.")

    group_id, group_ip = wait_for_group_info(args, args.group_timeout)
    dataplane = nm.Dataplane(str(args.config))
    log(f"Joining multicast group id={group_id} ({group_ip})...", args.quiet)
    dataplane.join_group(group_id)

    sink_path = args.sink_path
    if sink_path is None and args.artifact_dir:
        suffix = args.node_id if args.node_id is not None else "receiver"
        sink_path = args.artifact_dir / f"receiver-{suffix}.bin"

    sid = dataplane.reliable_receive_file_rs(
        group_ip,
        args.source_node_id,
        expected_bytes=args.expected_bytes,
        chunk_size=args.chunk_size,
        src_port=args.src_port,
        dst_port=args.dst_port,
        sink_path=str(sink_path) if sink_path else None,
    )

    log(f"Started reliable receive session sid={sid}", args.quiet)
    if hasattr(dataplane, "reliable_wait"):
        ok = dataplane.reliable_wait(sid, timeout_ms=args.receive_timeout_ms)
        log(f"Receive completion: {ok}", args.quiet)
    else:
        log(
            "Dataplane lacks reliable_wait; receive completion signal unavailable.",
            args.quiet,
        )


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

    log(
        "Task completed. Keeping container alive (Ctrl+C to exit)...",
        quiet=False,
    )
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log("Received interrupt signal, exiting.", quiet=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
