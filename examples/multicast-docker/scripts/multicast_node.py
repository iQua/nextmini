#!/usr/bin/env python3
"""Utility entrypoint for the multicast docker example.

Each container boots an in-process dataplane via nextmini_py, coordinates
with Postgres to discover the multicast group metadata, and either sends
or receives multicast payloads depending on the assigned role.
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
from typing import Dict, Iterator, List, Tuple

import psycopg

try:
    import nextmini_py as nm
    from nextmini_py import FrozenBuffer
except ImportError as exc:  # pragma: no cover - surfaced at launch time
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with `maturin build` (the "
        "run_multicast_node.sh helper bootstraps it automatically inside the container)."
    ) from exc

METADATA_FILE = "tensor-metadata.json"
CHUNK_HEADER = struct.Struct("!QI")
CHUNK_HEADER_LEN = CHUNK_HEADER.size


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drive the multicast docker example.")
    parser.add_argument("--role", choices=("source", "receiver"), required=True)
    parser.add_argument(
        "--config", type=Path, required=True, help="Dataplane config TOML."
    )
    parser.add_argument(
        "--group-label", required=True, help="Label used when creating the group."
    )
    parser.add_argument("--postgres-host", default="postgres")
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.add_argument("--postgres-user", default="pgusr")
    parser.add_argument("--postgres-password", default="pgpwrd")
    parser.add_argument("--postgres-db", default="nextmini")
    parser.add_argument(
        "--group-timeout", type=int, default=90, help="Seconds to wait for group."
    )
    parser.add_argument(
        "--member-timeout",
        type=int,
        default=60,
        help="Seconds to wait for membership propagation after join.",
    )
    parser.add_argument(
        "--payload-count",
        type=int,
        default=8,
        help="Number of payloads the source should send.",
    )
    parser.add_argument(
        "--payload-size",
        type=int,
        default=1024,
        help="Payload size in bytes for each multicast packet.",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Delay between multicast sends (source role only).",
    )
    parser.add_argument(
        "--expected",
        type=int,
        default=8,
        help="Number of payloads receivers should wait for.",
    )
    parser.add_argument(
        "--expected-bytes",
        type=int,
        default=None,
        help="Total bytes receivers should reconstruct when streaming tensors.",
    )
    parser.add_argument(
        "--receive-timeout-ms",
        type=int,
        default=5000,
        help="Timeout when awaiting each payload on receivers.",
    )
    parser.add_argument(
        "--source-node-id",
        type=int,
        default=1,
        help="Node ID of the source (used to register multicast receivers).",
    )
    parser.add_argument(
        "--node-id",
        type=int,
        default=None,
        help="Local node ID (required for receiver role).",
    )
    parser.add_argument(
        "--src-port",
        type=int,
        default=None,
        help="Optional TCP source port override.",
    )
    parser.add_argument(
        "--dst-port",
        type=int,
        default=None,
        help="Optional TCP destination port override.",
    )
    parser.add_argument(
        "--expected-subscribers",
        type=int,
        default=2,
        help="Number of receivers that must subscribe before the source starts sending.",
    )
    parser.add_argument(
        "--tensor-path",
        type=Path,
        default=None,
        help="Optional tensor file to stream instead of random payloads.",
    )
    parser.add_argument(
        "--generate-tensor",
        action="store_true",
        help="Generate a tensor file (>=1GB) inside the container before streaming.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=32768,
        help="Chunk size when splitting tensors (defaults to 32768 bytes).",
    )
    parser.add_argument(
        "--flow-window",
        type=int,
        default=256,
        help="Maximum number of in-flight chunks awaiting acknowledgements.",
    )
    parser.add_argument(
        "--flow-poll-ms",
        type=int,
        default=100,
        help="Milliseconds to wait before re-checking acknowledgements when the window is full.",
    )
    parser.add_argument(
        "--sink-path",
        type=Path,
        default=None,
        help="Where receivers should write the reconstructed tensor (optional).",
    )
    parser.add_argument(
        "--verify-checksum",
        action="store_true",
        help="Compute SHA-256 of the stream and compare with the source output.",
    )
    parser.add_argument(
        "--checksum-path",
        type=Path,
        default=None,
        help="Shared checksum file path (defaults to <artifact_dir>/<group_label>.sha256).",
    )
    parser.add_argument(
        "--checksum-wait-seconds",
        type=int,
        default=300,
        help="Seconds receivers wait for the checksum file when verify mode is enabled.",
    )
    parser.add_argument(
        "--metadata-wait-seconds",
        type=int,
        default=300,
        help="Seconds receivers wait for tensor metadata when auto-generation is enabled.",
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=Path("/artifacts"),
        help="Directory used to persist tensors/checksums between containers.",
    )
    parser.add_argument("--quiet", action="store_true", help="Reduce log noise.")
    return parser.parse_args()


def build_conninfo(args: argparse.Namespace) -> str:
    return (
        f"dbname={args.postgres_db} user={args.postgres_user} "
        f"password={args.postgres_password} host={args.postgres_host} port={args.postgres_port}"
    )


def log(message: str, quiet: bool = False) -> None:
    if quiet:
        return
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def wait_for_group(conninfo: str, label: str, timeout: int) -> Tuple[int, str]:
    deadline = time.monotonic() + timeout
    with psycopg.connect(conninfo) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            while time.monotonic() < deadline:
                cur.execute(
                    "SELECT id, group_ip FROM groups WHERE label = %s", (label,)
                )
                row = cur.fetchone()
                if row:
                    return int(row[0]), row[1]
                time.sleep(1)
    raise TimeoutError(f"Timed out waiting for multicast group '{label}'.")


def wait_for_count(
    conninfo: str,
    group_id: int,
    target: int,
    timeout: int,
    *,
    specific_node: int | None = None,
    ready_only: bool = False,
) -> None:
    deadline = time.monotonic() + timeout
    with psycopg.connect(conninfo) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            table = "group_members_ready" if ready_only else "group_members"
            while time.monotonic() < deadline:
                if specific_node is not None:
                    cur.execute(
                        f"SELECT 1 FROM {table} WHERE group_id = %s AND node_id = %s",
                        (group_id, specific_node),
                    )
                    if cur.fetchone():
                        return
                else:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE group_id = %s",
                        (group_id,),
                    )
                    count = cur.fetchone()[0]
                    if count >= target:
                        return

                time.sleep(1)

    if specific_node is not None:
        raise TimeoutError(
            f"Timed out waiting for node {specific_node} in {table} for group {group_id}."
        )
    raise TimeoutError(
        f"Timed out waiting for {target} rows in {table} for group {group_id}."
    )


def await_group_ready_via_api(
    dataplane: "nm.Dataplane", *, label: str, timeout: int, quiet: bool
) -> Tuple[int, str]:
    timeout_ms = max(timeout, 1) * 1000
    event = dataplane.group_is_ready(timeout_ms=timeout_ms)
    if event is None:
        raise TimeoutError(
            f"Timed out waiting for controller confirmation after CreateGroup '{label}'."
        )

    group_id, group_ip, src_node_id = event
    log(
        f"Controller reported group ready: id={group_id} ip={group_ip} (source node {src_node_id}).",
        quiet,
    )
    return group_id, group_ip


def ensure_ready_table(conninfo: str) -> None:
    with psycopg.connect(conninfo) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS group_members_ready (
                    group_id INT NOT NULL,
                    node_id INT NOT NULL,
                    PRIMARY KEY (group_id, node_id)
                )
                """
            )


def ensure_transfer_tables(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tensor_chunk_acks (
                group_label TEXT NOT NULL,
                chunk_index INT NOT NULL,
                receiver_node INT NOT NULL,
                acked_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (group_label, chunk_index, receiver_node)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS tensor_chunk_acks_idx
                ON tensor_chunk_acks(group_label, chunk_index)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tensor_chunk_repairs (
                group_label TEXT NOT NULL,
                chunk_index INT NOT NULL,
                receiver_node INT NOT NULL,
                requested_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (group_label, chunk_index, receiver_node)
            )
            """
        )


def ack_chunk(
    conn: psycopg.Connection, group_label: str, chunk_index: int, receiver_node: int
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tensor_chunk_acks (group_label, chunk_index, receiver_node)
            VALUES (%s, %s, %s)
            ON CONFLICT (group_label, chunk_index, receiver_node)
            DO UPDATE SET acked_at = NOW()
            """,
            (group_label, chunk_index, receiver_node),
        )


def request_repair(
    conn: psycopg.Connection, group_label: str, chunk_index: int, receiver_node: int
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tensor_chunk_repairs (group_label, chunk_index, receiver_node)
            VALUES (%s, %s, %s)
            ON CONFLICT (group_label, chunk_index, receiver_node)
            DO UPDATE SET requested_at = NOW()
            """,
            (group_label, chunk_index, receiver_node),
        )


def clear_repair_request(
    conn: psycopg.Connection, group_label: str, chunk_index: int, receiver_node: int
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM tensor_chunk_repairs
            WHERE group_label = %s AND chunk_index = %s AND receiver_node = %s
            """,
            (group_label, chunk_index, receiver_node),
        )


def _format_in_clause(values: List[int]) -> Tuple[str, List[int]]:
    placeholders = ",".join(["%s"] * len(values))
    return placeholders, values


def fetch_acknowledged_chunks(
    conn: psycopg.Connection,
    group_label: str,
    chunk_indices: List[int],
    expected_receivers: int,
) -> List[int]:
    if not chunk_indices:
        return []
    placeholders, vals = _format_in_clause(chunk_indices)
    query = f"""
        SELECT chunk_index
        FROM tensor_chunk_acks
        WHERE group_label = %s AND chunk_index IN ({placeholders})
        GROUP BY chunk_index
        HAVING COUNT(*) >= %s
    """
    with conn.cursor() as cur:
        cur.execute(query, [group_label, *vals, expected_receivers])
        rows = cur.fetchall()
    return [int(row[0]) for row in rows]


def clear_completed_chunks(
    conn: psycopg.Connection, group_label: str, chunk_indices: List[int]
) -> None:
    if not chunk_indices:
        return
    placeholders, vals = _format_in_clause(chunk_indices)
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM tensor_chunk_acks WHERE group_label = %s AND chunk_index IN ({placeholders})",
            [group_label, *vals],
        )
        cur.execute(
            f"DELETE FROM tensor_chunk_repairs WHERE group_label = %s AND chunk_index IN ({placeholders})",
            [group_label, *vals],
        )


def fetch_pending_repairs(
    conn: psycopg.Connection, group_label: str
) -> List[Tuple[int, int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT chunk_index, receiver_node
            FROM tensor_chunk_repairs
            WHERE group_label = %s
            ORDER BY requested_at
            """,
            (group_label,),
        )
        return [(int(row[0]), int(row[1])) for row in cur.fetchall()]

def chunk_count(total_bytes: int, chunk_size: int) -> int:
    if total_bytes <= 0:
        return 0
    return math.ceil(total_bytes / chunk_size)


def encode_chunk(chunk_index: int, chunk: bytes) -> bytes:
    return CHUNK_HEADER.pack(chunk_index, len(chunk)) + chunk


def decode_chunk(payload: bytes) -> Tuple[int, bytes]:
    if len(payload) < CHUNK_HEADER_LEN:
        raise ValueError("payload shorter than chunk header")
    chunk_index, length = CHUNK_HEADER.unpack(payload[:CHUNK_HEADER_LEN])
    end = CHUNK_HEADER_LEN + length
    if end > len(payload):
        raise ValueError(
            f"chunk {chunk_index} truncated (expected {length} bytes, saw {len(payload) - CHUNK_HEADER_LEN})"
        )
    return int(chunk_index), payload[CHUNK_HEADER_LEN:end]


def stream_tensor_chunks(path: Path, chunk_size: int) -> Iterator[bytes]:
    if chunk_size <= 0:
        raise ValueError("chunk-size must be positive")
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            yield chunk


def resolve_checksum_path(args: argparse.Namespace) -> Path:
    if args.checksum_path is not None:
        return args.checksum_path
    return args.artifact_dir / f"{args.group_label}.sha256"


def metadata_path(args: argparse.Namespace) -> Path:
    return args.artifact_dir / METADATA_FILE


def wait_for_checksum_file(path: Path, timeout: int) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return path.read_text().strip()
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for checksum file at {path}.")


def write_tensor_metadata(
    args: argparse.Namespace, tensor_path: Path, size: int
) -> None:
    if not args.artifact_dir:
        return
    meta = {"path": str(tensor_path), "bytes": size}
    meta_path = metadata_path(args)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta))


def load_tensor_metadata_if_needed(args: argparse.Namespace) -> None:
    if args.tensor_path is not None and args.expected_bytes is not None:
        return
    meta_path = metadata_path(args)
    deadline = time.monotonic() + args.metadata_wait_seconds
    while time.monotonic() < deadline:
        if meta_path.exists():
            data = json.loads(meta_path.read_text())
            if args.tensor_path is None:
                args.tensor_path = Path(data["path"])
            if args.expected_bytes is None:
                args.expected_bytes = int(data["bytes"])
            return
        time.sleep(1)
    raise TimeoutError(f"Timed out waiting for tensor metadata at {meta_path}.")


def generate_tensor_if_needed(args: argparse.Namespace) -> Path | None:
    if not args.generate_tensor:
        return None

    tensor_dir = (
        args.tensor_path.parent if args.tensor_path else Path("/workspace/tensors")
    )
    tensor_dir.mkdir(parents=True, exist_ok=True)
    output = tensor_dir / "tensor-auto-1g.pt"

    log(f"Generating ~1GB tensor at {output}...", args.quiet)
    try:
        import torch
    except (
        ImportError
    ) as exc:  # pragma: no cover - only hits if torch missing in container
        raise SystemExit(
            "PyTorch is required for --generate-tensor but is not installed in this container."
        ) from exc

    torch.manual_seed(42)
    # 256 * 1024 * 1024 * float32 ~= 1 GiB
    tensor = torch.randn(256, 1024, 1024, dtype=torch.float32).contiguous().cpu()
    output.parent.mkdir(parents=True, exist_ok=True)
    torch.save(tensor, output)
    size = output.stat().st_size
    log(f"Generated tensor ({size} bytes).", args.quiet)

    args.tensor_path = output
    args.expected_bytes = size
    write_tensor_metadata(args, output, size)
    return output


def mark_receiver_ready(conninfo: str, group_id: int, node_id: int) -> None:
    ensure_ready_table(conninfo)
    with psycopg.connect(conninfo) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO group_members_ready (group_id, node_id)
                VALUES (%s, %s)
                ON CONFLICT (group_id, node_id) DO NOTHING
                """,
                (group_id, node_id),
            )


def run_source(args: argparse.Namespace, conninfo: str) -> None:
    generate_tensor_if_needed(args)
    dataplane = nm.Dataplane(str(args.config))
    log(f"Requesting multicast group '{args.group_label}'...", args.quiet)
    dataplane.create_group(args.group_label)
    group_id, group_ip = await_group_ready_via_api(
        dataplane, label=args.group_label, timeout=args.group_timeout, quiet=args.quiet
    )

    ensure_ready_table(conninfo)
    wait_for_count(
        conninfo,
        group_id,
        target=args.expected_subscribers,
        timeout=args.member_timeout,
        specific_node=None,
    )
    wait_for_count(
        conninfo,
        group_id,
        target=args.expected_subscribers,
        timeout=args.member_timeout,
        ready_only=True,
    )
    log(
        f"Observed {args.expected_subscribers} ready subscriber(s); starting multicast sends.",
        args.quiet,
    )

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

    delay = args.sleep_ms / 1000 if args.sleep_ms else 0.0
    chunk_window = max(1, args.flow_window)
    poll_interval = max(1, args.flow_poll_ms) / 1000

    with psycopg.connect(conninfo) as transfer_conn:
        transfer_conn.autocommit = True
        ensure_transfer_tables(transfer_conn)

        inflight: Dict[int, int] = {}
        chunk_cache: Dict[int, bytes] = {}
        sha = hashlib.sha256() if tensor_mode and args.verify_checksum else None
        bytes_sent = 0

        def flush_acks() -> None:
            if not inflight:
                return
            acked = fetch_acknowledged_chunks(
                transfer_conn,
                args.group_label,
                sorted(inflight.keys()),
                args.expected_subscribers,
            )
            if not acked:
                return
            clear_completed_chunks(transfer_conn, args.group_label, acked)
            for idx in acked:
                inflight.pop(idx, None)
                chunk_cache.pop(idx, None)

        def handle_repairs() -> None:
            pending = fetch_pending_repairs(transfer_conn, args.group_label)
            if not pending:
                return
            for chunk_index, receiver_node in pending:
                payload = chunk_cache.get(chunk_index)
                if payload is None:
                    log(
                        f"WARN: receiver {receiver_node} requested chunk {chunk_index} but payload is no longer cached.",
                        args.quiet,
                    )
                    clear_repair_request(
                        transfer_conn, args.group_label, chunk_index, receiver_node
                    )
                    continue
                message_id = dataplane.send_to_ip(
                    group_ip,
                    FrozenBuffer(payload),
                    src_port=args.src_port,
                    dst_port=args.dst_port,
                )
                clear_repair_request(
                    transfer_conn, args.group_label, chunk_index, receiver_node
                )
                log(
                    f"Resent chunk {chunk_index} for receiver {receiver_node} (message {message_id}).",
                    args.quiet,
                )

        def wait_for_window() -> None:
            while len(inflight) >= chunk_window:
                flush_acks()
                handle_repairs()
                time.sleep(poll_interval)

        def drain_window() -> None:
            while inflight:
                flush_acks()
                handle_repairs()
                time.sleep(poll_interval)

        def send_chunk(chunk_index: int, chunk: bytes) -> None:
            nonlocal bytes_sent
            wait_for_window()
            encoded = encode_chunk(chunk_index, chunk)
            message_id = dataplane.send_to_ip(
                group_ip,
                FrozenBuffer(encoded),
                src_port=args.src_port,
                dst_port=args.dst_port,
            )
            inflight[chunk_index] = message_id
            chunk_cache[chunk_index] = encoded
            bytes_sent += len(chunk)
            if sha:
                sha.update(chunk)
            log(
                f"[{chunk_index}/{payload_goal}] sent {len(chunk)} bytes to {group_ip} (message {message_id}).",
                args.quiet,
            )
            if delay:
                time.sleep(delay)
            flush_acks()
            handle_repairs()

        if tensor_mode:
            sent_chunks = 0
            for chunk_index, chunk in enumerate(
                stream_tensor_chunks(args.tensor_path, args.chunk_size), start=1
            ):
                if chunk_index > payload_goal:
                    break
                send_chunk(chunk_index, chunk)
                sent_chunks += 1
            if sent_chunks != payload_goal:
                log(
                    f"WARN: streamed {sent_chunks} chunks but expected {payload_goal}; check chunk-size/expected-bytes",
                    args.quiet,
                )
        else:
            for chunk_index in range(1, payload_goal + 1):
                send_chunk(chunk_index, os.urandom(args.payload_size))

        drain_window()

        if sha:
            checksum_path = resolve_checksum_path(args)
            checksum_path.parent.mkdir(parents=True, exist_ok=True)
            checksum_path.write_text(sha.hexdigest() + "\n")
            log(f"Wrote checksum to {checksum_path}", args.quiet)
        if tensor_mode:
            log(f"Source streamed {bytes_sent} bytes from {args.tensor_path}", args.quiet)

    log("Source finished sending multicast payloads.", args.quiet)


def run_receiver(args: argparse.Namespace, conninfo: str) -> None:
    if args.node_id is None:
        raise SystemExit("Receiver role requires --node-id.")

    load_tensor_metadata_if_needed(args)

    dataplane = nm.Dataplane(str(args.config))
    group_id, group_ip = wait_for_group(conninfo, args.group_label, args.group_timeout)
    log(f"Joining multicast group id={group_id} ({group_ip})...", args.quiet)
    dataplane.join_group(group_id)
    wait_for_count(
        conninfo,
        group_id,
        target=0,
        timeout=args.member_timeout,
        specific_node=args.node_id,
    )
    log("Controller recorded membership; waiting for local route install.", args.quiet)

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

    log(
        f"Local multicast routes installed ({len(routes)} entries); marking receiver ready.",
        args.quiet,
    )
    mark_receiver_ready(conninfo, group_id, args.node_id)
    log("Routes confirmed, starting to receive...", args.quiet)

    expected_chunks = args.expected
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
    if sink_path:
        sink_path.parent.mkdir(parents=True, exist_ok=True)
    sink_file = sink_path.open("wb") if sink_path else None
    sha = hashlib.sha256() if args.verify_checksum else None
    total_bytes = 0

    with psycopg.connect(conninfo) as transfer_conn:
        transfer_conn.autocommit = True
        ensure_transfer_tables(transfer_conn)

        pending_chunks: Dict[int, bytes] = {}
        expected_chunk = 1

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
                    f"[{expected_chunk}/{expected_chunks}] committed {len(chunk_bytes)} bytes from group {group_id}",
                    args.quiet,
                )
                expected_chunk += 1

        while expected_chunk <= expected_chunks:
            delivery = receiver.recv(timeout_ms=args.receive_timeout_ms)
            if delivery is None:
                request_repair(
                    transfer_conn, args.group_label, expected_chunk, args.node_id
                )
                log(
                    f"Timeout waiting for chunk {expected_chunk}; requested repair.",
                    args.quiet,
                )
                continue

            payload_bytes = delivery.payload
            try:
                chunk_index, chunk_data = decode_chunk(payload_bytes)
            except ValueError as exc:
                log(f"WARN: failed to decode chunk payload: {exc}", args.quiet)
                continue

            ack_chunk(transfer_conn, args.group_label, chunk_index, args.node_id)

            if chunk_index > expected_chunks:
                log(
                    f"WARN: received chunk {chunk_index} beyond expected total {expected_chunks}; skipping.",
                    args.quiet,
                )
                continue

            if chunk_index < expected_chunk:
                continue

            pending_chunks[chunk_index] = chunk_data
            flush_ready_chunks()

    if sink_file:
        sink_file.flush()
        sink_file.close()
        log(
            f"Wrote reconstructed tensor to {sink_path} ({total_bytes} bytes)",
            args.quiet,
        )

    if sha:
        checksum_path = resolve_checksum_path(args)
        expected_digest = wait_for_checksum_file(
            checksum_path, args.checksum_wait_seconds
        )
        digest = sha.hexdigest()
        if digest != expected_digest:
            raise RuntimeError(
                f"Checksum mismatch: expected {expected_digest}, received {digest}."
            )
        log("Checksum verified successfully.", args.quiet)

    log("Receiver observed all expected multicast payloads.", args.quiet)


def main() -> int:
    args = parse_args()
    if args.tensor_path is None:
        args.generate_tensor = True
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be positive.")
    if args.artifact_dir:
        args.artifact_dir.mkdir(parents=True, exist_ok=True)
    conninfo = build_conninfo(args)
    try:
        if args.role == "source":
            run_source(args, conninfo)
        else:
            run_receiver(args, conninfo)
    except TimeoutError as exc:
        log(f"ERROR: {exc}", quiet=False)
        return 1
    except Exception as exc:  # pragma: no cover - surfaced in docker logs
        log(f"ERROR: {exc}", quiet=False)
        return 1

    # Keep container running after completion
    log(
        "Task completed. Keeping container alive (press Ctrl+C to exit)...", quiet=False
    )
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log("Received interrupt signal, exiting.", quiet=False)

    return 0


if __name__ == "__main__":
    sys.exit(main())
