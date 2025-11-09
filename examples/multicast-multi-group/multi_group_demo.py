#!/usr/bin/env python3
"""
Simple multi-group multicast demo for the Nextmini Python API.

Run as "source" to create two multicast groups, send a few payloads, and verify
traffic still flows after a receiver leaves one group. Run as "receiver" to join
the groups, receive payloads, and exercise the `leave_group` API.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

try:
    import nextmini_py as nm
    from nextmini_py import FrozenBuffer
except ImportError as exc:  # pragma: no cover - surfaced at launch time
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with `maturin build` and "
        "install it before running this example."
    ) from exc


@dataclass
class GroupMeta:
    """Tiny container holding the controller metadata for one group."""

    label: str
    group_id: int
    group_ip: str
    src_node_id: int


@dataclass
class ReceiverSession:
    """Couples the multicast metadata with the registered PacketReceiver."""

    meta: GroupMeta
    packet_receiver: nm.PacketReceiver


def parse_args() -> argparse.Namespace:
    """Expose shared CLI for both roles so we can reuse the script."""

    parser = argparse.ArgumentParser(description="Exercise multiple multicast groups via nextmini_py.")
    parser.add_argument("--role", choices=("source", "receiver"), required=True)
    parser.add_argument("--config", type=Path, required=True, help="Dataplane config TOML.")
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("./tmp/multicast-multi-group"),
        help="Directory used to exchange small JSON markers between roles.",
    )
    parser.add_argument(
        "--group-labels",
        nargs="+",
        default=["loss-stream", "activation-stream"],
        help="Names for the multicast groups created by the source.",
    )
    parser.add_argument(
        "--initial-count",
        type=int,
        default=3,
        help="Packets to send (and expect) per group before leaving.",
    )
    parser.add_argument(
        "--post-leave-count",
        type=int,
        default=2,
        help="Packets to send after the receiver leaves the first group.",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=100,
        help="Delay between packets so logs stay readable.",
    )
    parser.add_argument(
        "--controller-timeout-ms",
        type=int,
        default=30000,
        help="Timeout for controller acks (group_is_ready, membership, routes).",
    )
    parser.add_argument(
        "--handshake-timeout",
        type=int,
        default=120,
        help="Seconds to wait for metadata/ready/left markers in the state dir.",
    )
    parser.add_argument(
        "--recv-timeout-ms",
        type=int,
        default=5000,
        help="Per-packet receive timeout on the receiver role.",
    )
    parser.add_argument(
        "--leave-label",
        type=str,
        default=None,
        help="Group label the receiver should leave (defaults to the first label).",
    )
    parser.add_argument(
        "--src-port",
        type=int,
        default=None,
        help="Optional TCP source port override (keep default to use config values).",
    )
    parser.add_argument(
        "--dst-port",
        type=int,
        default=None,
        help="Optional TCP destination port override (keep default to use config values).",
    )
    return parser.parse_args()


def log(message: str) -> None:
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def ensure_dir(path: Path) -> None:
    """Idempotently create a directory tree."""

    path.mkdir(parents=True, exist_ok=True)


def metadata_path(state_dir: Path, label: str) -> Path:
    """Return the metadata file path for a specific group label."""

    return state_dir / f"{label}.json"


def ready_path(state_dir: Path, label: str) -> Path:
    """Return the marker path indicating the receiver is ready."""

    return state_dir / f"{label}.ready"


def left_path(state_dir: Path, label: str) -> Path:
    """Return the marker path indicating the receiver left a group."""

    return state_dir / f"{label}.left"


def write_json_atomically(path: Path, payload: dict) -> None:
    """Write JSON using a temporary file to avoid partial reads."""

    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2))
    tmp_path.replace(path)


def wait_for_json(path: Path, timeout_s: int) -> dict:
    """Poll until a JSON blob exists on disk and return it."""

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            try:
                return json.loads(path.read_text())
            except json.JSONDecodeError:
                time.sleep(0.1)
        time.sleep(0.2)
    raise TimeoutError(f"Timed out waiting for JSON file {path}")


def wait_for_file(path: Path, timeout_s: int) -> None:
    """Poll for a plain marker file to appear."""

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.2)
    raise TimeoutError(f"Timed out waiting for marker {path}")


def broadcast_payloads(
    dataplane: nm.Dataplane,
    metas: Iterable[GroupMeta],
    count: int,
    sleep_ms: int,
    tag: str,
    src_port: Optional[int],
    dst_port: Optional[int],
) -> None:
    """Send deterministic payloads to each group so logs stay traceable."""

    delay = sleep_ms / 1000 if sleep_ms else 0.0
    for meta in metas:
        for idx in range(count):
            payload = FrozenBuffer(f"{meta.label}:{tag}:{idx}".encode("utf-8"))
            dataplane.send_to_ip(
                meta.group_ip,
                payload,
                src_port=src_port,
                dst_port=dst_port,
            )
            log(f"[{tag}] sent payload {idx + 1}/{count} to group '{meta.label}' (ip={meta.group_ip})")
            if delay:
                time.sleep(delay)


def run_source(args: argparse.Namespace) -> None:
    ensure_dir(args.state_dir)
    dataplane = nm.Dataplane(str(args.config))

    metas: list[GroupMeta] = []
    for label in args.group_labels:
        # 1) Create each multicast group and persist controller response so
        #    receivers can learn the group_id/group_ip without querying Postgres.
        log(f"Creating multicast group '{label}'...")
        dataplane.create_group(label)
        event = dataplane.group_is_ready(timeout_ms=args.controller_timeout_ms)
        if event is None:
            raise RuntimeError(f"Controller did not confirm group '{label}' in time.")
        group_id, group_ip, src_node_id = event
        meta = GroupMeta(label=label, group_id=group_id, group_ip=group_ip, src_node_id=src_node_id)
        metas.append(meta)
        write_json_atomically(
            metadata_path(args.state_dir, label),
            {
                "group_id": group_id,
                "group_ip": group_ip,
                "src_node_id": src_node_id,
            },
        )
        log(f"Metadata for '{label}' written to {metadata_path(args.state_dir, label)}.")

    for meta in metas:
        # 2) Wait for receivers to signal they have joined, installed routes,
        #    and registered a PacketReceiver for the flow.
        marker = ready_path(args.state_dir, meta.label)
        log(f"Waiting for receiver readiness marker {marker}...")
        wait_for_file(marker, args.handshake_timeout)

    # 3) Send the first burst while all groups are active.
    log(f"Broadcasting {args.initial_count} packet(s) per group (pre-leave).")
    broadcast_payloads(
        dataplane,
        metas,
        args.initial_count,
        args.sleep_ms,
        tag="initial",
        src_port=args.src_port,
        dst_port=args.dst_port,
    )

    leave_label = args.leave_label or metas[0].label
    leave_marker = left_path(args.state_dir, leave_label)
    log(f"Waiting for receiver to leave group '{leave_label}' (marker: {leave_marker})...")
    wait_for_file(leave_marker, args.handshake_timeout)

    # 4) After the receiver leaves, send more traffic (it should only arrive on
    #    the groups that are still joined).
    log(f"Receiver left '{leave_label}'. Broadcasting {args.post_leave_count} more packet(s) per group.")
    broadcast_payloads(
        dataplane,
        metas,
        args.post_leave_count,
        args.sleep_ms,
        tag="post-leave",
        src_port=args.src_port,
        dst_port=args.dst_port,
    )
    log("Source role complete. Press Ctrl+C to exit when ready.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log("Source exiting.")


def receive_packets(
    session: ReceiverSession,
    count: int,
    timeout_ms: int,
    phase: str,
) -> None:
    """Helper that blocks until the expected number of payloads arrive."""

    for idx in range(count):
        payload = session.packet_receiver.recv(timeout_ms=timeout_ms)
        if payload is None:
            raise TimeoutError(
                f"Timed out waiting for payload {idx + 1}/{count} on group '{session.meta.label}' during {phase}."
            )
        log(
            f"[{phase}] group '{session.meta.label}' received packet "
            f"{idx + 1}/{count} ({len(payload)} bytes)."
        )


def run_receiver(args: argparse.Namespace) -> None:
    ensure_dir(args.state_dir)
    dataplane = nm.Dataplane(str(args.config))

    sessions: list[ReceiverSession] = []
    for label in args.group_labels:
        # 1) Wait for the source to publish controller metadata.
        meta_json = wait_for_json(metadata_path(args.state_dir, label), args.handshake_timeout)
        meta = GroupMeta(
            label=label,
            group_id=int(meta_json["group_id"]),
            group_ip=str(meta_json["group_ip"]),
            src_node_id=int(meta_json["src_node_id"]),
        )
        log(f"Joining group '{label}' (id={meta.group_id}, ip={meta.group_ip})...")
        # 2) Join the group and wait for controller + dataplane confirmation.
        dataplane.join_group(meta.group_id)
        if not dataplane.wait_for_local_membership(meta.group_id, timeout_ms=args.controller_timeout_ms):
            raise RuntimeError(f"Local membership for '{label}' not confirmed in time.")
        routes = dataplane.wait_for_routes_installed(
            meta.group_id,
            src_node_id=meta.src_node_id,
            timeout_ms=args.controller_timeout_ms,
        )
        if routes is None:
            raise RuntimeError(f"Routes for group '{label}' were not installed in time.")
        log(f"Routes ready for '{label}': {routes}")
        receiver = dataplane.register_receiver_for_group(
            meta.src_node_id,
            meta.group_ip,
            src_port=args.src_port,
            dst_port=args.dst_port,
        )
        sessions.append(ReceiverSession(meta=meta, packet_receiver=receiver))
        ready_marker = ready_path(args.state_dir, label)
        ready_marker.write_text(str(int(time.time())))
        log(f"Wrote readiness marker {ready_marker}.")

    # 3) Receive the first burst while still joined to every group.
    log(f"Waiting for {args.initial_count} packet(s) per group.")
    for session in sessions:
        receive_packets(session, args.initial_count, args.recv_timeout_ms, phase="initial")

    leave_label = args.leave_label or args.group_labels[0]
    session_to_leave = next((s for s in sessions if s.meta.label == leave_label), None)
    if session_to_leave is None:
        raise RuntimeError(f"No session matches leave label '{leave_label}'.")

    log(f"Leaving group '{leave_label}' to exercise leave_group().")
    dataplane.leave_group(session_to_leave.meta.group_id)
    leave_marker = left_path(args.state_dir, leave_label)
    leave_marker.write_text(str(int(time.time())))
    log(f"Wrote leave marker {leave_marker}.")

    still_joined = [s for s in sessions if s.meta.label != leave_label]
    if still_joined and args.post_leave_count > 0:
        log(
            f"Waiting for {args.post_leave_count} additional packet(s) on "
            f"{len(still_joined)} remaining group(s)."
        )
        for session in still_joined:
            receive_packets(session, args.post_leave_count, args.recv_timeout_ms, phase="post-leave")

    # Final assertion: no more payloads should arrive for the group we left.
    log(f"Confirming no packets arrive on '{leave_label}' after leave_group().")
    leaked = session_to_leave.packet_receiver.recv(timeout_ms=args.recv_timeout_ms)
    if leaked is not None:
        raise RuntimeError(
            f"Unexpected payload ({len(leaked)} bytes) received after leaving group '{leave_label}'."
        )
    log(f"No payloads observed for '{leave_label}' post-leave. Demo complete.")
    log("Press Ctrl+C to exit when ready.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log("Receiver exiting.")


def main() -> int:
    args = parse_args()
    try:
        if args.role == "source":
            run_source(args)
        else:
            run_receiver(args)
    except TimeoutError as exc:
        log(f"ERROR: {exc}")
        return 1
    except Exception as exc:  # pragma: no cover - surfaced in manual runs
        log(f"ERROR: {exc}")
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())
