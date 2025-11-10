#!/usr/bin/env python3
"""
Multicast multi-group demo that coordinates everything via TOML.

Run with `--role source` to create groups and broadcast payloads, or with
`--role receiver` to join groups, leave one, and verify delivery stops.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import tomllib

try:
    import nextmini_py as nm
    from nextmini_py import FrozenBuffer
except ImportError as exc:  # pragma: no cover - surfaced at runtime
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with `maturin build` and "
        "install it before running this example."
    ) from exc


@dataclass
class GroupMeta:
    """Metadata published by the source once the controller creates a group."""

    label: str
    group_id: int
    group_ip: str
    src_node_id: int


@dataclass
class ReceiverSession:
    """Tracks the PacketReceiver bound to a specific multicast group."""

    meta: GroupMeta
    packet_receiver: nm.PacketReceiver


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exercise multiple multicast groups via TOML configs.")
    parser.add_argument("--role", choices=("source", "receiver"), required=True)
    parser.add_argument("--config", type=Path, required=True, help="Dataplane config TOML.")
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path("./tmp/multigroup-state"),
        help="Shared directory for TOML metadata between roles.",
    )
    parser.add_argument(
        "--group-labels",
        nargs="+",
        default=["loss-stream", "activation-stream"],
        help="Human-friendly labels for the groups to create/join.",
    )
    parser.add_argument(
        "--leave-label",
        type=str,
        default=None,
        help="Label the receiver should leave (defaults to the first label).",
    )
    parser.add_argument(
        "--initial-count",
        type=int,
        default=3,
        help="Packets per group before the receiver leaves.",
    )
    parser.add_argument(
        "--post-leave-count",
        type=int,
        default=2,
        help="Packets per group after the receiver leaves one group.",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=100,
        help="Delay between packets to keep logs readable.",
    )
    parser.add_argument(
        "--controller-timeout-ms",
        type=int,
        default=30000,
        help="Timeout for controller acknowledgements (group ready/routes).",
    )
    parser.add_argument(
        "--handshake-timeout",
        type=int,
        default=120,
        help="Seconds to wait for TOML metadata/markers to appear.",
    )
    parser.add_argument(
        "--recv-timeout-ms",
        type=int,
        default=5000,
        help="Per-packet timeout on the receiver role.",
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
        "--preserve-state-dir",
        action="store_true",
        help="Skip clearing the shared state directory on startup (defaults to cleanup).",
    )
    parser.add_argument(
        "--participant-tag",
        type=str,
        default="default",
        help="Identifier for this receiver when writing readiness/leave markers.",
    )
    parser.add_argument(
        "--wait-tags",
        nargs="+",
        default=None,
        help="Receiver tags the source should wait for (defaults to ['default']).",
    )
    parser.add_argument(
        "--leave-tag",
        type=str,
        default=None,
        help="Receiver tag whose leave marker unblocks the source (defaults to first wait tag).",
    )
    return parser.parse_args()


def log(message: str) -> None:
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


class StateDirCoordinator:
    """Coordinates cleanup so concurrent roles don't clobber each other."""

    def __init__(self, state_dir: Path, preserve_existing: bool) -> None:
        self.state_dir = state_dir
        self.preserve_existing = preserve_existing
        self.lock_path = state_dir / ".state.lock"
        self.count_path = state_dir / ".state.runners"

    def __enter__(self) -> StateDirCoordinator:
        self.enter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - teardown
        self.exit()

    def enter(self) -> None:
        ensure_dir(self.state_dir)
        if self.preserve_existing:
            log(f"Preserving existing state dir at {self.state_dir} (cleanup disabled).")
            return

        with self._locked():
            count = self._read_count()
            if count == 0:
                self._cleanup_files(phase="pre-run")
            self._write_count(count + 1)

    def exit(self) -> None:
        if self.preserve_existing:
            return

        remove_lock = False
        with self._locked():
            count = self._read_count()
            if count <= 1:
                self._cleanup_files(phase="post-run")
                self._remove_count_file()
                remove_lock = True
            else:
                self._write_count(count - 1)

        if remove_lock:
            self._remove_lock_file()
            self._maybe_remove_state_dir()

    def _locked(self):
        class _Lock:
            def __init__(self, outer: StateDirCoordinator) -> None:
                self.outer = outer
                self.fd: int | None = None

            def __enter__(self) -> None:
                ensure_dir(self.outer.state_dir)
                fd = os.open(self.outer.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
                fcntl.flock(fd, fcntl.LOCK_EX)
                self.fd = fd

            def __exit__(self, exc_type, exc, tb) -> None:
                if self.fd is not None:
                    fcntl.flock(self.fd, fcntl.LOCK_UN)
                    os.close(self.fd)

        return _Lock(self)

    def _read_count(self) -> int:
        try:
            return int(self.count_path.read_text())
        except (FileNotFoundError, ValueError):
            return 0

    def _write_count(self, value: int) -> None:
        self.count_path.write_text(str(value))

    def _remove_count_file(self) -> None:
        try:
            self.count_path.unlink()
        except FileNotFoundError:
            pass

    def _remove_lock_file(self) -> None:
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            pass

    def _maybe_remove_state_dir(self) -> None:
        try:
            next(self.state_dir.iterdir())
        except StopIteration:
            try:
                self.state_dir.rmdir()
            except OSError:
                pass
        except FileNotFoundError:
            pass

    def _cleanup_files(self, *, phase: str) -> None:
        removed = 0
        for pattern in ("*.toml", "*.toml.tmp"):
            for candidate in self.state_dir.glob(pattern):
                try:
                    candidate.unlink()
                    removed += 1
                except FileNotFoundError:
                    continue
        if removed:
            log(f"Cleared {removed} state file(s) from {self.state_dir} ({phase}).")


def metadata_path(state_dir: Path, label: str) -> Path:
    return state_dir / f"{label}.toml"


def tag_suffix(tag: str | None) -> str:
    if tag:
        cleaned = tag.strip()
        if cleaned:
            return f".{cleaned}"
    return ""


def ready_path(state_dir: Path, label: str, tag: str | None = None) -> Path:
    return state_dir / f"{label}{tag_suffix(tag)}.ready.toml"


def left_path(state_dir: Path, label: str, tag: str | None = None) -> Path:
    return state_dir / f"{label}{tag_suffix(tag)}.left.toml"


def toml_format_value(value: Any) -> str:
    if isinstance(value, str):
        escaped = value.replace('"', '\\"')
        return f'"{escaped}"'
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    raise TypeError(f"Unsupported TOML value type: {type(value)}")


def toml_dumps(data: dict[str, Any]) -> str:
    lines = [f"{key} = {toml_format_value(value)}" for key, value in data.items()]
    return "\n".join(lines) + "\n"


def write_toml_atomically(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(toml_dumps(payload))
    tmp_path.replace(path)


def wait_for_toml(path: Path, timeout_s: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if path.exists():
            try:
                return tomllib.loads(path.read_text())
            except tomllib.TOMLDecodeError:
                time.sleep(0.1)
        time.sleep(0.2)
    raise TimeoutError(f"Timed out waiting for TOML file {path}")


def broadcast_payloads(
    dataplane: nm.Dataplane,
    metas: Iterable[GroupMeta],
    count: int,
    sleep_ms: int,
    tag: str,
    src_port: Optional[int],
    dst_port: Optional[int],
) -> None:
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


def receive_packets(
    session: ReceiverSession,
    count: int,
    timeout_ms: int,
    phase: str,
) -> None:
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


def run_source(args: argparse.Namespace) -> None:
    ensure_dir(args.state_dir)
    dataplane = nm.Dataplane(str(args.config))

    metas: list[GroupMeta] = []
    for label in args.group_labels:
        log(f"Creating multicast group '{label}'...")
        dataplane.create_group(label)
        event = dataplane.group_is_ready(timeout_ms=args.controller_timeout_ms)
        if event is None:
            raise RuntimeError(f"Controller did not confirm group '{label}' in time.")
        group_id, group_ip, src_node_id = event
        meta = GroupMeta(label=label, group_id=group_id, group_ip=group_ip, src_node_id=src_node_id)
        metas.append(meta)
        write_toml_atomically(
            metadata_path(args.state_dir, label),
            {
                "label": label,
                "group_id": group_id,
                "group_ip": group_ip,
                "src_node_id": src_node_id,
                "written_at": int(time.time()),
            },
        )
        log(f"Metadata for '{label}' written to {metadata_path(args.state_dir, label)}.")

    wait_tags = args.wait_tags or ["default"]
    for meta in metas:
        for tag in wait_tags:
            marker = ready_path(args.state_dir, meta.label, tag)
            log(f"Waiting for receiver ({tag}) readiness marker {marker}...")
            wait_for_toml(marker, args.handshake_timeout)

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
    leave_tag = args.leave_tag or wait_tags[0]
    leave_marker = left_path(args.state_dir, leave_label, leave_tag)
    log(
        f"Waiting for receiver ({leave_tag}) to leave group '{leave_label}' "
        f"(marker: {leave_marker})..."
    )
    wait_for_toml(leave_marker, args.handshake_timeout)

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


def run_receiver(args: argparse.Namespace) -> None:
    ensure_dir(args.state_dir)
    dataplane = nm.Dataplane(str(args.config))

    sessions: list[ReceiverSession] = []
    for label in args.group_labels:
        meta_toml = wait_for_toml(metadata_path(args.state_dir, label), args.handshake_timeout)
        meta = GroupMeta(
            label=label,
            group_id=int(meta_toml["group_id"]),
            group_ip=str(meta_toml["group_ip"]),
            src_node_id=int(meta_toml["src_node_id"]),
        )
        log(f"Joining group '{label}' (id={meta.group_id}, ip={meta.group_ip})...")
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
        ready_marker = ready_path(args.state_dir, label, args.participant_tag)
        write_toml_atomically(
            ready_marker,
            {
                "label": label,
                "status": "ready",
                "timestamp": int(time.time()),
                "participant_tag": args.participant_tag,
            },
        )
        log(f"Wrote readiness marker {ready_marker}.")

    log(f"Waiting for {args.initial_count} packet(s) per group.")
    for session in sessions:
        receive_packets(session, args.initial_count, args.recv_timeout_ms, phase="initial")

    leave_label = args.leave_label or args.group_labels[0]
    session_to_leave = next((s for s in sessions if s.meta.label == leave_label), None)
    if session_to_leave is None:
        raise RuntimeError(f"No session matches leave label '{leave_label}'.")

    log(f"Leaving group '{leave_label}' to exercise leave_group().")
    dataplane.leave_group(session_to_leave.meta.group_id)
    leave_marker = left_path(args.state_dir, leave_label, args.participant_tag)
    write_toml_atomically(
        leave_marker,
        {
            "label": leave_label,
            "status": "left",
            "timestamp": int(time.time()),
            "participant_tag": args.participant_tag,
        },
    )

    still_joined = [s for s in sessions if s.meta.label != leave_label]
    if still_joined and args.post_leave_count > 0:
        log(
            f"Waiting for {args.post_leave_count} additional packet(s) on "
            f"{len(still_joined)} remaining group(s)."
        )
        for session in still_joined:
            receive_packets(session, args.post_leave_count, args.recv_timeout_ms, phase="post-leave")

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
    coordinator = StateDirCoordinator(args.state_dir, preserve_existing=args.preserve_state_dir)
    coordinator.enter()
    try:
        if args.role == "source":
            run_source(args)
        else:
            run_receiver(args)
    except TimeoutError as exc:
        log(f"ERROR: {exc}")
        return 1
    except Exception as exc:  # pragma: no cover - surfaced during manual runs
        log(f"ERROR: {exc}")
        raise
    finally:
        coordinator.exit()
    return 0


if __name__ == "__main__":
    sys.exit(main())
