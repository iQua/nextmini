#!/usr/bin/env python3
"""Utility entrypoint for the multicast docker example.

Each container boots an in-process dataplane via nextmini_py, coordinates
with Postgres to discover the multicast group metadata, and either sends
or receives multicast payloads depending on the assigned role.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Tuple

import psycopg

try:
    import nextmini_py as nm
    from nextmini_py import FrozenBuffer
except ImportError as exc:  # pragma: no cover - surfaced at launch time
    raise SystemExit(
        "nextmini_py is not installed. Build the wheel with `maturin build` (the "
        "run_multicast_node.sh helper bootstraps it automatically inside the container)."
    ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drive the multicast docker example.")
    parser.add_argument("--role", choices=("source", "receiver"), required=True)
    parser.add_argument("--config", type=Path, required=True, help="Dataplane config TOML.")
    parser.add_argument("--group-label", required=True, help="Label used when creating the group.")
    parser.add_argument("--postgres-host", default="postgres")
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.add_argument("--postgres-user", default="pgusr")
    parser.add_argument("--postgres-password", default="pgpwrd")
    parser.add_argument("--postgres-db", default="nextmini")
    parser.add_argument("--group-timeout", type=int, default=90, help="Seconds to wait for group.")
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
                cur.execute("SELECT id, group_ip FROM groups WHERE label = %s", (label,))
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
    dataplane = nm.Dataplane(str(args.config))
    log(f"Requesting multicast group '{args.group_label}'...", args.quiet)
    dataplane.create_group(args.group_label)
    group_id, group_ip = await_group_ready_via_api(
        dataplane, label=args.group_label, timeout=args.group_timeout, quiet=args.quiet
    )

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

    payload = os.urandom(args.payload_size)
    frozen = FrozenBuffer(payload)
    delay = args.sleep_ms / 1000 if args.sleep_ms else 0.0

    for idx in range(1, args.payload_count + 1):
        dataplane.send_to_ip(
            group_ip,
            frozen,
            src_port=args.src_port,
            dst_port=args.dst_port,
        )
        log(
            f"[{idx}/{args.payload_count}] sent {len(payload)} bytes to {group_ip}",
            args.quiet,
        )
        if delay:
            time.sleep(delay)

    log("Source finished sending multicast payloads.", args.quiet)


def run_receiver(args: argparse.Namespace, conninfo: str) -> None:
    if args.node_id is None:
        raise SystemExit("Receiver role requires --node-id.")

    dataplane = nm.Dataplane(str(args.config))
    group_id, group_ip = wait_for_group(conninfo, args.group_label, args.group_timeout)
    log(f"Joining multicast group id={group_id} ({group_ip})...", args.quiet)
    dataplane.join_group(group_id)
    wait_for_count(
        conninfo, group_id, target=0, timeout=args.member_timeout, specific_node=args.node_id
    )
    log("Controller recorded membership; awaiting data plane routes.", args.quiet)
    mark_receiver_ready(conninfo, group_id, args.node_id)
    log("Routes should be installed, starting to receive...", args.quiet)

    receiver = dataplane.register_receiver_for_group(
        src_node_id=args.source_node_id,
        group_ip=group_ip,
        src_port=args.src_port,
        dst_port=args.dst_port,
    )

    received = 0
    while received < args.expected:
        payload = receiver.recv(timeout_ms=args.receive_timeout_ms)
        if payload is None:
            raise TimeoutError("Receiver timed out while waiting for multicast payloads.")
        received += 1
        log(
            f"[{received}/{args.expected}] received {len(payload)} bytes from group {group_id}",
            args.quiet,
        )

    log("Receiver observed all expected multicast payloads.", args.quiet)


def main() -> int:
    args = parse_args()
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
    log("Task completed. Keeping container alive (press Ctrl+C to exit)...", quiet=False)
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log("Received interrupt signal, exiting.", quiet=False)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
