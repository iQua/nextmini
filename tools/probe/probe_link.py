#!/usr/bin/env python3
"""Insert a link probe request and wait for the bandwidth result.

Requires the controller and dataplane nodes to be running, with
PostgreSQL reachable from this machine.

Examples:
    # Probe from node 2 to node 1 with default 1.36 MB payload:
    python probe_link.py --from 2 --to 1

    # Custom payload size and DB port:
    python probe_link.py --from 3 --to 1 --bytes 5000000 --db-port 15432

    # Just insert the request without waiting:
    python probe_link.py --from 2 --to 1 --no-wait
"""

import argparse
import sys
import time

import psycopg2


def parse_args():
    p = argparse.ArgumentParser(description="Trigger a link bandwidth probe.")
    p.add_argument("--from", dest="from_node", type=int, required=True,
                    help="Sender node ID")
    p.add_argument("--to", dest="to_node", type=int, required=True,
                    help="Receiver node ID")
    p.add_argument("--bytes", type=int, default=1_360_000,
                    help="Total probe payload bytes (default: 1360000, ~1000 packets)")
    p.add_argument("--timeout", type=int, default=30,
                    help="Seconds to wait for a result (default: 30)")
    p.add_argument("--no-wait", action="store_true",
                    help="Insert the request and exit without waiting for result")
    p.add_argument("--db-host", default="localhost")
    p.add_argument("--db-port", type=int, default=5432)
    p.add_argument("--db-name", default="nextmini")
    p.add_argument("--db-user", default="pgusr")
    p.add_argument("--db-pass", default="pgpwrd")
    return p.parse_args()


def main():
    args = parse_args()
    dsn = (f"host={args.db_host} port={args.db_port} "
           f"dbname={args.db_name} user={args.db_user} password={args.db_pass}")

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()

    # Sanity check
    cur.execute(
        "SELECT EXISTS "
        "(SELECT 1 FROM information_schema.tables WHERE table_name = 'probe_requests')"
    )
    if not cur.fetchone()[0]:
        print("ERROR: probe_requests table does not exist. Is the controller running?")
        sys.exit(1)

    # Insert probe request
    cur.execute(
        "INSERT INTO probe_requests (from_node_id, to_node_id, probe_bytes) "
        "VALUES (%s, %s, %s) RETURNING id",
        (args.from_node, args.to_node, args.bytes),
    )
    req_id = cur.fetchone()[0]
    print(f"probe_request id={req_id}: node {args.from_node} -> node {args.to_node}, "
          f"{args.bytes} bytes")

    if args.no_wait:
        cur.close()
        conn.close()
        return

    # Poll for result
    print(f"Waiting up to {args.timeout}s for result ...")
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        cur.execute(
            "SELECT probe_id, from_node_id, to_node_id, bandwidth_mbps, created_at "
            "FROM probe_results WHERE probe_id = %s",
            (req_id,),
        )
        row = cur.fetchone()
        if row:
            _, from_id, to_id, bw, ts = row
            print(f"OK: node {from_id} -> node {to_id} = {bw:.2f} Mbps  ({ts})")
            cur.close()
            conn.close()
            return
        time.sleep(0.5)

    cur.close()
    conn.close()
    print(f"FAIL: no result after {args.timeout}s.")
    sys.exit(1)


if __name__ == "__main__":
    main()
