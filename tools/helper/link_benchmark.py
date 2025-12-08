#!/usr/bin/env python3
"""
Link Benchmark: Measure and store throughput between all pairs of nextmini nodes.

This script:
1. Connects to the nextmini controller database
2. Queries all registered nodes and their underlay addresses
3. Runs iperf3 throughput tests between node pairs
4. Stores results in the link_throughput table

Usage:
    # Measure all node pairs (requires iperf3 servers running on all nodes)
    python link_benchmark.py measure --db-host 172.16.8.2

    # Query stored throughput data
    python link_benchmark.py query --db-host 172.16.8.2

    # Start iperf3 server on this node
    python link_benchmark.py server

Prerequisites:
    - iperf3 installed on all nodes
    - psycopg2 installed: pip install psycopg2-binary
    - Nodes registered in nextmini controller database
"""

import argparse
import json
import time
import subprocess
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from itertools import combinations, permutations
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("Error: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)


@dataclass
class Node:
    """Represents a nextmini node."""

    id: int
    private_network_name: Optional[str]
    private_network_addr: str
    public_network_addr: str

    @property
    def private_ip(self) -> str:
        """Extract IP from addr:port format."""
        return self.private_network_addr.split(":")[0]

    @property
    def public_ip(self) -> str:
        """Extract IP from addr:port format."""
        return self.public_network_addr.split(":")[0]


@dataclass
class ThroughputMeasurement:
    """Result of a throughput measurement."""

    src_node_id: int
    dst_node_id: int
    bandwidth_bps: float
    bandwidth_mbps: float
    bytes_transferred: int
    duration_secs: float
    protocol: str
    retransmits: Optional[int]
    jitter_ms: Optional[float]
    success: bool
    error: Optional[str]


class LinkBenchmark:
    """Benchmark tool for measuring link throughput between nodes."""

    def __init__(
        self,
        db_host: str = "172.16.8.2",
        db_port: int = 5432,
        db_name: str = "nextmini",
        db_user: str = "pgusr",
        db_password: str = "pgpwrd",
        iperf_port: int = 5201,
    ):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.iperf_port = iperf_port
        self._conn = None

    def connect_db(self):
        """Connect to the PostgreSQL database."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
            )
        return self._conn

    def close_db(self):
        """Close database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def get_nodes(self) -> List[Node]:
        """Fetch all registered nodes from the database."""
        conn = self.connect_db()
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT id, private_network_name, private_network_addr, public_network_addr
                FROM nodes
                ORDER BY id
            """)
            rows = cur.fetchall()

        return [
            Node(
                id=row["id"],
                private_network_name=row["private_network_name"],
                private_network_addr=row["private_network_addr"],
                public_network_addr=row["public_network_addr"],
            )
            for row in rows
        ]

    def measure_throughput(
        self,
        target_ip: str,
        duration: int = 10,
        protocol: str = "tcp",
        port: int = None,
    ) -> Dict:
        """
        Run iperf3 client and return parsed results.

        Returns dict with bandwidth_bps, bytes_transferred, etc.
        """
        port = port or self.iperf_port
        cmd = [
            "iperf3",
            "-c",
            target_ip,
            "-p",
            str(port),
            "-t",
            str(duration),
            "-J",  # JSON output
        ]

        if protocol == "udp":
            cmd.extend(["-u", "-b", "0"])  # Unlimited bandwidth for UDP

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=duration + 30,
            )

            if result.returncode != 0:
                return {"success": False, "error": result.stderr}

            data = json.loads(result.stdout)

            if "error" in data:
                return {"success": False, "error": data["error"]}

            end = data.get("end", {})

            if protocol == "tcp":
                sum_sent = end.get("sum_sent", {})
                if not sum_sent:
                    streams = end.get("streams", [])
                    if streams:
                        sum_sent = streams[0].get("sender", {})

                return {
                    "success": True,
                    "bandwidth_bps": sum_sent.get("bits_per_second", 0),
                    "bytes_transferred": sum_sent.get("bytes", 0),
                    "duration_secs": sum_sent.get("seconds", 0),
                    "retransmits": sum_sent.get("retransmits"),
                    "jitter_ms": None,
                    "protocol": "tcp",
                }
            else:
                sum_data = end.get("sum", {})
                return {
                    "success": True,
                    "bandwidth_bps": sum_data.get("bits_per_second", 0),
                    "bytes_transferred": sum_data.get("bytes", 0),
                    "duration_secs": sum_data.get("seconds", 0),
                    "retransmits": None,
                    "jitter_ms": sum_data.get("jitter_ms"),
                    "protocol": "udp",
                }

        except subprocess.TimeoutExpired:
            return {"success": False, "error": "iperf3 timeout"}
        except json.JSONDecodeError as e:
            return {"success": False, "error": f"JSON parse error: {e}"}
        except FileNotFoundError:
            return {"success": False, "error": "iperf3 not found"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def store_measurement(self, measurement: ThroughputMeasurement):
        """Store a throughput measurement in the database."""
        if not measurement.success:
            print(f"  Skipping failed measurement: {measurement.error}")
            return

        conn = self.connect_db()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO link_throughput 
                (src_node_id, dst_node_id, bandwidth_bps, bandwidth_mbps,
                 bytes_transferred, duration_secs, protocol, retransmits, jitter_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    measurement.src_node_id,
                    measurement.dst_node_id,
                    measurement.bandwidth_bps,
                    measurement.bandwidth_mbps,
                    measurement.bytes_transferred,
                    measurement.duration_secs,
                    measurement.protocol,
                    measurement.retransmits,
                    measurement.jitter_ms,
                ),
            )
        conn.commit()

    def benchmark_all_pairs(
        self,
        duration: int = 10,
        protocol: str = "tcp",
        bidirectional: bool = True,
        use_private: bool = True,
        parallel: bool = False,
    ) -> List[ThroughputMeasurement]:
        """
        Benchmark throughput between all pairs of nodes.

        Args:
            duration: Test duration per pair in seconds
            protocol: "tcp" or "udp"
            bidirectional: If True, test both A->B and B->A
            use_private: If True, use private_network_addr, else public_network_addr
            parallel: If True, run tests in parallel (may affect accuracy)

        Returns:
            List of ThroughputMeasurement objects
        """
        nodes = self.get_nodes()

        if len(nodes) < 2:
            print(f"Error: Need at least 2 nodes, found {len(nodes)}")
            return []

        print(f"Found {len(nodes)} nodes:")
        for node in nodes:
            print(
                f"  Node {node.id}: private={node.private_ip}, public={node.public_ip}"
            )

        # Generate pairs
        if bidirectional:
            pairs = list(permutations(nodes, 2))
        else:
            pairs = list(combinations(nodes, 2))

        print(f"\nTesting {len(pairs)} node pairs...")

        measurements = []

        def run_test(src_node: Node, dst_node: Node) -> ThroughputMeasurement:
            target_ip = dst_node.private_ip if use_private else dst_node.public_ip
            print(f"  Testing {src_node.id} -> {dst_node.id} ({target_ip})...")

            result = self.measure_throughput(target_ip, duration, protocol)

            bandwidth_bps = result.get("bandwidth_bps", 0)
            measurement = ThroughputMeasurement(
                src_node_id=src_node.id,
                dst_node_id=dst_node.id,
                bandwidth_bps=bandwidth_bps,
                bandwidth_mbps=bandwidth_bps / 1e6,
                bytes_transferred=result.get("bytes_transferred", 0),
                duration_secs=result.get("duration_secs", 0),
                protocol=protocol,
                retransmits=result.get("retransmits"),
                jitter_ms=result.get("jitter_ms"),
                success=result.get("success", False),
                error=result.get("error"),
            )

            if measurement.success:
                print(f"    {measurement.bandwidth_mbps:.1f} Mbps")
            else:
                print(f"    FAILED: {measurement.error}")

            return measurement

        if parallel:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(run_test, src, dst): (src, dst)
                    for src, dst in pairs
                }
                for future in as_completed(futures):
                    measurement = future.result()
                    measurements.append(measurement)
                    if measurement.success:
                        self.store_measurement(measurement)
        else:
            for src_node, dst_node in pairs:
                measurement = run_test(src_node, dst_node)
                measurements.append(measurement)
                if measurement.success:
                    self.store_measurement(measurement)

        return measurements

    def query_throughput(
        self,
        src_node_id: int = None,
        dst_node_id: int = None,
        protocol: str = None,
        latest_only: bool = True,
    ) -> List[Dict]:
        """
        Query stored throughput measurements.

        Args:
            src_node_id: Filter by source node
            dst_node_id: Filter by destination node
            protocol: Filter by protocol ("tcp" or "udp")
            latest_only: If True, only return the most recent measurement per pair

        Returns:
            List of measurement dictionaries
        """
        conn = self.connect_db()

        if latest_only:
            query = """
                SELECT DISTINCT ON (src_node_id, dst_node_id, protocol)
                    id, src_node_id, dst_node_id, bandwidth_bps, bandwidth_mbps,
                    bytes_transferred, duration_secs, protocol, retransmits,
                    jitter_ms, measured_at
                FROM link_throughput
                WHERE 1=1
            """
        else:
            query = """
                SELECT id, src_node_id, dst_node_id, bandwidth_bps, bandwidth_mbps,
                       bytes_transferred, duration_secs, protocol, retransmits,
                       jitter_ms, measured_at
                FROM link_throughput
                WHERE 1=1
            """

        params = []
        if src_node_id is not None:
            query += " AND src_node_id = %s"
            params.append(src_node_id)
        if dst_node_id is not None:
            query += " AND dst_node_id = %s"
            params.append(dst_node_id)
        if protocol is not None:
            query += " AND protocol = %s"
            params.append(protocol)

        if latest_only:
            query += " ORDER BY src_node_id, dst_node_id, protocol, measured_at DESC"
        else:
            query += " ORDER BY measured_at DESC"

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        return [dict(row) for row in rows]

    def get_throughput_matrix(
        self, protocol: str = "tcp"
    ) -> Dict[Tuple[int, int], float]:
        """
        Get a matrix of throughput values between all node pairs.

        Returns:
            Dict mapping (src_id, dst_id) -> bandwidth_mbps
        """
        results = self.query_throughput(protocol=protocol, latest_only=True)
        return {
            (r["src_node_id"], r["dst_node_id"]): r["bandwidth_mbps"] for r in results
        }

    def print_throughput_matrix(self, protocol: str = "tcp"):
        """Print throughput matrix in a readable format."""
        nodes = self.get_nodes()
        matrix = self.get_throughput_matrix(protocol)

        if not nodes:
            print("No nodes found.")
            return

        # Header
        node_ids = [n.id for n in nodes]
        print(f"\nThroughput Matrix ({protocol.upper()}) - Mbps:")
        print("     " + " ".join(f"{nid:>8}" for nid in node_ids))
        print("     " + "-" * (9 * len(node_ids)))

        # Rows
        for src_id in node_ids:
            row = []
            for dst_id in node_ids:
                if src_id == dst_id:
                    row.append("   -    ")
                else:
                    bw = matrix.get((src_id, dst_id))
                    if bw is not None:
                        row.append(f"{bw:>7.1f} ")
                    else:
                        row.append("   N/A  ")
            print(f"{src_id:>4} |" + "".join(row))


def start_iperf_server(port: int = 5201, bind: str = "0.0.0.0"):
    """Start iperf3 server."""
    print(f"Starting iperf3 server on {bind}:{port}...")
    print("Press Ctrl+C to stop.\n")

    try:
        subprocess.run(["iperf3", "-s", "-p", str(port), "-B", bind])
    except FileNotFoundError:
        print("Error: iperf3 not found. Please install iperf3.")
    except KeyboardInterrupt:
        print("\nServer stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="Measure and store throughput between nextmini nodes"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Common DB arguments
    db_args = argparse.ArgumentParser(add_help=False)
    db_args.add_argument("--db-host", default="172.16.8.2", help="Database host")
    db_args.add_argument("--db-port", type=int, default=5432, help="Database port")
    db_args.add_argument("--db-name", default="nextmini", help="Database name")
    db_args.add_argument("--db-user", default="pgusr", help="Database user")
    db_args.add_argument("--db-password", default="pgpwrd", help="Database password")
    db_args.add_argument("--iperf-port", type=int, default=5201, help="iperf3 port")

    # Measure command
    measure_parser = subparsers.add_parser(
        "measure", parents=[db_args], help="Measure throughput between all node pairs"
    )
    measure_parser.add_argument(
        "-t", "--duration", type=int, default=10, help="Test duration per pair"
    )
    measure_parser.add_argument("-u", "--udp", action="store_true", help="Use UDP")
    measure_parser.add_argument(
        "--one-way", action="store_true", help="Only test A->B, not B->A"
    )
    measure_parser.add_argument(
        "--public", action="store_true", help="Use public addresses"
    )
    measure_parser.add_argument(
        "--parallel", action="store_true", help="Run tests in parallel"
    )

    # Query command
    query_parser = subparsers.add_parser(
        "query", parents=[db_args], help="Query stored throughput data"
    )
    query_parser.add_argument("--src", type=int, help="Filter by source node ID")
    query_parser.add_argument("--dst", type=int, help="Filter by destination node ID")
    query_parser.add_argument(
        "-u", "--udp", action="store_true", help="Filter by UDP protocol"
    )
    query_parser.add_argument(
        "--all", action="store_true", help="Show all measurements, not just latest"
    )

    # Matrix command
    matrix_parser = subparsers.add_parser(
        "matrix", parents=[db_args], help="Show throughput matrix"
    )
    matrix_parser.add_argument(
        "-u", "--udp", action="store_true", help="Show UDP matrix"
    )

    # Nodes command
    nodes_parser = subparsers.add_parser(
        "nodes", parents=[db_args], help="List all registered nodes"
    )

    # Server command
    server_parser = subparsers.add_parser("server", help="Start iperf3 server")
    server_parser.add_argument(
        "-p", "--port", type=int, default=5201, help="Server port"
    )
    server_parser.add_argument("-B", "--bind", default="0.0.0.0", help="Bind address")

    args = parser.parse_args()

    if args.command == "server":
        start_iperf_server(args.port, args.bind)
        return

    if args.command is None:
        parser.print_help()
        return

    # Initialize benchmark tool with DB settings
    benchmark = LinkBenchmark(
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_user=args.db_user,
        db_password=args.db_password,
        iperf_port=args.iperf_port,
    )

    try:
        if args.command == "nodes":
            nodes = benchmark.get_nodes()
            print(f"Registered nodes ({len(nodes)}):")
            for node in nodes:
                print(f"  Node {node.id}:")
                print(f"    Private: {node.private_network_addr}")
                print(f"    Public:  {node.public_network_addr}")
                if node.private_network_name:
                    print(f"    Network: {node.private_network_name}")

        elif args.command == "measure":
            protocol = "udp" if args.udp else "tcp"
            measurements = benchmark.benchmark_all_pairs(
                duration=args.duration,
                protocol=protocol,
                bidirectional=not args.one_way,
                use_private=not args.public,
                parallel=args.parallel,
            )

            # Summary
            successful = [m for m in measurements if m.success]
            failed = [m for m in measurements if not m.success]

            print(f"\n{'=' * 60}")
            print(f"BENCHMARK COMPLETE")
            print(f"{'=' * 60}")
            print(f"Successful: {len(successful)}/{len(measurements)}")
            if successful:
                avg_bw = sum(m.bandwidth_mbps for m in successful) / len(successful)
                min_bw = min(m.bandwidth_mbps for m in successful)
                max_bw = max(m.bandwidth_mbps for m in successful)
                print(
                    f"Bandwidth: avg={avg_bw:.1f} Mbps, min={min_bw:.1f} Mbps, max={max_bw:.1f} Mbps"
                )
            if failed:
                print(f"Failed pairs:")
                for m in failed:
                    print(f"  {m.src_node_id} -> {m.dst_node_id}: {m.error}")

        elif args.command == "query":
            protocol = "udp" if args.udp else None
            results = benchmark.query_throughput(
                src_node_id=args.src,
                dst_node_id=args.dst,
                protocol=protocol,
                latest_only=not args.all,
            )

            print(f"Found {len(results)} measurements:")
            for r in results:
                print(
                    f"  {r['src_node_id']} -> {r['dst_node_id']}: "
                    f"{r['bandwidth_mbps']:.1f} Mbps ({r['protocol']}) "
                    f"@ {r['measured_at']}"
                )

        elif args.command == "matrix":
            protocol = "udp" if args.udp else "tcp"
            benchmark.print_throughput_matrix(protocol)

    finally:
        benchmark.close_db()


if __name__ == "__main__":
    main()
