#!/usr/bin/env python3
"""
This script launches the docker containers and database, and handles insertion of flows with flow dependencies
"""

import sys
import time
import random
from typing import Optional, List, Tuple, Dict, Set
import psycopg2
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from pathlib import Path
import subprocess
import shutil
import socket
import os
import json
from fractions import Fraction
import numpy as np
from math import gcd
from functools import reduce
import argparse
from collections import defaultdict


def get_db_host():
    """
    Returns 'postgres' if running inside Docker (where internal DNS resolves service names),
    otherwise returns 'localhost' for host-based execution.
    """
    try:
        # Check cgroup info to detect if running in Docker
        with open("/proc/1/cgroup", "rt") as f:
            in_docker = any("docker" in line for line in f)
        return "postgres" if in_docker else "localhost"
    except Exception:
        # Fallback to localhost if detection fails
        return "localhost"


def launch_docker_compose():
    print("Running: docker compose up --build -d")
    try:
        subprocess.run(["docker", "compose", "up", "--build", "-d"], check=True)
        print("Docker containers started.")
    except subprocess.CalledProcessError as e:
        print("Failed to run docker compose:", e)
        sys.exit(1)


def wait_for_postgres(timeout=30):
    host = get_db_host()
    port = 5432
    print("Waiting for PostgreSQL and 'flows' table to become available...")
    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(
                user="pgusr",
                password="pgpwrd",
                host=host,
                port=port,
                database="nextmini",
            )
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM flows LIMIT 1;")
            cur.close()
            conn.close()
            print("PostgreSQL is ready and 'flows' table exists.")
            return
        except psycopg2.Error as e:
            print(f"Still waiting: {str(e).strip()}")
            time.sleep(1)
            if time.time() - start > timeout:
                print("Timeout: PostgreSQL or 'flows' table did not become available.")
                sys.exit(1)


class FlowDatabase:
    """Database connection and operations for flows table."""

    def __init__(self):
        """Initialize database connection using same credentials as dashboard.py"""
        try:
            self.connection = psycopg2.connect(
                user="pgusr",
                password="pgpwrd",
                host="127.0.0.1",
                port="5432",
                database="nextmini",
            )
            self.connection.autocommit = True
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            sys.exit(1)

    def get_all_flows(self) -> List[Tuple]:
        cursor = self.connection.cursor()
        try:
            query = """
                SELECT id, src_node_id, dst_node_id, flow_len_type,
                       flow_len_bytes, flow_len_duration, flow_rate,
                       flow_weight, start_time, finish_time, is_finished
                FROM flows
                ORDER BY id ASC
            """
            cursor.execute(query)
            return cursor.fetchall()
        except psycopg2.Error as e:
            print(f"Error fetching flows: {e}")
            return []
        finally:
            cursor.close()

    def get_flow_stats(self) -> Tuple[int, int, int]:
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM flows")
            total = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM flows WHERE is_finished = FALSE")
            active = cursor.fetchone()[0]

            finished = total - active

            return total, active, finished
        except psycopg2.Error as e:
            print(f"Error getting flow stats: {e}")
            return 0, 0, 0
        finally:
            cursor.close()

    def insert_flow(
        self,
        src_node_id: int,
        dst_node_id: int,
        flow_len_type: str,
        flow_len_bytes: Optional[int] = None,
        flow_len_duration: Optional[float] = None,
        flow_rate: Optional[int] = None,
        flow_weight: Optional[int] = None,
    ) -> Optional[int]:
        """Insert a new flow into the database."""
        cursor = self.connection.cursor()
        try:
            if flow_len_type not in ["bytes", "duration"]:
                print("Error: flow_len_type must be 'bytes' or 'duration'")
                return None

            if flow_len_type == "bytes" and flow_len_bytes is None:
                print(
                    "Error: flow_len_bytes must be provided when flow_len_type is 'bytes'"
                )
                return None

            if flow_len_type == "duration" and flow_len_duration is None:
                print(
                    "Error: flow_len_duration must be provided when flow_len_type is 'duration'"
                )
                return None

            query = """
                INSERT INTO flows (src_node_id, dst_node_id, flow_len_type,
                                 flow_len_bytes, flow_len_duration, flow_rate,
                                 flow_weight, is_finished)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """

            cursor.execute(
                query,
                (
                    src_node_id,
                    dst_node_id,
                    flow_len_type,
                    flow_len_bytes,
                    flow_len_duration,
                    flow_rate,
                    flow_weight,
                    False,
                ),
            )

            flow_id = cursor.fetchone()[0]
            return flow_id

        except psycopg2.Error as e:
            print(f"Error inserting flow: {e}")
            return None
        finally:
            cursor.close()

    def close(self):
        if self.connection:
            self.connection.close()


class DependencyManager:
    def __init__(self, json_path: str, db):
        self.json_path = json_path
        self.db = db
        self.flow_map: Dict[str, dict] = {}
        self.dep_graph: Dict[
            str, Set[str]
        ] = {}  # Map of flow IDs to sets of flow IDs they depend on
        self.flow_db_ids: Dict[
            str, int
        ] = {}  # Map of flow IDs (JSON) to the actual DB IDs once inserted

    # Parse the JSON file and store each flow's metadata in flow_map and each flow's dependencies in self.dep_graph (as a set of string flow IDs)
    def load_config(self):
        with open(self.json_path, "r") as f:
            config = json.load(f)

        for key, flow in config.items():
            if key in ("link_to_edge", "link_capacities", "type"):
                continue
            if not isinstance(flow, dict):
                continue
            self.flow_map[key] = flow
            self.dep_graph[key] = set(str(dep) for dep in flow.get("dependencies", []))

    # Return a set of Postgres IDs representing finished flows
    def get_finished_flow_ids(self) -> Set[int]:
        cursor = self.db.connection.cursor()
        try:
            cursor.execute("SELECT id FROM flows WHERE is_finished = TRUE")
            return set(row[0] for row in cursor.fetchall())
        finally:
            cursor.close()

    # Insert a flow into the database using its metadata and the above insert_flow fn and return its Postgres ID
    def insert_flow(self, flow_id: str) -> Optional[int]:
        if flow_id not in self.flow_map:
            print(f"[Error] Flow ID '{flow_id}' not found in flow_map.")
            return None
        flow = self.flow_map[flow_id]

        # TODO: Here we may use our algorithm to calculate flow rate / weight before insertion
        try:
            db_id = self.db.insert_flow(
                src_node_id=flow["src"],
                dst_node_id=flow["dst"],
                flow_len_type="bytes",
                flow_len_bytes=flow["total"],
                flow_rate=flow.get(
                    "bps"
                ),  # the bps field is calculated by the algorithm
                flow_weight=1,  # flow.get("flow_weight") # the flow_weight field is calculated by the algorithm
            )
            if db_id is not None:
                self.flow_db_ids[flow_id] = db_id
            return db_id

        except Exception as e:
            print(f"[Error] Exception inserting flow {flow_id}: {e}")
            return None

    # Insert all flows which have not yet been inserted and have all their upstream predecessors being finished
    def maybe_insert_dependent_flows(self):
        finished = self.get_finished_flow_ids()
        inserted = set(self.flow_db_ids.keys())

        for fid, deps in self.dep_graph.items():
            if fid in inserted:
                continue
            if all(
                str(dep) in self.flow_db_ids and self.flow_db_ids[str(dep)] in finished
                for dep in deps
            ):
                self.insert_flow(fid)


def display_flows(flows: List[Tuple], console: Console):
    if not flows:
        console.print(Panel("No flows found in database", style="yellow"))
        return

    table = Table(title="Flows Table")
    table.add_column("ID", justify="center", style="cyan")
    table.add_column("Source Node", justify="center", style="green")
    table.add_column("Dest Node", justify="center", style="green")
    table.add_column("Type", justify="center", style="magenta")
    table.add_column("Bytes", justify="right", style="blue")
    table.add_column("Duration (s)", justify="right", style="blue")
    table.add_column("Rate (bps)", justify="right", style="yellow")
    table.add_column("Weight", justify="right", style="yellow")
    table.add_column("Start Time", justify="right", style="cyan")
    table.add_column("Finish Time", justify="right", style="cyan")
    table.add_column("Status", justify="center", style="red")

    for flow in flows:
        (
            flow_id,
            src_node_id,
            dst_node_id,
            flow_len_type,
            flow_len_bytes,
            flow_len_duration,
            flow_rate,
            flow_weight,
            start_time,
            finish_time,
            is_finished,
        ) = flow

        # Determine status
        if is_finished:
            status = "✓ Finished"
            status_style = "green"
        elif start_time is not None:
            status = "⟳ Running"
            status_style = "yellow"
        else:
            status = "⋯ Pending"
            status_style = "dim"

        table.add_row(
            str(flow_id),
            str(src_node_id),
            str(dst_node_id),
            flow_len_type,
            str(flow_len_bytes) if flow_len_bytes is not None else "-",
            str(flow_len_duration) if flow_len_duration is not None else "-",
            str(flow_rate) if flow_rate is not None else "-",
            str(flow_weight) if flow_weight is not None else "-",
            str(start_time) if start_time is not None else "-",
            str(finish_time) if finish_time is not None else "-",
            f"[{status_style}]{status}[/{status_style}]",
        )

    console.print(table)


def display_stats(total: int, active: int, finished: int, console: Console):
    stats_text = Text()
    stats_text.append(f"Total Flows: {total}\n", style="bold cyan")
    stats_text.append(f"Active Flows: {active}\n", style="bold green")
    stats_text.append(f"Finished Flows: {finished}", style="bold red")

    console.print(Panel(stats_text, title="Flow Statistics", style="blue"))


"""Hard-coded source and destination node IDs."""


def generate_random_flow() -> dict:
    flow = {
        "src_node_id": random.randint(1, 3),
        "dst_node_id": random.randint(1, 3),
        "flow_len_type": "bytes",
        "flow_rate": random.randint(100000, 10000000),  # 100KB to 10MB per second
        "flow_weight": random.randint(1, 100),
    }

    # Ensure source and destination nodes are not the same
    while flow["src_node_id"] == flow["dst_node_id"]:
        flow["dst_node_id"] = random.randint(1, 3)  # since now we have only 3 nodes

    flow["flow_len_bytes"] = random.randint(1000000, 100000000)  # 1MB to 100MB
    flow["flow_len_duration"] = None

    return flow


def main():
    # Parse algorithm mode
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        "--method",
        choices=[
            "dynamicAlloc",
            "weightAlloc",
            "multiRing",
            "dataAwareAlloc",
            "multiRingWeight",
            "dataAware",
            "equalAlloc",
            "equalOutOfOrderAlloc",
        ],
        default="dynamicAlloc",
        help="Choose an algorithm to use",
    )
    args = parser.parse_args()
    method = args.method

    # Launch the docker containers, database
    # launch_docker_compose()

    host = get_db_host()
    wait_for_postgres()
    console = Console()
    db = FlowDatabase()

    # Read the filename written during config conversion
    with open("flow_source.jsoninfo", "r") as f:
        json_filename = f.read().strip()
    base_name = Path(json_filename).stem

    # Run the optimization algorithm through new_run_experiment_nextmini.py if it's a non-concurrent-vs-concurrent folder script
    if method != "dataAware":
        try:
            print(f"Running algorithm: {method}")
            subprocess.run(
                [
                    "uv",
                    "run",
                    "../shared_scripts/new_run_experiment_nextmini.py",
                    "-r",
                    "results",
                    "-p",
                    base_name,
                    "-c",
                    str((Path.cwd() / json_filename).resolve()),
                    "-m",
                    method,
                ],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"Error running optimization script: {e}")
            sys.exit(1)

    # Else run the optimization algorithm through run_experiment.py if it's a stellar folder script
    else:
        # Run through run_experiment_nextmini.py for dataAwareAlloc
        try:
            subprocess.run(
                [
                    "uv",
                    "run",
                    "../shared_scripts/run_experiment_nextmini.py",
                    "-r",
                    "results",
                    "-p",
                    base_name,
                    "-c",
                    str((Path.cwd() / json_filename).resolve()),
                    "-o",
                    str((Path.cwd() / f"{base_name}_optimization.json").resolve()),
                    "-m",
                    "dataAwareAlloc",
                ],
                check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"Error running dataAwareAlloc optimization: {e}")
            sys.exit(1)

    # Load the output of optimization
    result_json_path = Path("results") / base_name / method / "result.json"
    with open(result_json_path, "r") as f:
        result = json.load(f)
    flow_rates = result["flow_rate"]
    if method == "weightAlloc":
        weights = result["weights"]

        # We must convert the double 0->1 weights into int weights having similar ratios
        fractions = [
            Fraction(w).limit_denominator(1000000) for w in weights.values()
        ]  # Convert weights to approximate fractions
        if any(f == 0 for f in fractions):
            raise ValueError(
                "One or more weights rounded to zero after conversion to fractions"
            )
        lcm = np.lcm.reduce([f.denominator for f in fractions])
        int_weights_list = [
            int(f.numerator * (lcm // f.denominator)) for f in fractions
        ]  # Convert the weights to integers

        # Reduce to smallest integer ratios
        g = np.gcd.reduce(int_weights_list)
        if g > 1:
            int_weights_list = [w // g for w in int_weights_list]

        int_weights = dict(zip(weights.keys(), int_weights_list))

    # Initialize the flow dependency manager class which parses from the original JSON in the folder
    dep_manager = DependencyManager(json_filename, db)
    dep_manager.load_config()

    # Dicts to track average collective completion time
    collective_to_flows: Dict[str, List[str]] = defaultdict(list)
    flow_finish_times: Dict[str, float] = {}
    collective_finish_times: Dict[str, float] = {}

    for fid, flow in dep_manager.flow_map.items():
        k = flow["collective_id"]
        collective_to_flows[str(k)].append(fid)

    # Add to enforce the artificial dependencies that come from non-concurrent algorithms
    if method in ("multiRing" or "multiRingWeight"):
        flow_dependencies = result.get("flow_dependencies", {})
        for fid, deps in flow_dependencies.items():
            fid_str = str(fid)
            if fid_str not in dep_manager.dep_graph:
                dep_manager.dep_graph[fid_str] = set()
            dep_manager.dep_graph[fid_str].update(str(d) for d in deps)

    for fid, flow in dep_manager.flow_map.items():
        k = flow["collective_id"]
        n = flow["group_id"]
        group_key = f"{k}_{n}"
        if method == "weightAlloc":
            flow["flow_weight"] = 1  # int_weights[group_key]
        else:
            flow["flow_weight"] = 1
        flow["bps"] = int(flow_rates[fid] * 1024 * 1024 * 8)

    # Insert all dependency-free flows initially
    for flow_id, deps in dep_manager.dep_graph.items():
        if not deps:
            dep_manager.insert_flow(flow_id)
            console.print(
                f"\n[bold green]Inserted new flow with ID: {flow_id}[/bold green]"
            )

    # Continually insert flows whose dependencies have completed
    start_time = None
    last_active_count = None
    last_progress_time = time.time()
    try:
        elapsed = 0.0
        avg_collective_completion_time = 0.0
        while True:
            # Print the current time
            console.clear()
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")
            console.print(f"[bold blue]Current Time: {current_time}[/bold blue]\n")

            # Record flow end times and display all flows both completed and in transmission
            flows = db.get_all_flows()
            for flow_row in flows:
                db_id, *_, is_finished = flow_row
                if is_finished and str(db_id) not in flow_finish_times:
                    if start_time is not None:
                        flow_finish_times[str(db_id)] = time.time() - start_time

            display_flows(flows, console)
            console.print()
            total, active, finished = db.get_flow_stats()

            # Start the timer
            if start_time is None and total > 0:
                start_time = time.time()
            display_stats(total, active, finished, console)

            if start_time is None and total > 0:
                start_time = time.time()
            display_stats(total, active, finished, console)

            # Early exit tracking
            if last_active_count is None:
                last_active_count = active
                last_progress_time = time.time()
            elif active != last_active_count:
                last_active_count = active
                last_progress_time = time.time()
            elif time.time() - last_progress_time > 30:
                console.print(
                    "[bold red]Early exit triggered due to lack of progress for 30 seconds.[/bold red]"
                )
                end_time = time.time()
                if start_time is not None:
                    elapsed = end_time - start_time

                for collective_id, fids in collective_to_flows.items():
                    latest = 0.0
                    for fid in fids:
                        db_id = dep_manager.flow_db_ids.get(fid)
                        if db_id is not None:
                            finish = flow_finish_times.get(str(db_id))
                            if finish is not None:
                                latest = max(latest, finish)
                    collective_finish_times[collective_id] = latest

                avg_collective_completion_time = np.mean(
                    list(collective_finish_times.values())
                )
                objective_output_dir = Path("results") / base_name / method
                objective_output_dir.mkdir(parents=True, exist_ok=True)
                with open(objective_output_dir / "objective_time.json", "w") as f:
                    json.dump(
                        {
                            "total_flow_completion_time": elapsed,
                            "avg_collective_completion_time": avg_collective_completion_time,
                            "early_exit": True,
                        },
                        f,
                        indent=4,
                    )
                break

            # Insert any flows which can now be inserted after dependent flows have completed
            dep_manager.maybe_insert_dependent_flows()
            total, active, finished = db.get_flow_stats()

            # End the timer if all flows are complete
            if total > 0 and finished == total and start_time is not None:
                end_time = time.time()
                elapsed = end_time - start_time
                console.print(
                    f"\nCompletion time of all flows occured in {elapsed:.2f} seconds."
                )

                # Compute collective completion times
                for collective_id, fids in collective_to_flows.items():
                    latest = 0.0
                    for fid in fids:
                        db_id = dep_manager.flow_db_ids.get(fid)
                        if db_id is not None:
                            db_id_str = str(db_id)
                            finish = flow_finish_times.get(db_id_str)
                            if finish is not None:
                                latest = max(latest, finish)
                    collective_finish_times[collective_id] = latest

                avg_collective_completion_time = np.mean(
                    list(collective_finish_times.values())
                )
                console.print(
                    f"Average collective completion time: {avg_collective_completion_time:.2f} seconds."
                )

                # Save both metrics to file
                objective_output_dir = Path("results") / base_name / method
                objective_output_dir.mkdir(parents=True, exist_ok=True)
                objective_file_path = objective_output_dir / "objective_time.json"

                with open(objective_file_path, "w") as f:
                    json.dump(
                        {
                            "total_flow_completion_time": elapsed,
                            "avg_collective_completion_time": avg_collective_completion_time,
                            "early_exit": False,
                        },
                        f,
                        indent=4,
                    )
                break

            # waits for 1 second before the next iteration.
            time.sleep(1)

    # Handle user and error exit
    except KeyboardInterrupt:
        console.print("\n\n[bold yellow]Flow Manager stopped by user[/bold yellow]")
    except Exception as e:
        console.print(f"\n\n[bold red]Error: {e}[/bold red]")
    finally:
        db.close()
        console.print("[dim]Database connection closed[/dim]")


if __name__ == "__main__":
    main()
