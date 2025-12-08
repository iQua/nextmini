"""
This script is just a simple example of how to insert flows into the Nextmini Controller database.
"""

#!/usr/bin/env python3
"""
Continuously displays flows and inserts new flows every 5 seconds.
"""

import random
import sys
import time
from typing import List, Optional, Tuple

import psycopg2
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


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
                      flow_weight, route_id, start_time, finish_time, is_finished
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
        route_id: Optional[int] = None,
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
                                 flow_weight, route_id, is_finished)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    route_id,
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
    table.add_column("Route", justify="right", style="magenta")
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
            route_id,
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
            str(route_id) if route_id is not None else "-",
            str(start_time) if start_time is not None else "-",
            str(finish_time) if finish_time is not None else "-",
            f"[{status_style}]{status}[/{status_style}]",
        )

    console.print(table)


def display_stats(total: int, active: int, finished: int, console: Console):
    stats_text = Text()
    stats_text.append(f"Active Flows: {active}\n", style="bold green")
    stats_text.append(f"Finished Flows: {finished}", style="bold red")

    console.print(Panel(stats_text, title="Flow Statistics", style="blue"))


def generate_random_flow(max_nodes: int = 2) -> dict:
    """Generate a random flow specification.

    Args:
        max_nodes: Maximum node ID (default 2 for 2-node setup)

    Returns:
        Flow specification dict
    """
    flow_types = ["bytes", "duration"]
    flow_type = random.choice(flow_types)

    src_node_id = random.randint(1, max_nodes)
    dst_node_id = random.randint(1, max_nodes)

    # Ensure source and destination nodes are not the same
    while src_node_id == dst_node_id:
        dst_node_id = random.randint(1, max_nodes)
    flow = {
        "src_node_id": src_node_id,
        "dst_node_id": dst_node_id,
        "flow_len_type": flow_type,
        "flow_weight": random.randint(1, 10),
    }

    if flow_type == "bytes":
        flow["flow_len_bytes"] = random.randint(5_000_000, 20_000_000)  # 5MB to 20MB
        flow["flow_len_duration"] = None
        flow["flow_rate"] = None  # Let dataplane use default rate
    else:
        flow["flow_len_bytes"] = None
        flow["flow_len_duration"] = round(random.uniform(3.0, 8.0), 2)  # 3 to 8 seconds
        flow["flow_rate"] = random.randint(
            1_000_000, 3_000_000
        )  # 1-3 Mbps for duration flows

    return flow


def main():
    console = Console()
    db = FlowDatabase()

    console.print(Panel("User-Space Flow Insertion Tool", style="bold green"))
    console.print("[bold yellow]Prerequisites:[/bold yellow]")
    console.print("  1. Controller must be running")
    console.print("  2. Dataplane nodes must be connected")
    console.print("  3. Routes must be configured\n")
    console.print(
        "[dim]This script inserts flows every 5 seconds and monitors their status[/dim]"
    )
    console.print("[dim]Press Ctrl+C to stop[/dim]\n")

    try:
        while True:
            console.clear()

            current_time = time.strftime("%Y-%m-%d %H:%M:%S")
            console.print(f"[bold blue]Current Time: {current_time}[/bold blue]\n")

            flows = db.get_all_flows()
            display_flows(flows, console)

            console.print()
            total, active, finished = db.get_flow_stats()
            display_stats(total, active, finished, console)

            new_flow = generate_random_flow()
            flow_id = db.insert_flow(**new_flow)

            if flow_id:
                console.print(
                    f"\n[bold green]Inserted new flow with ID: {flow_id}[/bold green]"
                )
                console.print(
                    f"   [dim]Source: {new_flow['src_node_id']} → Destination: {new_flow['dst_node_id']}[/dim]"
                )
                console.print(
                    f"   [dim]Type: {new_flow['flow_len_type']}, Rate: {new_flow['flow_rate']} bps[/dim]"
                )
            else:
                console.print("\n[bold red]Failed to insert new flow[/bold red]")

            console.print("\n[dim]Next update in 5 seconds...[/dim]")

            # waits for 5 seconds before the next iteration.
            time.sleep(5)

    except KeyboardInterrupt:
        console.print("\n\n[bold yellow]Flow Manager stopped by user[/bold yellow]")
    except Exception as e:
        console.print(f"\n\n[bold red]Error: {e}[/bold red]")
    finally:
        db.close()
        console.print("[dim]Database connection closed[/dim]")


if __name__ == "__main__":
    main()
