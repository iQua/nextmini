#!/usr/bin/env python3
"""
This script inserts two sequential flows from node1 to node2, waiting for the first flow
to complete before inserting the second flow.
"""

import sys
import time
from typing import Optional, List, Tuple
import psycopg2
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.live import Live


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
                database="nextmini"
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
                       flow_weight, is_finished
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

    def is_flow_finished(self, flow_id: int) -> bool:
        """Check if a specific flow has finished"""
        cursor = self.connection.cursor()
        try:
            query = "SELECT is_finished FROM flows WHERE id = %s"
            cursor.execute(query, (flow_id,))
            result = cursor.fetchone()
            return result[0] if result else False
        except psycopg2.Error as e:
            print(f"Error checking flow status: {e}")
            return False
        finally:
            cursor.close()

    def insert_flow(self, src_node_id: int, dst_node_id: int, flow_len_type: str,
                   flow_len_bytes: Optional[int] = None, flow_len_duration: Optional[float] = None,
                   flow_rate: Optional[int] = None, flow_weight: Optional[int] = None) -> Optional[int]:
        """Insert a new flow into the database."""
        cursor = self.connection.cursor()
        try:
            if flow_len_type not in ['bytes', 'duration']:
                print("Error: flow_len_type must be 'bytes' or 'duration'")
                return None

            if flow_len_type == 'bytes' and flow_len_bytes is None:
                print("Error: flow_len_bytes must be provided when flow_len_type is 'bytes'")
                return None

            if flow_len_type == 'duration' and flow_len_duration is None:
                print("Error: flow_len_duration must be provided when flow_len_type is 'duration'")
                return None

            query = """
                INSERT INTO flows (src_node_id, dst_node_id, flow_len_type,
                                 flow_len_bytes, flow_len_duration, flow_rate,
                                 flow_weight, is_finished)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """

            cursor.execute(query, (
                src_node_id, dst_node_id, flow_len_type,
                flow_len_bytes, flow_len_duration, flow_rate,
                flow_weight, False
            ))

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


def create_flow(src_node_id: int, dst_node_id: int, is_bytes: bool = True,
                flow_len: Optional[int] = 1000000, duration: Optional[float] = None,
                flow_weight: Optional[int] = 1) -> dict:
    """Create a flow specification"""
    flow_type = 'bytes' if is_bytes else 'duration'

    flow = {
        'src_node_id': src_node_id,
        'dst_node_id': dst_node_id,
        'flow_len_type': flow_type,
        'flow_rate': None,  # Not setting flow_rate as requested
        'flow_weight': flow_weight
    }

    if is_bytes:
        flow['flow_len_bytes'] = flow_len
        flow['flow_len_duration'] = None
    else:
        flow['flow_len_bytes'] = None
        flow['flow_len_duration'] = duration or 5.0  # Default to 5 seconds if not specified

    return flow


def display_flows(flows: List[Tuple], console: Console) -> Table:
    """Create a table displaying flows information"""
    table = Table(title="Flows Table")
    table.add_column("ID", justify="center", style="cyan")
    table.add_column("Source Node", justify="center", style="green")
    table.add_column("Dest Node", justify="center", style="green")
    table.add_column("Type", justify="center", style="magenta")
    table.add_column("Bytes", justify="right", style="blue")
    table.add_column("Duration (s)", justify="right", style="blue")
    table.add_column("Rate (bps)", justify="right", style="yellow")
    table.add_column("Weight", justify="right", style="yellow")
    table.add_column("Finished", justify="center", style="red")

    if not flows:
        table.add_row("No flows found", "", "", "", "", "", "", "", "")
        return table

    for flow in flows:
        (flow_id, src_node_id, dst_node_id, flow_len_type,
         flow_len_bytes, flow_len_duration, flow_rate,
         flow_weight, is_finished) = flow

        table.add_row(
            str(flow_id),
            str(src_node_id),
            str(dst_node_id),
            flow_len_type,
            str(flow_len_bytes) if flow_len_bytes is not None else "-",
            str(flow_len_duration) if flow_len_duration is not None else "-",
            str(flow_rate) if flow_rate is not None else "-",
            str(flow_weight) if flow_weight is not None else "-",
            "✓" if is_finished else "✗"
        )

    return table


def main():
    console = Console()
    db = FlowDatabase()

    # Clear the console and show header
    console.clear()
    console.print(Panel("Sequential Flow Test: node1->node2", style="bold green"))
    console.print("This script will insert two flows from node1 to node2 sequentially.\n")
    console.print("First flow will be inserted immediately.\n")
    console.print("Second flow will be inserted immediately after the first flow completes.\n")

    # Define flow parameters - small enough to complete quickly
    flow1_params = create_flow(
        src_node_id=1,
        dst_node_id=2,
        is_bytes=True,
        flow_len=2000000000,  # 2MB
        flow_weight=1
    )

    # Insert the first flow
    flow1_id = db.insert_flow(**flow1_params)

    if not flow1_id:
        console.print("\n[bold red]Failed to insert the first flow![/bold red]")
        db.close()
        return

    console.print(f"\n[bold green]First flow inserted with ID: {flow1_id}[/bold green]")
    console.print(f"Source: 1 → Destination: 2")
    console.print(f"Type: {flow1_params['flow_len_type']}, Size: {flow1_params['flow_len_bytes']} bytes")

    # Wait for the first flow to complete while showing status
    console.print("\n[bold yellow]Waiting for the first flow to complete...[/bold yellow]")

    with Live(console=console, refresh_per_second=2) as live:
        is_finished = False
        start_time = time.time()
        poll_interval = 0.5  # seconds

        while not is_finished:
            # Check if flow is finished
            is_finished = db.is_flow_finished(flow1_id)

            # Update display
            flows = db.get_all_flows()
            table = display_flows(flows, console)

            elapsed = time.time() - start_time
            status_text = Text()
            status_text.append(f"Waiting for flow {flow1_id} to complete...\n", style="bold yellow")
            status_text.append(f"Elapsed time: {elapsed:.2f} seconds\n", style="dim")

            # Combine into a renderable group
            live.update(Panel.fit(table, title=f"Flow Status (Updated: {time.strftime('%H:%M:%S')})",
                              subtitle=f"Elapsed: {elapsed:.2f}s"))

            if is_finished:
                break

            time.sleep(poll_interval)

    # First flow is now complete
    console.print(f"\n[bold green]First flow (ID: {flow1_id}) has completed![/bold green]")
    console.print("\n[bold yellow]Inserting the second flow immediately...[/bold yellow]")

    # Insert the second flow with the same parameters but a different size
    flow2_params = create_flow(
        src_node_id=1,
        dst_node_id=2,
        is_bytes=True,
        flow_len=3000000000,  # (slightly different from first flow)
        flow_weight=1
    )

    flow2_id = db.insert_flow(**flow2_params)

    if not flow2_id:
        console.print("\n[bold red]Failed to insert the second flow![/bold red]")
        db.close()
        return

    console.print(f"\n[bold green]Second flow inserted with ID: {flow2_id}[/bold green]")
    console.print(f"Source: 1 → Destination: 2")
    console.print(f"Type: {flow2_params['flow_len_type']}, Size: {flow2_params['flow_len_bytes']} bytes")

    # Monitor the second flow
    console.print("\n[bold yellow]Monitoring the second flow...[/bold yellow]")

    with Live(console=console, refresh_per_second=2) as live:
        is_finished = False
        start_time = time.time()
        timeout = 60  # seconds
        poll_interval = 0.5  # seconds

        while not is_finished and (time.time() - start_time < timeout):
            # Check if flow is finished
            is_finished = db.is_flow_finished(flow2_id)

            # Update display
            flows = db.get_all_flows()
            table = display_flows(flows, console)

            elapsed = time.time() - start_time
            status_text = Text()
            status_text.append(f"Monitoring flow {flow2_id}...\n", style="bold yellow")
            status_text.append(f"Elapsed time: {elapsed:.2f} seconds\n", style="dim")

            # Combine into a renderable group
            live.update(Panel.fit(table, title=f"Flow Status (Updated: {time.strftime('%H:%M:%S')})",
                              subtitle=f"Elapsed: {elapsed:.2f}s"))

            if is_finished:
                break

            time.sleep(poll_interval)

    # Final status report
    if is_finished:
        console.print(f"\n[bold green]Second flow (ID: {flow2_id}) has completed successfully![/bold green]")
    else:
        console.print(f"\n[bold red]Second flow (ID: {flow2_id}) did not complete within the timeout period![/bold red]")
        console.print("[bold yellow]This may indicate a problem with sequential flows between the same nodes.[/bold yellow]")

    # Display all flows one last time
    flows = db.get_all_flows()
    final_table = display_flows(flows, console)
    console.print(Panel.fit(final_table, title="Final Flow Status"))

    # Clean up
    db.close()
    console.print("\n[dim]Test completed. Database connection closed.[/dim]")


if __name__ == "__main__":
    main()
