import os
from datetime import datetime
from time import sleep

import psycopg2
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

DB_USER = os.environ.get("NEXTMINI_DB_USER", "pgusr")
DB_PASSWORD = os.environ.get("NEXTMINI_DB_PASSWORD", "pgpwrd")
DB_HOST = os.environ.get("NEXTMINI_DB_HOST", "localhost")
DB_PORT = os.environ.get("NEXTMINI_DB_PORT", "5432")
DB_NAME = os.environ.get("NEXTMINI_DB_NAME", "nextmini")


def format_rate(rate_mbps):
    """Format rate with colors based on value"""
    if rate_mbps >= 100:
        return Text(f"{rate_mbps}", style="bold red")
    elif rate_mbps >= 10:
        return Text(f"{rate_mbps}", style="bold yellow")
    elif rate_mbps >= 1:
        return Text(f"{rate_mbps}", style="bold green")
    else:
        return Text(f"{rate_mbps}", style="dim")


def format_bytes(bytes_val):
    """Format bytes with appropriate unit"""
    if bytes_val >= 1e9:
        return f"{bytes_val/1e9} GB"
    elif bytes_val >= 1e6:
        return f"{bytes_val/1e6} MB"
    elif bytes_val >= 1e3:
        return f"{bytes_val/1e3} KB"
    else:
        return f"{bytes_val} B"


def format_timestamp_ms(timestamp_ms):
    if timestamp_ms:
        try:
            ts = datetime.fromtimestamp(timestamp_ms / 1000.0)
            return ts.strftime("%H:%M:%S.%f")[:-3]
        except (OSError, ValueError, OverflowError):
            return "-"
    return "-"


def format_duration_ms(start_ms, finish_ms):
    if start_ms and finish_ms:
        duration = finish_ms - start_ms
        if duration < 0:
            return None
        return duration
    return None


def styled_duration(duration):
    if duration is None:
        return "[dim]-[/dim]"
    if duration < 1000:
        return f"[green]{duration}[/green]"
    if duration < 5000:
        return f"[yellow]{duration}[/yellow]"
    return f"[red]{duration}[/red]"


def styled_timestamp(timestamp_ms):
    value = format_timestamp_ms(timestamp_ms)
    if value == "-":
        return "[dim]-[/dim]"
    return f"[dim]{value}[/dim]"


class Database:
    def __init__(self):
        self.connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
        )
        self.connection.autocommit = (
            True  # Enable autocommit to avoid transaction issues
        )
        self.t_node = None
        self.t_link = None
        self.t_flow = None
        self.t_app_flows = None
        self.t_user_flows = None

    def update_t_node(self):
        cursor = self.connection.cursor()
        # Get total sent per node (local_node_id represents the sender)
        query = """
            SELECT local_node_id, SUM(bytes * 8.0 / 5.0) AS total_rate_bps
            FROM metrics
            WHERE time_read >= NOW() - INTERVAL '5 seconds'
            GROUP BY local_node_id
            ORDER BY local_node_id ASC;
        """
        cursor.execute(query)
        sent = cursor.fetchall()

        # Get total received per node (remote_node_id represents the receiver)
        query = """
            SELECT remote_node_id, SUM(bytes * 8.0 / 5.0) AS total_rate_bps
            FROM metrics
            WHERE time_read >= NOW() - INTERVAL '5 seconds'
            GROUP BY remote_node_id
            ORDER BY remote_node_id ASC;
        """
        cursor.execute(query)
        recv = cursor.fetchall()

        sent_dict = {node_id: rate_bps or 0 for node_id, rate_bps in sent}
        recv_dict = {node_id: rate_bps or 0 for node_id, rate_bps in recv}
        all_nodes = set(sent_dict.keys()) | set(recv_dict.keys())

        self.t_node = Table(title="Data Rate Per Node", show_header=True)
        self.t_node.add_column("Node ID", justify="center")
        self.t_node.add_column("Sent Rate (Mbps)", justify="center")
        self.t_node.add_column("Recv Rate (Mbps)", justify="center")

        for node_id in sorted(all_nodes):
            sent_rate = sent_dict.get(node_id, 0)
            recv_rate = recv_dict.get(node_id, 0)
            self.t_node.add_row(
                str(node_id),
                str(float(sent_rate) / 1000000.0),
                str(float(recv_rate) / 1000000.0),
            )
        cursor.close()

    def update_t_link(self):
        cursor = self.connection.cursor()

        query = """
            SELECT local_node_id, remote_node_id,
                   SUM(bytes * 8.0 / 5.0) AS total_rate_bps
            FROM metrics
            WHERE time_read >= NOW() - INTERVAL '5 seconds'
            GROUP BY local_node_id, remote_node_id
            HAVING COUNT(*) > 0
            ORDER BY total_rate_bps DESC;
        """
        cursor.execute(query)
        metrics = cursor.fetchall()

        self.t_link = Table(title="Data Rate Per Link", show_header=True)
        self.t_link.add_column("Source Node ID", justify="center")
        self.t_link.add_column("Destination Node ID", justify="center")
        self.t_link.add_column("Rate (Mbps)", justify="center")

        for src, dst, rate_bps in metrics:
            rate_mbps = float(rate_bps or 0) / 1000000.0
            self.t_link.add_row(str(src), str(dst), str(rate_mbps))
        cursor.close()

    def update_t_flow(self):
        cursor = self.connection.cursor()

        query = """
            SELECT format('%s.%s.%s.%s:%s → %s.%s.%s.%s:%s',
                          get_byte(flow_id,0), get_byte(flow_id,1), get_byte(flow_id,2), get_byte(flow_id,3),
                          (get_byte(flow_id,8)::int << 8) + get_byte(flow_id,9),
                          get_byte(flow_id,4), get_byte(flow_id,5), get_byte(flow_id,6), get_byte(flow_id,7),
                          (get_byte(flow_id,10)::int << 8) + get_byte(flow_id,11)
                   ) AS flow_tuple,
                   local_node_id,
                   remote_node_id,
                   SUM(bytes * 8.0 / 5.0) AS total_rate_bps
            FROM metrics
            WHERE time_read >= NOW() - INTERVAL '5 seconds'
            GROUP BY flow_id, local_node_id, remote_node_id
            HAVING COUNT(*) > 0
            ORDER BY total_rate_bps DESC
            LIMIT 20;
        """
        cursor.execute(query)
        metrics = cursor.fetchall()

        self.t_flow = Table(
            title="Data Rate Per Flow (Last 5s)", show_header=True
        )
        self.t_flow.add_column("Flow ID", overflow="fold", style="dim")
        self.t_flow.add_column("Local→Remote", justify="center")
        self.t_flow.add_column("Rate (Mbps)", justify="center")

        for flow_tuple, local_node, remote_node, total_rate_bps in metrics:
            rate_mbps = float(total_rate_bps or 0) / 1000000.0

            self.t_flow.add_row(
                flow_tuple or "[dim]N/A[/dim]",
                f"[cyan]{local_node}[/cyan]→[magenta]{remote_node}[/magenta]",
                format_rate(rate_mbps),
            )
        cursor.close()

    def update_t_app_flows(self):
        cursor = self.connection.cursor()

        query = """
            SELECT af.id,
                   format('%s.%s.%s.%s:%s → %s.%s.%s.%s:%s',
                          get_byte(af.flow_id,0), get_byte(af.flow_id,1), get_byte(af.flow_id,2), get_byte(af.flow_id,3),
                          (get_byte(af.flow_id,8)::int << 8) + get_byte(af.flow_id,9),
                          get_byte(af.flow_id,4), get_byte(af.flow_id,5), get_byte(af.flow_id,6), get_byte(af.flow_id,7),
                          (get_byte(af.flow_id,10)::int << 8) + get_byte(af.flow_id,11)
                   ) AS flow_tuple,
                   af.src_node_id,
                   af.dst_node_id,
                   af.route_id,
                   af.is_finished,
                   af.time,
                   af.finish_time
            FROM app_flows af
            WHERE af.src_node_id IS NOT NULL
            ORDER BY af.id DESC
            LIMIT 30;
        """
        cursor.execute(query)
        flows = cursor.fetchall()

        self.t_app_flows = Table(
            title="App Flows (TUN Interface)", show_header=True
        )
        self.t_app_flows.add_column("ID", justify="left", style="bold")
        self.t_app_flows.add_column("Flow ID", overflow="fold", style="dim")
        self.t_app_flows.add_column("Src→Dst", justify="center")
        self.t_app_flows.add_column("Route", justify="center")
        self.t_app_flows.add_column("Start Time", justify="center", style="dim")
        self.t_app_flows.add_column("Finish Time", justify="center", style="dim")
        self.t_app_flows.add_column("Duration (ms)", justify="center")
        self.t_app_flows.add_column("Status", justify="center")

        for flow_id_int, flow_tuple, src, dst, route_id, is_finished, time_ms, finish_time_ms in flows:
            src_dst = (
                f"[cyan]{src}[/cyan]→[magenta]{dst}[/magenta]"
                if src and dst
                else "[dim]N/A[/dim]"
            )
            route_str = (
                f"[yellow]{route_id}[/yellow]" if route_id else "[dim]-[/dim]"
            )
            
            start_time_display = styled_timestamp(time_ms)
            finish_time_display = styled_timestamp(finish_time_ms)

            duration_value = (
                format_duration_ms(time_ms, finish_time_ms) if is_finished else None
            )
            duration_display = styled_duration(duration_value)

            if is_finished:
                status_mark = "[green]✓[/green]"
                id_style = "dim"
            else:
                status_mark = "[red]✗[/red]"
                id_style = "bold cyan"

            self.t_app_flows.add_row(
                f"[{id_style}]{flow_id_int}[/{id_style}]",
                flow_tuple or "[dim]N/A[/dim]",
                src_dst,
                route_str,
                start_time_display,
                finish_time_display,
                duration_display,
                status_mark,
            )
        cursor.close()

    def update_t_user_flows(self):
        cursor = self.connection.cursor()

        query = """
            SELECT f.id,
                   f.src_node_id,
                   f.dst_node_id,
                   f.flow_len_type,
                   f.flow_len_bytes,
                   f.flow_len_duration,
                   f.flow_rate,
                   f.flow_weight,
                   f.is_finished,
                   f.start_time,
                   f.finish_time
            FROM flows f
            ORDER BY f.id DESC
            LIMIT 30;
        """
        cursor.execute(query)
        flows = cursor.fetchall()

        self.t_user_flows = Table(
            title="User-space Flows (Configured)", show_header=True
        )
        self.t_user_flows.add_column("ID", justify="left", style="bold")
        self.t_user_flows.add_column("Src→Dst", justify="center")
        self.t_user_flows.add_column("Start Time", justify="center", style="dim")
        self.t_user_flows.add_column("Finish Time", justify="center", style="dim")
        self.t_user_flows.add_column("Duration (ms)", justify="center")
        self.t_user_flows.add_column("Length", justify="center")
        self.t_user_flows.add_column("Rate", justify="center")
        self.t_user_flows.add_column("Weight", justify="center")
        self.t_user_flows.add_column("Status", justify="center")

        for (
            fid,
            src,
            dst,
            len_type,
            len_bytes,
            len_duration,
            rate,
            weight,
            is_finished,
            start_time,
            finish_time,
        ) in flows:
            src_dst = f"[cyan]{src}[/cyan]→[magenta]{dst}[/magenta]"

            if len_type == "bytes":
                length = (
                    f"[green]{format_bytes(len_bytes)}[/green]"
                    if len_bytes
                    else "[dim]-[/dim]"
                )
            elif len_type == "duration":
                length = (
                    f"[yellow]{len_duration}s[/yellow]"
                    if len_duration
                    else "[dim]-[/dim]"
                )
            else:
                length = "[dim]-[/dim]"

            rate_str = (
                f"[blue]{format_bytes(rate)}/s[/blue]"
                if rate
                else "[dim]-[/dim]"
            )
            weight_str = (
                f"[magenta]{weight}[/magenta]" if weight else "[dim]-[/dim]"
            )

            if is_finished:
                finished_mark = "[green]✓[/green]"
                id_style = "dim"
            else:
                finished_mark = "[red]✗[/red]"
                id_style = "bold cyan"

            start_time_str = format_timestamp_ms(start_time)
            start_display = styled_timestamp(start_time)
            finish_display = styled_timestamp(finish_time)

            self.t_user_flows.add_row(
                f"[{id_style}]{fid}[/{id_style}]",
                src_dst,
                start_display,
                finish_display,
                styled_duration(
                    format_duration_ms(start_time, finish_time)
                    if is_finished
                    else None
                ),
                length,
                rate_str,
                weight_str,
                finished_mark,
            )
        cursor.close()


def show(live):
    db = Database()
    try:
        db.update_t_app_flows()
        db.update_t_user_flows()
        db.update_t_node()
        db.update_t_link()
        db.update_t_flow()

        layout = Table.grid(padding=(0, 1))
        layout.add_row(
            Panel(db.t_app_flows, border_style="cyan", padding=(1, 2))
        )
        layout.add_row(
            Panel(db.t_user_flows, border_style="green", padding=(1, 2))
        )
        layout.add_row(Panel(db.t_node, border_style="yellow", padding=(1, 2)))
        layout.add_row(Panel(db.t_link, border_style="magenta", padding=(1, 2)))
        layout.add_row(Panel(db.t_flow, border_style="blue", padding=(1, 2)))

        # Create status footer
        status_text = Text()
        status_text.append("Updated: ", style="bold yellow")
        status_text.append(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), style="bold cyan"
        )
        status_text.append(" | DB: ", style="bold yellow")
        status_text.append(f"{DB_HOST}:{DB_PORT}/{DB_NAME}", style="bold green")
        status_text.append(" | ", style="bold yellow")
        status_text.append("NextMini Dashboard", style="bold magenta")

        layout.add_row(Panel(status_text, border_style="white", padding=(0, 2)))

        live.update(layout)

    except Exception as error:
        error_text = Text()
        error_text.append("ERROR: ", style="bold red")
        error_text.append(str(error), style="red")
        live.update(Panel(error_text, border_style="red", padding=(1, 2)))


if __name__ == "__main__":
    from rich.align import Align
    from rich.live import Live

    console = Console()

    # Display startup banner
    banner = Text()
    banner.append("\n", style="bold yellow")
    banner.append("NextMini Network Dashboard", style="bold magenta")
    banner.append("\n", style="bold yellow")
    banner.append(
        f"Connected to: {DB_HOST}:{DB_PORT}/{DB_NAME}\n", style="cyan"
    )
    banner.append("Press Ctrl+C to exit\n", style="dim")

    console.print(
        Panel(
            Align.center(banner), border_style="bright_magenta", padding=(1, 2)
        )
    )
    sleep(1)

    with Live(console=console, refresh_per_second=2) as live:
        while True:
            show(live)
            sleep(0.5)
