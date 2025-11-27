import os
from datetime import datetime, timezone
from time import sleep

import psycopg2
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

DB_USER = os.environ.get("NEXTMINI_DB_USER", "pgusr")
DB_PASSWORD = os.environ.get("NEXTMINI_DB_PASSWORD", "pgpwrd")
DB_HOST = os.environ.get("NEXTMINI_DB_HOST", "localhost")
DB_PORT = os.environ.get("NEXTMINI_DB_PORT", "5432")
DB_NAME = os.environ.get("NEXTMINI_DB_NAME", "nextmini")


def fmt_rtt(rtt_ms):
    if rtt_ms is None:
        return Text("-", style="dim")
    if rtt_ms < 5:
        return Text(f"{rtt_ms:.2f}", style="green")
    if rtt_ms < 20:
        return Text(f"{rtt_ms:.2f}", style="yellow")
    return Text(f"{rtt_ms:.2f}", style="red")


def fmt_loss(loss_pct):
    if loss_pct is None:
        return Text("-", style="dim")
    if loss_pct == 0:
        return Text("0.0", style="green")
    if loss_pct < 5:
        return Text(f"{loss_pct:.2f}", style="yellow")
    return Text(f"{loss_pct:.2f}", style="red")


def fmt_mbps(mbps):
    if mbps is None:
        return Text("-", style="dim")
    if mbps >= 100:
        return Text(f"{mbps:.2f}", style="bold red")
    if mbps >= 10:
        return Text(f"{mbps:.2f}", style="yellow")
    if mbps >= 1:
        return Text(f"{mbps:.2f}", style="green")
    return Text(f"{mbps:.2f}", style="dim")


class LinkDb:
    def __init__(self):
        self.conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
        )
        self.conn.autocommit = True

    def latest_links(self):
        """Return the latest sample per src/dst."""
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ON (src_node_id, dst_node_id)
                src_node_id,
                dst_node_id,
                rtt_ms,
                loss_pct,
                mbps,
                samples,
                time_read
            FROM link_measurements
            ORDER BY src_node_id, dst_node_id, time_read DESC;
            """
        )
        rows = cur.fetchall()
        cur.close()
        return rows


def render_table(rows):
    tbl = Table(
        title="Link Probes (latest per edge)",
        show_header=True,
        header_style="bold",
    )
    tbl.add_column("Src", justify="center", style="cyan")
    tbl.add_column("Dst", justify="center", style="magenta")
    tbl.add_column("RTT (ms)", justify="right")
    tbl.add_column("Loss (%)", justify="right")
    tbl.add_column("Mbps", justify="right")
    tbl.add_column("Samples", justify="right", style="dim")
    tbl.add_column("Age (s)", justify="right", style="dim")

    now = datetime.now(timezone.utc)
    for src, dst, rtt_ms, loss_pct, mbps, samples, ts in rows:
        age_s = (now - ts).total_seconds()
        age_style = "green" if age_s < 15 else "yellow" if age_s < 60 else "red"
        tbl.add_row(
            str(src),
            str(dst),
            fmt_rtt(rtt_ms),
            fmt_loss(loss_pct),
            fmt_mbps(mbps),
            str(samples or 0),
            Text(f"{age_s:.1f}", style=age_style),
        )
    return tbl


def main():
    console = Console()
    db = LinkDb()

    banner = Text()
    banner.append("\nNextmini Link Probe Dashboard\n", style="bold magenta")
    banner.append(
        f"DB: {DB_HOST}:{DB_PORT}/{DB_NAME}\n", style="cyan",
    )
    banner.append("Press Ctrl+C to exit\n", style="dim")
    console.print(Panel(banner, border_style="bright_magenta", padding=(1, 2)))

    with Live(console=console, refresh_per_second=2) as live:
        while True:
            rows = db.latest_links()
            live.update(Panel(render_table(rows), border_style="blue", padding=(1, 2)))
            sleep(1)


if __name__ == "__main__":
    main()
