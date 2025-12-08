import os
import time
from datetime import datetime

import psycopg
from psycopg.rows import dict_row
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

DB_USER = os.environ.get("NEXTMINI_DB_USER", "pgusr")
DB_PASSWORD = os.environ.get("NEXTMINI_DB_PASSWORD", "pgpwrd")
DB_HOST = os.environ.get("NEXTMINI_DB_HOST", "localhost")
DB_PORT = int(os.environ.get("NEXTMINI_DB_PORT", "5432"))
DB_NAME = os.environ.get("NEXTMINI_DB_NAME", "nextmini")

REFRESH_INTERVAL_SEC = float(os.environ.get("REFRESH_INTERVAL_SEC", "1.0"))


def make_conn_string() -> str:
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def query_app_flows(conn: psycopg.Connection):
    sql = """
        SELECT af.id,
               encode(af.flow_id, 'hex') AS flow_hex,
               format('%s.%s.%s.%s:%s → %s.%s.%s.%s:%s',
                      get_byte(af.flow_id,0), get_byte(af.flow_id,1), get_byte(af.flow_id,2), get_byte(af.flow_id,3),
                      (get_byte(af.flow_id,8)::int << 8) + get_byte(af.flow_id,9),
                      get_byte(af.flow_id,4), get_byte(af.flow_id,5), get_byte(af.flow_id,6), get_byte(af.flow_id,7),
                      (get_byte(af.flow_id,10)::int << 8) + get_byte(af.flow_id,11)
               ) AS tuple,
               af.src_node_id, af.dst_node_id, af.is_finished,
               afr.route_id
        FROM app_flows af
        LEFT JOIN app_flow_routes afr ON af.flow_id = afr.flow_id
        ORDER BY af.id DESC
        LIMIT 30
        """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        return cur.fetchall()


def query_metrics(conn: psycopg.Connection):
    sql = """
        SELECT to_char(time_read, 'HH24:MI:SS') AS ts,
               encode(flow_id, 'hex') AS flow_hex,
               format('%s.%s.%s.%s:%s → %s.%s.%s.%s:%s',
                      get_byte(flow_id,0), get_byte(flow_id,1), get_byte(flow_id,2), get_byte(flow_id,3),
                      (get_byte(flow_id,8)::int << 8) + get_byte(flow_id,9),
                      get_byte(flow_id,4), get_byte(flow_id,5), get_byte(flow_id,6), get_byte(flow_id,7),
                      (get_byte(flow_id,10)::int << 8) + get_byte(flow_id,11)
               ) AS tuple,
               local_node_id, remote_node_id, bytes,
               round(bytes/5.0, 2) AS bytes_per_sec_est
        FROM metrics
        ORDER BY id DESC
        LIMIT 30
        """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        return cur.fetchall()


def build_app_flows_table(rows):
    table = Table(title="Application Flows", show_lines=False)
    table.add_column("id", justify="right")
    table.add_column("tuple", overflow="fold")
    table.add_column("src→dst")
    table.add_column("route_id", justify="right")
    table.add_column("finished")

    for r in rows:
        src_dst = f"{r.get('src_node_id')}→{r.get('dst_node_id')}"
        finished_mark = (
            "[green]✓[/green]" if r.get("is_finished") else "[red]✗[/red]"
        )
        table.add_row(
            str(r.get("id")),
            r.get("tuple") or "",
            src_dst,
            str(r.get("route_id")) if r.get("route_id") is not None else "",
            finished_mark,
        )
    return table


def build_metrics_table(rows):
    table = Table(title="Metrics (last 5s aggregated)", show_lines=False)
    table.add_column("time")
    table.add_column("tuple", overflow="fold")
    table.add_column("local→remote")
    table.add_column("bytes", justify="right")
    table.add_column("~B/s", justify="right")

    for r in rows:
        pair = f"{r.get('local_node_id')}→{r.get('remote_node_id')}"
        table.add_row(
            r.get("ts") or "",
            r.get("tuple") or "",
            pair,
            str(r.get("bytes")),
            str(r.get("bytes_per_sec_est")),
        )
    return table


def main():
    console = Console()
    conn_str = make_conn_string()

    with psycopg.connect(conn_str) as conn:
        # autocommit read-only
        conn.read_only = True
        conn.autocommit = True

        with Live(
            console=console,
            refresh_per_second=max(1, int(1 / REFRESH_INTERVAL_SEC)),
        ) as live:
            while True:
                try:
                    flows = query_app_flows(conn)
                    metrics = query_metrics(conn)

                    layout = Table.grid(padding=(0, 1))
                    layout.add_row(
                        Panel(build_app_flows_table(flows), border_style="cyan")
                    )
                    layout.add_row(
                        Panel(
                            build_metrics_table(metrics), border_style="magenta"
                        )
                    )
                    layout.add_row(
                        Panel(
                            f"Updated: {datetime.now().strftime('%H:%M:%S')}  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}",
                            border_style="green",
                        )
                    )

                    live.update(layout)
                    time.sleep(REFRESH_INTERVAL_SEC)
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    live.update(f"[red]Error:[/red] {e}")
                    time.sleep(2)


if __name__ == "__main__":
    main()
