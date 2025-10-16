from time import sleep
from collections import defaultdict
import psycopg2
import numpy as np
import os

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from datetime import datetime

class Database:
    def __init__(self):
        self.connection = psycopg2.connect(
            user="pgusr",
            password="pgpwrd",
            host="172.16.8.2",
            port="5432",
            database="nextmini"
        )
        self.connection.autocommit = True  # Enable autocommit to avoid transaction issues
        self.t_node = None
        self.t_link = None
        self.t_flow = None
        self.t_app_flows = None
        self.t_user_flows = None

    def update_t_node(self):
        cursor = self.connection.cursor()
        # Get total sent per node (local_node_id represents the sender)
        query = '''
            SELECT local_node_id, SUM(bytes * 8.0 / 5.0) AS total_rate_bps
            FROM metrics
            WHERE time_read >= NOW() - INTERVAL '5 seconds'
            GROUP BY local_node_id
            ORDER BY local_node_id ASC;
        '''
        cursor.execute(query)
        sent = cursor.fetchall()

        # Get total received per node (remote_node_id represents the receiver)
        query = '''
            SELECT remote_node_id, SUM(bytes * 8.0 / 5.0) AS total_rate_bps
            FROM metrics
            WHERE time_read >= NOW() - INTERVAL '5 seconds'
            GROUP BY remote_node_id
            ORDER BY remote_node_id ASC;
        '''
        cursor.execute(query)
        recv = cursor.fetchall()

        sent_dict = {node_id: rate_bps or 0 for node_id, rate_bps in sent}
        recv_dict = {node_id: rate_bps or 0 for node_id, rate_bps in recv}
        all_nodes = set(sent_dict.keys()) | set(recv_dict.keys())

        self.t_node = Table(title="Data Rate Per Node")
        self.t_node.add_column("Node ID", justify="center")
        self.t_node.add_column("Sent Rate (Mbps)", justify="center")
        self.t_node.add_column("Recv Rate (Mbps)", justify="center")

        for node_id in sorted(all_nodes):
            sent_rate = sent_dict.get(node_id, 0)
            recv_rate = recv_dict.get(node_id, 0)
            self.t_node.add_row(
                str(node_id),
                str(float(sent_rate) / 1000000.0),
                str(float(recv_rate) / 1000000.0)
            )
        cursor.close()

    def update_t_link(self):
        cursor = self.connection.cursor()

        query = '''
            SELECT local_node_id, remote_node_id,
                   SUM(bytes * 8.0 / 5.0) AS total_rate_bps
            FROM metrics
            WHERE time_read >= NOW() - INTERVAL '5 seconds'
            GROUP BY local_node_id, remote_node_id
            HAVING COUNT(*) > 0
            ORDER BY total_rate_bps DESC;
        '''
        cursor.execute(query)
        metrics = cursor.fetchall()

        self.t_link = Table(title="Data Rate Per Link")
        self.t_link.add_column("Source Node ID", justify="center")
        self.t_link.add_column("Destination Node ID", justify="center")
        self.t_link.add_column("Rate (Mbps)", justify="center")

        for (src, dst, rate_bps) in metrics:
            rate_mbps = float(rate_bps or 0) / 1000000.0
            self.t_link.add_row(str(src), str(dst), str(rate_mbps))
        cursor.close()

    def update_t_flow(self):
        cursor = self.connection.cursor()

        query = '''
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
        '''
        cursor.execute(query)
        metrics = cursor.fetchall()

        self.t_flow = Table(title="Data Rate Per Flow (Recent 5s)")
        self.t_flow.add_column("Flow ID", overflow="fold")
        self.t_flow.add_column("Local→Remote", justify="center")
        self.t_flow.add_column("Rate (Mbps)", justify="right")

        for (flow_tuple, local_node, remote_node, total_rate_bps) in metrics:
            rate_mbps = float(total_rate_bps or 0) / 1000000.0
            link = f"{local_node}→{remote_node}"
            self.t_flow.add_row(
                flow_tuple or "N/A",
                link,
                f"{rate_mbps:.3f}"
            )
        cursor.close()

    def update_t_app_flows(self):
        cursor = self.connection.cursor()

        query = '''
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
                   af.is_finished
            FROM app_flows af
            WHERE af.src_node_id IS NOT NULL
            ORDER BY af.id DESC
            LIMIT 30;
        '''
        cursor.execute(query)
        flows = cursor.fetchall()

        self.t_app_flows = Table(title="App Flows (TUN Interface)")
        self.t_app_flows.add_column("ID", justify="right")
        self.t_app_flows.add_column("Flow ID", overflow="fold")
        self.t_app_flows.add_column("Src→Dst", justify="center")
        self.t_app_flows.add_column("Route", justify="right")
        self.t_app_flows.add_column("Finished")

        for (flow_id_int, flow_tuple, src, dst, route_id, is_finished) in flows:
            src_dst = f"{src}→{dst}" if src and dst else "N/A"
            route_str = str(route_id) if route_id else "-"
            finished_mark = "✓" if is_finished else ""
            
            self.t_app_flows.add_row(
                str(flow_id_int),
                flow_tuple or "N/A",
                src_dst,
                route_str,
                finished_mark
            )
        cursor.close()

    def update_t_user_flows(self):
        cursor = self.connection.cursor()

        query = '''
            SELECT f.id,
                   f.src_node_id,
                   f.dst_node_id,
                   f.flow_len_type,
                   f.flow_len_bytes,
                   f.flow_len_duration,
                   f.flow_rate,
                   f.flow_weight,
                   f.is_finished
            FROM flows f
            ORDER BY f.id DESC
            LIMIT 30;
        '''
        cursor.execute(query)
        flows = cursor.fetchall()

        self.t_user_flows = Table(title="User-space Flows (Configured)")
        self.t_user_flows.add_column("ID", justify="right")
        self.t_user_flows.add_column("Src→Dst", justify="center")
        self.t_user_flows.add_column("Length", justify="right")
        self.t_user_flows.add_column("Rate", justify="right")
        self.t_user_flows.add_column("Weight", justify="right")
        self.t_user_flows.add_column("Finished")

        for (fid, src, dst, len_type, len_bytes, len_duration, rate, weight, is_finished) in flows:
            src_dst = f"{src}→{dst}"
            
            if len_type == "bytes":
                length = f"{len_bytes} B" if len_bytes else "-"
            elif len_type == "duration":
                length = f"{len_duration} s" if len_duration else "-"
            else:
                length = "-"
            
            rate_str = f"{rate} B/s" if rate else "-"
            weight_str = str(weight) if weight else "-"
            finished_mark = "✓" if is_finished else ""
            
            self.t_user_flows.add_row(
                str(fid),
                src_dst,
                length,
                rate_str,
                weight_str,
                finished_mark
            )
        cursor.close()


def show(console):
    db = Database()
    try:
        db.update_t_app_flows()
        db.update_t_user_flows()
        db.update_t_node()
        db.update_t_link()
        db.update_t_flow()

        with console.capture() as capture:
            console.print(
                Panel(db.t_app_flows, border_style="cyan"),
                Panel(db.t_user_flows, border_style="green"),
                Panel(db.t_node, border_style="yellow"),
                Panel(db.t_link, border_style="magenta"),
                Panel(db.t_flow, border_style="blue"),
                Panel(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", border_style="white"),
                sep="\n"
            )

        os.system("clear")
        print(capture.get())

    except Exception as error:
        print(str(f"Error: {error}"))

if __name__ == "__main__":
    console = Console(record=True)
    while True:
        show(console)
        sleep(0.5)
