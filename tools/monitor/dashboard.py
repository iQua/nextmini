from time import sleep
from collections import defaultdict
import psycopg2
import numpy as np
import os

from rich.console import Console
from rich.table import Table

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
            SELECT flow_id, local_node_id, remote_node_id,
                   SUM(bytes * 8.0 / 5.0) AS total_rate_bps
            FROM metrics
            WHERE time_read >= NOW() - INTERVAL '5 seconds'
            GROUP BY flow_id, local_node_id, remote_node_id
            HAVING COUNT(*) > 0
            ORDER BY total_rate_bps DESC;
        '''
        cursor.execute(query)
        metrics = cursor.fetchall()

        self.t_flow = Table(title="Data Rate Per Flow")
        self.t_flow.add_column("Flow ID (hex)", justify="center")
        self.t_flow.add_column("Local Node ID", justify="center")
        self.t_flow.add_column("Remote Node ID", justify="center")
        self.t_flow.add_column("Rate (Mbps)", justify="center")

        for (flow_id, local_node, remote_node, total_rate_bps) in metrics:
            flow_hex = flow_id.hex()[:16] + "..." if len(flow_id.hex()) > 16 else flow_id.hex()
            rate_mbps = float(total_rate_bps or 0) / 1000000.0
            self.t_flow.add_row(
                flow_hex,
                str(local_node),
                str(remote_node),
                str(rate_mbps)
            )
        cursor.close()


def show(console):
    db = Database()
    try:
        db.update_t_node()
        db.update_t_link()
        db.update_t_flow()

        with console.capture() as capture:
            console.print(db.t_node, db.t_link, db.t_flow, sep="\n")

        os.system("clear")
        print(capture.get())

    except Exception as error:
        print(str(f"Error: {error}"))

if __name__ == "__main__":
    console = Console(record=True)
    while True:
        show(console)
        sleep(0.5)
