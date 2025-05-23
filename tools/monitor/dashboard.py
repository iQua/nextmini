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
        self.t_route= None

    def update_t_node(self):
        cursor = self.connection.cursor()
        query = '''
            SELECT prev_hop_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE (prev_hop_id, time_read) IN (
                SELECT prev_hop_id, MAX(time_read)
                FROM metrics
                GROUP BY prev_hop_id)
            GROUP BY prev_hop_id
            ORDER BY prev_hop_id ASC;
        '''
        cursor.execute(query)
        sent = cursor.fetchall()

        query = '''
            SELECT hop_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY hop_id)
            GROUP BY hop_id
            ORDER BY hop_id ASC;
        '''
        cursor.execute(query)
        recv = cursor.fetchall()

        metrics = zip(sent, recv)

        self.t_node = Table(title="Total BPS Per Node")
        self.t_node.add_column("Node ID", justify="center")
        self.t_node.add_column("Total Sent (MiBits)", justify="center")
        self.t_node.add_column("Total Recv (MiBits)", justify="center")
        for (sent, recv) in metrics:
            self.t_node.add_row(str(sent[0]), str(sent[1]/1000000), str(recv[1]/1000000))
        cursor.close()

    def update_t_link(self):
        cursor = self.connection.cursor()
        query = '''
            SELECT prev_hop_id, hop_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY hop_id)
            GROUP BY prev_hop_id, hop_id
            ORDER BY prev_hop_id ASC;
        '''
        cursor.execute(query)
        metrics = cursor.fetchall()

        self.t_link = Table(title="Total BPS Per Link")
        self.t_link.add_column("Source Node ID", justify="center")
        self.t_link.add_column("Destination Node ID", justify="center")
        self.t_link.add_column("Total BPS (MiBits)", justify="center")
        for (src, dst, bps) in metrics:
            self.t_link.add_row(str(src), str(dst), str(bps/1000000))
        cursor.close()

    def update_t_route(self):
        cursor = self.connection.cursor()
        query = '''
            SELECT src_id, dst_id, route_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY hop_id)
                AND hop_id=dst_id
            GROUP BY src_id, dst_id, route_id
        '''
        cursor.execute(query)

        metrics = [(arr[0], arr[1],arr[2],arr[3]) for arr in cursor.fetchall()]

        self.t_route = Table(title="Total BPS Per Route")
        self.t_route.add_column("Source Node ID", justify="center")
        self.t_route.add_column("Destination Node ID", justify="center")
        self.t_route.add_column("Route ID", justify="center")
        self.t_route.add_column("Total BPS (MiBits)", justify="center")
        for (src, dst, route, bps) in metrics:
            self.t_route.add_row(str(src), str(dst), str(route), str(bps/1000000))
        cursor.close()


def show(console):
    db = Database()
    try:
        db.update_t_node()
        db.update_t_link()
        db.update_t_route()

        with console.capture() as capture:
            console.print(db.t_node, db.t_link, db.t_route, sep="\n")

        os.system("clear")
        print(capture.get())

    except Exception as error:
        print(str(f"Error: {error}"))

if __name__ == "__main__":
    console = Console(record=True)
    while True:
        show(console)
        sleep(0.5)
