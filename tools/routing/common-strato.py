import json
from collections import defaultdict
from functools import reduce

import psycopg2


class Database:
    def __init__(self, db_creds: dict):
        self.connection = psycopg2.connect(**db_creds)
        self.connection.autocommit = (
            True  # Add autocommit to avoid transaction issues
        )
        self.print_buffer = []

    def get_cursor(self):
        return self.connection.cursor()

    def get_all_routes(self):
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT src_node_id, dst_node_id, route_id, hops
            FROM "routes"
        """)
        return [(arr[0], arr[1], arr[2], arr[3]) for arr in cursor.fetchall()]

    def get_all_nodes(self):
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT id
            FROM "nodes"
        """)
        return [(arr[0]) for arr in cursor.fetchall()]

    def get_newest_pernode(self):
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT prev_hop_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY prev_hop_id)
            GROUP BY prev_hop_id
            ORDER BY prev_hop_id ASC;
        """)
        sent = cursor.fetchall()

        cursor.execute("""
            SELECT hop_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY hop_id)
            GROUP BY hop_id
            ORDER BY hop_id ASC;
        """)
        recv = cursor.fetchall()
        metrics = zip(sent, recv)
        cursor.close()
        return metrics

    def get_newest_perlink(self):
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT prev_hop_id, hop_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY hop_id)
                AND prev_hop_id != hop_id
            GROUP BY prev_hop_id, hop_id
            ORDER BY prev_hop_id ASC;
        """)

        metrics = cursor.fetchall()
        ret = {(arr[0], arr[1]): arr[2] for arr in metrics}
        cursor.close()
        return ret

    def get_newest_perflow(self):
        """
        Get the most recent bps for each flow (each unique src/dst pair);
        return an dictionary of {(src_id, dst_id): bps}.
        We calculate bps of each flow based on the sum of its bps in all its link segments.
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT src_id, dst_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY hop_id)
                AND hop_id=dst_id
            GROUP BY src_id, dst_id
        """)
        ret = {(arr[0], arr[1]): arr[2] for arr in cursor.fetchall()}
        ret = defaultdict(lambda: 0, ret)
        cursor.close()
        return ret

    def get_newest_perroute(self):
        """
        Get the most recent bps for each route; return an dictionary of {(src_id, dst_id, route_id): bps}.
        We calculate bps of the entire path based on its bps in its last link segment.
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT src_id, dst_id, route_id, prev_hop_id, hop_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY hop_id)
                AND hop_id=dst_id
            GROUP BY src_id, dst_id, route_id, prev_hop_id, hop_id
        """)
        ret = {(arr[0], arr[1], arr[2]): arr[5] for arr in cursor.fetchall()}
        ret = defaultdict(lambda: 0, ret)
        cursor.close()
        return ret

    def get_newest_perroutelink(self):
        """
        Get the most recent bps for each flow in each link segment.
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT src_id, dst_id, route_id, prev_hop_id, hop_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY hop_id)
            GROUP BY src_id, dst_id, route_id, prev_hop_id, hop_id
        """)
        ret = {
            (arr[0], arr[1], arr[2], arr[3], arr[4], arr[5]): arr[6]
            for arr in cursor.fetchall()
        }
        cursor.close()
        return ret

    def get_newest_perstream(self):
        """
        Get the most recent bps for each stream at the source node
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT src_id, dst_id, route_id, stream_id, SUM(bps) AS min_bps
            FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY hop_id)
            GROUP BY src_id, dst_id, route_id, stream_id
        """)

        # src_id, dst_id, route_id, stream_id, bps
        ret = [
            (arr[0], arr[1], arr[2], arr[3], arr[4])
            for arr in cursor.fetchall()
        ]
        ret = filter(lambda x: False if x[3] == "0:0" else True, ret)
        ret = [i for i in ret]
        cursor.close()
        return ret

    def get_newest_splitting_ratio(self):
        """
        returns a list of [src_id, dst_id, stream_id, splitting_ratio] where splitting_ratio represents the ratio of bps for each connection within the flow.
        """
        # Get the newest perstream data. The type of data is [src_id, dst_id, route_id, stream_id, bps][]
        data = self.get_newest_perstream()

        # Get the total out-going bps for each flow pair
        def reduce_total(acc, item):
            acc[(item[0], item[1])] += item[-1]
            return acc

        total = reduce(reduce_total, data, defaultdict(lambda: 0))

        # Get the ratio for each stream
        def map_ratio(item):
            return [
                item[0],
                item[1],
                item[3],
                item[-1] / total[item[0], item[1], item[2]],
            ]

        return map(map_ratio, data)

    def get_path_hops(self, src_id, dst_id, route_id):
        """
        Get the path hops based on src_id, dst_id, and route_id
        """
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT hops
            FROM "routes"
            WHERE src_node_id=%s AND dst_node_id=%s AND route_id=%s
        """,
            (src_id, dst_id, route_id),
        )
        ret = [arr[0] for arr in cursor.fetchall()][0]
        cursor.close()
        return ret

    def get_newest_routes_streams(self):
        """
        Get a dictionary of all streams assigned to each route
        """
        ret = defaultdict(lambda: [])
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT src_id, dst_id, route_id, stream_id FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY hop_id)
            GROUP BY src_id, dst_id, route_id, stream_id
        """)
        print("------DB Fetch Routes Streams-----")
        for arr in cursor.fetchall():
            print(arr)
            if arr[3] == "0:0":
                continue
            ret[(arr[0], arr[1], arr[2])].append(arr[3])
        cursor.close()
        return ret

    def get_newest_streambps(self):
        """
        Get a dictionary of all streams, with their bps
        This differs from get_newest_perstream() in that it returns a dictionary instead of a list.
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT src_id, dst_id, stream_id, bps FROM metrics
            WHERE (hop_id, time_read) IN (
                SELECT hop_id, MAX(time_read)
                FROM metrics
                GROUP BY hop_id) AND stream_id != '0:0'
        """)
        ret = {(arr[0], arr[1], arr[2]): arr[3] for arr in cursor.fetchall()}
        ret = defaultdict(lambda: 0, ret)
        cursor.close()
        return ret

    def install_route(self, cursor, route_id, src, dst, path, streams):
        print(f"DEBUG - Original streams: {streams}")

        formatted_streams = []
        for stream in streams:
            if len(stream) >= 2:
                formatted_streams.append(f"{stream[0]}:{stream[1]}")

        print(f"DEBUG - Formatted streams: {formatted_streams}")

        streams = json.dumps(formatted_streams)

        print(f"DEBUG - JSON string to be stored in DB: {streams}")

        query = """
        INSERT INTO "routes" (src_node_id, dst_node_id, route_id, hops, streams) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (src_node_id, dst_node_id, route_id) DO UPDATE SET hops = EXCLUDED.hops, streams = EXCLUDED.streams;
        """
        cursor.execute(query, (src, dst, route_id, path, streams))
        print(
            f"DEBUG - SQL executed: INSERT/UPDATE route ({src}, {dst}, {route_id}) with streams: {streams}"
        )

    def install_route_and_sync(self, cursor, route_id, src, dst, path, streams):
        cursor = self.connection.cursor()

        print(f"DEBUG - [sync] Original streams: {streams}")

        formatted_streams = []
        for stream in streams:
            if len(stream) >= 2:
                formatted_streams.append(f"{stream[0]}:{stream[1]}")

        print(f"DEBUG - [sync] Formatted streams: {formatted_streams}")

        streams = json.dumps(formatted_streams)

        print(f"DEBUG - [sync] JSON string to be stored in DB: {streams}")

        query = """
        INSERT INTO "routes" (src_node_id, dst_node_id, route_id, hops, streams) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (src_node_id, dst_node_id, route_id) DO UPDATE SET hops = EXCLUDED.hops, streams = EXCLUDED.streams;
        """
        cursor.execute(query, (src, dst, route_id, path, streams))
        print(
            f"DEBUG - [sync] SQL executed: INSERT/UPDATE route ({src}, {dst}, {route_id}) with streams: {streams}"
        )
        self.connection.commit()
        cursor.close()

    def sync_db(self, cursor):
        query = """
        NOTIFY sync_routes;
        """
        cursor.execute(query)
        self.connection.commit()
        cursor.close()
