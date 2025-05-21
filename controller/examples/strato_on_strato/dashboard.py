import sys
sys.dont_write_bytecode = True # Disable PyCache

import psycopg2
import numpy as np
import os
from time import sleep
from collections import defaultdict

# Nodes
#         Column         |           Type           | Collation | Nullable | Default | Storage  | Stats target | Description 
# -----------------------+--------------------------+-----------+----------+---------+----------+--------------+-------------
#  id                    | integer                  |           | not null |         | plain    |              | 
#  private_network_addr  | character varying(255)   |           | not null |         | extended |              | 
#  public_network_addr   | character varying(255)   |           | not null |         | extended |              | 
#  virtural_network_addr | character varying(255)   |           | not null |         | extended |              | 
#  connections           | integer[]                |           | not null |         | extended |              | 
#  createdAt             | timestamp with time zone |           | not null |         | plain    |              | 
#  updatedAt             | timestamp with time zone |           | not null |         | plain    |              |   

# Metrics
#    Column    |           Type           | Collation | Nullable |                Default                
# -------------+--------------------------+-----------+----------+---------------------------------------
#  id          | integer                  |           | not null | nextval('"Metrics_id_seq"'::regclass)
#  src_node_id | integer                  |           |          | 
#  dst_node_id | integer                  |           |          | 
#  flow_id     | integer[]                |           |          | 
#  time_read   | timestamp with time zone |           |          | 
#  bps         | integer                  |           |          | 
#  createdAt   | timestamp with time zone |           | not null | 
#  updatedAt   | timestamp with time zone |           | not null | 

# Flows
#     Column    |           Type           | Collation | Nullable | Default | Storage  | Stats target | Description 
# --------------+--------------------------+-----------+----------+---------+----------+--------------+-------------
#  src_node_id  | integer                  |           | not null |         | plain    |              | 
#  dst_node_id  | integer                  |           | not null |         | plain    |              | 
#  route_id     | integer                  |           | not null |         | plain    |              | 
#  flow_weight  | integer                  |           | not null |         | plain    |              | 
#  route_weight | integer                  |           | not null |         | plain    |              | 
#  hops         | integer[]                |           | not null |         | extended |              | 
#  createdAt    | timestamp with time zone |           | not null |         | plain    |              | 
#  updatedAt    | timestamp with time zone |           | not null |         | plain    |              | 

class Database:
    def __init__(self):
        self.connection = psycopg2.connect(
            user="pgusr",
            password="pgpwrd",
            host="127.0.0.1",
            port="5432",
            database="strato"
        )

    def get_db_pernode(self):
        cursor = self.connection.cursor()
        cursor.execute('''
            SELECT src_node_id, SUM(bps) AS total_bps
            FROM "Metrics"
            WHERE "time_read" = (SELECT MAX(time_read) FROM "Metrics")
            GROUP BY src_node_id
            ORDER BY src_node_id ASC;
        ''')
        metrics = cursor.fetchall()
    
        if not metrics:
            return np.empty((0, 0, 0), dtype=object)
        
        table = np.empty((1, len(metrics), 2), dtype=object)

        for i, (src_node_id, bps) in enumerate(metrics):
            table[0, i, :] = [src_node_id, bps]
            
        return table    

    def get_db_perlink(self):
        cursor = self.connection.cursor()
        cursor.execute('''
            SELECT src_node_id, dst_node_id, total_bps, time_read
            FROM (
            SELECT src_node_id, dst_node_id, total_bps, time_read, ROW_NUMBER() OVER (PARTITION BY src_node_id, dst_node_id ORDER BY time_read DESC) AS row_num
            FROM (SELECT src_node_id, dst_node_id, SUM(bps) AS total_bps, time_read FROM "Metrics" GROUP BY src_node_id, dst_node_id, time_read) AS subquery1
            ) AS subquery
            WHERE row_num = 1;
        ''')
        metrics = cursor.fetchall()

        if not metrics:
            return np.empty((0, 0, 0), dtype=object)
        
        table = np.empty((1, len(metrics), 3), dtype=object)

        for i, (src_node_id, dst_node_id, bps, _) in enumerate(metrics):
            table[0, i, :] = [src_node_id, dst_node_id, bps]
                
        return table
    
    def get_db_perflow(self):
        cursor = self.connection.cursor()
        # Find all flow ids
        cursor.execute('''
            SELECT DISTINCT flow_id
            FROM "Metrics"
            WHERE "time_read" = (SELECT MAX(time_read) FROM "Metrics")
        ''')
        flow_ids = [arr[0] for arr in cursor.fetchall()]

        # Map flow -> dest id
        flow_to_dst = []
        for flow_id in flow_ids:
            dst_flow_id = '.'.join(map(str, flow_id[4:])) # dest part only
            cursor.execute('''
                SELECT id
                FROM "Nodes"
                WHERE virtural_network_addr = %s
            ''', (dst_flow_id,)) 
            flow_to_dst.append((flow_id, [arr[0] for arr in cursor.fetchall()][0]))

        conditions = []
        params = []
        for (flow_id, dst_node_id) in flow_to_dst:
            conditions.append("(flow_id = %s AND dst_node_id = %s)")
            params.extend([flow_id, dst_node_id])
        where_clause = " OR ".join(conditions)

        if not conditions:
            return np.empty((0, 0, 0), dtype=object)

        query = f'''
            SELECT flow_id, SUM(bps) AS total_bps
            FROM "Metrics"
            WHERE ({where_clause}) AND "time_read" = (SELECT MAX(time_read) FROM "Metrics")
            GROUP BY flow_id
        '''
        cursor.execute(query, params)
        metrics = cursor.fetchall()

        if not metrics:
            return np.empty((0, 0, 0), dtype=object)
        
        table = np.empty((1, len(metrics), 2), dtype=object)

        for i, (flow_id, bps) in enumerate(metrics):
            table[0, i, :] = [flow_id, bps]
                
        return table    

    # ------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------

    def get_historic_db_pernode(self) -> np.ndarray:
        """
        Returns:
            3d array of [time_read: timestamp with time zone, src_node_id: int, bps: int],
            grouped by identical time_read
        """
        
        cursor = self.connection.cursor()
        cursor.execute('''
            SELECT time_read, src_node_id, SUM(bps) AS total_bps
            FROM "Metrics"
            GROUP BY time_read, src_node_id
            ORDER BY time_read ASC;
        ''')
        metrics = cursor.fetchall()

        # temporary storage
        time_to_metrics = defaultdict(list)
        for row in metrics:
            time_read, src_node_id, bps = row
            time_to_metrics[time_read].append([time_read, src_node_id, bps])

        if not time_to_metrics:
            return np.empty((0, 0, 0), dtype=object)

        # max number of columns
        max_records = max(len(records) for records in time_to_metrics.values())

        # num of times X num of metrics X num of columns
        table = np.empty((len(time_to_metrics), max_records, 3), dtype=object)

        for i, (time_read, records) in enumerate(time_to_metrics.items()):
            for j, record in enumerate(records):
                table[i, j, :] = record
    
        return table

    def get_historic_db_perlink(self) -> np.ndarray:
        """
        Returns:
            3d array of [time_read: timestamp with time zone, src_node_id: int, dst_node_id: int, bps: int]
            grouped by identical time_read
        """
        
        cursor = self.connection.cursor()
        cursor.execute('''
            SELECT time_read, src_node_id, dst_node_id, SUM(bps) AS total_bps
            FROM "Metrics"
            GROUP BY time_read, src_node_id, dst_node_id
            ORDER BY time_read ASC;
        ''')
        metrics = cursor.fetchall()

        # temporary storage
        time_to_metrics = defaultdict(list)
        for row in metrics:
            time_read, src_node_id, dst_node_id, bps = row
            time_to_metrics[time_read].append([time_read, src_node_id, dst_node_id, bps])

        if not time_to_metrics:
            return np.empty((0, 0, 0), dtype=object)

        # max number of columns
        max_records = max(len(records) for records in time_to_metrics.values())

        # num of times X num of metrics X num of columns
        table = np.empty((len(time_to_metrics), max_records, 4), dtype=object)

        for i, (time_read, records) in enumerate(time_to_metrics.items()):
            for j, record in enumerate(records):
                table[i, j, :] = record
        return table

    def get_historic_db_perflow(self) -> np.ndarray:
        """
        Returns:
            3d array of [time_read: timestamp with time zone, flow_id: int[8], bps: int]
            grouped by identical time_read
        """
        
        cursor = self.connection.cursor()
        cursor.execute('''
            SELECT DISTINCT flow_id AS total_bps 
            FROM "Metrics"
            WHERE "time_read" = (SELECT MAX(time_read) FROM "Metrics")
        ''')
        flow_ids = [arr[0] for arr in cursor.fetchall()]
        
        # Map flow -> dest id
        flow_to_dst = []
        for flow_id in flow_ids:
            dst_flow_id = '.'.join(map(str, flow_id[4:])) # dest part only
            cursor.execute('''
                SELECT id
                FROM "Nodes"
                WHERE virtual_network_addr = %s
            ''', (dst_flow_id,))
            flow_to_dst.append((flow_id, [arr[0] for arr in cursor.fetchall()][0]))

        conditions = []
        params = []
        for (flow_id, dst_node_id) in flow_to_dst:
            conditions.append("(flow_id = %s AND dst_node_id = %s)")
            params.extend([flow_id, dst_node_id])
        where_clause = " OR ".join(conditions)

        if not conditions:
            return np.empty((0, 0, 0), dtype=object)
            
        query = f'''
            SELECT time_read, flow_id, SUM(bps) AS total_bps
            FROM "Metrics"
            WHERE ({where_clause})
            GROUP BY time_read, flow_id
            ORDER BY time_read ASC;
        '''
        cursor.execute(query, params)
        metrics = cursor.fetchall()

        # temporary storage
        time_to_metrics = defaultdict(list)
        for row in metrics:
            time_read, flow_id, bps = row
            time_to_metrics[time_read].append([time_read, flow_id, bps])

        if not time_to_metrics:
            return np.empty((0, 0, 0), dtype=object)

        # max number of columns
        max_records = max(len(records) for records in time_to_metrics.values())

        # num of times X num of metrics X num of columns
        table = np.empty((len(time_to_metrics), max_records, 3), dtype=object)
        for i, (time_read, records) in enumerate(time_to_metrics.items()):
            for j, record in enumerate(records):
                table[i, j, :] = record
        return table
    
    def get_flow_routes(self) -> np.ndarray:
        cursor = self.connection.cursor()
        cursor.execute('''
            WITH metrics_data AS (
                SELECT flow_id, src_node_id, dst_node_id, bps
                FROM "Metrics"
                WHERE "time_read" = (SELECT MAX(time_read) FROM "Metrics")
            ),
            filtered_flows AS (
                SELECT f.route_id, f.hops, f.route_weight, m.flow_id, m.bps
                FROM "Flows" AS f
                JOIN metrics_data AS m ON m.src_node_id = f.src_node_id AND m.dst_node_id = ANY(f.hops)
            ),
            min_bps AS (
                SELECT flow_id, hops, MIN(bps) as hops_bps
                FROM filtered_flows
                GROUP BY flow_id, hops
            )
            SELECT f.flow_id, f.hops, m.hops_bps, f.route_weight
            FROM filtered_flows AS f
            JOIN min_bps AS m ON f.flow_id = m.flow_id AND f.hops = m.hops
            GROUP BY f.flow_id, f.hops, m.hops_bps, f.route_weight
        ''')
        
        metrics = [(tuple(flow_id), tuple(hops), bps, route_weight) for flow_id, hops, bps, route_weight in cursor.fetchall()]
        return np.array(metrics, dtype=object)    

def display_db_pernode(table: np.ndarray):
    """
    Display table per node
    """
        
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║                    Total BPS Per Node                        ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print("║       Source Node ID      │        Total BPS (MiBits)        ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    if len(table) == 0:
        print("║" + "No Data Available".center(62) + "║")
    else:
        for columns in table:
            for column in columns:
                i = 0 if table.shape[2] == 2 else 1
                j = 1 if table.shape[2] == 2 else 2
                if column.all():
                    print(f"║ {column[i]:^25} │ {column[j]/1000000:^32} ║")
    print("╚══════════════════════════════════════════════════════════════╝")    

def display_db_perlink(table: np.ndarray):    
    """
    Display table per link
    """
        
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║                      Total BPS Per Link                      ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print("║ Source Node ID │ Destination Node ID │   Total BPS(MiBits)   ║")
    print("╟────────────────┼─────────────────────┼───────────────────────╢")

    if len(table) == 0:
        print("║" + "No Data Available".center(62) + "║")
    else: 
        for columns in table:
            for column in columns:
                i = 0 if table.shape[2] == 3 else 1
                j = 1 if table.shape[2] == 3 else 2
                k = 2 if table.shape[2] == 3 else 3
                if column.all():
                    print(f"║ {column[i]:^14} │ {column[j]:^19} │ {column[k]/1000000:^21} ║")
    print("╚══════════════════════════════════════════════════════════════╝")     

def display_db_perflow(table: np.ndarray):    
    """
    Display table per flow
    """
        
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║                      Total BPS Per Flow                      ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print("║              Flow ID                 │   Total BPS (MiBits)  ║")
    print("╟──────────────────────────────────────┼───────────────────────╢")
    if len(table) == 0:
        print("║" + "No Data Available".center(62) + "║")
    else: 
        for columns in table:
            for column in columns:
                if column.all():
                    i = 0 if table.shape[2] == 2 else 1
                    j = 1 if table.shape[2] == 2 else 2
                    flow_id_display = ".".join(map(str, column[i][:4])) + " -> " + ".".join(map(str, column[i][4:]))
                    print(f"║ {flow_id_display:^37}│ {column[j]/1000000:^21} ║")
    print("╚══════════════════════════════════════════════════════════════╝")        

def main():
    db = Database()

    try:
        display_db_pernode(db.get_db_pernode())
        display_db_perlink(db.get_db_perlink())
        display_db_perflow(db.get_db_perflow())
        display_db_pernode(db.get_historic_db_pernode())
        display_db_perlink(db.get_historic_db_perlink())
        display_db_perflow(db.get_historic_db_perflow())
    except Exception as error:
        print(f"Error: {error}")

if __name__ == "__main__":
    while True:
        os.system("clear")
        main()
        sleep(0.5)
