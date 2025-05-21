from time import sleep
from collections import defaultdict
import psycopg2
import numpy as np
import os

# Nodes
#         Column         |           Type           | Collation | Nullable | Default | Storage  | Stats target | Description 
# -----------------------+--------------------------+-----------+----------+---------+----------+--------------+-------------
#  id                    | integer                  |           | not null |         | plain    |              | 
#  private_network_addr  | character varying(255)   |           | not null |         | extended |              | 
#  public_network_addr   | character varying(255)   |           | not null |         | extended |              | 
#  virtual_network_addr | character varying(255)   |           | not null |         | extended |              | 
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
            database="db"
        )

    def display_db_pernode(self):
        cursor = self.connection.cursor()
        cursor.execute('''
            SELECT src_node_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE "time_read" = (SELECT MAX(time_read) FROM metrics)
            GROUP BY src_node_id
            ORDER BY src_node_id ASC;
        ''')
        metrics = cursor.fetchall()
        
        print("╔══════════════════════════════════════════════════════════════╗")
        print("║                    Total BPS Per Node                        ║")
        print("╠══════════════════════════════════════════════════════════════╣")
        print("║       Source Node ID      │        Total BPS (MiBits)        ║")
        print("╠══════════════════════════════════════════════════════════════╣")
        if len(metrics) == 0:
            print("║" + "No Data Available".center(62) + "║")
        else:
            for metric in metrics:
                print(f"║ {metric[0]:^25} │ {metric[1]/1000000:^32} ║")
        print("╚══════════════════════════════════════════════════════════════╝")      

    def display_db_perlink(self):
        cursor = self.connection.cursor()
        cursor.execute('''
            SELECT src_node_id, dst_node_id, SUM(bps) AS total_bps
            FROM metrics
            WHERE "time_read" = (SELECT MAX(time_read) FROM metrics)
            GROUP BY src_node_id, dst_node_id
            ORDER BY src_node_id ASC;
        ''')
        metrics = cursor.fetchall()

        print("╔══════════════════════════════════════════════════════════════╗")
        print("║                      Total BPS Per Link                      ║")
        print("╠══════════════════════════════════════════════════════════════╣")
        print("║ Source Node ID │ Destination Node ID │   Total BPS(MiBits)   ║")
        print("╟────────────────┼─────────────────────┼───────────────────────╢")
        if len(metrics) == 0:
            print("║" + "No Data Available".center(62) + "║")
        else: 
            for metric in metrics:
                print(f"║ {metric[0]:^14} │ {metric[1]:^19} │ {metric[2]/1000000:^21} ║")
        print("╚══════════════════════════════════════════════════════════════╝")        
    
    def display_db_perflow(self):
        cursor = self.connection.cursor()
        # Find all flow ids
        cursor.execute('''
            SELECT DISTINCT flow_id
            FROM metrics
            WHERE "time_read" = (SELECT MAX(time_read) FROM metrics)
        ''')
        flow_ids = [arr[0] for arr in cursor.fetchall()]

        # Map flow -> dest id
        flow_to_dst = defaultdict(int)
        for flow_id in flow_ids:
            dst_flow_id = '.'.join(map(str, flow_id[4:])) # dest part only
            cursor.execute('''
                SELECT id
                FROM "Nodes"
                WHERE virtual_network_addr = %s
            ''', (dst_flow_id,))

            flow_to_dst[tuple(flow_id)] = [arr[0] for arr in cursor.fetchall()][0]

        # Map dst id -> total bps
        flow_to_bps = defaultdict(int)
        for flow_id, dst_node_id in flow_to_dst.items():
            cursor.execute('''
                SELECT SUM(bps) AS total_bps
                FROM metrics
                WHERE "time_read" = (SELECT MAX(time_read) FROM metrics)
                AND flow_id = %s AND dst_node_id = %s
            ''', (list(flow_id), dst_node_id))
            flow_to_bps[flow_id] = [arr[0] for arr in cursor.fetchall()][0]

        print("╔══════════════════════════════════════════════════════════════╗")
        print("║                      Total BPS Per Flow                      ║")
        print("╠══════════════════════════════════════════════════════════════╣")
        print("║              Flow ID                 │   Total BPS (MiBits)  ║")
        print("╟──────────────────────────────────────┼───────────────────────╢")
        if len(flow_to_dst) == 0:
            print("║" + "No Data Available".center(62) + "║")
        else: 
            for flow_id, bps in sorted(flow_to_bps.items(), key=lambda x: x[1], reverse=True): # sort from max bps to min
                flow_id_display = ".".join(map(str, flow_id[:4])) + " -> " + ".".join(map(str, flow_id[4:]))
                print(f"║ {flow_id_display:^37}│ {bps/1000000:^21} ║")
        print("╚══════════════════════════════════════════════════════════════╝")      

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
            FROM metrics
            GROUP BY time_read, src_node_id
            ORDER BY time_read ASC;
        ''')
        metrics = cursor.fetchall()
        
        # temporary storage
        time_to_metrics = defaultdict(list)
        for row in metrics:
            time_read, src_node_id, bps = row
            time_to_metrics[time_read].append([time_read, src_node_id, bps])
        
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
            FROM metrics
            GROUP BY time_read, src_node_id, dst_node_id
            ORDER BY time_read ASC;
        ''')
        metrics = cursor.fetchall()
        
        # temporary storage
        time_to_metrics = defaultdict(list)
        for row in metrics:
            time_read, src_node_id, dst_node_id, bps = row
            time_to_metrics[time_read].append([time_read, src_node_id, dst_node_id, bps])
        
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
            FROM metrics
            WHERE "time_read" = (SELECT MAX(time_read) FROM metrics)
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

        query = f'''
            SELECT time_read, flow_id, SUM(bps) AS total_bps
            FROM metrics
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
        
        # max number of columns
        max_records = max(len(records) for records in time_to_metrics.values())

        # num of times X num of metrics X num of columns
        table = np.empty((len(time_to_metrics), max_records, 3), dtype=object)
        for i, (time_read, records) in enumerate(time_to_metrics.items()):
            for j, record in enumerate(records):
                table[i, j, :] = record
        return table
    

def display_historic_db_pernode(table: np.ndarray):
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
                if column.all():
                    print(f"║ {column[1]:^25} │ {column[2]/1000000:^32} ║")
    print("╚══════════════════════════════════════════════════════════════╝")    

def display_historic_db_perlink(table: np.ndarray):
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
                if column.all():
                    print(f"║ {column[1]:^14} │ {column[2]:^19} │ {column[3]/1000000:^21} ║")
    print("╚══════════════════════════════════════════════════════════════╝")     

def display_historic_db_perflow(table: np.ndarray):
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
                    flow_id_display = ".".join(map(str, column[1][:4])) + " -> " + ".".join(map(str, column[1][4:]))
                    print(f"║ {flow_id_display:^37}│ {column[2]/1000000:^21} ║")
    print("╚══════════════════════════════════════════════════════════════╝")        

def main():
    db = Database()
    try:
        db.display_db_pernode()
        db.display_db_perlink()
        db.display_db_perflow()
        db.get_historic_db_perflow()
        #display_historic_db_pernode(db.get_historic_db_pernode())
        #display_historic_db_perlink(db.get_historic_db_perlink())
        display_historic_db_perflow(db.get_historic_db_perflow())
    except Exception as error:
        print(f"Error: {error}")

if __name__ == "__main__":
    while True:
        os.system("clear")
        main()
        sleep(0.5)