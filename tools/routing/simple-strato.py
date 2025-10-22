'''
This is a simple showcase of a routing algorithm.
The goal is simply to split the number of flows by a given ratio among the available paths.
'''
from common import Database
from collections import defaultdict
from time import sleep

class Algorithm:
    def __init__(self, db_creds: dict, npaths: int, weights: list):
        self.db = Database(db_creds)
        self.npaths = npaths
        self.total = sum(weights)
        self.ratios = [(weight/self.total) for weight in weights]

    def run(self, update_interval: int):
        while True:
            data = self.db.get_newest_perstream()
            #Get the total number of streams per flow.
            total_conn=defaultdict(lambda: 0)
            for (src_id, dst_id, route_id, sock_id, _) in data:
                #Get the total number of streams for each flow.
                total_conn[(src_id, dst_id)] += 1
            
            #Assign streams to different paths based on its ratio.
            conn_count=defaultdict(lambda: 0)
            conn_assign=defaultdict(lambda: [])
            for (src_id, dst_id, route_id, sock_id, _) in data:
                if sock_id=="0:0": continue
                
                for i in range(self.npaths):
                    total = total_conn[src_id, dst_id]
                    target = int(total*self.ratios[i])
                    if conn_count[src_id, dst_id, i] < target:
                        conn_count[src_id, dst_id, i] += 1
                        conn_assign[src_id, dst_id, i].append(sock_id)
                        break
            
            print(conn_assign)
            #Install the route assignment.
            cursor = self.db.get_cursor()
            for key, val in conn_assign.items():
                (src_id, dst_id, route_id) = key
                conns = [[int(s) for s in conn.split(":")] for conn in val]
                path = self.db.get_path_hops(src_id, dst_id, route_id)
                self.db.install_route(
                        cursor=cursor,
                        route_id=route_id,
                        src=src_id,
                        dst=dst_id,
                        path=path,
                        streams=conns
                    )
        
            #Sync to db.
            self.db.sync_db(cursor)
            
            sleep(update_interval)

creds={"user": "pgusr",
        "password":"pgpwrd",
        "host":"127.0.0.1",
        "port":"5432",
        "database":"strato"}
alg = Algorithm(creds, 3, (2,1,1))
alg.run(30)