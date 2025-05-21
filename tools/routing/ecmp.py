'''
This is an implementation of the equal cost multi path algorithm.
This algorithm splits connections (approximately) equallly among all paths

Note: Traffic splitting is acheived through assigning streams to different
paths, which means that we cannot achieve theoretical equal-split. Instead,
what we aim to do here is to find a solution to the discrete optimization
problem of assigning streams to different paths such that traffic is
distributed as evenly as possible. Specifically, we are trying to find the
approximate solution to a loosely constrained bin-packing problem, where 
the size of each bin is the theoretical equal-split, and the the constraint
is "exceeding the size of each bin a little as possible". We employ a classic
first-fit algorithm to approximate optimal solution.
'''
from common import Database
from collections import defaultdict
from time import sleep

class Algorithm:
    def __init__(self, db_creds: dict, npaths: int):
        self.db = Database(db_creds)
        self.npaths = npaths
        
    def run(self, update_interval: int):
        #The theoretical ecmp bps per path
        ecmp_bps = 1/self.npaths
        while True:
            data = self.db.get_newest_splitting_ratio()
            print(data)
            #Assign streams to different paths based its ratio
            conn_budget = defaultdict(lambda: ecmp_bps)
            conn_assign = defaultdict(lambda: [])
            for (src_id, dst_id, sock_id, bps) in data:
                max_budget = -float("inf")
                max_route = -1
                f_break = False
                #Loop through all the paths, and place the stream in the first path with enough budget
                for route_id in self.npaths:
                    budget = conn_budget[(src_id, dst_id, route_id)]       
                    if budget >= bps:
                        conn_budget[(src_id, dst_id, route_id)] -= bps
                        conn_assign[(src_id, dst_id, route_id)].append([int(s) for s in sock_id.split(":")])
                        f_break = True
                        break
                    #Keep track of the route with the most remaining budget for future
                    if conn_budget[(src_id, dst_id, route_id)] > max_budget:
                        max_budget = budget
                        max_route = route_id
                        
                #if no route has enough remaining budget, we assign to the route with largest remaining
                if not f_break:
                    conn_budget[(src_id, dst_id, max_route)] -= bps
                    conn_assign[(src_id, dst_id, max_route)].append([int(s) for s in sock_id.split(":")])
                
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

if __name__ == "__main__":
    creds={"user": "pgusr",
            "password":"pgpwrd",
            "host":"127.0.0.1",
            "port":"5432",
            "database":"strato"}
    alg = Algorithm(creds, 2)
    alg.run(30)

'''
1.To quickly test if the algorithm works, starts two stato nodes with the following configuration in the controller's config.json:
{
    "reset_db" : true,
    "protocol": "quic",
    "num_paths": 3,
    "multi_path_method": "stream", 
    "routes_preset": {
        "type": "full_mesh",
        "n_nodes": 3,
        "route_ids": [0,1]
    }
}

2.Then, attach to each of the strato nodes, any one of them can be the server and the other will be the client.

3.On server node, run:
iperf3 -s & iperf3 -s -p 5202 & iperf3 -s -p 5203

4.On client node, run:
iperf3 -c 10.0.0.2 -p 5201 -u -b 200M -n 10G & iperf3 -c 10.0.0.2 -p 5202 -u -b 100M -n 10G & iperf3 -c 10.0.0.2 -p 5203 -u -b 100M -n 10G

5.Observe the flow distribution among the two paths between the two nodes, the first path should have around 300mbps throughput, while the second path 100.

6.Now, run this script, and wait for a few seconds to allow the routes to be installed.

7.Observer the flow distribution again, it should now be around 200 on each path
'''
# 