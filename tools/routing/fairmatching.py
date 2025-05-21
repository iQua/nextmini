# TODO: implement

'''
This is an implementation of the optmatching algorithm.
This algorithm splits streams among paths based on their relative capacity.
'''
from common import Database
from collections import defaultdict
from time import sleep
import numpy as np
from mosek.fusion import *
import json

class Algorithm:
    def __init__(self, db_creds: dict, npaths: int):
        self.db = Database(db_creds)
        self.npaths = npaths
    
    def preprocess_topology(self): 
        routes = self.db.get_all_routes()
        #Get all the nodes, link, and routes from the routes information
        links = {}
        nodes = {}
        for (_, _, _, hops) in routes:
            for i in range(len(hops)-1):
                link = (hops[i], hops[i+1])
                links[link] = 0
                nodes[hops[i]] = 0
                nodes[hops[i+1]] = 0
            
        links = list(links.keys())
        nodes = list(nodes.keys())
        
        #Now we have all the links and nodes, we want to create a route to link matrix. Each row is a route, each column is a link.
        #The value in the matrix is one if link is part of the route, zero otherwise.
        rlm = np.zeros((len(routes), len(links)))
        route2links = defaultdict(lambda: [])
        for i in range(len(routes)):
            hops = routes[i][3]
            for j in range(len(hops)-1):
                link = (hops[j], hops[j+1])
                route2links[(routes[i][0], routes[i][1], routes[i][2])].append(link)
                rlm[i, links.index(link)] = 1
        
        #We also want to create a flow to route look up dictionary
        flow2routes = defaultdict(lambda: [])
        for i in range(len(routes)):
            flow2routes[(routes[i][0], routes[i][1])].append(routes[i][2])
            
        return (links, routes,rlm, route2links, flow2routes)
    
    def opt(self, streams, k, routes, links, route2links, flow2route, capacities, rlm):
        # Initialize the model
        model = Model("MinimizeMaxLinkUtilization")
        n = len(streams)
        
        # Variables, each variable represents a stream-route assignment
        x = model.variable("x", n*k, Domain.inRange(0.0,1.0))
        
        #Stream assignment constraint: each row corresponding to a stream, sums up to 1 (each stream can only be assigned to one route)
        A_f = np.zeros((n, n*k))
        for i in range(n):
            for j in range(i*k, (i+1)*k):
                A_f[i,j] = 1
        B_f = np.ones(n)
        model.constraint(Expr.mul(A_f, x), Domain.equalsTo(B_f))

        #Link utilization constraint: each link utilization is less than the capacity.
        A_l = np.zeros((len(links), n*k))
        for i in range(n):
            (src_id, dst_id, _, _, _) = streams[i]
            routes = flow2route[(src_id, dst_id)]
            for r in range(k):
                for (prev_id, hop_id) in route2links[(src_id, dst_id, r)]:
                    A_l[links.index((prev_id, hop_id)), i*k + r] = 1
        B_l = capacities
        model.constraint(Expr.mul(A_l, x), Domain.lessThan(B_l))
        
        # Solve the model
        model.solve()
        return x.level()
                
    def run(self, update_interval: int):
        rounds = 0
        link_caps = defaultdict(lambda: 0)
        link_bps = defaultdict(lambda: 0)
        (links, routes,rlm, route2links, flow2routes) = self.preprocess_topology()
        
        while True:
            ##=========Update the link capacities===========##
            #Calculate the total bps in each link and find the min bps of all routes
            routes_bps = self.db.get_newest_perroute()
            min_route_bps = float("inf")
            min_bps_thresh = 1000000
            for (route, bps) in routes_bps.items():
                if bps < min_route_bps and bps >= min_bps_thresh:
                    min_route_bps = bps
                for link in route2links[route]:
                    link_bps[link] += bps
            
            for (link, bps) in link_bps.items():
                if link_bps[link] > link_caps[link]:
                    link_caps[link] = bps

            capacities = np.array([float(int(link_caps[link]/min_bps_thresh)) for link in links])
            ##=========Solve the problem using LP===========##
            streams = self.db.get_newest_perstream()
            x = self.opt(streams, self.npaths, routes, links, route2links, flow2routes, capacities, rlm)
            
            ##=========Install the routes===========##
            stream_assign = {}
            for stream_id in len(streams):
                for k in range(self.npaths):
                    if x[stream_id,k] > 0:
                        (src_id, dst_id, route_id) = (streams[stream_id][0], streams[stream_id][1], k)
                        stream_assign[(src_id, dst_id, route_id)] = route2links[(src_id, dst_id, route_id)]
                        
            # Install the route assignment.
            cursor = self.db.get_cursor()
            f_has_install = False
            for key, val in stream_assign.items():
                f_has_install = True
                (src_id, dst_id, route_id) = key
                print(f"Round {rounds}: Assigning {val} to route {route_id}")
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
            if f_has_install:
                self.db.sync_db(cursor)
            sleep(update_interval)
            rounds += 1

    
if __name__ == "__main__":
    creds={"user": "pgusr",
            "password":"pgpwrd",
            "host":"127.0.0.1",
            "port":"5432",
            "database":"strato"}
    alg = Algorithm(creds, 3)
    alg.run(2)
