# TODO: implement

'''
This is an implementation of the optmatching algorithm.
This algorithm splits streams among paths based on their relative capacity.
'''
from common import Database
from collections import defaultdict
from time import sleep
import numpy as np
import mosek
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
            hops = json.loads(hops)
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
        route_links = defaultdict(lambda: [])
        for i in range(len(routes)):
            hops = json.loads(routes[i][3])
            for j in range(len(hops)-1):
                link = (hops[j], hops[j+1])
                route_links[(routes[i][0], routes[i][1], routes[i][2])].append(link)
                rlm[i, links.index(link)] = 1
        
        return (links, routes, route_links,rlm)
    
    def opt(self, streams, routes, links, capacities, rlm):
        # Initialize the model
        model = Model("MinimizeMaxLinkUtilization")
        n = len(streams)
        m = len(routes)
        # Variables
        x = model.variable("x", [n, m], Domain.binary())
        Z = model.variable("Z", Domain.greaterThan(0.0))

        # Flow Assignment Constraints
        for f in range(n):
            model.constraint(Expr.sum(x.slice([f, 0], [f+1, m])), Domain.equalsTo(1))

        # Link Utilization Constraints
        for l in range(len(links)):
            expr = Expr.constTerm(0)
            for s, stream in enumerate(streams):
                (src_id, dst_id, _, _, _) = stream
                for r, route in enumerate(routes):
                    if route[0] == src_id and route[1] == dst_id:
                        expr = Expr.add(expr, Expr.mul(rlm[r, l], x.index(s, r)))
            model.constraint(Expr.sub(Expr.mul(1.0/capacities[l], expr), Z), Domain.lessThan(0))

        # Set objective
        model.objective(ObjectiveSense.Minimize, Z)

        # Solve the model
        model.solve()

        # Extract and print solution
        solZ = Z.level()
        print("Optimal maximum link utilization: %g" % solZ[0])
        return Z.level(), x.level()
        # Additional code to extract flow-path assignments

                
    def run(self, update_interval: int):
        rounds = 0
        link_caps = defaultdict(lambda: 2**32)
        link_expected = defaultdict(lambda: -1*2**32)
        stream_size = defaultdict(lambda: 0)
        stream_expected = defaultdict(lambda: 2**32)
        expected_modifier = 0.8
        (links, routes, route_links, rlm) = self.preprocess_topology()
        
        while True:
            route_link_bps = self.db.get_newest_perroutelink()
            flows = [key for key in self.db.get_newest_perflow().keys()]
            
            ##========Automatic Link Capacity Probing============##
            #We first check to see if any link failed to meet the expected bps
            link_bps = self.db.get_newest_perlink()
            for link in links:
                if link_bps[link] < link_expected[link]*expected_modifier:
                    #If the actual bps is less than expected, we set the capacity to the actual bps
                    link_caps[link] = link_bps[link]
                    # print(f"Round {rounds}: Link {link} has less bps than expected. Setting capacity to {link_bps[link]}")
            
            #We then wait to find out the bottlenecked links for each route
            bottlenecked_thresh = 0.8
            bottlenecked_links = {}
            for (src_id, dst_id) in flows:
                for route_id in self.npaths:
                    max_bps = -1*float('inf')
                    #First, we find the link with the max bps
                    for (prev_id, hop_id) in route_links[src_id, dst_id, route_id]:
                        bps = route_link_bps[(src_id, dst_id, route_id, prev_id, hop_id)]
                        if bps > max_bps:
                            max_bps = bps
                    
                    #Then, we check for links with significantly lower bps than the max, which indicates a bottleneck
                    for (prev_id, hop_id) in route_links[src_id, dst_id, route_id]:
                        bps = route_link_bps[(src_id, dst_id, route_id, prev_id, hop_id)]
                        if bps < bottlenecked_thresh*max_bps:
                            bottlenecked_links[(prev_id, hop_id)] = True #place holder value for the dictionary. We just want unique key sentries.
            bottlenecked_links = list(bottlenecked_links.keys())
            
            #Now that we have all the bottleneck links, we can update their capacities as the observed bps
            for (prev_id, hop_id) in bottlenecked_links:
                link_caps = link_bps[(prev_id, hop_id)]
                
            
            ##========Automatic Stream Size Probing============##
            #Loop through all the streams and see if any stream execeeds the expected bps.
            #If so, we set the stream size to the actual bps.
            stream_bps = self.db.get_newest_streambps()
            for stream, bps in stream_bps.items():
                if bps > stream_expected[stream]*(1/expected_modifier):
                    stream_size[stream] = bps
            
            ##=========Solve the problem using MIPS===========##
            streams = self.db.get_newest_perstream()
            (z,x) = self.opt(streams, routes, links, link_caps, rlm)
            
            
            ##=========Install the routes===========##
            stream_assign = {}
            for n in x.dim(0):
                for m in x.dim(1):
                    if x.index(n,m) == 1:
                        stream_assign[(routes[m][0], routes[m][1], routes[m][2])] = streams[n][3]
            stop = 1
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
