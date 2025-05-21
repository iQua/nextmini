import networkx as nx
import os, sys
import json
import pickle

def generate_flows(G):
    flows = []
    for edge in G.edges.data():

        i = 0
        path = edge[2].get(f"path{i}", None)
        while path:
            if path == "sp":
                sp = nx.shortest_path(G, edge[0], edge[1]) 
                item = {
                    "src_node_id": edge[0],
                    "dst_node_id": edge[1],
                    "flow_weight": edge[2].get("flow_weight", 1),
                    "route_weight": edge[2].get("route_weight", 1),
                    "route_id": i,
                    "hops": sp
                }
                flows.append(item)
                sp = sp.copy()
                sp.reverse()
                item = {
                    "src_node_id": edge[1],
                    "dst_node_id": edge[0],
                    "flow_weight": edge[2].get("flow_weight", 1),
                    "route_weight": edge[2].get("route_weight", 1),
                    "route_id": i,
                    "hops": path1
                }
                flows.append(item)
            else:
                item = {
                    "src_node_id": edge[0],
                    "dst_node_id": edge[1],
                    "flow_weight": edge[2].get("flow_weight", 1),
                    "route_weight": edge[2].get("route_weight", 1),
                    "route_id": i,
                    "hops": path
                }
                flows.append(item)
                path1 = path.copy()
                path1.reverse()
                item = {
                    "src_node_id": edge[1],
                    "dst_node_id": edge[0],
                    "flow_weight": edge[2].get("flow_weight", 1),
                    "route_weight": edge[2].get("route_weight", 1),
                    "route_id": i,
                    "hops": path1
                }
                flows.append(item)

            i += 1
            path = edge[2].get(f"path{i}", None)

    # print("\"flows\":")
    # print(json.dumps(flows, indent=4))
    return flows

def generate_caps(G):
    links = []
    for edge in G.edges.data():
        if edge[2].get("rate", None):
            item = {
                "src_node_id": edge[0],
                "dst_node_id": edge[1],
                "rate": edge[2]["rate"]
            }
            links.append(item)
            item = {
                "src_node_id": edge[1],
                "dst_node_id": edge[0],
                "rate": edge[2]["rate"]
            }
            links.append(item)
    # print("\"link_rates\":")
    # print(links)

    return links

def append_json(filename, out_filename=None, **args):
    with open(filename, "r") as f:
        data = json.load(f)
    data.update(args)
    if out_filename:
        with open(out_filename, "w") as f:
            json.dump(data, f, indent=4)
    return data

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    if len(sys.argv) < 3:
        print("Usage: python generate_config.py <file>, <out_file>")
        exit(1)
    with open(sys.argv[1], 'rb') as f:
        topology, flows = pickle.load(f)
    
    flows = generate_flows(flows)
    caps = generate_caps(topology)

    append_json("config_base.json", out_filename=sys.argv[2], flows=flows, link_rates=caps)


