from LP import *
import os
import sqlalchemy as sql
import random
import pandas as pd
import numpy as np
import torch
from collections import defaultdict
from time import sleep

N_NODES = 6

def connect_db():
    user="pgusr"
    password="pgpwrd"
    host="127.0.0.1"
    port = "5432"
    database="strato"

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    
    engine = sql.create_engine(url)

    return engine.connect()

def upsert_row(df,row):
    active_routes = df[(df['src_node_id'] == row['src_node_id']) & (df['dst_node_id'] == row['dst_node_id']) & (df['route_weight'] > 0 )] 
    if not active_routes.empty:
        # Find the max route_id with non-zero route_weight
        max_route_id = active_routes['route_id'].max()
        route_id = max_route_id + 1
        row['route_id'] = route_id
    
    # Insert the new row]
    existing_row = df[(df['src_node_id'] == row['src_node_id']) & (df['dst_node_id'] == row['dst_node_id']) & (df['route_id'] == route_id)]
    if existing_row.empty:
        df = df.append(row, ignore_index=True)
    else:
        df.loc[(df['src_node_id'] == row['src_node_id']) & (df['dst_node_id'] == row['dst_node_id']) & (df['route_id'] == route_id)] = row

    return df

def install_routes(df,routes,flows):
    for i in range(N_NODES):
        for j in range(N_NODES):
            for k in range(N_NODES):
                weight = routes[(k, i, j)]
                new_row = {'src_node_id': flows[k][0], 'dst_node_id': flows[k][1], 'route_id': 0, 'flow_weight': 0, 'route_weight': weight, 'hops': [i, j]}
                df = upsert_row(df, new_row)
    return

def initialize_routes(conn):
    query = 'SELECT * FROM "Flows"'
    df = pd.read_sql_query(query, conn)

    # Set the entire "route_weight" column to 0
    df['route_weight'] = 0

    return df
    
def measure_links(conn, prev_read):
    metrics = conn.execute(sql.text('''
        SELECT src_node_id, dst_node_id, total_bps, time_read
        FROM (
        SELECT src_node_id, dst_node_id, total_bps, time_read, ROW_NUMBER() OVER (PARTITION BY src_node_id, dst_node_id ORDER BY time_read DESC) AS row_num
        FROM (SELECT src_node_id, dst_node_id, SUM(bps) AS total_bps, time_read FROM "Metrics" GROUP BY src_node_id, dst_node_id, time_read) AS subquery1
        ) AS subquery
        WHERE row_num = 1;
    '''))
    ret = np.zeros((N_NODES, N_NODES))
    for metric in metrics:
        if not prev_read.get((metric[0], metric[1])) or prev_read[(metric[0], metric[1])] < metric[3]:
            ret[metric[0]-1][metric[1]-1] = metric[2]
            prev_read[(metric[0], metric[1])] = metric[3]
        else:
            ret[metric[0]-1][metric[1]-1] = 0
        
        print("{}, {}, {}".format(metric[0], metric[1], ret[metric[0]-1][metric[1]-1]))

    return ret, prev_read

def measure_flows(conn, prev_read):
    # Get all unique flow ids
    # Find all flow ids
    res = conn.execute(sql.text('''
        SELECT DISTINCT flow_id
        FROM "Metrics"
    '''))
    flow_ids = [arr[0] for arr in res]

    # Map flow -> dest id
    flow_to_dst = defaultdict(int)
    for flow_id in flow_ids:
        dst_flow_id = '.'.join(map(str, flow_id[4:])) # dest part only
        res = conn.execute(sql.text('''
            SELECT id
            FROM "Nodes"
            WHERE virtual_network_addr = '{}'
        '''.format(dst_flow_id,)))

        flow_to_dst[tuple(flow_id)] = [arr[0] for arr in res][0]
    
    # Map flow -> src id
    flow_to_src = defaultdict(int)
    for flow_id in flow_ids:
        src_flow_id = '.'.join(map(str, flow_id[:4])) # dest part only
        res = conn.execute(sql.text('''
            SELECT id
            FROM "Nodes"
            WHERE virtual_network_addr = '{}'
        '''.format(src_flow_id,)))

        flow_to_src[tuple(flow_id)] = [arr[0] for arr in res][0]

    # Get the latest metric for each flow
    metrics = conn.execute(sql.text('''
        SELECT flow_id, dst_node_id, total_bps, time_read
        FROM (
        SELECT flow_id, dst_node_id, total_bps, time_read, ROW_NUMBER() OVER (PARTITION BY flow_id, dst_node_id ORDER BY time_read DESC) AS row_num
        FROM (SELECT flow_id, dst_node_id, SUM(bps) AS total_bps, time_read FROM "Metrics" GROUP BY flow_id, dst_node_id, time_read) AS subquery1
        ) AS subquery
        WHERE row_num = 1;
    '''))
    ret = []
    for metric in metrics:
        flow_id = metric[0]
        dst_node_id = flow_to_dst[tuple(flow_id)]
        src_node_id = flow_to_src[tuple(flow_id)]
        if flow_to_dst[tuple(flow_id)] == metric[1]:
            last_read = prev_read.get((src_node_id, dst_node_id), None)
            if not last_read or last_read < metric[3]:
                ret.append([src_node_id, dst_node_id, metric[2]])
            else:
                ret.append([src_node_id, dst_node_id, 0])
            prev_read[(src_node_id, dst_node_id)] = metric[3]
    
    return ret, prev_read

def get_splitting_ratio(optimal_flows):
    ret = []
    for k in range(len(N_NODES)):
        for i in range(len(N_NODES)):
            total = sum([optimal_flows[(k, i, j)] for j in range(len(N_NODES))])
            for j in range(len(N_NODES)):
                flow = optimal_flows[(k, i, j)]
                if flow > 0:
                    print(f"Splitting ratio {k} from node {i} to node {j}: {flow/total}")
                    ret.append((k, i, j, flow/total))
    return ret

def main():
    UPDATE_INTERVAL = 2 # seconds
    # load mapping matrix
    mapping = torch.load(os.path.join(os.path.dirname(__file__), 'mapping.pt'))
    mapping = mapping.numpy()

    # load link states
    link_states = torch.load(os.path.join(os.path.dirname(__file__), 'link_states.pt'))

    # intialize overlay capacity as the first link state
    over_cap = link_states.T[0].flatten().to_numpy()

    # calculate the underlay capacities
    under_cap = mapping @ over_cap.unsqueeze(0)

    # Notify the console that the ML router is ready
    print("ML router is ready")

    # Calculate the demands, which is the initial flows
    # We need two readings to determine which links are used
    conn = connect_db()
    prev_read = {}
    _, prev_read = measure_flows(conn, prev_read)
    sleep(2)
    flows_state, prev_read = measure_flows(conn, prev_read)

    demands = np.zeros((len(flows_state), N_NODES**2))
    for flow in enumerate(flows_state):
        demands[flow[0] -1][flow[1] - 1] = flow[2]

    #Get the initial link states
    _, prev_read = measure_links(conn, {})
    # Calculate the optimal flows
    while True:
        if (link_state.sum() == 0):
            print("No active flows, exiting...")
            sleep(UPDATE_INTERVAL)
            return 
        
        optimal_flows = solve_max_min_fairness_throughput_overlay(under_cap, mapping, demands)
        # Calculate the splitting ratio
        splitting_ratio = get_splitting_ratio(optimal_flows)
        
        #Install the routes
        df = initialize_routes(conn)
        df = install_routes(df, optimal_flows, splitting_ratio)
        df.to_sql('Flows', conn, index=False, if_exists='replace')

        sleep(UPDATE_INTERVAL)

        # Measure the link states
        link_state, prev_read = measure_links(conn, prev_read)

        # Update the capacities
        over_cap = link_states.flatten().to_numpy()
        under_cap = mapping @ over_cap

def test():
    conn = connect_db()
    res, prev_read = measure_links(conn, {})
    res, prev_read = measure_flows(conn, {})
    df = initialize_routes(conn)
    return

if __name__ == "__main__":
    test()
    main()




    
