from LP import *
import os
import sqlalchemy as sql
import random
import pandas as pd
import numpy as np
import torch
from linear_constraints import generate_LC_map
from collections import defaultdict
from measurements.measurements import measure_links, measure_flows
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

def install_routes(conn,df,splitting_ratios, demands2flow):
    route_ids = {}
    for splitting_ratio in splitting_ratios:
        demand_id = splitting_ratio[0]
        hop1 = splitting_ratio[1] + 1 #offset by 1 since nodeid starts at 1
        hop2 = splitting_ratio[2] + 1 #offset by 1 since nodeid starts at 1
        src_node_id = demands2flow[demand_id][0]
        dst_node_id = demands2flow[demand_id][1]
        weight = splitting_ratio[3]
        if weight == 0:
            continue
        route_id = route_ids.get((src_node_id, dst_node_id), 0)
        route_ids[(src_node_id, dst_node_id)] = route_id + 1
        new_row = {'src_node_id':src_node_id, 'dst_node_id': dst_node_id, 'route_id': route_id, 'flow_weight': 1, 'route_weight': weight, 'hops': [hop1, hop2]}
        df = df._append(new_row, ignore_index=True)
    df['createdAt'] = pd.Timestamp.now()
    df['updatedAt'] = pd.Timestamp.now()

    #Truncate the table
    conn.execute(sql.text('''
        TRUNCATE TABLE "Flows" RESTART IDENTITY
    '''))
    #Inert the new rows
    df.to_sql('Flows', conn, index=False, if_exists='append')
    conn.commit()
    # Send a custom notification indicating table replacement
    channel = 'new_table'
    payload = '{"op":"REPLACE"}'

    notify_query = sql.sql.text(f"NOTIFY {channel}, '{payload}'")
    conn.execute(notify_query)

def initialize_dataframe(conn):
    query = 'SELECT * FROM "Flows" LIMIT 1'
    df = pd.read_sql_query(query, conn)

    df = df.head(0)
    return df

def get_splitting_ratio(optimal_flows):
    ret = []
    for k in range(N_NODES):
        for i in range(N_NODES):
            total = sum([optimal_flows[(k, i, j)] for j in range(N_NODES)])
            for j in range(N_NODES):
                flow = optimal_flows[(k, i, j)]
                if flow > 0:
                    print(f"Splitting flow {k} from node {i+1} to node {j+1}: {flow/total}")
                    ret.append((k, i, j, int(100*flow/total)))
    return ret

def main():
    UPDATE_INTERVAL = 15 # seconds
    DEMAND_MEASURE_DURATION = 15 # seconds
    SEED = 0
    torch.manual_seed(SEED)
    # load link states
    link_states = torch.load(os.path.join(os.path.dirname(__file__), 'link_states.pt'))

    #Check if mapping.pt exists
    mapping_path =os.path.join(os.path.dirname(__file__), 'mapping.pt') 
    if not os.path.exists(mapping_path):
        model_path = os.path.join(os.path.dirname(__file__), os.path.relpath('./models/LC6NodesV75.ckpt'))
        #Gernerate linear constraints mapping
        mapping = generate_LC_map(link_states, model_path, threshold=0.6)
        torch.save(mapping, './mapping.pt')
    else:
        mapping = torch.load(mapping_path)
    mapping = mapping.squeeze()

    # intialize overlay capacity as the first link state
    link_states = link_states.squeeze()
    over_cap = link_states.T[0].flatten()

    # calculate the underlay capacities
    under_cap = mapping @ over_cap.unsqueeze(1)
    under_cap = under_cap.reshape(6,6)

    # Notify the console that the ML router is ready
    print("ML router is ready")

    # Calculate the demands, which is the initial flows
    # We find the demand by measuring for DEMAND_MEASURE_DURATION seconds and find the top N nodes with the highest traffic
    conn = connect_db()
    prev_read = {}
    total_flows = np.zeros((N_NODES, N_NODES))
    for i in range(DEMAND_MEASURE_DURATION):
        _, prev_read = measure_flows(conn, prev_read, N_NODES)
        sleep(1)
        flows_state, prev_read = measure_flows(conn, prev_read, N_NODES)
        total_flows += flows_state

    #find the indices of the top N_NODES flows
    total_flows = torch.tensor(total_flows)
    _, indices = torch.topk(total_flows.view(-1), N_NODES)
    mask = torch.zeros(total_flows.shape)
    mask.view(-1)[indices] = 1
    total_flows = total_flows * mask

    demands = []
    demands2flow = {}
    k = 0
    for i in range(N_NODES):
        for j in range(N_NODES):
            if(total_flows[i, j] > 0):
                demand = np.zeros(N_NODES)
                demand[i] = total_flows[i, j]
                demand[j] = - total_flows[i, j]
                demands.append(demand)
                demands2flow[k] = (i+1, j+1)
                k += 1
    print("Demands: ", torch.tensor(demands))
    #Select only the top N_NODES demands

    #Get the initial link states
    link_state, prev_read = measure_links(conn, {}, N_NODES)
    # Calculate the optimal flows
    while True:
        if (link_state.sum() == 0):
            print("No active flows, exiting...")
            sleep(UPDATE_INTERVAL)
            return 
        
        _, optimal_flows,_ = solve_max_min_fairness_throughput_overlay(under_cap, mapping, demands)
        # Calculate the splitting ratio
        splitting_ratio = get_splitting_ratio(optimal_flows)
        
        #Install the routes
        print("Installing routes...")
        df = initialize_dataframe(conn)
        install_routes(conn, df, splitting_ratio, demands2flow)

        sleep(UPDATE_INTERVAL)

        # Measure the link states
        link_state, prev_read = measure_links(conn, prev_read, N_NODES)

        # Update the capacities
        over_cap =over_cap.flatten().unsqueeze(1)
        under_cap = mapping @ over_cap
        under_cap = under_cap.view(6,6)

def test():
    # conn = connect_db()
    # res, prev_read = measure_links(conn, {}, N_NODES)
    # res, prev_read = measure_flows(conn, {},N_NODES)
    # df = initialize_dataframe(conn)
    # install_routes(conn, df, [(0,0,1,100)], {0:(1,2)})
    return

if __name__ == "__main__":
    test()
    main()




    
