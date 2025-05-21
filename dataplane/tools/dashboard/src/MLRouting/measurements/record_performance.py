from measurements import *
from time import sleep
import sqlalchemy as sql
import sys
import pickle

def process_links(links_states, n_nodes):
    ret = [[] for _ in range(n_nodes**2)]
    for links_state in links_states:
        for i in range(n_nodes):
            for j in range(n_nodes):
                ret[i*n_nodes+j].append(links_state[i][j])
    return ret

def main(interval, n_nodes):
    user="pgusr"
    password="pgpwrd"
    host="127.0.0.1"
    port = "5432"
    database="strato"

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    
    engine = sql.create_engine(url)

    conn = engine.connect()

    links_states = []
    flows_states = []
    prev_read_links = {}
    prev_read_flows = {}
    time = 0
    while True:
        links_state, prev_read_links = measure_links(conn, prev_read_links, 6)
        flows_state, prev_read_flows = measure_flows(conn, prev_read_flows)
        print("Time: ", time)
        print("Links state: ", links_state)
        print("Flows state: ", flows_state)
        links_states.append(links_state)
        flows_states.append(flows_state)

        with open('links_states.pkl', 'wb') as f:
            pickle.dump(links_states, f)
        with open('flows_states.pkl', 'wb') as f:
            pickle.dump(flows_states, f)

        if links_state.sum() == 0:
            break
        sleep(interval)
        time += interval
    
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 record_performance.py <interval> <n_nodes>")
        exit(1)
    interval = int(sys.argv[1])
    n_nodes = int(sys.argv[2])
    main(interval, n_nodes)
