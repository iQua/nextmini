import socket
import time
import os
import threading
import psycopg2
import torch
from linear_constraints import generate_LC_map

#Change cwd to the file dir
file_path = os.path.abspath(__file__)
file_directory = os.path.dirname(file_path)
os.chdir(file_directory)

# add2node = {
#    "10.0.0.1": 1,
#    "10.0.0.2": 2,
#    "10.0.0.3": 3,
#    "10.0.0.4": 4,
#    "10.0.0.5": 5,
#    "10.0.0.6": 6
# }

# node2addr = { v:k for k, v in add2node.items()}

vadd2node = {
   "10.0.0.1": 1,
   "10.0.0.2": 2,
   "10.0.0.3": 3,
   "10.0.0.4": 4,
   "10.0.0.5": 5,
   "10.0.0.6": 6
}
node2vaddr = { v:k for k, v in vadd2node.items()}

N_NODES = 6

add2node = {
    "192.168.196.208": 1,
    "192.168.196.93": 2,
    "192.168.196.197": 3,
    "192.168.196.194": 4,
    "192.168.196.21": 5,
    "192.168.196.158": 6
}

node2addr = { v:k for k, v in add2node.items()}

# vadd2node = {
#     "192.168.196.208": 1,
#     "192.168.196.93": 2,
#     "192.168.196.197": 3,
#     "192.168.196.194": 4,
#     "192.168.196.21": 5,
#     "192.168.196.158": 6
# }

# node2vaddr = { v:k for k, v in vadd2node.items()}

action_sequece = []
for i in range(1,7):
    for j in range(1,7):
        if i==j:
            action_sequece.append((1, [], []))
            continue
        action_sequece.append((1, [], [(i,j)]))

model_path ="./models/LC6Nodes.ckpt"
def connect_db():
    conn = psycopg2.connect(
        user="pgusr",
        password="pgpwrd",
        host="127.0.0.1",
        port = '5432',
        database='strato')
    return conn

def measure_links(conn, prev_read):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT src_node_id, dst_node_id, total_bps, time_read
        FROM (
        SELECT src_node_id, dst_node_id, total_bps, time_read, ROW_NUMBER() OVER (PARTITION BY src_node_id, dst_node_id ORDER BY time_read DESC) AS row_num
        FROM (SELECT src_node_id, dst_node_id, SUM(bps) AS total_bps, time_read FROM "Metrics" GROUP BY src_node_id, dst_node_id, time_read) AS subquery1
        ) AS subquery
        WHERE row_num = 1;
    ''')
    metrics = cursor.fetchall()
    link_state = {}
    for metric in metrics:
        if not prev_read.get((metric[0], metric[1])) or prev_read[(metric[0], metric[1])] < metric[3]:
            link_state[(metric[0], metric[1])] = metric[2]
            prev_read[(metric[0], metric[1])] = metric[3]
        else:
            link_state[(metric[0], metric[1])] = 0
        
        print("{}, {}, {}".format(metric[0], metric[1], link_state[(metric[0], metric[1])]))
    for i in range(1, N_NODES+1):
        link_state[(i,i)] = 0
    return link_state, prev_read

def process_link_states(link_states):
    output = []
    temp  = [0 for link_state in link_states]
    for i in range(N_NODES):
        for j in range(N_NODES):
            temp = []
            for link_state in link_states:
                temp.append(link_state[(i+1,j+1)])
            output.append(temp)
    return output

print(node2vaddr)
def server(nodes):
    host = '0.0.0.0'
    port = 12354
    buffer_size = 4096

    # Create a socket object
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Bind the socket to a specific address and port
    server_socket.bind((host, port))

    # Listen for incoming connections
    server_socket.listen(1)
    
    print("Server listening on {}:{}".format(host, port))

    # Accept a connection from a node
    while True:
        client_socket, addr = server_socket.accept()

        nodeID = add2node[addr[0]]
        nodes[nodeID] = client_socket

def send_command(sock, command):

    # Send the command to the client
    sock.sendall(command.encode())

def controller():
    conn = connect_db()

    nodes = {}
    # Start the server in a thread
    print("Starting measurement controller")
    server_thread = threading.Thread(target=server, args=(nodes,))
    server_thread.daemon = True
    server_thread.start()

    #wait for all nodes to connect
    while len(nodes.items()) < len(add2node.items()):
        time.sleep(1)
    print("All nodes connected to controller")

    print(node2vaddr)
    #make all nodes connect to each other
    for node_id, sock in nodes.items():
        for node_id2,_ in nodes.items():
            if node_id != node_id2:
                send_command(sock, f'connect,{node_id2},{node2vaddr[node_id2]}\n')
    time.sleep(5)

    # Start the data transmission on all nodes
    start_time = time.time()
    for node_id, sock in nodes.items():
        for node_id2,_ in nodes.items():
            if node_id != node_id2:
                send_command(sock, f'start,{node_id2}, \n')
                print(f'Sent start command start, {node_id2} to node {node_id}')

    #wait for 1 second for all nodes to start sending
    time.sleep(5)
    
    #Get the current time as the epoch
    epoch = time.time()

    link_states=[]
    prev_read = {}
    #start/stop links according to the action sequence
    for action in action_sequece:
        print("Performing action Action: ", action)
        
        for connect in action[1]:
            node1 = connect[0]
            node2 = connect[1]
            send_command(nodes[node1], f'start,{node2},\n')
        for disconnect in action[2]:
            node1 = disconnect[0]
            node2 = disconnect[1]
            send_command(nodes[node1], f'stop,{node2},\n')
        
        time.sleep(action[0])
        link_state, prev_read = measure_links(conn, prev_read)
        link_states.append(link_state)

    for node_id, sock in nodes.items():
        send_command(sock, f'exit,,\n')
        sock.close()

    #Process the link states
    link_states = torch.tensor(process_link_states(link_states)).float().unsqueeze(0)
    torch.save(link_states, './link_states.pt')

    #Gernerate LC map]
    mapping = generate_LC_map(link_states, model_path)
    torch.save(mapping, './mapping.pt')


if __name__ == "__main__":
    controller()
