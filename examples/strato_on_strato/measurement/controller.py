import socket
import time
import threading
add2node = {
    "10.0.0.1": 1,
    "10.0.0.2": 2,
    "10.0.0.3": 3,
    "10.0.0.4": 4,
    "10.0.0.5": 5,
    "10.0.0.6": 6
}


node2addr = { v:k for k, v in add2node.items()}

vadd2node = {
    "10.0.0.1": 1,
    "10.0.0.2": 2,
    "10.0.0.3": 3,
    "10.0.0.4": 4,
    "10.0.0.5": 5,
    "10.0.0.6": 6
}
node2vaddr = { v:k for k, v in vadd2node.items()}

#add2node = {
#    "192.168.196.208": 1,
#    "192.168.196.93": 2,
#    "192.168.196.197": 3,
#    "192.168.196.194": 4,
#    "192.168.196.21": 5,
#    "192.168.196.158": 6
#}
#
#
#node2addr = { v:k for k, v in add2node.items()}
#
#vadd2node = {
#    "192.168.196.208": 1,
#    "192.168.196.93": 2,
#    "192.168.196.197": 3,
#    "192.168.196.194": 4,
#    "192.168.196.21": 5,
#    "192.168.196.158": 6
#}
#
#node2vaddr = { v:k for k, v in vadd2node.items()}


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
    action_sequece = [
        (1, [], [(1,2)]),
        (2, [], [(1,3)]),
        (3, [], [(1,4)]),
        (4, [], [(1,5)]),
        (5, [], [(1,6)]),
        (6, [], [(2,3)]),
        (7, [], [(2,4)]),
        (8, [], [(2,5)]),
        (9, [], [(2,6)]),
        (10, [], [(3,4)]),
        (11, [], [(3,5)]),
        (12, [], [(3,6)]),
        (13, [], [(4,5)]),
        (14, [], [(4,6)]),
        (15, [], [(5,6)])
    ]

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

    #start/stop links according to the action sequence
    for action in action_sequece:
        print("Performing action Action: ", action[0])
        time.sleep(action[0])
        for connect in action[1]:
            node1 = connect[0]
            node2 = connect[1]
            send_command(nodes[node1], f'start,{node2},\n')
        for disconnect in action[2]:
            node1 = disconnect[0]
            node2 = disconnect[1]
            send_command(nodes[node1], f'stop,{node2},\n')

    for node_id, sock in nodes.items():
        send_command(sock, f'exit,,\n')
        sock.close()

if __name__ == "__main__":
    controller()
