import socket
import time
import multiprocessing
import threading
def server(sockets):
    host = '0.0.0.0'  # Server IP address
    port = 18181  # Server port
    buffer_size = 4096  # Buffer size for receiving data

    # Create a socket object
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to a specific address and port
    server_socket.bind((host, port))

    # Listen for incoming connections
    server_socket.listen(1)

    print("Server listening on {}:{}".format(host, port))

    # Accept a connection from a client
    while True:
        sock, _ = server_socket.accept()
        sockets.append(sock)
    
    server_socket.close()

def send_data(socket):
    # Generate the payload data
    payload_size = 1024 * 1024  # Size of the payload (1MB in this example)
    data = b'A' * payload_size  # A payload filled with 'A' characters

    total_data = 0

    while True:
        socket.sendall(data)
        total_data += payload_size

def receive_data(sockets):
    while True:
        for sock in sockets:
            try:
                data = sock.recv(1024)
                if not data:
                    continue
            except Exception as e:
                print(e)
                continue

def main():
    #Start the server in a thread
    sockets = []
    server_thread = threading.Thread(target=server, args=(sockets,))
    server_thread.daemon=True
    server_thread.start()

    #Start the receiver in a thread
    receiver_thread = threading.Thread(target=receive_data, args=(sockets,))
    receiver_thread.daemon=True
    receiver_thread.start()

    #Connect to the controller
    #controller_address = ('10.0.0.6', 12354)  # Controller IP address and port
    controller_address = ('192.168.196.158', 12354)  # Controller IP address and port
    controller_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while True:
        try:
            controller_socket.connect(controller_address)
            break
        except:
            print("Waiting for controller...")
            time.sleep(1)
            continue

    # Listen for commands from the controller
    connections = {}
    senders  = {}
    while True:
        commands = controller_socket.recv(1024).decode()
        commands = commands.split("\n")
        for command in commands:
            print(command)
            if command == "": continue
            action, node, addr = command.split(",")
            if action == 'start':
                # Start data transmission
                print("Received start command. Starting data transmission to node {}...", node)
                # Your code to start data transmission goes here
                if senders.get(node) is None:
                    senders[node] = multiprocessing.Process(target=send_data, args=(connections[node],))
                    senders[node].start()
                else:
                    print("Thread already started")

            elif action == 'stop':
                # Stop data transmission
                print("Received stop command. Stopping data transmission to node {}...", node)
                # Your code to stop data transmission goes here
                if senders.get(node) is not None:
                    senders[node].terminate()
                    senders[node] = None

            if action == 'connect':
                # Connect to a node
                print("Received connect command. Connecting to node {} at {}...".format(node, addr))
                # Your code to connect to a node goes here

                client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client_sock.connect((addr, 18181))
                connections[node] = client_sock

            elif action == 'exit':
                # Exit the program
                print("Received exit command. Exiting...")
                # Your code to exit the program goes here
                for k, i in connections.items():
                    i.close()
                return
main()
