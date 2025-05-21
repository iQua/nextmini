import threading
import subprocess
import argparse

def start_iperf_server(port):
    """Function to start iperf3 server on a specific port."""
    subprocess.Popen(["iperf3", "-s", "-p", str(port)])

def start_iperf_client(port, common_args):
    """Function to start iperf3 client connecting to a specific port."""
    client_args = common_args + ["-p", str(port)]
    subprocess.Popen(client_args)

def main(n, starting_port, mode, common_args=None):
    """Starts n iperf3 servers/clients, each on its own thread and port."""
    for i in range(n):
        port = starting_port + i
        if mode == 'server':
            thread = threading.Thread(target=start_iperf_server, args=(port,))
            print(f"iperf3 server started on port {port}")
        else:  # mode == 'client'
            thread = threading.Thread(target=start_iperf_client, args=(port, common_args))
            print(f"iperf3 client started to connect to port {port}")
        thread.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start multiple iperf3 servers or clients.")
    parser.add_argument("n", type=int, help="Number of iperf3 instances to start")
    parser.add_argument("--starting-port", type=int, default=5201, help="Starting port number")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-S", "--server", action="store_true", help="Run in server mode")
    group.add_argument("-C", "--client", action="store_true", help="Run in client mode")
    parser.add_argument("common_args", nargs=argparse.REMAINDER, help="Common arguments for all iperf3 clients including server address")
    args = parser.parse_args()

    mode = 'server' if args.server else 'client'
    main(args.n, args.starting_port, mode, args.common_args if args.client else None)
