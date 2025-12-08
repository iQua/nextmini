import textwrap
import os
import argparse
import re

# This script generates a docker-compose.yml file with a specified number of dataplane nodes.

# The script is located in examples/curl, and it modifies the docker-compose.yml in the same directory.
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_FILE = os.path.join(SCRIPT_DIR, "docker-compose.yml")
CONTROLLER_CONFIG_FILE = os.path.join(SCRIPT_DIR, "controller-config.toml")
CLIENT_FILE = os.path.join(SCRIPT_DIR, "src", "client.sh")

def generate_node_service(node_id):
    """Generates the YAML configuration for a single node service."""
    # IP addresses start from 172.16.8.5 for node2
    ip_last_octet = 3 + node_id

    if node_id == 2:
        depends_on_block = textwrap.dedent("""
        depends_on:
          - controller
        """).strip()
    else:
        depends_on_block = textwrap.dedent(f"""
        depends_on:
          - controller
          - node{node_id - 1}
        """).strip()

    # Calculate sleep time (node2 sleeps 7s, node3 sleeps 8s, etc.)
    sleep_time = 5 + node_id

    service = f"""
  node{node_id}:
    container_name: node{node_id}
    hostname: node{node_id}
    image: nextmini_datapath
    build:
      context: ../../
      dockerfile: ./dataplane/Dockerfile
    networks:
      network:
        ipv4_address: 172.16.8.{ip_last_octet}
    stdin_open: true
    privileged: true
    # environment:
    #   - RUST_LOG=debug
    volumes:
      - ./config.toml:/var/nextmini/config.toml
      - ../../tools/:/var/nextmini/tools
{textwrap.indent(depends_on_block, '    ')}
    cap_add:
      - NET_ADMIN
    command: /bin/bash -c "sleep {sleep_time} && /var/nextmini/nextmini ws://controller:3000"
"""
    return service

def generate_external_server_service(last_node_id):
    """Generates the YAML configuration for the external_server service."""
    # external_server IP should be after the last node
    server_ip_last_octet = 3 + last_node_id + 1
    sleep_time = 5 + last_node_id + 1

    service = f"""
  external_server:
    container_name: external_server
    hostname: external_server
    image: nextmini_external_server
    build:
      context: ./
      dockerfile: ./src/Dockerfile.server
    networks:
      network:
        ipv4_address: 172.16.8.{server_ip_last_octet}
    stdin_open: true
    privileged: true
    ports:
      - "8080:8080"
    cap_add:
      - NET_ADMIN
    command: /bin/sh -c "sleep {sleep_time} && /var/nextmini/src"
"""
    return service

def update_controller_config(num_nodes):
    """Updates the controller-config.toml file based on the number of dataplane nodes."""
    try:
        with open(CONTROLLER_CONFIG_FILE, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: {CONTROLLER_CONFIG_FILE} not found.")
        return False

    # Update n_nodes value
    content = re.sub(r'n_nodes = \d+', f'n_nodes = {num_nodes}', content)

    # Generate new route: [1, 2, 3, ..., num_nodes+1, num_nodes+2]
    # Node 1 = external client, Nodes 2 to num_nodes+1 = dataplane nodes, Node num_nodes+2 = external server
    route_nodes = list(range(1, num_nodes + 3))  # [1, 2, ..., num_nodes+1, num_nodes+2]
    new_route = f"route = {route_nodes}"

    # Replace all existing route lines with the new route
    content = re.sub(r'route = \[.*?\]', new_route, content)

    try:
        with open(CONTROLLER_CONFIG_FILE, 'w') as f:
            f.write(content)
        return True
    except IOError as e:
        print(f"Error writing to {CONTROLLER_CONFIG_FILE}: {e}")
        return False

def update_client_sh(num_nodes):
    """Updates the client.sh file with the correct target server IP address."""
    try:
        with open(CLIENT_FILE, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: {CLIENT_FILE} not found.")
        return False

    # Calculate server IP: 172.16.8.{3 + last_node_id + 1}
    # last_node_id = num_nodes + 1, so server IP = 172.16.8.{3 + num_nodes + 1 + 1} = 172.16.8.{num_nodes + 5}
    server_ip_last_octet = num_nodes + 5

    # Update the TARGET line
    old_pattern = r'TARGET=http://172\.16\.8\.\d+:8080/large_test\.dat'
    new_line = f'TARGET=http://172.16.8.{server_ip_last_octet}:8080/large_test.dat'

    content = re.sub(old_pattern, new_line, content)

    try:
        with open(CLIENT_FILE, 'w') as f:
            f.write(content)
        return True
    except IOError as e:
        print(f"Error writing to {CLIENT_FILE}: {e}")
        return False

def main():
    """
    Overwrites the docker-compose.yml to generate configurations for a specified number of dataplane nodes.
    It preserves the content of the file up to the 'external_client:' service definition and generates all nodes from scratch.
    """
    parser = argparse.ArgumentParser(description='Generate docker-compose.yml with specified number of dataplane nodes')
    parser.add_argument('--nodes', '-n', type=int, default=3,
                        help='Number of dataplane nodes to generate (default: 3, generates node2 to node4)')
    args = parser.parse_args()

    num_nodes = args.nodes
    if num_nodes < 1:
        print("Error: Number of nodes must be at least 1")
        return

    try:
        with open(DOCKER_COMPOSE_FILE, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: {DOCKER_COMPOSE_FILE} not found.")
        return

    # Find the start of node2 service to preserve everything before it
    content_before_nodes = []
    node2_found = False

    for line in lines:
        if line.strip() == "node2:":
            node2_found = True
            break
        content_before_nodes.append(line)

    if not node2_found:
        print("Warning: 'node2:' service not found in the original docker-compose.yml. The script will append nodes to the end.")

    try:
        with open(DOCKER_COMPOSE_FILE, 'w') as f:
            f.writelines(content_before_nodes)



            # Generate dataplane nodes from node2 to node(num_nodes+1)
            last_node_id = num_nodes + 1
            for i in range(2, last_node_id + 1):
                f.write(generate_node_service(i))

            # Generate external_server service
            f.write(generate_external_server_service(last_node_id))

        print(f"Successfully generated {DOCKER_COMPOSE_FILE} with {num_nodes} dataplane nodes (node2 to node{last_node_id}).")
        print(f"External server IP: 172.16.8.{3 + last_node_id + 1}")

        # Update controller config
        if update_controller_config(num_nodes):
            print(f"Successfully updated {CONTROLLER_CONFIG_FILE} for {num_nodes} dataplane nodes.")
            print(f"Route: [1, 2, ..., {last_node_id}, {last_node_id + 1}] (client -> dataplane nodes -> server)")
        else:
            print(f"Warning: Failed to update {CONTROLLER_CONFIG_FILE}")

        # Update client.sh
        if update_client_sh(num_nodes):
            server_ip = f"172.16.8.{num_nodes + 5}"
            print(f"Successfully updated {CLIENT_FILE} with target server IP: {server_ip}")
        else:
            print(f"Warning: Failed to update {CLIENT_FILE}")

    except IOError as e:
        print(f"Error writing to {DOCKER_COMPOSE_FILE}: {e}")

if __name__ == "__main__":
    main()
