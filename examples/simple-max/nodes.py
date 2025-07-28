import textwrap
import os
import argparse
import re

# This script generates a docker-compose.yml file with a specified number of dataplane nodes.

# The script is located in examples/simple-max, and it modifies the docker-compose.yml in the same directory.
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_FILE = os.path.join(SCRIPT_DIR, "docker-compose.yml")
CONTROLLER_CONFIG_FILE = os.path.join(SCRIPT_DIR, "controller-config.toml")

def generate_node_service(node_id):
    """Generates the YAML configuration for a single node service."""
    # IP addresses start from 172.16.8.4 for node1, 172.16.8.5 for node2, etc.
    ip_last_octet = 3 + node_id

    if node_id == 1:
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
{textwrap.indent(depends_on_block, '        ')}
        cap_add:
            - NET_ADMIN
        command: /bin/bash -c "sleep 7 && /var/nextmini/nextmini ws://controller:3000"
"""
    return service

def update_controller_config(num_nodes):
    """Updates only the n_nodes and routes in controller-config.toml file."""
    try:
        with open(CONTROLLER_CONFIG_FILE, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: {CONTROLLER_CONFIG_FILE} not found.")
        return False

    # Update n_nodes value
    content = re.sub(r'n_nodes = \d+', f'n_nodes = {num_nodes}', content)

    # Generate new routes: forward route [1, 2, 3, ..., num_nodes] and reverse route [num_nodes, ..., 3, 2, 1]
    forward_route = list(range(1, num_nodes + 1))
    reverse_route = list(range(num_nodes, 0, -1))

    # Replace the existing routes section
    routes_pattern = r'\[\[routes\]\].*?route = \[.*?\].*?\[\[routes\]\].*?route = \[.*?\]'
    new_routes = f"""[[routes]]
route = {forward_route}

[[routes]]
route = {reverse_route}"""

    content = re.sub(routes_pattern, new_routes, content, flags=re.DOTALL)

    try:
        with open(CONTROLLER_CONFIG_FILE, 'w') as f:
            f.write(content)
        return True
    except IOError as e:
        print(f"Error writing to {CONTROLLER_CONFIG_FILE}: {e}")
        return False

def main():
    """
    Overwrites the docker-compose.yml to generate configurations for a specified number of dataplane nodes.
    It preserves the content of the file up to the 'node1:' service definition and generates all nodes from scratch.
    """
    parser = argparse.ArgumentParser(description='Generate docker-compose.yml with specified number of dataplane nodes for simple-max example')
    parser.add_argument('--nodes', '-n', type=int, default=4,
                        help='Number of dataplane nodes to generate (default: 4, generates node1 to node4)')
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

    content_before_nodes = []
    node1_found = False
    for line in lines:
        if line.strip() == "node1:":
            node1_found = True
            break
        content_before_nodes.append(line)

    if not node1_found:
        print("Warning: 'node1:' service not found in the original docker-compose.yml. The script will append nodes to the end.")

    try:
        with open(DOCKER_COMPOSE_FILE, 'w') as f:
            f.writelines(content_before_nodes)

            # Generate dataplane nodes from node1 to node{num_nodes}
            for i in range(1, num_nodes + 1):
                f.write(generate_node_service(i))

        print(f"Successfully generated {DOCKER_COMPOSE_FILE} with {num_nodes} dataplane nodes (node1 to node{num_nodes}).")

        # Update controller config
        if update_controller_config(num_nodes):
            print(f"Successfully updated {CONTROLLER_CONFIG_FILE} for {num_nodes} dataplane nodes.")
            print(f"Forward route: {list(range(1, num_nodes + 1))}")
            print(f"Reverse route: {list(range(num_nodes, 0, -1))}")
        else:
            print(f"Warning: Failed to update {CONTROLLER_CONFIG_FILE}")

    except IOError as e:
        print(f"Error writing to {DOCKER_COMPOSE_FILE}: {e}")

if __name__ == "__main__":
    main() 