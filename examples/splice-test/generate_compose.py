import textwrap
import os

# This script generates a docker-compose.yml file with a specified number of nodes.

# The script is located in examples/multi-nodes, and it modifies the docker-compose.yml in the same directory.
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_FILE = os.path.join(SCRIPT_DIR, "docker-compose.yml")
NUM_NODES = 4

def generate_node_service(node_id):
    """Generates the YAML configuration for a single node service."""
    ip_last_octet = 3 + node_id

    base_indent = "        "

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

    indented_depends_on = textwrap.indent(depends_on_block, base_indent)

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
{indented_depends_on}
        cap_add:
            - NET_ADMIN
        command: /bin/bash -c "sleep 7 && /var/nextmini/nextmini ws://controller:3000"
"""
    return service

def main():
    """
    Overwrites the docker-compose.yml to generate configurations for a specified number of nodes.
    It preserves the content of the file up to the 'node1:' service definition and generates all nodes from scratch.
    """
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

            for i in range(1, NUM_NODES + 1):
                f.write(generate_node_service(i))
        print(f"Successfully generated {DOCKER_COMPOSE_FILE} with {NUM_NODES} nodes.")
    except IOError as e:
        print(f"Error writing to {DOCKER_COMPOSE_FILE}: {e}")

if __name__ == "__main__":
    main()
