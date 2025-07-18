#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_status "Docker is running"
}

# Function to check if this node is a swarm manager
check_is_manager() {
    if docker info --format '{{.Swarm.ControlAvailable}}' | grep -q "true"; then
        return 0 # It is a manager
    else
        return 1 # It is not a manager
    fi
}

# Function to initialize Docker Swarm
init_swarm() {
    print_status "Ensuring this node is a Swarm Manager..."
    
    if check_is_manager; then
        print_status "This node is already a Swarm Manager. Skipping initialization."
    else
        print_status "This node is not a manager. Attempting to leave any existing swarm..."
        # This command is safe. It will succeed on a worker and fail silently (due to || true) on an inactive node.
        sudo docker swarm leave --force >/dev/null 2>&1 || true
        
        print_status "Initializing a new Swarm on this node..."
        docker swarm init
        print_status "Docker Swarm initialized successfully"
    fi
    
    # Get swarm info
    MANAGER_IP=$(docker info --format '{{.Swarm.NodeAddr}}')
    WORKER_TOKEN=$(docker swarm join-token worker -q)
    
    # Label the manager node
    print_status "Labeling manager node with role=manager"
    docker node update --label-add role=manager $(docker info --format '{{.Swarm.NodeID}}')

    print_status "Manager IP: $MANAGER_IP"
    print_status "Worker token obtained"
}

# Function to build Docker images
build_images() {
    print_status "Building Docker images..."
    
    # Build controller image
    print_status "Building controller image..."
    docker build -t nextmini_controller -f ../../controller/Dockerfile ../../
    
    # Build dataplane image
    print_status "Building dataplane image..."
    docker build -t nextmini_datapath -f ../../dataplane/Dockerfile ../../
    
    print_status "Docker images built successfully"
}

# Function to deploy services
deploy_stack() {
    print_status "Deploying NextMini stack..."
    
    docker stack deploy -c docker-compose.swarm.yml nextmini-stack
    
    print_status "Stack deployment initiated. Waiting for services to be ready..."
    
    # Wait for services to be running
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        # We expect 2 services on the manager node (postgres, controller)
        # The number of dataplane nodes depends on the number of worker nodes
        local running_services=$(docker service ls --filter name=nextmini-stack --format "{{.Replicas}}" | grep -c "1/1" || true)
        
        if [ "$running_services" -ge 2 ]; then
            print_status "Core services (postgres, controller) are running."
            break
        fi
        
        print_status "Waiting for core services... (attempt $((attempt + 1))/$max_attempts)"
        sleep 10
        attempt=$((attempt + 1))
    done
    
    if [ $attempt -eq $max_attempts ]; then
        print_error "Core services failed to start within the expected time."
        docker service ls --filter name=nextmini-stack
        exit 1
    fi
}

# Function to display join commands
show_join_commands() {
    print_status "Deployment completed successfully!"
    echo
    print_status "To add worker nodes to this swarm, run the following command on each additional VM:"
    echo
    echo -e "${GREEN}docker swarm join --token $WORKER_TOKEN $MANAGER_IP:2377${NC}"
    echo
    print_status "After joining, label each worker node from the manager by running:"
    print_warning "docker node update --label-add role=worker <WORKER_NODE_ID>"
    echo
    print_status "To check service status:"
    echo "docker stack services nextmini-stack"
    echo
    print_status "To scale dataplane nodes (if not using global mode):"
    echo "docker service scale nextmini-stack_dataplane-node=<number>"
    echo
    print_status "To view service logs:"
    echo "docker service logs nextmini-stack_controller"
    echo "docker service logs nextmini-stack_dataplane-node"
}

# Function to cleanup on error
cleanup_on_error() {
    print_error "Deployment failed. Cleaning up..."
    docker stack rm nextmini-stack 2>/dev/null || true
    exit 1
}

# Main deployment function
main() {
    print_status "Starting NextMini Docker Swarm deployment..."
    
    # Set up error handling
    trap cleanup_on_error ERR
    
    # Check prerequisites
    check_docker
    
    # Initialize swarm
    init_swarm
    
    # Build images
    build_images
    
    # Deploy services
    deploy_stack
    
    # Show join commands
    show_join_commands
}

# Run main function
main "$@"