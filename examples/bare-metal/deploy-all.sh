#!/bin/bash
#
# Nextmini Auto-Deploy Script
# One-click deployment of all dataplane nodes to remote machines
#
# Features:
#   1. Build binaries
#   2. Generate TLS certificates
#   3. Prepare configuration for each node
#   4. Transfer files to all nodes in parallel
#   5. Start all nodes in parallel
#
# Usage:
#   ./deploy-all.sh                    # Use nodes.conf
#   ./deploy-all.sh --config my.conf   # Use custom config file
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Default configuration
CONFIG_FILE="$SCRIPT_DIR/nodes.conf"
CONTROLLER_IP="auto"
CONTROLLER_PORT=3000
SSH_USER="root"
SSH_KEY="$SCRIPT_DIR/ssh/id_rsa"
REMOTE_DEPLOY_DIR="/opt/nextmini"
SKIP_BUILD=false

# Temporary working directory
STAGING_DIR="$SCRIPT_DIR/.deploy-staging"

# ============================================================
# Function Definitions
# ============================================================

usage() {
    cat <<EOF
${GREEN}Nextmini Auto-Deploy Script${NC}

Usage: $0 [OPTIONS]

Options:
    --config FILE          Configuration file path (default: nodes.conf)
    --controller-ip IP     Controller IP (default: auto-detect)
    --skip-build          Skip build step
    -h, --help            Show this help

Examples:
    # Deploy with default configuration
    $0

    # Deploy with custom configuration
    $0 --config prod-nodes.conf

    # Skip build (use existing binary)
    $0 --skip-build

See nodes.conf for configuration file format.
EOF
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[⚠]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

log_step() {
    echo -e "\n${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}$1${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Load configuration file
load_config() {
    if [ ! -f "$CONFIG_FILE" ]; then
        log_error "Configuration file not found: $CONFIG_FILE"
        log_info "Please create a configuration file or use --config to specify one"
        exit 1
    fi
    
    log_info "Loading configuration: $CONFIG_FILE"
    
    # Read configuration items
    while IFS='=' read -r key value; do
        # Skip comments and empty lines
        [[ "$key" =~ ^#.*$ ]] && continue
        [[ -z "$key" ]] && continue
        [[ "$key" =~ ^[[:space:]]*$ ]] && continue
        
        # Trim whitespace
        key=$(echo "$key" | xargs)
        value=$(echo "$value" | xargs)
        
        case "$key" in
            CONTROLLER_IP) CONTROLLER_IP="$value" ;;
            CONTROLLER_PORT) CONTROLLER_PORT="$value" ;;
            SSH_USER) SSH_USER="$value" ;;
            SSH_KEY) SSH_KEY="$SCRIPT_DIR/$value" ;;
            REMOTE_DEPLOY_DIR) REMOTE_DEPLOY_DIR="$value" ;;
            SKIP_BUILD) SKIP_BUILD="$value" ;;
        esac
    done < <(grep -E "^[A-Z_]+=" "$CONFIG_FILE")
    
    # Auto-detect controller IP
    if [ "$CONTROLLER_IP" = "auto" ]; then
        CONTROLLER_IP=$(hostname -I | awk '{print $1}')
        log_info "Auto-detected Controller IP: $CONTROLLER_IP"
    fi
}

# Parse node list
parse_nodes() {
    NODES=()
    while IFS='|' read -r NODE_ID NODE_IP_PORT INTERFACE DATAPLANE_PORT; do
        # Skip comments and empty lines
        [[ "$NODE_ID" =~ ^#.*$ ]] && continue
        [[ -z "$NODE_ID" ]] && continue
        [[ "$NODE_ID" =~ ^[A-Z_]+=.* ]] && continue  # Skip configuration items
        
        # Trim whitespace
        NODE_ID=$(echo "$NODE_ID" | xargs)
        NODE_IP_PORT=$(echo "$NODE_IP_PORT" | xargs)
        INTERFACE=$(echo "$INTERFACE" | xargs)
        DATAPLANE_PORT=$(echo "$DATAPLANE_PORT" | xargs)
        
        # Parse NODE_IP and SSH_PORT (format: IP:PORT or just IP)
        if [[ "$NODE_IP_PORT" =~ ^(.+):([0-9]+)$ ]]; then
            NODE_IP="${BASH_REMATCH[1]}"
            SSH_PORT="${BASH_REMATCH[2]}"
        else
            NODE_IP="$NODE_IP_PORT"
            SSH_PORT="22"  # Default SSH port
        fi
        
        NODES+=("$NODE_ID|$NODE_IP|$SSH_PORT|$INTERFACE|$DATAPLANE_PORT")
    done < "$CONFIG_FILE"
    
    if [ ${#NODES[@]} -eq 0 ]; then
        log_error "No node definitions found in configuration file"
        log_info "Please add nodes in the format: NODE_ID|NODE_IP:SSH_PORT|INTERFACE|DATAPLANE_PORT"
        exit 1
    fi
    
    log_success "Found ${#NODES[@]} node(s)"
}

# Build binaries
build_binaries() {
    if [ "$SKIP_BUILD" = "true" ]; then
        log_warn "Skipping build step"
        return 0
    fi
    
    log_step "Step 1/6: Building Binaries"
    
    cd "$REPO_ROOT"
    
    # Build dataplane
    log_info "Building nextmini dataplane..."
    cargo build --release -p nextmini
    
    # Generate certificates
    if [ ! -f "$REPO_ROOT/server_cert.pem" ]; then
        log_info "Generating TLS certificates..."
        cargo run -p cert-gen
    fi
    
    log_success "Build complete"
}

# Prepare deployment packages
prepare_packages() {
    log_step "Step 2/6: Preparing Deployment Packages"
    
    # Clean old staging directory
    rm -rf "$STAGING_DIR"
    mkdir -p "$STAGING_DIR"
    
    for node_info in "${NODES[@]}"; do
        IFS='|' read -r NODE_ID NODE_IP SSH_PORT INTERFACE DATAPLANE_PORT <<< "$node_info"
        
        NODE_DIR="$STAGING_DIR/node$NODE_ID"
        mkdir -p "$NODE_DIR"
        
        log_info "Preparing Node $NODE_ID ($NODE_IP:$SSH_PORT)..."
        
        # Copy binary and certificates
        cp "$REPO_ROOT/target/release/nextmini" "$NODE_DIR/"
        cp "$REPO_ROOT/server_cert.pem" "$NODE_DIR/"
        cp "$REPO_ROOT/server_key.pem" "$NODE_DIR/"
        
        # Generate configuration file for this node
        cat > "$NODE_DIR/node.toml" <<EOF
# Nextmini Dataplane Node Configuration
# Auto-generated for Node $NODE_ID

# Network Configuration
private_network_interface = "$INTERFACE"
private_network_name = "node$NODE_ID"
ip_version = "ipv4"
public_network_port = "$DATAPLANE_PORT"

# Controller Connection
controller_addr = "ws://$CONTROLLER_IP:$CONTROLLER_PORT"
node_id = $NODE_ID

# Performance Tuning
num_tun_queues = 1
num_packet_processors = 4
channel_capacity = 4000
queue_capacity = 3000
feature = "concurrent"
EOF
        
        # Generate start script
        cat > "$NODE_DIR/start.sh" <<'STARTSCRIPT'
#!/bin/bash
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# Stop old process
if [ -f nextmini.pid ]; then
    OLD_PID=$(cat nextmini.pid)
    if ps -p $OLD_PID > /dev/null 2>&1; then
        echo "Stopping old process (PID: $OLD_PID)..."
        sudo kill $OLD_PID 2>/dev/null || true
        sleep 2
    fi
    rm -f nextmini.pid
fi

# Start
echo "Starting nextmini..."
export RUST_LOG=info
sudo -E nohup ./nextmini --config-path node.toml > node.log 2>&1 &
echo $! > nextmini.pid
echo "Started (PID: $(cat nextmini.pid))"
STARTSCRIPT
        
        chmod +x "$NODE_DIR/start.sh"
        
        # Generate stop script
        cat > "$NODE_DIR/stop.sh" <<'STOPSCRIPT'
#!/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$DIR/nextmini.pid" ]; then
    PID=$(cat "$DIR/nextmini.pid")
    if ps -p $PID > /dev/null 2>&1; then
        echo "Stopping process (PID: $PID)..."
        sudo kill $PID
        rm -f "$DIR/nextmini.pid"
        echo "Stopped"
    else
        echo "Process not running"
        rm -f "$DIR/nextmini.pid"
    fi
else
    echo "PID file not found"
fi
STOPSCRIPT
        
        chmod +x "$NODE_DIR/stop.sh"
    done
    
    log_success "Deployment packages prepared"
}

# Setup SSH access
setup_ssh() {
    log_step "Step 3/6: Setting up SSH Access"
    
    if [ ! -f "$SSH_KEY" ]; then
        log_error "SSH private key not found: $SSH_KEY"
        exit 1
    fi
    
    PUBLIC_KEY=$(cat "${SSH_KEY}.pub")
    
    for node_info in "${NODES[@]}"; do
        IFS='|' read -r NODE_ID NODE_IP SSH_PORT INTERFACE DATAPLANE_PORT <<< "$node_info"
        
        log_info "Configuring Node $NODE_ID ($NODE_IP:$SSH_PORT)..."
        
        SSH_OPTS="-i $SSH_KEY -p $SSH_PORT -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
        
        ssh $SSH_OPTS "$SSH_USER@$NODE_IP" "
            mkdir -p ~/.ssh
            chmod 700 ~/.ssh
            echo '$PUBLIC_KEY' >> ~/.ssh/authorized_keys
            chmod 600 ~/.ssh/authorized_keys
            sort -u ~/.ssh/authorized_keys -o ~/.ssh/authorized_keys
        " 2>/dev/null && log_success "SSH configured" || log_warn "SSH setup failed (may already be configured)"
    done
}

# Transfer files (parallel)
transfer_files() {
    log_step "Step 4/6: Transferring Files to All Nodes"
    
    for node_info in "${NODES[@]}"; do
        IFS='|' read -r NODE_ID NODE_IP SSH_PORT INTERFACE DATAPLANE_PORT <<< "$node_info"
        
        (
            log_info "[Node $NODE_ID] Transferring to $NODE_IP:$SSH_PORT..."
            
            SSH_OPTS="-i $SSH_KEY -p $SSH_PORT -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
            SCP_OPTS="-i $SSH_KEY -P $SSH_PORT -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
            
            # Create remote directory
            ssh $SSH_OPTS "$SSH_USER@$NODE_IP" "mkdir -p $REMOTE_DEPLOY_DIR/node$NODE_ID" 2>/dev/null
            
            # Transfer files (note: SCP uses -P for port, SSH uses -p)
            scp $SCP_OPTS -r "$STAGING_DIR/node$NODE_ID/"* "$SSH_USER@$NODE_IP:$REMOTE_DEPLOY_DIR/node$NODE_ID/" 2>/dev/null
            
            if [ $? -eq 0 ]; then
                log_success "[Node $NODE_ID] Transfer complete"
            else
                log_error "[Node $NODE_ID] Transfer failed"
            fi
        ) &
    done
    
    # Wait for all transfers to complete
    wait
    log_success "All file transfers complete"
}

# Start nodes (parallel)
start_nodes() {
    log_step "Step 5/6: Starting All Nodes"
    
    for node_info in "${NODES[@]}"; do
        IFS='|' read -r NODE_ID NODE_IP SSH_PORT INTERFACE DATAPLANE_PORT <<< "$node_info"
        
        (
            log_info "[Node $NODE_ID] Starting..."
            
            SSH_OPTS="-i $SSH_KEY -p $SSH_PORT -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
            
            ssh $SSH_OPTS "$SSH_USER@$NODE_IP" "cd $REMOTE_DEPLOY_DIR/node$NODE_ID && ./start.sh" 2>/dev/null
            
            if [ $? -eq 0 ]; then
                log_success "[Node $NODE_ID] Started"
            else
                log_error "[Node $NODE_ID] Start failed"
            fi
        ) &
    done
    
    # Wait for all starts to complete
    wait
    log_success "All nodes started"
}

# Verify deployment
verify_deployment() {
    log_step "Step 6/6: Verifying Deployment"
    
    sleep 2
    
    for node_info in "${NODES[@]}"; do
        IFS='|' read -r NODE_ID NODE_IP SSH_PORT INTERFACE DATAPLANE_PORT <<< "$node_info"
        
        log_info "Checking Node $NODE_ID ($NODE_IP:$SSH_PORT)..."
        
        SSH_OPTS="-i $SSH_KEY -p $SSH_PORT -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
        
        STATUS=$(ssh $SSH_OPTS "$SSH_USER@$NODE_IP" "
            if [ -f $REMOTE_DEPLOY_DIR/node$NODE_ID/nextmini.pid ]; then
                PID=\$(cat $REMOTE_DEPLOY_DIR/node$NODE_ID/nextmini.pid)
                if ps -p \$PID > /dev/null 2>&1; then
                    echo 'RUNNING:\$PID'
                else
                    echo 'STOPPED'
                fi
            else
                echo 'NO_PID'
            fi
        " 2>/dev/null)
        
        case "$STATUS" in
            RUNNING:*)
                PID="${STATUS#RUNNING:}"
                log_success "Node $NODE_ID is running (PID: $PID)"
                ;;
            *)
                log_error "Node $NODE_ID is not running"
                ;;
        esac
    done
}

# Show management information
show_management_info() {
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}Deployment Complete!${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo "Node Configuration:"
    for node_info in "${NODES[@]}"; do
        IFS='|' read -r NODE_ID NODE_IP SSH_PORT INTERFACE DATAPLANE_PORT <<< "$node_info"
        echo "  • Node $NODE_ID: $NODE_IP (SSH: $SSH_PORT, Dataplane: $DATAPLANE_PORT, Interface: $INTERFACE)"
    done
    echo ""
    echo "Each node's configuration (generated automatically):"
    echo "  controller_addr:  ws://$CONTROLLER_IP:$CONTROLLER_PORT"
    echo "  node_id:          Unique for each node (1, 2, 3, ...)"
    echo "  interface:        From nodes.conf"
    echo "  dataplane_port:   From nodes.conf"
    echo ""
    echo "Management Commands:"
    echo ""
    for node_info in "${NODES[@]}"; do
        IFS='|' read -r NODE_ID NODE_IP SSH_PORT INTERFACE DATAPLANE_PORT <<< "$node_info"
        echo "Node $NODE_ID ($NODE_IP:$SSH_PORT):"
        echo "  View logs:   ssh -i $SSH_KEY -p $SSH_PORT $SSH_USER@$NODE_IP 'tail -f $REMOTE_DEPLOY_DIR/node$NODE_ID/node.log'"
        echo "  View config: ssh -i $SSH_KEY -p $SSH_PORT $SSH_USER@$NODE_IP 'cat $REMOTE_DEPLOY_DIR/node$NODE_ID/node.toml'"
        echo "  Stop node:   ssh -i $SSH_KEY -p $SSH_PORT $SSH_USER@$NODE_IP '$REMOTE_DEPLOY_DIR/node$NODE_ID/stop.sh'"
        echo "  Start node:  ssh -i $SSH_KEY -p $SSH_PORT $SSH_USER@$NODE_IP '$REMOTE_DEPLOY_DIR/node$NODE_ID/start.sh'"
        echo ""
    done
    echo "Stop all nodes:"
    echo "  for node in ${NODES[@]}; do"
    echo "    IFS='|' read -r ID IP SSH_PORT _ _ <<< \"\$node\""
    echo "    ssh -i $SSH_KEY -p \$SSH_PORT $SSH_USER@\$IP '$REMOTE_DEPLOY_DIR/node'\$ID'/stop.sh'"
    echo "  done"
    echo ""
}

# ============================================================
# Main Flow
# ============================================================

main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            --controller-ip)
                CONTROLLER_IP="$2"
                shift 2
                ;;
            --skip-build)
                SKIP_BUILD=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    # Show banner
    echo -e "${GREEN}"
    cat <<'BANNER'
╔═══════════════════════════════════════════════════════╗
║                                                       ║
║         Nextmini Auto-Deploy Script                  ║
║                                                       ║
╚═══════════════════════════════════════════════════════╝
BANNER
    echo -e "${NC}"
    
    # Load configuration
    load_config
    parse_nodes
    
    # Display configuration
    log_info "Controller: ws://$CONTROLLER_IP:$CONTROLLER_PORT"
    log_info "SSH User: $SSH_USER"
    log_info "Number of nodes: ${#NODES[@]}"
    log_info "Remote deploy directory: $REMOTE_DEPLOY_DIR"
    
    # Execute deployment flow
    build_binaries
    prepare_packages
    setup_ssh
    transfer_files
    start_nodes
    verify_deployment
    
    # Show management information
    show_management_info
    
    # Cleanup staging directory
    rm -rf "$STAGING_DIR"
}

# Run main flow
main "$@"
