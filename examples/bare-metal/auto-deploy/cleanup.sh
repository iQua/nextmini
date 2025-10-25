#!/bin/bash
#
# Nextmini Auto-Deploy Cleanup Script
# Stop and cleanup all deployed controller and dataplane nodes
#
# Features:
#   1. Stop controller
#   2. Stop PostgreSQL database
#   3. Stop all remote dataplane nodes
#   4. Cleanup deployment directories (optional)
#
# Usage:
#   ./cleanup.sh                  # Stop all & remove deployment dirs
#   ./cleanup.sh --keep-dirs      # Stop all but keep deployment dirs
#   ./cleanup.sh --nodes-only     # Only stop remote nodes
#   ./cleanup.sh --controller-only # Only stop controller
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
BARE_METAL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Default configuration
CONFIG_FILE="$SCRIPT_DIR/nodes.conf"
KEEP_DIRS=false
NODES_ONLY=false
CONTROLLER_ONLY=false
DEPLOY_DIR="$BARE_METAL_DIR/controller-deploy"

# SSH Configuration (will be loaded from config)
SSH_USER="root"
SSH_KEY="$SCRIPT_DIR/ssh/id_rsa"
REMOTE_DEPLOY_DIR="/opt/nextmini"

# ============================================================
# Function Definitions
# ============================================================

usage() {
    cat <<EOF
${GREEN}Nextmini Auto-Deploy Cleanup Script${NC}

Usage: $0 [OPTIONS]

Options:
    --keep-dirs            Keep deployment directories (only stop processes)
    --nodes-only           Only stop remote nodes (keep controller running)
    --controller-only      Only stop controller (keep remote nodes running)
    --config FILE          Configuration file path (default: nodes.conf)
    -h, --help            Show this help

Examples:
    # Stop everything and remove deployment directories
    $0

    # Stop everything but keep directories
    $0 --keep-dirs

    # Only stop remote nodes
    $0 --nodes-only

    # Only stop controller
    $0 --controller-only
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
        log_warn "Configuration file not found: $CONFIG_FILE"
        log_info "Will skip remote node cleanup"
        return 0
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
        # Remove inline comments (everything after #) and trim whitespace
        value=$(echo "$value" | sed 's/#.*//' | xargs)
        
        case "$key" in
            SSH_USER) SSH_USER="$value" ;;
            SSH_KEY)
                # Remove leading ./ if present, then make it relative to SCRIPT_DIR
                value="${value#./}"
                SSH_KEY="$SCRIPT_DIR/$value"
                ;;
            REMOTE_DEPLOY_DIR) REMOTE_DEPLOY_DIR="$value" ;;
        esac
    done < <(grep -E "^[A-Z_]+=" "$CONFIG_FILE")
}

# Parse node list
parse_nodes() {
    NODES=()
    
    if [ ! -f "$CONFIG_FILE" ]; then
        return 0
    fi
    
    while IFS='|' read -r NODE_ID NODE_IP_PORT INTERFACE DATAPLANE_PORT; do
        # Skip comments and empty lines
        [[ "$NODE_ID" =~ ^#.*$ ]] && continue
        [[ -z "$NODE_ID" ]] && continue
        [[ "$NODE_ID" =~ ^[A-Z_]+=.* ]] && continue  # Skip configuration items
        
        # Trim whitespace
        NODE_ID=$(echo "$NODE_ID" | xargs)
        NODE_IP_PORT=$(echo "$NODE_IP_PORT" | xargs)
        
        # Parse NODE_IP and SSH_PORT (format: IP:PORT or just IP)
        if [[ "$NODE_IP_PORT" =~ ^(.+):([0-9]+)$ ]]; then
            NODE_IP="${BASH_REMATCH[1]}"
            SSH_PORT="${BASH_REMATCH[2]}"
        else
            NODE_IP="$NODE_IP_PORT"
            SSH_PORT="22"  # Default SSH port
        fi
        
        NODES+=("$NODE_ID|$NODE_IP|$SSH_PORT")
    done < "$CONFIG_FILE"
    
    if [ ${#NODES[@]} -gt 0 ]; then
        log_success "Found ${#NODES[@]} node(s) to cleanup"
    fi
}

# Stop controller
stop_controller() {
    if [ "$NODES_ONLY" = "true" ]; then
        log_info "Skipping controller cleanup (--nodes-only specified)"
        return 0
    fi
    
    log_step "Step 1/3: Stopping Controller"
    
    # Stop controller process
    if [ -f "$DEPLOY_DIR/controller.pid" ]; then
        PID=$(cat "$DEPLOY_DIR/controller.pid")
        if ps -p $PID > /dev/null 2>&1; then
            log_info "Stopping controller (PID: $PID)..."
            kill $PID 2>/dev/null && sleep 2
            if ps -p $PID > /dev/null 2>&1; then
                kill -9 $PID 2>/dev/null || true
            fi
            log_success "Controller stopped"
        else
            log_warn "Controller process not running"
        fi
        rm -f "$DEPLOY_DIR/controller.pid"
    else
        log_warn "Controller PID file not found"
    fi
    
    # Also try pkill as fallback
    if pkill -9 controller 2>/dev/null; then
        log_info "Killed any remaining controller processes"
    fi
}

# Stop database
stop_database() {
    if [ "$NODES_ONLY" = "true" ]; then
        log_info "Skipping database cleanup (--nodes-only specified)"
        return 0
    fi
    
    log_step "Step 2/3: Stopping PostgreSQL Database"
    
    if docker ps | grep -q nextmini-database; then
        log_info "Stopping database container..."
        docker stop nextmini-database >/dev/null 2>&1
        docker rm nextmini-database >/dev/null 2>&1
        log_success "Database stopped and removed"
    else
        log_warn "Database container not running"
    fi
}

# Stop remote nodes
stop_remote_nodes() {
    if [ "$CONTROLLER_ONLY" = "true" ]; then
        log_info "Skipping remote nodes cleanup (--controller-only specified)"
        return 0
    fi
    
    log_step "Step 3/3: Stopping Remote Dataplane Nodes"
    
    if [ ${#NODES[@]} -eq 0 ]; then
        log_warn "No nodes found in configuration"
        return 0
    fi
    
    if [ ! -f "$SSH_KEY" ]; then
        log_error "SSH private key not found: $SSH_KEY"
        log_warn "Cannot stop remote nodes"
        return 0
    fi
    
    for node_info in "${NODES[@]}"; do
        IFS='|' read -r NODE_ID NODE_IP SSH_PORT <<< "$node_info"
        
        (
            log_info "[Node $NODE_ID] Stopping node on $NODE_IP:$SSH_PORT..."
            
            SSH_OPTS="-i $SSH_KEY -p $SSH_PORT -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o LogLevel=ERROR"
            
            # Try to stop using stop script
            ssh $SSH_OPTS "$SSH_USER@$NODE_IP" "
                if [ -f $REMOTE_DEPLOY_DIR/node$NODE_ID/stop.sh ]; then
                    cd $REMOTE_DEPLOY_DIR/node$NODE_ID && ./stop.sh
                else
                    # Fallback: kill by name
                    sudo pkill -9 nextmini || true
                fi
            " 2>/dev/null
            
            if [ $? -eq 0 ]; then
                log_success "[Node $NODE_ID] Stopped"
            else
                log_warn "[Node $NODE_ID] Failed to stop (may not be running)"
            fi
        ) &
    done
    
    # Wait for all stops to complete
    wait
    log_success "All remote nodes processed"
}

# Cleanup deployment directories
cleanup_directories() {
    if [ "$KEEP_DIRS" = "true" ]; then
        log_info "Keeping deployment directories (--keep-dirs specified)"
        return 0
    fi
    
    echo ""
    log_step "Cleanup: Removing Deployment Directories"
    
    # Remove controller deployment directory
    if [ "$NODES_ONLY" != "true" ]; then
        if [ -d "$DEPLOY_DIR" ]; then
            log_info "Removing $DEPLOY_DIR..."
            rm -rf "$DEPLOY_DIR"
            log_success "Controller deployment directory removed"
        fi
    fi
    
    # Remove staging directory if exists
    if [ -d "$SCRIPT_DIR/.deploy-staging" ]; then
        rm -rf "$SCRIPT_DIR/.deploy-staging"
        log_info "Removed staging directory"
    fi
}

# Show cleanup summary
show_summary() {
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}Cleanup Complete!${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    if [ "$NODES_ONLY" = "true" ]; then
        echo "Stopped: Remote dataplane nodes"
        echo "Kept running: Controller and database"
    elif [ "$CONTROLLER_ONLY" = "true" ]; then
        echo "Stopped: Controller and database"
        echo "Remote nodes: Not touched"
    else
        echo "Stopped: Controller, database, and all remote nodes"
    fi
    
    if [ "$KEEP_DIRS" = "true" ]; then
        echo "Deployment directories: Kept"
    else
        echo "Deployment directories: Removed"
    fi
    
    echo ""
    echo "To redeploy:"
    echo "  cd $SCRIPT_DIR"
    if [ "$NODES_ONLY" = "true" ]; then
        echo "  ./deploy-all.sh"
    elif [ "$CONTROLLER_ONLY" = "true" ]; then
        echo "  ./deploy-controller.sh"
    else
        echo "  ./deploy-controller.sh"
        echo "  ./deploy-all.sh"
    fi
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# ============================================================
# Main Flow
# ============================================================

main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --keep-dirs)
                KEEP_DIRS=true
                shift
                ;;
            --nodes-only)
                NODES_ONLY=true
                shift
                ;;
            --controller-only)
                CONTROLLER_ONLY=true
                shift
                ;;
            --config)
                CONFIG_FILE="$2"
                shift 2
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
    
    # Validate options
    if [ "$NODES_ONLY" = "true" ] && [ "$CONTROLLER_ONLY" = "true" ]; then
        log_error "Cannot use --nodes-only and --controller-only together"
        exit 1
    fi
    
    # Show banner
    echo -e "${GREEN}"
    cat <<'BANNER'
╔═══════════════════════════════════════════════════════╗
║                                                       ║
║         Nextmini Auto-Deploy Cleanup Script          ║
║                                                       ║
╚═══════════════════════════════════════════════════════╝
BANNER
    echo -e "${NC}"
    
    # Load configuration
    load_config
    parse_nodes
    
    # Execute cleanup flow
    stop_controller
    stop_database
    stop_remote_nodes
    cleanup_directories
    
    # Show summary
    show_summary
}

# Run main flow
main "$@"

