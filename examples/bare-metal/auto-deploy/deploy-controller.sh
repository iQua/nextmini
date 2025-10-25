#!/bin/bash
#
# Nextmini Controller Auto-Deploy Script
# One-command deployment of controller with database
#
# Features:
#   1. Build controller binary
#   2. Generate TLS certificates
#   3. Start PostgreSQL database
#   4. Start controller
#
# Usage:
#   ./deploy-controller.sh
#   ./deploy-controller.sh --skip-build
#   ./deploy-controller.sh --port 3001
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
CONTROLLER_PORT=3000
SKIP_BUILD=false
DB_PASSWORD="pgpwrd"
DEPLOY_DIR="$BARE_METAL_DIR/controller-deploy"

# ============================================================
# Function Definitions
# ============================================================

usage() {
    cat <<EOF
${GREEN}Nextmini Controller Auto-Deploy Script${NC}

Usage: $0 [OPTIONS]

Options:
    --port PORT           Controller port (default: 3000)
    --db-password PASS    PostgreSQL password (default: pgpwrd)
    --skip-build         Skip building binaries
    -h, --help           Show this help

Examples:
    # Full deployment (build + start)
    $0

    # Skip build (use existing binary)
    $0 --skip-build

    # Custom port
    $0 --port 3001
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

# Build binaries
build_binaries() {
    if [ "$SKIP_BUILD" = "true" ]; then
        log_warn "Skipping build step"
        return 0
    fi
    
    log_step "Step 1/5: Building Binaries"
    
    cd "$REPO_ROOT"
    
    # Build controller
    log_info "Building controller..."
    cargo build --release -p controller
    
    # Generate certificates if not exist
    if [ ! -f "$REPO_ROOT/server_cert.pem" ]; then
        log_info "Generating TLS certificates..."
        cargo run -p cert-gen
    fi
    
    log_success "Build complete"
}

# Start database
start_database() {
    log_step "Step 2/5: Starting PostgreSQL Database"
    
    # Check if database is already running
    if docker ps | grep -q nextmini-database; then
        log_warn "Database container already running"
        return 0
    fi
    
    # Check if start-database.sh exists
    if [ ! -f "$REPO_ROOT/start-database.sh" ]; then
        log_error "start-database.sh not found at $REPO_ROOT"
        exit 1
    fi
    
    log_info "Starting database container..."
    cd "$REPO_ROOT"
    bash start-database.sh
    
    log_info "Waiting for database to be ready..."
    
    # Poll for database readiness (max 30 seconds)
    MAX_ATTEMPTS=30
    ATTEMPT=0
    while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
        if docker exec nextmini-database pg_isready -U pgusr >/dev/null 2>&1; then
            log_success "Database process is ready (took ${ATTEMPT}s)"
            
            # Give database a bit more time to complete initialization
            log_info "Waiting for database initialization to complete..."
            sleep 2
            
            log_success "Database ready for connections"
            return 0
        fi
        ATTEMPT=$((ATTEMPT + 1))
        sleep 1
    done
    
    log_error "Database failed to become ready after ${MAX_ATTEMPTS} seconds"
    exit 1
}

# Prepare deployment
prepare_deployment() {
    log_step "Step 3/5: Preparing Deployment"
    
    # Create deployment directory
    mkdir -p "$DEPLOY_DIR"
    log_info "Deployment directory: $DEPLOY_DIR"
    
    # Check if binary exists
    if [ ! -f "$REPO_ROOT/target/release/controller" ]; then
        log_error "Controller binary not found at $REPO_ROOT/target/release/controller"
        log_info "Please build it first or remove --skip-build flag"
        exit 1
    fi
    
    # Check certificates
    if [ ! -f "$REPO_ROOT/server_cert.pem" ]; then
        log_error "TLS certificates not found"
        exit 1
    fi
    
    # Copy files
    log_info "Copying files..."
    cp "$REPO_ROOT/target/release/controller" "$DEPLOY_DIR/"
    cp "$REPO_ROOT/server_cert.pem" "$DEPLOY_DIR/"
    cp "$REPO_ROOT/server_key.pem" "$DEPLOY_DIR/"
    
    # Use controller-config.toml from auto-deploy directory if exists, otherwise from bare-metal
    if [ -f "$SCRIPT_DIR/controller-config.toml" ]; then
        cp "$SCRIPT_DIR/controller-config.toml" "$DEPLOY_DIR/config.toml"
        log_info "Using controller config from auto-deploy directory"
    else
        cp "$BARE_METAL_DIR/controller-config.toml" "$DEPLOY_DIR/config.toml"
        log_info "Using controller config from bare-metal directory"
    fi
    
    log_success "Files prepared"
}

# Start controller
start_controller() {
    log_step "Step 4/5: Starting Controller"
    
    # Stop old controller if running
    if [ -f "$DEPLOY_DIR/controller.pid" ]; then
        OLD_PID=$(cat "$DEPLOY_DIR/controller.pid")
        if ps -p $OLD_PID > /dev/null 2>&1; then
            log_info "Stopping old controller (PID: $OLD_PID)..."
            kill $OLD_PID 2>/dev/null || true
            sleep 2
        fi
        rm -f "$DEPLOY_DIR/controller.pid"
    fi
    
    # Start controller
    log_info "Starting controller..."
    export RUST_LOG=info
    cd "$DEPLOY_DIR"
    nohup ./controller > controller.log 2>&1 &
    CONTROLLER_PID=$!
    echo $CONTROLLER_PID > controller.pid
    cd "$SCRIPT_DIR"
    
    # Wait and verify
    sleep 2
    if ps -p $CONTROLLER_PID > /dev/null 2>&1; then
        log_success "Controller started (PID: $CONTROLLER_PID)"
    else
        log_error "Controller failed to start"
        log_info "Last 20 lines of log:"
        tail -20 "$DEPLOY_DIR/controller.log"
        exit 1
    fi
}

# Show deployment info
show_deployment_info() {
    log_step "Step 5/5: Deployment Information"
    
    # Get IP addresses
    PUBLIC_IP=$(curl -s ifconfig.me 2>/dev/null || echo "unknown")
    LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "unknown")
    
    CONTROLLER_PID=$(cat "$DEPLOY_DIR/controller.pid" 2>/dev/null || echo "unknown")
    
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}Controller Deployment Complete!${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo "Controller Status:"
    echo "  PID:           $CONTROLLER_PID"
    echo "  Port:          $CONTROLLER_PORT"
    echo "  Config:        $DEPLOY_DIR/config.toml"
    echo "  Log:           $DEPLOY_DIR/controller.log"
    echo ""
    echo "Network Addresses:"
    echo "  Local IP:      ws://$LOCAL_IP:$CONTROLLER_PORT"
    echo "  Public IP:     ws://$PUBLIC_IP:$CONTROLLER_PORT"
    echo ""
    echo "For Dataplane Nodes:"
    echo "  • Same network:    Use ws://$LOCAL_IP:$CONTROLLER_PORT"
    echo "  • Remote/Internet: Use ws://$PUBLIC_IP:$CONTROLLER_PORT"
    echo ""
    echo "Management Commands:"
    echo "  View logs:   tail -f $DEPLOY_DIR/controller.log"
    echo "  Stop:        kill $CONTROLLER_PID"
    echo "  Restart:     $0"
    echo ""
    echo "Next Step:"
    echo "  Edit nodes.conf with your node information, then run:"
    echo "  cd $SCRIPT_DIR"
    echo "  ./deploy-all.sh"
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
            --port)
                CONTROLLER_PORT="$2"
                shift 2
                ;;
            --db-password)
                DB_PASSWORD="$2"
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
║       Nextmini Controller Auto-Deploy Script         ║
║                                                       ║
╚═══════════════════════════════════════════════════════╝
BANNER
    echo -e "${NC}"
    
    # Execute deployment flow
    build_binaries
    start_database
    prepare_deployment
    start_controller
    show_deployment_info
}

# Run main flow
main "$@"

