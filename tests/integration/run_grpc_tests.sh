#!/bin/bash
# DataCore gRPC Streaming Integration Test Runner
# Runs gRPC streaming tests with proper setup and cleanup

set -e

# Configuration
GRPC_HOST="${GRPC_HOST:-localhost}"
GRPC_PORT="${GRPC_PORT:-9093}"
KAFKA_HOST="${KAFKA_HOST:-localhost}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Check if DataCore broker is running
check_broker() {
    log_info "Checking if DataCore broker is running..."
    
    # Check Kafka port
    if ! nc -z $KAFKA_HOST $KAFKA_PORT 2>/dev/null; then
        log_error "DataCore Kafka port ($KAFKA_HOST:$KAFKA_PORT) is not accessible"
        log_info "Please start the broker with: ./bin/datacore broker start --config=config.toml"
        return 1
    fi
    
    # Check gRPC port
    if ! nc -z $GRPC_HOST $GRPC_PORT 2>/dev/null; then
        log_error "DataCore gRPC port ($GRPC_HOST:$GRPC_PORT) is not accessible"
        log_info "Please ensure gRPC is enabled in config.toml"
        return 1
    fi
    
    log_success "DataCore broker is running"
    return 0
}

# Check Python dependencies
check_python() {
    log_info "Checking Python environment..."
    
    if ! command -v python3 &> /dev/null; then
        log_error "python3 not found"
        return 1
    fi
    
    PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
    log_info "Python version: $PYTHON_VERSION"
    
    return 0
}

# Check optional Kafka CLI tools
check_kafka_cli() {
    log_info "Checking Kafka CLI tools (optional)..."
    
    if command -v kafka-topics.sh &> /dev/null || command -v kafka-topics &> /dev/null; then
        log_success "Kafka CLI tools found"
        return 0
    else
        log_warn "Kafka CLI tools not found - some tests will be skipped"
        log_info "Install with: brew install kafka (macOS) or download from kafka.apache.org"
        return 1
    fi
}

# Run tests
run_tests() {
    log_info "Running gRPC streaming tests..."
    echo ""
    
    cd "$SCRIPT_DIR"
    
    python3 test_grpc_streaming.py \
        --grpc-host "$GRPC_HOST" \
        --grpc-port "$GRPC_PORT" \
        --kafka-host "$KAFKA_HOST" \
        --kafka-port "$KAFKA_PORT"
    
    return $?
}

# Main
main() {
    echo "=========================================="
    echo "  DataCore gRPC Streaming Test Runner"
    echo "=========================================="
    echo ""
    echo "Configuration:"
    echo "  gRPC Server:  $GRPC_HOST:$GRPC_PORT"
    echo "  Kafka Server: $KAFKA_HOST:$KAFKA_PORT"
    echo ""
    
    # Pre-flight checks
    if ! check_broker; then
        exit 1
    fi
    
    if ! check_python; then
        exit 1
    fi
    
    check_kafka_cli || true  # Optional
    
    echo ""
    
    # Run tests
    if run_tests; then
        echo ""
        log_success "All tests completed successfully!"
        exit 0
    else
        echo ""
        log_error "Some tests failed"
        exit 1
    fi
}

# Show usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run DataCore gRPC streaming integration tests.

Options:
    -h, --help              Show this help message
    --grpc-host HOST        gRPC server host (default: localhost)
    --grpc-port PORT        gRPC server port (default: 9093)
    --kafka-host HOST       Kafka server host (default: localhost)
    --kafka-port PORT       Kafka server port (default: 9092)

Environment Variables:
    GRPC_HOST               Override gRPC host
    GRPC_PORT               Override gRPC port
    KAFKA_HOST              Override Kafka host
    KAFKA_PORT              Override Kafka port

Examples:
    # Run with default settings
    $0

    # Run with custom gRPC port
    $0 --grpc-port 9094

    # Run with environment variables
    GRPC_HOST=192.168.1.100 GRPC_PORT=9093 $0

Prerequisites:
    1. DataCore broker must be running
    2. Python 3.6+ installed
    3. Kafka CLI tools (optional, for cross-compatibility tests)

EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        --grpc-host)
            GRPC_HOST="$2"
            shift 2
            ;;
        --grpc-port)
            GRPC_PORT="$2"
            shift 2
            ;;
        --kafka-host)
            KAFKA_HOST="$2"
            shift 2
            ;;
        --kafka-port)
            KAFKA_PORT="$2"
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Run main
main
