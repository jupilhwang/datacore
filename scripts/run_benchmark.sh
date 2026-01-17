#!/bin/bash
# DataCore Performance Benchmark Runner
# Runs comprehensive performance benchmarks

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║           DataCore Performance Benchmark Runner              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

cd "$PROJECT_ROOT"

# Build with optimizations
echo "▶ Building with optimizations..."
v -prod -o bin/benchmark src/infra/performance/benchmark_runner.v 2>/dev/null || {
    echo "  Creating benchmark runner..."
    cat > src/infra/performance/benchmark_runner.v << 'EOF'
import infra.performance

fn main() {
    performance.run_quick_benchmark()
}
EOF
    v -prod -o bin/benchmark src/infra/performance/benchmark_runner.v
}

echo ""
echo "▶ Running benchmarks..."
echo ""

./bin/benchmark

# Cleanup
rm -f src/infra/performance/benchmark_runner.v

echo ""
echo "▶ Benchmark complete!"
