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

mkdir -p bin
v -prod -o bin/benchmark cmd/benchmark/ 2>&1 || {
    echo "Build with -prod failed. Trying without..."
    v -o bin/benchmark cmd/benchmark/
}

echo ""
echo "▶ Running benchmarks..."
echo ""

./bin/benchmark

echo ""
echo "▶ Benchmark complete!"
