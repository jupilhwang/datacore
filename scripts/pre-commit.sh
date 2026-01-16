#!/bin/bash
# Pre-commit hook script

set -e

echo "🔍 Running pre-commit checks..."

# Format check
if ! v fmt -verify src/ 2>/dev/null; then
    echo "❌ Format check failed. Run: v fmt -w src/"
    exit 1
fi

# Vet
v vet src/ 2>/dev/null || true

# Unit tests (if exist)
if [ -d "tests/unit" ] && [ "$(ls -A tests/unit 2>/dev/null)" ]; then
    v test tests/unit/
fi

echo "✅ Pre-commit checks passed!"
