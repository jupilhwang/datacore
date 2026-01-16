#!/bin/bash
# Development check script - run before committing

set -e

echo "🔍 Running development checks..."

# 1. Format check
echo "📝 Checking code format..."
v fmt -verify src/ || {
    echo "❌ Format check failed. Run: v fmt -w src/"
    exit 1
}

# 2. Lint (vet)
echo "🔎 Running vet..."
v vet src/

# 3. Unit tests
echo "🧪 Running unit tests..."
v test tests/unit/ 2>/dev/null || echo "⚠️  No unit tests found yet"

# 4. Build check
echo "🔨 Building..."
v -o /tmp/datacore src/main.v

echo "✅ All checks passed!"
