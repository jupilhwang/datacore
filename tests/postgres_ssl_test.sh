#!/bin/bash
# PostgreSQL SSL 연결 테스트 스크립트

set -e

echo "==================================="
echo "PostgreSQL SSL Connection Test"
echo "==================================="
echo ""

# 환경 변수 확인
if [ -z "$DATACORE_PG_HOST" ]; then
	echo "⚠️  DATACORE_PG_HOST not set"
	echo ""
	echo "To run PostgreSQL tests, set the following environment variables:"
	echo "  export DATACORE_PG_HOST=localhost"
	echo "  export DATACORE_PG_PORT=5432"
	echo "  export DATACORE_PG_USER=your_user"
	echo "  export DATACORE_PG_PASSWORD=your_password"
	echo "  export DATACORE_PG_DATABASE=datacore_test"
	echo "  export DATACORE_PG_SSLMODE=disable  # or require, prefer, etc."
	echo ""
	echo "Skipping PostgreSQL SSL tests..."
	exit 0
fi

echo "📋 Test Configuration:"
echo "  Host: $DATACORE_PG_HOST"
echo "  Port: ${DATACORE_PG_PORT:-5432}"
echo "  User: $DATACORE_PG_USER"
echo "  Database: ${DATACORE_PG_DATABASE:-datacore_test}"
echo "  SSL Mode: ${DATACORE_PG_SSLMODE:-disable}"
echo ""

# 테스트 실행
echo "🧪 Running PostgreSQL SSL tests..."
echo ""

cd "$(dirname "$0")/.."

# SSL 관련 테스트만 실행
v test src/infra/storage/plugins/postgres/adapter_test.v -stats

echo ""
echo "✅ PostgreSQL SSL tests completed!"
