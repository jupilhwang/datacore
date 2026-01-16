#!/bin/bash
# Schema Registry REST API Integration Tests
# Tests the Schema Registry endpoints using curl

set -e

# Configuration
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
TEST_SUBJECT="test-subject-$(date +%s)"
TIMEOUT=10

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

log_info() { echo -e "${YELLOW}[INFO]${NC} $1"; }
log_pass() { echo -e "${GREEN}[PASS]${NC} $1"; ((TESTS_PASSED++)); }
log_fail() { echo -e "${RED}[FAIL]${NC} $1"; ((TESTS_FAILED++)); }

run_test() {
    ((TESTS_RUN++))
    echo ""
    log_info "Running: $1"
    if $2; then
        log_pass "$1"
    else
        log_fail "$1"
    fi
}

# ============================================
# Schema Registry Tests
# ============================================

test_list_subjects_empty() {
    local response=$(curl -s -X GET "$SCHEMA_REGISTRY_URL/subjects")
    [[ "$response" == "[]" ]] || [[ "$response" == *"[]"* ]]
}

test_register_schema() {
    local schema='{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}'
    
    local response=$(curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": \"$(echo $schema | sed 's/"/\\"/g')\"}" \
        "$SCHEMA_REGISTRY_URL/subjects/${TEST_SUBJECT}/versions")
    
    echo "$response" | grep -q '"id"'
}

test_get_schema_by_id() {
    # First register a schema
    local schema='{"type":"record","name":"Order","fields":[{"name":"orderId","type":"int"}]}'
    local subject="${TEST_SUBJECT}-order"
    
    local register_response=$(curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": \"$(echo $schema | sed 's/"/\\"/g')\"}" \
        "$SCHEMA_REGISTRY_URL/subjects/${subject}/versions")
    
    local schema_id=$(echo "$register_response" | grep -o '"id":[0-9]*' | cut -d':' -f2)
    
    if [[ -z "$schema_id" ]]; then
        return 1
    fi
    
    # Get schema by ID
    local response=$(curl -s -X GET "$SCHEMA_REGISTRY_URL/schemas/ids/${schema_id}")
    echo "$response" | grep -q '"schema"'
}

test_list_subjects() {
    local response=$(curl -s -X GET "$SCHEMA_REGISTRY_URL/subjects")
    echo "$response" | grep -q "$TEST_SUBJECT"
}

test_list_versions() {
    local response=$(curl -s -X GET "$SCHEMA_REGISTRY_URL/subjects/${TEST_SUBJECT}/versions")
    echo "$response" | grep -q "1"
}

test_get_schema_by_version() {
    local response=$(curl -s -X GET "$SCHEMA_REGISTRY_URL/subjects/${TEST_SUBJECT}/versions/1")
    echo "$response" | grep -q '"subject"' && echo "$response" | grep -q '"version"'
}

test_get_latest_schema() {
    local response=$(curl -s -X GET "$SCHEMA_REGISTRY_URL/subjects/${TEST_SUBJECT}/versions/latest")
    echo "$response" | grep -q '"version"'
}

test_get_raw_schema() {
    local response=$(curl -s -X GET "$SCHEMA_REGISTRY_URL/subjects/${TEST_SUBJECT}/versions/1/schema")
    echo "$response" | grep -q '"type"'
}

test_get_compatibility() {
    local response=$(curl -s -X GET "$SCHEMA_REGISTRY_URL/config/${TEST_SUBJECT}")
    echo "$response" | grep -q '"compatibilityLevel"'
}

test_set_compatibility() {
    local response=$(curl -s -X PUT \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d '{"compatibility": "FULL"}' \
        "$SCHEMA_REGISTRY_URL/config/${TEST_SUBJECT}")
    
    echo "$response" | grep -q 'FULL'
}

test_check_compatibility() {
    local schema='{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"email","type":"string"}]}'
    
    local response=$(curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": \"$(echo $schema | sed 's/"/\\"/g')\"}" \
        "$SCHEMA_REGISTRY_URL/compatibility/subjects/${TEST_SUBJECT}/versions/latest")
    
    echo "$response" | grep -q '"is_compatible"'
}

test_register_multiple_versions() {
    local subject="${TEST_SUBJECT}-multi"
    
    # Version 1
    local schema_v1='{"type":"record","name":"Event","fields":[{"name":"id","type":"int"}]}'
    curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": \"$(echo $schema_v1 | sed 's/"/\\"/g')\"}" \
        "$SCHEMA_REGISTRY_URL/subjects/${subject}/versions" > /dev/null
    
    # Set compatibility to NONE for testing
    curl -s -X PUT \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d '{"compatibility": "NONE"}' \
        "$SCHEMA_REGISTRY_URL/config/${subject}" > /dev/null
    
    # Version 2
    local schema_v2='{"type":"record","name":"Event","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}'
    curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": \"$(echo $schema_v2 | sed 's/"/\\"/g')\"}" \
        "$SCHEMA_REGISTRY_URL/subjects/${subject}/versions" > /dev/null
    
    # Check versions
    local response=$(curl -s -X GET "$SCHEMA_REGISTRY_URL/subjects/${subject}/versions")
    echo "$response" | grep -q "1" && echo "$response" | grep -q "2"
}

test_delete_version() {
    local subject="${TEST_SUBJECT}-delete"
    
    # Register schema
    local schema='{"type":"string"}'
    curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": \"$(echo $schema | sed 's/"/\\"/g')\"}" \
        "$SCHEMA_REGISTRY_URL/subjects/${subject}/versions" > /dev/null
    
    # Delete version
    local response=$(curl -s -X DELETE "$SCHEMA_REGISTRY_URL/subjects/${subject}/versions/1")
    
    # Should return the schema ID
    echo "$response" | grep -qE '^[0-9]+$' || [[ "$response" == *"id"* ]]
}

test_delete_subject() {
    local subject="${TEST_SUBJECT}-delete-subject"
    
    # Register schema
    local schema='{"type":"int"}'
    curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": \"$(echo $schema | sed 's/"/\\"/g')\"}" \
        "$SCHEMA_REGISTRY_URL/subjects/${subject}/versions" > /dev/null
    
    # Delete subject
    local response=$(curl -s -X DELETE "$SCHEMA_REGISTRY_URL/subjects/${subject}")
    
    # Should return list of deleted versions
    [[ "$response" == *"1"* ]]
}

test_json_schema() {
    local subject="${TEST_SUBJECT}-json"
    local schema='{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"required":["id"]}'
    
    local response=$(curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schemaType\": \"JSON\", \"schema\": \"$(echo $schema | sed 's/"/\\"/g')\"}" \
        "$SCHEMA_REGISTRY_URL/subjects/${subject}/versions")
    
    echo "$response" | grep -q '"id"'
}

test_protobuf_schema() {
    local subject="${TEST_SUBJECT}-protobuf"
    local schema='syntax = \"proto3\"; message User { int32 id = 1; string name = 2; }'
    
    local response=$(curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schemaType\": \"PROTOBUF\", \"schema\": \"$(echo $schema | sed 's/"/\\"/g')\"}" \
        "$SCHEMA_REGISTRY_URL/subjects/${subject}/versions")
    
    echo "$response" | grep -q '"id"'
}

test_schema_not_found() {
    local response=$(curl -s -w "\n%{http_code}" -X GET "$SCHEMA_REGISTRY_URL/schemas/ids/999999")
    local http_code=$(echo "$response" | tail -n1)
    [[ "$http_code" == "404" ]]
}

test_subject_not_found() {
    local response=$(curl -s -w "\n%{http_code}" -X GET "$SCHEMA_REGISTRY_URL/subjects/nonexistent-subject/versions")
    local http_code=$(echo "$response" | tail -n1)
    [[ "$http_code" == "404" ]]
}

# ============================================
# Main
# ============================================

main() {
    echo "=========================================="
    echo "  Schema Registry REST API Tests"
    echo "=========================================="
    echo ""
    echo "Schema Registry URL: $SCHEMA_REGISTRY_URL"
    echo ""
    
    # Check connectivity
    if ! curl -s --connect-timeout 5 "$SCHEMA_REGISTRY_URL/subjects" > /dev/null 2>&1; then
        echo -e "${RED}Error: Cannot connect to Schema Registry at $SCHEMA_REGISTRY_URL${NC}"
        echo "Please ensure DataCore broker with Schema Registry is running."
        exit 1
    fi
    
    # Run tests
    run_test "Register Schema" test_register_schema
    run_test "Get Schema by ID" test_get_schema_by_id
    run_test "List Subjects" test_list_subjects
    run_test "List Versions" test_list_versions
    run_test "Get Schema by Version" test_get_schema_by_version
    run_test "Get Latest Schema" test_get_latest_schema
    run_test "Get Raw Schema" test_get_raw_schema
    run_test "Get Compatibility" test_get_compatibility
    run_test "Set Compatibility" test_set_compatibility
    run_test "Check Compatibility" test_check_compatibility
    run_test "Register Multiple Versions" test_register_multiple_versions
    run_test "Delete Version" test_delete_version
    run_test "Delete Subject" test_delete_subject
    run_test "JSON Schema" test_json_schema
    run_test "Protobuf Schema" test_protobuf_schema
    run_test "Schema Not Found (404)" test_schema_not_found
    run_test "Subject Not Found (404)" test_subject_not_found
    
    # Summary
    echo ""
    echo "=========================================="
    echo "  Test Summary"
    echo "=========================================="
    echo "Total:  $TESTS_RUN"
    echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Failed: ${RED}$TESTS_FAILED${NC}"
    echo ""
    
    if [[ $TESTS_FAILED -gt 0 ]]; then
        exit 1
    fi
}

main "$@"
