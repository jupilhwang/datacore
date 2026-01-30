#!/bin/bash
# =============================================================================
# Message Format Compatibility Tests
# Tests JSON, AVRO, Protobuf, and JSON Schema formats
# =============================================================================

set -e

# Configuration
BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9092}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
KAFKA_HOME="${KAFKA_HOME:-/opt/kafka}"
TIMEOUT=30
TEST_PREFIX="test-msg-format-$(date +%s)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Track test subjects for cleanup
declare -a TEST_SUBJECTS=()

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_pass() {
	echo -e "${GREEN}[PASS]${NC} $1"
	((TESTS_PASSED++))
	((TESTS_RUN++))
}
log_fail() {
	echo -e "${RED}[FAIL]${NC} $1"
	((TESTS_FAILED++))
	((TESTS_RUN++))
}
log_section() { echo -e "\n${YELLOW}=== $1 ===${NC}\n"; }

# Producer config file path
PRODUCER_CONFIG="/tmp/${TEST_PREFIX}_producer.properties"
CONSUMER_CONFIG="/tmp/${TEST_PREFIX}_consumer.properties"

# =============================================================================
# Setup and Cleanup
# =============================================================================

setup_configs() {
	log_info "Setting up producer/consumer configs..."

	# Create producer config
	cat >"$PRODUCER_CONFIG" <<EOF
bootstrap.servers=$BOOTSTRAP_SERVERS
acks=all
retries=3
linger.ms=10
EOF

	# Create consumer config
	cat >"$CONSUMER_CONFIG" <<EOF
bootstrap.servers=$BOOTSTRAP_SERVERS
auto.offset.reset=earliest
enable.auto.commit=true
EOF
}

cleanup() {
	log_info "Cleaning up test resources..."
	rm -f "$PRODUCER_CONFIG" "$CONSUMER_CONFIG"

	# Delete test subjects from schema registry
	for subject in "${TEST_SUBJECTS[@]}"; do
		curl -s -X DELETE "$SCHEMA_REGISTRY_URL/subjects/${subject}" >/dev/null 2>&1 || true
	done
}

trap cleanup EXIT

# =============================================================================
# Helper Functions
# =============================================================================

register_avro_schema() {
	local subject="$1"
	local schema="$2"
	local response=$(curl -s -X POST \
		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
		-d "{\"schema\": \"$(echo "$schema" | sed 's/"/\\"/g')\"}" \
		"$SCHEMA_REGISTRY_URL/subjects/${subject}/versions")
	echo "$response" | grep -q '"id"'
}

register_protobuf_schema() {
	local subject="$1"
	local schema="$2"
	local response=$(curl -s -X POST \
		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
		-d "{\"schemaType\": \"PROTOBUF\", \"schema\": \"$(echo "$schema" | sed 's/"/\\"/g')\"}" \
		"$SCHEMA_REGISTRY_URL/subjects/${subject}/versions")
	echo "$response" | grep -q '"id"'
}

register_json_schema() {
	local subject="$1"
	local schema="$2"
	local response=$(curl -s -X POST \
		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
		-d "{\"schemaType\": \"JSON\", \"schema\": \"$(echo "$schema" | sed 's/"/\\"/g')\"}" \
		"$SCHEMA_REGISTRY_URL/subjects/${subject}/versions")
	echo "$response" | grep -q '"id"'
}

create_topic() {
	local topic="$1"
	"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" \
		--create --topic "$topic" --partitions 1 --replication-factor 1 \
		--config retention.ms=86400000 2>/dev/null || true
}

delete_topic() {
	local topic="$1"
	"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVERS" \
		--delete --topic "$topic" 2>/dev/null || true
}

# =============================================================================
# JSON Format Tests
# =============================================================================

test_json_basic() {
	log_section "JSON Basic Format Test"

	local topic="${TEST_PREFIX}-json-basic"
	create_topic "$topic"

	# Produce basic JSON
	echo '{"id": 1, "name": "test-user", "active": true}' | timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-producer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--producer.config "$PRODUCER_CONFIG" 2>/dev/null

	# Consume and verify
	local result=$(timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--consumer.config "$CONSUMER_CONFIG" 2>/dev/null)

	echo "$result" | grep -q '"id":1' &&
		echo "$result" | grep -q '"name":"test-user"' &&
		echo "$result" | grep -q '"active":true'
}

test_json_nested() {
	log_section "JSON Nested Objects Test"

	local topic="${TEST_PREFIX}-json-nested"
	create_topic "$topic"

	local json_payload='{
        "user": {
            "id": 100,
            "profile": {
                "name": "nested-user",
                "email": "nested@example.com"
            }
        },
        "metadata": {
            "created_at": "2024-01-01T00:00:00Z",
            "version": 1
        }
    }'

	echo "$json_payload" | timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-producer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--producer.config "$PRODUCER_CONFIG" 2>/dev/null

	local result=$(timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--consumer.config "$CONSUMER_CONFIG" 2>/dev/null)

	echo "$result" | grep -q '"user"' &&
		echo "$result" | grep -q '"profile"' &&
		echo "$result" | grep -q '"metadata"'
}

test_json_arrays() {
	log_section "JSON Arrays Test"

	local topic="${TEST_PREFIX}-json-arrays"
	create_topic "$topic"

	local json_payload='{
        "items": [1, 2, 3, 4, 5],
        "names": ["alice", "bob", "charlie"],
        "mixed": [true, "string", 42, null],
        "objects": [
            {"id": 1, "value": "first"},
            {"id": 2, "value": "second"}
        ]
    }'

	echo "$json_payload" | timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-producer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--producer.config "$PRODUCER_CONFIG" 2>/dev/null

	local result=$(timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--consumer.config "$CONSUMER_CONFIG" 2>/dev/null)

	echo "$result" | grep -q '"items"' &&
		echo "$result" | grep -q '"names"' &&
		echo "$result" | grep -q '"mixed"' &&
		echo "$result" | grep -q '"objects"'
}

test_json_special_chars() {
	log_section "JSON Special Characters Test"

	local topic="${TEST_PREFIX}-json-special"
	create_topic "$topic"

	local json_payload='{
        "text": "Hello \"World\"!",
        "unicode": "한글 테스트 🎉",
        "newline": "line1\nline2",
        "tab": "col1\tcol2",
        "slash": "path\\to\\file",
        "html": "<div class=\"container\">&nbsp;</div>"
    }'

	echo "$json_payload" | timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-producer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--producer.config "$PRODUCER_CONFIG" 2>/dev/null

	local result=$(timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--consumer.config "$CONSUMER_CONFIG" 2>/dev/null)

	echo "$result" | grep -q '"unicode"' &&
		echo "$result" | grep -q '🎉'
}

test_json_large_payload() {
	log_section "JSON Large Payload Test"

	local topic="${TEST_PREFIX}-json-large"
	create_topic "$topic"

	# Generate large JSON (~100KB)
	local json_payload='{"data": ['
	for i in $(seq 1 100); do
		if [ $i -gt 1 ]; then
			json_payload+=','
		fi
		json_payload+="{\"id\":$i,\"name\":\"item-$i\",\"value\":$((i * 100)),\"tags\":[\"tag$i\",\"test\"]}"
	done
	json_payload+=']}'

	echo "$json_payload" | timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-producer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--producer.config "$PRODUCER_CONFIG" 2>/dev/null

	local result=$(timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--consumer.config "$CONSUMER_CONFIG" 2>/dev/null)

	echo "$result" | grep -q '"data"' &&
		echo "$result" | grep -q '"id":1' &&
		echo "$result" | grep -q '"id":100'
}

# =============================================================================
# AVRO Format Tests
# =============================================================================

test_avro_basic() {
	log_section "AVRO Basic Format Test"

	local topic="${TEST_PREFIX}-avro-basic"
	local subject="${TEST_PREFIX}-avro-basic-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	local avro_schema='{
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "email", "type": ["null", "string"], "default": null}
        ]
    }'

	if ! register_avro_schema "$subject" "$avro_schema"; then
		log_fail "AVRO Basic: Failed to register schema"
		return 1
	fi

	# Produce with AVRO serializer (using kafka-avro-console-producer)
	local avro_payload='{"id": 1, "name": "avro-user", "email": {"string": "avro@example.com"}}'

	if [ -x "$KAFKA_HOME/bin/kafka-avro-console-producer.sh" ]; then
		echo "$avro_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-avro-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property value.schema="$avro_schema" 2>/dev/null

		local result=$(timeout 15 \
			"$KAFKA_HOME/bin/kafka-avro-console-consumer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 1 \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" 2>/dev/null)

		echo "$result" | grep -q 'avro-user'
	else
		log_info "AVRO CLI tools not available, skipping AVRO format tests"
		return 0
	fi
}

test_avro_complex_schema() {
	log_section "AVRO Complex Schema Test"

	local topic="${TEST_PREFIX}-avro-complex"
	local subject="${TEST_PREFIX}-avro-complex-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	local avro_schema='{
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "order_id", "type": "long"},
            {"name": "customer", "type": {
                "type": "record",
                "name": "Customer",
                "fields": [
                    {"name": "customer_id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {"name": "addresses", "type": {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "Address",
                            "fields": [
                                {"name": "street", "type": "string"},
                                {"name": "city", "type": "string"},
                                {"name": "zipcode", "type": ["null", "string"], "default": null}
                            ]
                        }
                    }}
                ]
            }},
            {"name": "items", "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "OrderItem",
                    "fields": [
                        {"name": "product_id", "type": "int"},
                        {"name": "quantity", "type": "int"},
                        {"name": "price", "type": "double"}
                    ]
                }
            }},
            {"name": "status", "type": {
                "type": "enum",
                "name": "OrderStatus",
                "symbols": ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
            }}
        ]
    }'

	if ! register_avro_schema "$subject" "$avro_schema"; then
		log_fail "AVRO Complex: Failed to register schema"
		return 1
	fi

	if [ -x "$KAFKA_HOME/bin/kafka-avro-console-producer.sh" ]; then
		local avro_payload='{
            "order_id": 1001,
            "customer": {
                "customer_id": 1,
                "name": "Complex Customer",
                "addresses": [
                    {"street": "123 Main St", "city": "Seoul", "zipcode": null},
                    {"street": "456 Other Ave", "city": "Busan", "zipcode": "12345"}
                ]
            },
            "items": [
                {"product_id": 101, "quantity": 2, "price": 19.99},
                {"product_id": 102, "quantity": 1, "price": 49.99}
            ],
            "status": "PROCESSING"
        }'

		echo "$avro_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-avro-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property value.schema="$avro_schema" 2>/dev/null

		local result=$(timeout 15 \
			"$KAFKA_HOME/bin/kafka-avro-console-consumer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 1 \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" 2>/dev/null)

		echo "$result" | grep -q 'Complex Customer' &&
			echo "$result" | grep -q 'PROCESSING'
	else
		log_info "AVRO CLI tools not available, skipping AVRO complex tests"
		return 0
	fi
}

test_avro_union_types() {
	log_section "AVRO Union Types Test"

	local topic="${TEST_PREFIX}-avro-union"
	local subject="${TEST_PREFIX}-avro-union-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	local avro_schema='{
        "type": "record",
        "name": "FlexibleRecord",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "data", "type": ["null", "string", "int", "long", "double", "boolean"]},
            {"name": "tags", "type": {"type": "array", "items": ["null", "string"]}},
            {"name": "value", "type": ["null", {
                "type": "record",
                "name": "Nested",
                "fields": [{"name": "inner", "type": "string"}]
            }]}
        ]
    }'

	if ! register_avro_schema "$subject" "$avro_schema"; then
		log_fail "AVRO Union: Failed to register schema"
		return 1
	fi

	if [ -x "$KAFKA_HOME/bin/kafka-avro-console-producer.sh" ]; then
		local avro_payload='{
            "id": 1,
            "data": {"string": "test-string"},
            "tags": ["tag1", null, "tag2"],
            "value": {"Nested": {"inner": "nested-value"}}
        }'

		echo "$avro_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-avro-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property value.schema="$avro_schema" 2>/dev/null

		local result=$(timeout 15 \
			"$KAFKA_HOME/bin/kafka-avro-console-consumer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 1 \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" 2>/dev/null)

		echo "$result" | grep -q 'test-string' &&
			echo "$result" | grep -q 'nested-value'
	else
		log_info "AVRO CLI tools not available, skipping AVRO union tests"
		return 0
	fi
}

test_avro_schema_evolution() {
	log_section "AVRO Schema Evolution Test"

	local topic="${TEST_PREFIX}-avro-evolution"
	local subject="${TEST_PREFIX}-avro-evolution-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	# Register initial schema (backward compatible - added optional field)
	local schema_v1='{
        "type": "record",
        "name": "EventV1",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
        ]
    }'

	if ! register_avro_schema "$subject" "$schema_v1"; then
		log_fail "AVRO Evolution: Failed to register initial schema"
		return 1
	fi

	if [ -x "$KAFKA_HOME/bin/kafka-avro-console-producer.sh" ]; then
		# Produce with V1 schema
		local v1_payload='{"id": 1, "name": "event-v1"}'
		echo "$v1_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-avro-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property value.schema="$schema_v1" 2>/dev/null

		# Register evolved schema (V2 - backward compatible with V1)
		local schema_v2='{
            "type": "record",
            "name": "EventV2",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "description", "type": ["null", "string"], "default": null}
            ]
        }'

		# Set compatibility to BACKWARD
		curl -s -X PUT \
			-H "Content-Type: application/vnd.schemaregistry.v1+json" \
			-d '{"compatibility": "BACKWARD"}' \
			"$SCHEMA_REGISTRY_URL/config/$subject" >/dev/null

		# Register V2 schema
		register_avro_schema "$subject" "$schema_v2"

		# Produce with V2 schema
		local v2_payload='{"id": 2, "name": "event-v2", "description": {"string": "new event"}}'
		echo "$v2_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-avro-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property value.schema="$schema_v2" 2>/dev/null

		# Consume and verify both messages are readable
		local result=$(timeout 15 \
			"$KAFKA_HOME/bin/kafka-avro-console-consumer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 2 \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" 2>/dev/null)

		echo "$result" | grep -q 'event-v1' &&
			echo "$result" | grep -q 'event-v2'
	else
		log_info "AVRO CLI tools not available, skipping AVRO evolution tests"
		return 0
	fi
}

# =============================================================================
# Protobuf Format Tests
# =============================================================================

test_protobuf_basic() {
	log_section "Protobuf Basic Format Test"

	local topic="${TEST_PREFIX}-protobuf-basic"
	local subject="${TEST_PREFIX}-protobuf-basic-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	local proto_schema='syntax = "proto3";
message User {
    int32 id = 1;
    string name = 2;
    string email = 3;
    bool active = 4;
}'

	if ! register_protobuf_schema "$subject" "$proto_schema"; then
		log_fail "Protobuf Basic: Failed to register schema"
		return 1
	fi

	if [ -x "$KAFKA_HOME/bin/kafka-protobuf-console-producer.sh" ]; then
		# Protobuf messages need to be in binary format, but for CLI testing
		# we use JSON representation that the CLI tool converts
		local proto_payload='{"id": 1, "name": "proto-user", "email": "proto@example.com", "active": true}'

		echo "$proto_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-protobuf-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property protobuf.schema="$proto_schema" 2>/dev/null

		local result=$(timeout 15 \
			"$KAFKA_HOME/bin/kafka-protobuf-console-consumer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 1 \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" 2>/dev/null)

		echo "$result" | grep -q 'proto-user'
	else
		log_info "Protobuf CLI tools not available, skipping Protobuf tests"
		return 0
	fi
}

test_protobuf_complex() {
	log_section "Protobuf Complex Message Test"

	local topic="${TEST_PREFIX}-protobuf-complex"
	local subject="${TEST_PREFIX}-protobuf-complex-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	local proto_schema='syntax = "proto3";
message Address {
    string street = 1;
    string city = 2;
    string zipcode = 3;
}
message OrderItem {
    int32 product_id = 1;
    int32 quantity = 2;
    double price = 3;
}
message Order {
    int64 order_id = 1;
    User customer = 2;
    repeated OrderItem items = 3;
    string status = 4;
}
message User {
    int32 id = 1;
    string name = 2;
    string email = 3;
    repeated Address addresses = 4;
}'

	if ! register_protobuf_schema "$subject" "$proto_schema"; then
		log_fail "Protobuf Complex: Failed to register schema"
		return 1
	fi

	if [ -x "$KAFKA_HOME/bin/kafka-protobuf-console-producer.sh" ]; then
		local proto_payload='{
            "order_id": 1001,
            "customer": {"id": 1, "name": "proto-customer", "email": "proto@example.com", "addresses": []},
            "items": [
                {"product_id": 101, "quantity": 2, "price": 19.99},
                {"product_id": 102, "quantity": 1, "price": 49.99}
            ],
            "status": "PROCESSING"
        }'

		echo "$proto_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-protobuf-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property protobuf.schema="$proto_schema" 2>/dev/null

		local result=$(timeout 15 \
			"$KAFKA_HOME/bin/kafka-protobuf-console-consumer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 1 \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" 2>/dev/null)

		echo "$result" | grep -q 'proto-customer' &&
			echo "$result" | grep -q 'PROCESSING'
	else
		log_info "Protobuf CLI tools not available, skipping Protobuf complex tests"
		return 0
	fi
}

test_protobuf_oneof_fields() {
	log_section "Protobuf Oneof Fields Test"

	local topic="${TEST_PREFIX}-protobuf-oneof"
	local subject="${TEST_PREFIX}-protobuf-oneof-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	local proto_schema='syntax = "proto3";
message Message {
    int32 id = 1;
    oneof content {
        string text_content = 2;
        int32 number_content = 3;
        double decimal_content = 4;
    }
    string sender = 5;
}'

	if ! register_protobuf_schema "$subject" "$proto_schema"; then
		log_fail "Protobuf Oneof: Failed to register schema"
		return 1
	fi

	if [ -x "$KAFKA_HOME/bin/kafka-protobuf-console-producer.sh" ]; then
		# Test with different oneof types
		local proto_payload='{"id": 1, "text_content": "Hello Protobuf", "sender": "sender1"}'

		echo "$proto_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-protobuf-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property protobuf.schema="$proto_schema" 2>/dev/null

		local result=$(timeout 15 \
			"$KAFKA_HOME/bin/kafka-protobuf-console-consumer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 1 \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" 2>/dev/null)

		echo "$result" | grep -q 'Hello Protobuf'
	else
		log_info "Protobuf CLI tools not available, skipping Protobuf oneof tests"
		return 0
	fi
}

# =============================================================================
# JSON Schema Format Tests
# =============================================================================

test_json_schema_basic() {
	log_section "JSON Schema Basic Validation Test"

	local topic="${TEST_PREFIX}-jsonschema-basic"
	local subject="${TEST_PREFIX}-jsonschema-basic-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	local json_schema='{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "active": {"type": "boolean"}
        },
        "required": ["id", "name"]
    }'

	if ! register_json_schema "$subject" "$json_schema"; then
		log_fail "JSON Schema Basic: Failed to register schema"
		return 1
	fi

	# Produce valid message
	local valid_payload='{"id": 1, "name": "json-schema-user", "active": true}'

	if [ -x "$KAFKA_HOME/bin/kafka-json-schema-console-producer.sh" ]; then
		echo "$valid_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-json-schema-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property value.schema="$json_schema" 2>/dev/null

		local result=$(timeout 15 \
			"$KAFKA_HOME/bin/kafka-json-schema-console-consumer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 1 \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" 2>/dev/null)

		echo "$result" | grep -q 'json-schema-user'
	else
		log_info "JSON Schema CLI tools not available, skipping JSON Schema tests"
		return 0
	fi
}

test_json_schema_strict() {
	log_section "JSON Schema Strict Validation Test"

	local topic="${TEST_PREFIX}-jsonschema-strict"
	local subject="${TEST_PREFIX}-jsonschema-strict-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	local json_schema='{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "userId": {"type": "integer", "minimum": 1, "maximum": 999999},
            "username": {"type": "string", "minLength": 3, "maxLength": 50, "pattern": "^[a-zA-Z0-9_]+$"},
            "email": {"type": "string", "format": "email"},
            "tags": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 10}
        },
        "required": ["userId", "username"],
        "additionalProperties": false
    }'

	if ! register_json_schema "$subject" "$json_schema"; then
		log_fail "JSON Schema Strict: Failed to register schema"
		return 1
	fi

	if [ -x "$KAFKA_HOME/bin/kafka-json-schema-console-producer.sh" ]; then
		# Valid payload
		local valid_payload='{"userId": 123, "username": "test_user_123", "email": "test@example.com", "tags": ["vip", "premium"]}'

		echo "$valid_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-json-schema-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property value.schema="$json_schema" 2>/dev/null

		local result=$(timeout 15 \
			"$KAFKA_HOME/bin/kafka-json-schema-console-consumer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 1 \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" 2>/dev/null)

		echo "$result" | grep -q 'test_user_123'
	else
		log_info "JSON Schema CLI tools not available, skipping JSON Schema strict tests"
		return 0
	fi
}

test_json_schema_complex_types() {
	log_section "JSON Schema Complex Types Test"

	local topic="${TEST_PREFIX}-jsonschema-complex"
	local subject="${TEST_PREFIX}-jsonschema-complex-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	local json_schema='{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "data": {
                "type": "object",
                "properties": {
                    "nestedString": {"type": "string"},
                    "nestedNumber": {"type": "number"},
                    "nestedBool": {"type": "boolean"}
                },
                "required": ["nestedString"]
            },
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "itemId": {"type": "integer"},
                        "itemName": {"type": "string"},
                        "metadata": {
                            "type": "object",
                            "properties": {
                                "created": {"type": "string", "format": "date-time"},
                                "version": {"type": "integer"}
                            }
                        }
                    },
                    "required": ["itemId", "itemName"]
                }
            },
            "status": {
                "enum": ["pending", "processing", "completed", "failed"]
            }
        },
        "required": ["id", "data"]
    }'

	if ! register_json_schema "$subject" "$json_schema"; then
		log_fail "JSON Schema Complex: Failed to register schema"
		return 1
	fi

	if [ -x "$KAFKA_HOME/bin/kafka-json-schema-console-producer.sh" ]; then
		local valid_payload='{
            "id": 100,
            "data": {
                "nestedString": "test",
                "nestedNumber": 42.5,
                "nestedBool": true
            },
            "items": [
                {"itemId": 1, "itemName": "item1", "metadata": {"created": "2024-01-01T00:00:00Z", "version": 1}},
                {"itemId": 2, "itemName": "item2", "metadata": {"created": "2024-01-02T00:00:00Z", "version": 2}}
            ],
            "status": "processing"
        }'

		echo "$valid_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-json-schema-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property value.schema="$json_schema" 2>/dev/null

		local result=$(timeout 15 \
			"$KAFKA_HOME/bin/kafka-json-schema-console-consumer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 1 \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" 2>/dev/null)

		echo "$result" | grep -q 'test' &&
			echo "$result" | grep -q 'processing'
	else
		log_info "JSON Schema CLI tools not available, skipping JSON Schema complex tests"
		return 0
	fi
}

test_json_schema_evolution() {
	log_section "JSON Schema Evolution Test"

	local topic="${TEST_PREFIX}-jsonschema-evolution"
	local subject="${TEST_PREFIX}-jsonschema-evolution-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	# V1 schema
	local json_schema_v1='{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"}
        },
        "required": ["id"]
    }'

	if ! register_json_schema "$subject" "$json_schema_v1"; then
		log_fail "JSON Schema Evolution: Failed to register initial schema"
		return 1
	fi

	# Set compatibility
	curl -s -X PUT \
		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
		-d '{"compatibility": "FORWARD"}' \
		"$SCHEMA_REGISTRY_URL/config/$subject" >/dev/null

	# V2 schema (forward compatible - added optional field)
	local json_schema_v2='{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "description": {"type": "string"},
            "metadata": {
                "type": "object",
                "properties": {
                    "createdAt": {"type": "string", "format": "date-time"},
                    "updatedAt": {"type": "string", "format": "date-time"}
                }
            }
        },
        "required": ["id"]
    }'

	# Register V2 schema
	register_json_schema "$subject" "$json_schema_v2"

	if [ -x "$KAFKA_HOME/bin/kafka-json-schema-console-producer.sh" ]; then
		# Produce with V1 schema
		local v1_payload='{"id": 1, "name": "event-v1"}'
		echo "$v1_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-json-schema-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property value.schema="$json_schema_v1" 2>/dev/null

		# Produce with V2 schema
		local v2_payload='{"id": 2, "name": "event-v2", "description": "new event", "metadata": {"createdAt": "2024-01-01T00:00:00Z"}}'
		echo "$v2_payload" | timeout 15 \
			"$KAFKA_HOME/bin/kafka-json-schema-console-producer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" \
			--property value.schema="$json_schema_v2" 2>/dev/null

		local result=$(timeout 15 \
			"$KAFKA_HOME/bin/kafka-json-schema-console-consumer.sh" \
			--bootstrap-server "$BOOTSTRAP_SERVERS" \
			--topic "$topic" \
			--from-beginning \
			--max-messages 2 \
			--property schema.registry.url="$SCHEMA_REGISTRY_URL" 2>/dev/null)

		echo "$result" | grep -q 'event-v1' &&
			echo "$result" | grep -q 'event-v2'
	else
		log_info "JSON Schema CLI tools not available, skipping JSON Schema evolution tests"
		return 0
	fi
}

# =============================================================================
# Error Case Tests
# =============================================================================

test_error_invalid_schema() {
	log_section "Error Cases Test"

	local topic="${TEST_PREFIX}-error-invalid"
	local subject="${TEST_PREFIX}-error-invalid-value"
	TEST_SUBJECTS+=("$subject")
	create_topic "$topic"

	# Test with malformed schema
	local invalid_schema='{"type": "record", "name": "Invalid", "fields": []'

	# Should fail to register
	if curl -s -X POST \
		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
		-d "{\"schema\": \"$(echo "$invalid_schema" | sed 's/"/\\"/g')\"}" \
		"$SCHEMA_REGISTRY_URL/subjects/${subject}/versions" 2>/dev/null | grep -q "error"; then
		log_pass "Invalid schema detection"
	else
		log_fail "Invalid schema detection"
	fi
}

test_error_malformed_message() {
	log_section "Malformed Message Test"

	local topic="${TEST_PREFIX}-error-malformed"
	create_topic "$topic"

	# Produce malformed JSON
	echo '{"id": 1, name: "missing quotes"}' | timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-producer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--producer.config "$PRODUCER_CONFIG" 2>/dev/null || true

	# Should be able to consume (as raw bytes)
	local result=$(timeout 10 \
		"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
		--bootstrap-server "$BOOTSTRAP_SERVERS" \
		--topic "$topic" \
		--from-beginning \
		--max-messages 1 \
		--consumer.config "$CONSUMER_CONFIG" 2>/dev/null)

	# Message was produced (even if malformed)
	[[ -n "$result" ]]
}

# =============================================================================
# Main
# =============================================================================

main() {
	echo "=========================================="
	echo "  Message Format Compatibility Tests"
	echo "=========================================="
	echo ""
	echo "Bootstrap Servers: $BOOTSTRAP_SERVERS"
	echo "Schema Registry:   $SCHEMA_REGISTRY_URL"
	echo "Kafka Home:        $KAFKA_HOME"
	echo ""

	# Check connectivity
	log_info "Checking Kafka connectivity..."
	if ! timeout 5 bash -c "echo > /dev/tcp/${BOOTSTRAP_SERVERS%%:*}/${BOOTSTRAP_SERVERS##*:}" 2>/dev/null; then
		echo -e "${RED}Error: Cannot connect to Kafka at $BOOTSTRAP_SERVERS${NC}"
		echo "Please ensure Kafka broker is running."
		exit 1
	fi

	log_info "Checking Schema Registry connectivity..."
	if ! curl -s --connect-timeout 5 "$SCHEMA_REGISTRY_URL/subjects" >/dev/null 2>&1; then
		echo -e "${RED}Error: Cannot connect to Schema Registry at $SCHEMA_REGISTRY_URL${NC}"
		echo "Note: Schema Registry tests will be skipped if not available."
	fi

	setup_configs

	# Run JSON tests
	log_section "Running JSON Format Tests"
	run_test "JSON Basic" test_json_basic
	run_test "JSON Nested Objects" test_json_nested
	run_test "JSON Arrays" test_json_arrays
	run_test "JSON Special Characters" test_json_special_chars
	run_test "JSON Large Payload" test_json_large_payload

	# Run AVRO tests (require Schema Registry)
	if curl -s --connect-timeout 5 "$SCHEMA_REGISTRY_URL/subjects" >/dev/null 2>&1; then
		log_section "Running AVRO Format Tests"
		run_test "AVRO Basic" test_avro_basic
		run_test "AVRO Complex Schema" test_avro_complex_schema
		run_test "AVRO Union Types" test_avro_union_types
		run_test "AVRO Schema Evolution" test_avro_schema_evolution

		# Run Protobuf tests
		log_section "Running Protobuf Format Tests"
		run_test "Protobuf Basic" test_protobuf_basic
		run_test "Protobuf Complex" test_protobuf_complex
		run_test "Protobuf Oneof Fields" test_protobuf_oneof_fields

		# Run JSON Schema tests
		log_section "Running JSON Schema Format Tests"
		run_test "JSON Schema Basic" test_json_schema_basic
		run_test "JSON Schema Strict" test_json_schema_strict
		run_test "JSON Schema Complex Types" test_json_schema_complex_types
		run_test "JSON Schema Evolution" test_json_schema_evolution
	else
		log_info "Schema Registry not available. Skipping AVRO, Protobuf, and JSON Schema tests."
	fi

	# Run error case tests
	log_section "Running Error Case Tests"
	run_test "Invalid Schema Detection" test_error_invalid_schema
	run_test "Malformed Message Handling" test_error_malformed_message

	# Cleanup topics
	log_info "Cleaning up test topics..."
	delete_topic "${TEST_PREFIX}-json-basic"
	delete_topic "${TEST_PREFIX}-json-nested"
	delete_topic "${TEST_PREFIX}-json-arrays"
	delete_topic "${TEST_PREFIX}-json-special"
	delete_topic "${TEST_PREFIX}-json-large"
	delete_topic "${TEST_PREFIX}-avro-basic"
	delete_topic "${TEST_PREFIX}-avro-complex"
	delete_topic "${TEST_PREFIX}-avro-union"
	delete_topic "${TEST_PREFIX}-avro-evolution"
	delete_topic "${TEST_PREFIX}-protobuf-basic"
	delete_topic "${TEST_PREFIX}-protobuf-complex"
	delete_topic "${TEST_PREFIX}-protobuf-oneof"
	delete_topic "${TEST_PREFIX}-jsonschema-basic"
	delete_topic "${TEST_PREFIX}-jsonschema-strict"
	delete_topic "${TEST_PREFIX}-jsonschema-complex"
	delete_topic "${TEST_PREFIX}-jsonschema-evolution"
	delete_topic "${TEST_PREFIX}-error-invalid"
	delete_topic "${TEST_PREFIX}-error-malformed"

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
		echo -e "${RED}Some tests failed. Please review the output above.${NC}"
		exit 1
	else
		echo -e "${GREEN}All tests passed!${NC}"
		exit 0
	fi
}

# Run all tests
main "$@"
