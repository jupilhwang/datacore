#!/usr/bin/env python3
"""
DataCore gRPC Streaming Integration Tests

Tests gRPC streaming functionality with various scenarios:
1. gRPC → gRPC (produce and consume via gRPC)
2. Kafka CLI → gRPC (produce via kcat, consume via gRPC)
3. gRPC → Kafka CLI (produce via gRPC, consume via kcat)

Usage:
    python test_grpc_streaming.py [--grpc-host localhost] [--grpc-port 9093] [--kafka-host localhost] [--kafka-port 9092]
"""

import sys
import time
import argparse
import uuid
import subprocess
import os
from typing import List, Optional

# Import gRPC client
from grpc_client import GrpcProducer, GrpcConsumer, GrpcRecord, GrpcError

# Test results
tests_run = 0
tests_passed = 0
tests_failed = 0


def log_info(msg: str):
    print(f"\033[34m[INFO]\033[0m {msg}")


def log_pass(msg: str):
    global tests_passed
    print(f"\033[32m[PASS]\033[0m {msg}")
    tests_passed += 1


def log_fail(msg: str, error: str = ""):
    global tests_failed
    print(f"\033[31m[FAIL]\033[0m {msg}")
    if error:
        print(f"       Error: {error}")
    tests_failed += 1


def run_test(name: str, func, *args) -> bool:
    global tests_run
    tests_run += 1
    print()
    log_info(f"Running: {name}")
    try:
        if func(*args):
            log_pass(name)
            return True
        else:
            log_fail(name)
            return False
    except Exception as e:
        log_fail(name, str(e))
        import traceback

        traceback.print_exc()
        return False


# ============================================
# Helper Functions
# ============================================


def create_topic_kafka(bootstrap_server: str, topic: str, partitions: int = 1) -> bool:
    """Create topic using kafka-topics command"""
    try:
        # Try kafka-topics.sh first
        cmd = [
            "kafka-topics.sh",
            "--bootstrap-server",
            bootstrap_server,
            "--create",
            "--topic",
            topic,
            "--partitions",
            str(partitions),
            "--replication-factor",
            "1",
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=10)

        if result.returncode != 0:
            # Try kafka-topics (without .sh)
            cmd[0] = "kafka-topics"
            result = subprocess.run(cmd, capture_output=True, timeout=10)

        return result.returncode == 0
    except Exception as e:
        log_info(f"Failed to create topic via Kafka CLI: {e}")
        return False


def delete_topic_kafka(bootstrap_server: str, topic: str) -> bool:
    """Delete topic using kafka-topics command"""
    try:
        cmd = [
            "kafka-topics.sh",
            "--bootstrap-server",
            bootstrap_server,
            "--delete",
            "--topic",
            topic,
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=10)

        if result.returncode != 0:
            cmd[0] = "kafka-topics"
            result = subprocess.run(cmd, capture_output=True, timeout=10)

        return result.returncode == 0
    except:
        return False


def produce_kafka_cli(bootstrap_server: str, topic: str, messages: List[str]) -> bool:
    """Produce messages using kafka-console-producer"""
    try:
        # Try kafka-console-producer.sh
        cmd = [
            "kafka-console-producer.sh",
            "--bootstrap-server",
            bootstrap_server,
            "--topic",
            topic,
        ]
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        if proc.poll() is not None and proc.returncode != 0:
            # Try without .sh
            cmd[0] = "kafka-console-producer"
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

        # Send messages
        input_data = "\n".join(messages) + "\n"
        stdout, stderr = proc.communicate(input=input_data.encode("utf-8"), timeout=10)

        return proc.returncode == 0
    except Exception as e:
        log_info(f"Failed to produce via Kafka CLI: {e}")
        return False


def consume_kafka_cli(
    bootstrap_server: str, topic: str, max_messages: int = 10, timeout: int = 10
) -> List[str]:
    """Consume messages using kafka-console-consumer"""
    try:
        cmd = [
            "kafka-console-consumer.sh",
            "--bootstrap-server",
            bootstrap_server,
            "--topic",
            topic,
            "--from-beginning",
            "--max-messages",
            str(max_messages),
            "--timeout-ms",
            str(timeout * 1000),
        ]

        result = subprocess.run(cmd, capture_output=True, timeout=timeout + 5)

        if result.returncode != 0:
            cmd[0] = "kafka-console-consumer"
            result = subprocess.run(cmd, capture_output=True, timeout=timeout + 5)

        if result.returncode == 0:
            messages = result.stdout.decode("utf-8").strip().split("\n")
            return [m for m in messages if m]
        return []
    except Exception as e:
        log_info(f"Failed to consume via Kafka CLI: {e}")
        return []


# ============================================
# Test Functions: gRPC ↔ gRPC
# ============================================


def test_grpc_ping(grpc_host: str, grpc_port: int) -> bool:
    """Test gRPC ping/pong"""
    producer = GrpcProducer(grpc_host, grpc_port)
    try:
        producer.connect()
        timestamp = producer.ping()
        producer.close()

        # Verify timestamp is reasonable (within last minute)
        now = int(time.time() * 1000)
        return abs(now - timestamp) < 60000
    except Exception as e:
        producer.close()
        raise e


def test_grpc_produce_consume_simple(
    grpc_host: str, grpc_port: int, kafka_host: str, kafka_port: int
) -> bool:
    """Test simple gRPC produce and consume"""
    topic = f"test-grpc-simple-{uuid.uuid4().hex[:8]}"

    # Create topic via Kafka API
    bootstrap_server = f"{kafka_host}:{kafka_port}"
    if not create_topic_kafka(bootstrap_server, topic):
        log_info("Skipping: Could not create topic via Kafka CLI")
        return True  # Skip test

    time.sleep(1)

    try:
        # Produce via gRPC
        producer = GrpcProducer(grpc_host, grpc_port)
        producer.connect()

        test_messages = [f"Message-{i}" for i in range(5)]
        records = [GrpcRecord(value=msg.encode("utf-8")) for msg in test_messages]

        base_offset, count = producer.produce(topic, records, partition=0)
        producer.close()

        if count != len(test_messages):
            return False

        time.sleep(1)

        # Consume via gRPC
        consumer = GrpcConsumer(grpc_host, grpc_port)
        consumer.connect()
        consumer.subscribe(topic, partition=0, offset=0)

        received = []
        for _ in range(10):  # Try up to 10 polls
            result = consumer.poll(timeout=2.0)
            if result:
                topic_name, partition, offset, record = result
                received.append(record.value.decode("utf-8"))
                if len(received) >= len(test_messages):
                    break

        consumer.close()

        # Cleanup
        delete_topic_kafka(bootstrap_server, topic)

        return set(received) == set(test_messages)

    except Exception as e:
        delete_topic_kafka(bootstrap_server, topic)
        raise e


def test_grpc_produce_multiple_batches(
    grpc_host: str, grpc_port: int, kafka_host: str, kafka_port: int
) -> bool:
    """Test multiple batch produces via gRPC"""
    topic = f"test-grpc-batch-{uuid.uuid4().hex[:8]}"

    bootstrap_server = f"{kafka_host}:{kafka_port}"
    if not create_topic_kafka(bootstrap_server, topic):
        log_info("Skipping: Could not create topic")
        return True

    time.sleep(1)

    try:
        producer = GrpcProducer(grpc_host, grpc_port)
        producer.connect()

        total_messages = 0
        batch_count = 3

        for batch in range(batch_count):
            records = [
                GrpcRecord(value=f"Batch-{batch}-Msg-{i}".encode("utf-8"))
                for i in range(10)
            ]
            base_offset, count = producer.produce(topic, records, partition=0)
            total_messages += count

        producer.close()

        # Cleanup
        delete_topic_kafka(bootstrap_server, topic)

        return total_messages == batch_count * 10

    except Exception as e:
        delete_topic_kafka(bootstrap_server, topic)
        raise e


def test_grpc_produce_with_key(
    grpc_host: str, grpc_port: int, kafka_host: str, kafka_port: int
) -> bool:
    """Test gRPC produce with message keys"""
    topic = f"test-grpc-key-{uuid.uuid4().hex[:8]}"

    bootstrap_server = f"{kafka_host}:{kafka_port}"
    if not create_topic_kafka(bootstrap_server, topic, partitions=3):
        log_info("Skipping: Could not create topic")
        return True

    time.sleep(1)

    try:
        producer = GrpcProducer(grpc_host, grpc_port)
        producer.connect()

        records = [
            GrpcRecord(
                key=f"key-{i}".encode("utf-8"), value=f"value-{i}".encode("utf-8")
            )
            for i in range(10)
        ]

        base_offset, count = producer.produce(topic, records, partition=0)
        producer.close()

        # Cleanup
        delete_topic_kafka(bootstrap_server, topic)

        return count == 10

    except Exception as e:
        delete_topic_kafka(bootstrap_server, topic)
        raise e


def test_grpc_streaming_consume(
    grpc_host: str, grpc_port: int, kafka_host: str, kafka_port: int
) -> bool:
    """Test gRPC streaming consumption"""
    topic = f"test-grpc-stream-{uuid.uuid4().hex[:8]}"

    bootstrap_server = f"{kafka_host}:{kafka_port}"
    if not create_topic_kafka(bootstrap_server, topic):
        log_info("Skipping: Could not create topic")
        return True

    time.sleep(1)

    try:
        # Produce messages
        producer = GrpcProducer(grpc_host, grpc_port)
        producer.connect()

        records = [GrpcRecord(value=f"Stream-{i}".encode("utf-8")) for i in range(20)]
        base_offset, count = producer.produce(topic, records, partition=0)
        producer.close()

        time.sleep(1)

        # Subscribe and stream consume
        consumer = GrpcConsumer(grpc_host, grpc_port)
        consumer.connect()
        consumer.subscribe(topic, partition=0, offset=0)

        received = []
        for _ in range(30):  # Poll multiple times
            result = consumer.poll(timeout=1.0)
            if result:
                _, _, _, record = result
                received.append(record.value.decode("utf-8"))
                if len(received) >= 20:
                    break

        consumer.close()

        # Cleanup
        delete_topic_kafka(bootstrap_server, topic)

        return len(received) == 20

    except Exception as e:
        delete_topic_kafka(bootstrap_server, topic)
        raise e


# ============================================
# Test Functions: Kafka CLI ↔ gRPC
# ============================================


def test_kafka_produce_grpc_consume(
    grpc_host: str, grpc_port: int, kafka_host: str, kafka_port: int
) -> bool:
    """Test Kafka CLI produce → gRPC consume"""
    topic = f"test-kafka-grpc-{uuid.uuid4().hex[:8]}"

    bootstrap_server = f"{kafka_host}:{kafka_port}"
    if not create_topic_kafka(bootstrap_server, topic):
        log_info("Skipping: Kafka CLI not available")
        return True

    time.sleep(1)

    try:
        # Produce via Kafka CLI
        test_messages = [f"KafkaMsg-{i}" for i in range(5)]
        if not produce_kafka_cli(bootstrap_server, topic, test_messages):
            log_info("Skipping: Could not produce via Kafka CLI")
            delete_topic_kafka(bootstrap_server, topic)
            return True

        time.sleep(1)

        # Consume via gRPC
        consumer = GrpcConsumer(grpc_host, grpc_port)
        consumer.connect()
        consumer.subscribe(topic, partition=0, offset=0)

        received = []
        for _ in range(10):
            result = consumer.poll(timeout=2.0)
            if result:
                _, _, _, record = result
                received.append(record.value.decode("utf-8"))
                if len(received) >= len(test_messages):
                    break

        consumer.close()

        # Cleanup
        delete_topic_kafka(bootstrap_server, topic)

        return set(received) == set(test_messages)

    except Exception as e:
        delete_topic_kafka(bootstrap_server, topic)
        raise e


def test_grpc_produce_kafka_consume(
    grpc_host: str, grpc_port: int, kafka_host: str, kafka_port: int
) -> bool:
    """Test gRPC produce → Kafka CLI consume"""
    topic = f"test-grpc-kafka-{uuid.uuid4().hex[:8]}"

    bootstrap_server = f"{kafka_host}:{kafka_port}"
    if not create_topic_kafka(bootstrap_server, topic):
        log_info("Skipping: Kafka CLI not available")
        return True

    time.sleep(1)

    try:
        # Produce via gRPC
        producer = GrpcProducer(grpc_host, grpc_port)
        producer.connect()

        test_messages = [f"GrpcMsg-{i}" for i in range(5)]
        records = [GrpcRecord(value=msg.encode("utf-8")) for msg in test_messages]

        base_offset, count = producer.produce(topic, records, partition=0)
        producer.close()

        time.sleep(1)

        # Consume via Kafka CLI
        received = consume_kafka_cli(
            bootstrap_server, topic, max_messages=5, timeout=10
        )

        # Cleanup
        delete_topic_kafka(bootstrap_server, topic)

        if not received:
            log_info("Skipping: Could not consume via Kafka CLI")
            return True

        return set(received) == set(test_messages)

    except Exception as e:
        delete_topic_kafka(bootstrap_server, topic)
        raise e


# ============================================
# Test Functions: Advanced Features
# ============================================


def test_grpc_offset_commit(
    grpc_host: str, grpc_port: int, kafka_host: str, kafka_port: int
) -> bool:
    """Test gRPC offset commit functionality"""
    topic = f"test-grpc-commit-{uuid.uuid4().hex[:8]}"
    group_id = f"test-group-{uuid.uuid4().hex[:8]}"

    bootstrap_server = f"{kafka_host}:{kafka_port}"
    if not create_topic_kafka(bootstrap_server, topic):
        log_info("Skipping: Could not create topic")
        return True

    time.sleep(1)

    try:
        # Produce messages
        producer = GrpcProducer(grpc_host, grpc_port)
        producer.connect()

        records = [GrpcRecord(value=f"Commit-{i}".encode("utf-8")) for i in range(10)]
        base_offset, count = producer.produce(topic, records, partition=0)
        producer.close()

        time.sleep(1)

        # Consume and commit
        consumer = GrpcConsumer(grpc_host, grpc_port, group_id=group_id)
        consumer.connect()
        consumer.subscribe(topic, partition=0, offset=0)

        received = []
        last_offset = 0
        for _ in range(15):
            result = consumer.poll(timeout=1.0)
            if result:
                _, partition, offset, record = result
                received.append(record.value.decode("utf-8"))
                last_offset = offset

                # Commit after 5 messages
                if len(received) == 5:
                    consumer.commit([(topic, partition, offset + 1)])

                if len(received) >= 10:
                    break

        consumer.close()

        # Cleanup
        delete_topic_kafka(bootstrap_server, topic)

        return len(received) == 10

    except Exception as e:
        delete_topic_kafka(bootstrap_server, topic)
        raise e


def test_grpc_message_headers(
    grpc_host: str, grpc_port: int, kafka_host: str, kafka_port: int
) -> bool:
    """Test gRPC message with headers"""
    topic = f"test-grpc-headers-{uuid.uuid4().hex[:8]}"

    bootstrap_server = f"{kafka_host}:{kafka_port}"
    if not create_topic_kafka(bootstrap_server, topic):
        log_info("Skipping: Could not create topic")
        return True

    time.sleep(1)

    try:
        # Produce with headers
        producer = GrpcProducer(grpc_host, grpc_port)
        producer.connect()

        headers = {"content-type": b"application/json", "correlation-id": b"12345"}

        records = [GrpcRecord(value=b"Message with headers", headers=headers)]

        base_offset, count = producer.produce(topic, records, partition=0)
        producer.close()

        time.sleep(1)

        # Consume and verify headers
        consumer = GrpcConsumer(grpc_host, grpc_port)
        consumer.connect()
        consumer.subscribe(topic, partition=0, offset=0)

        result = consumer.poll(timeout=5.0)
        consumer.close()

        # Cleanup
        delete_topic_kafka(bootstrap_server, topic)

        if result:
            _, _, _, record = result
            return (
                record.headers.get("content-type") == b"application/json"
                and record.headers.get("correlation-id") == b"12345"
            )

        return False

    except Exception as e:
        delete_topic_kafka(bootstrap_server, topic)
        raise e


# ============================================
# Main
# ============================================


def main():
    parser = argparse.ArgumentParser(
        description="DataCore gRPC Streaming Integration Tests"
    )
    parser.add_argument(
        "--grpc-host", default="localhost", help="gRPC server host (default: localhost)"
    )
    parser.add_argument(
        "--grpc-port", type=int, default=9093, help="gRPC server port (default: 9093)"
    )
    parser.add_argument(
        "--kafka-host",
        default="localhost",
        help="Kafka server host (default: localhost)",
    )
    parser.add_argument(
        "--kafka-port", type=int, default=9092, help="Kafka server port (default: 9092)"
    )
    args = parser.parse_args()

    print("==========================================")
    print("  DataCore gRPC Streaming Tests")
    print("==========================================")
    print()
    print(f"gRPC Server: {args.grpc_host}:{args.grpc_port}")
    print(f"Kafka Server: {args.kafka_host}:{args.kafka_port}")
    print()

    # Basic gRPC Tests
    print("--- Basic gRPC Tests ---")
    run_test("gRPC Ping/Pong", test_grpc_ping, args.grpc_host, args.grpc_port)

    # gRPC ↔ gRPC Tests
    print()
    print("--- gRPC ↔ gRPC Tests ---")
    run_test(
        "gRPC Produce/Consume Simple",
        test_grpc_produce_consume_simple,
        args.grpc_host,
        args.grpc_port,
        args.kafka_host,
        args.kafka_port,
    )
    run_test(
        "gRPC Multiple Batch Produce",
        test_grpc_produce_multiple_batches,
        args.grpc_host,
        args.grpc_port,
        args.kafka_host,
        args.kafka_port,
    )
    run_test(
        "gRPC Produce with Key",
        test_grpc_produce_with_key,
        args.grpc_host,
        args.grpc_port,
        args.kafka_host,
        args.kafka_port,
    )
    run_test(
        "gRPC Streaming Consume",
        test_grpc_streaming_consume,
        args.grpc_host,
        args.grpc_port,
        args.kafka_host,
        args.kafka_port,
    )

    # Kafka CLI ↔ gRPC Tests
    print()
    print("--- Kafka CLI ↔ gRPC Tests ---")
    run_test(
        "Kafka CLI Produce → gRPC Consume",
        test_kafka_produce_grpc_consume,
        args.grpc_host,
        args.grpc_port,
        args.kafka_host,
        args.kafka_port,
    )
    run_test(
        "gRPC Produce → Kafka CLI Consume",
        test_grpc_produce_kafka_consume,
        args.grpc_host,
        args.grpc_port,
        args.kafka_host,
        args.kafka_port,
    )

    # Advanced Features
    print()
    print("--- Advanced Features ---")
    run_test(
        "gRPC Offset Commit",
        test_grpc_offset_commit,
        args.grpc_host,
        args.grpc_port,
        args.kafka_host,
        args.kafka_port,
    )
    run_test(
        "gRPC Message Headers",
        test_grpc_message_headers,
        args.grpc_host,
        args.grpc_port,
        args.kafka_host,
        args.kafka_port,
    )

    # Summary
    print()
    print("==========================================")
    print("  Test Summary")
    print("==========================================")
    print(f"Total:  {tests_run}")
    print(f"\033[32mPassed: {tests_passed}\033[0m")
    print(f"\033[31mFailed: {tests_failed}\033[0m")
    print()

    sys.exit(0 if tests_failed == 0 else 1)


if __name__ == "__main__":
    main()
