#!/usr/bin/env python3
"""
DataCore Kafka Python Client Compatibility Tests

Tests compatibility with kafka-python library.
Requires: pip install kafka-python

Usage:
    python test_kafka_python.py [--bootstrap-server localhost:9092]
"""

import sys
import time
import argparse
import uuid
from typing import List, Tuple

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
        return False


# ============================================
# Test Functions
# ============================================


def test_admin_client(bootstrap_servers: str) -> bool:
    """Test AdminClient operations"""
    from kafka.admin import KafkaAdminClient, NewTopic

    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    # Create topic
    topic_name = f"test-admin-{uuid.uuid4().hex[:8]}"
    new_topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)

    try:
        admin.create_topics([new_topic])
        time.sleep(1)

        # List topics
        topics = admin.list_topics()
        if topic_name not in topics:
            return False

        # Delete topic
        admin.delete_topics([topic_name])

        admin.close()
        return True
    except Exception as e:
        admin.close()
        raise e


def test_producer_simple(bootstrap_servers: str) -> bool:
    """Test simple message production"""
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic

    topic_name = f"test-produce-{uuid.uuid4().hex[:8]}"

    # Create topic first
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        )
    except:
        pass
    admin.close()
    time.sleep(1)

    # Produce messages
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, acks=1)

    try:
        for i in range(10):
            future = producer.send(topic_name, f"Message {i}".encode("utf-8"))
            future.get(timeout=10)

        producer.flush()
        producer.close()

        # Cleanup
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        admin.delete_topics([topic_name])
        admin.close()

        return True
    except Exception as e:
        producer.close()
        raise e


def test_producer_with_key(bootstrap_servers: str) -> bool:
    """Test production with message keys"""
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic

    topic_name = f"test-key-{uuid.uuid4().hex[:8]}"

    # Create topic
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=3, replication_factor=1)]
        )
    except:
        pass
    admin.close()
    time.sleep(1)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=str.encode,
        value_serializer=str.encode,
    )

    try:
        for i in range(10):
            future = producer.send(topic_name, key=f"key-{i}", value=f"value-{i}")
            future.get(timeout=10)

        producer.flush()
        producer.close()

        # Cleanup
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        admin.delete_topics([topic_name])
        admin.close()

        return True
    except Exception as e:
        producer.close()
        raise e


def test_consumer_basic(bootstrap_servers: str) -> bool:
    """Test basic message consumption"""
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic

    topic_name = f"test-consume-{uuid.uuid4().hex[:8]}"
    test_messages = [f"Message-{i}" for i in range(5)]

    # Create topic
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        )
    except:
        pass
    admin.close()
    time.sleep(1)

    # Produce messages
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    for msg in test_messages:
        producer.send(topic_name, msg.encode("utf-8")).get(timeout=10)
    producer.flush()
    producer.close()

    time.sleep(1)

    # Consume messages
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        group_id=None,  # Disable consumer group
    )

    received = []
    try:
        for message in consumer:
            received.append(message.value.decode("utf-8"))
            if len(received) >= len(test_messages):
                break

        consumer.close()

        # Cleanup
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        admin.delete_topics([topic_name])
        admin.close()

        return set(received) == set(test_messages)
    except Exception as e:
        consumer.close()
        raise e


def test_consumer_group(bootstrap_servers: str) -> bool:
    """Test consumer group functionality"""
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic

    topic_name = f"test-group-{uuid.uuid4().hex[:8]}"
    group_id = f"test-group-{uuid.uuid4().hex[:8]}"

    # Create topic
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=3, replication_factor=1)]
        )
    except:
        pass
    admin.close()
    time.sleep(1)

    # Produce messages
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    for i in range(10):
        producer.send(topic_name, f"Message-{i}".encode("utf-8")).get(timeout=10)
    producer.flush()
    producer.close()

    time.sleep(1)

    # Consume with consumer group
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        group_id=group_id,
    )

    received = []
    try:
        for message in consumer:
            received.append(message.value.decode("utf-8"))
            if len(received) >= 10:
                break

        consumer.close()

        # Verify group exists
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        groups = admin.list_consumer_groups()
        group_ids = [g[0] for g in groups]

        # Cleanup
        admin.delete_topics([topic_name])
        admin.close()

        return len(received) == 10 and group_id in group_ids
    except Exception as e:
        consumer.close()
        raise e


def test_offset_management(bootstrap_servers: str) -> bool:
    """Test offset commit and fetch"""
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic

    topic_name = f"test-offset-{uuid.uuid4().hex[:8]}"
    group_id = f"test-offset-group-{uuid.uuid4().hex[:8]}"

    # Create topic
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        )
    except:
        pass
    admin.close()
    time.sleep(1)

    # Produce messages
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    for i in range(10):
        producer.send(topic_name, f"Message-{i}".encode("utf-8")).get(timeout=10)
    producer.flush()
    producer.close()

    time.sleep(1)

    # First consumer - read 5 messages and commit
    consumer1 = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
        group_id=group_id,
    )

    count = 0
    for message in consumer1:
        count += 1
        if count >= 5:
            consumer1.commit()
            break
    consumer1.close()

    time.sleep(1)

    # Second consumer - should start from offset 5
    consumer2 = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        group_id=group_id,
    )

    received = []
    try:
        for message in consumer2:
            received.append(message.value.decode("utf-8"))
            if len(received) >= 5:
                break

        consumer2.close()

        # Cleanup
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        admin.delete_topics([topic_name])
        admin.close()

        # Should have received messages 5-9
        return len(received) == 5 and "Message-5" in received[0] if received else False
    except Exception as e:
        consumer2.close()
        raise e


# ============================================
# Compression Tests
# ============================================


def test_compression_snappy(bootstrap_servers: str) -> bool:
    """Test Snappy compression"""
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic

    topic_name = f"test-compression-snappy-{uuid.uuid4().hex[:8]}"
    message = "Snappy test message"

    # Create topic
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        )
    except:
        pass
    admin.close()
    time.sleep(1)

    # Produce with Snappy compression
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers, compression_type="snappy"
    )
    try:
        future = producer.send(topic_name, value=message.encode("utf-8"))
        record_metadata = future.get(timeout=10)
        print(f"Produced to topic {topic_name} at offset {record_metadata.offset}")
    finally:
        producer.close()

    time.sleep(1)

    # Consume and verify
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    received = ""
    try:
        for message in consumer:
            received = message.value.decode("utf-8")
            break
        consumer.close()
    except:
        consumer.close()

    # Cleanup
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    admin.delete_topics([topic_name])
    admin.close()

    return received == message


def test_compression_gzip(bootstrap_servers: str) -> bool:
    """Test Gzip compression"""
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic

    topic_name = f"test-compression-gzip-{uuid.uuid4().hex[:8]}"
    message = "Gzip test message"

    # Create topic
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        )
    except:
        pass
    admin.close()
    time.sleep(1)

    # Produce with Gzip compression
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers, compression_type="gzip"
    )
    try:
        future = producer.send(topic_name, value=message.encode("utf-8"))
        record_metadata = future.get(timeout=10)
        print(f"Produced to topic {topic_name} at offset {record_metadata.offset}")
    finally:
        producer.close()

    time.sleep(1)

    # Consume and verify
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    received = ""
    try:
        for message in consumer:
            received = message.value.decode("utf-8")
            break
        consumer.close()
    except:
        consumer.close()

    # Cleanup
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    admin.delete_topics([topic_name])
    admin.close()

    return received == message


def test_compression_lz4(bootstrap_servers: str) -> bool:
    """Test LZ4 compression"""
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic

    topic_name = f"test-compression-lz4-{uuid.uuid4().hex[:8]}"
    message = "LZ4 test message"

    # Create topic
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        )
    except:
        pass
    admin.close()
    time.sleep(1)

    # Produce with LZ4 compression
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers, compression_type="lz4"
    )
    try:
        future = producer.send(topic_name, value=message.encode("utf-8"))
        record_metadata = future.get(timeout=10)
        print(f"Produced to topic {topic_name} at offset {record_metadata.offset}")
    finally:
        producer.close()

    time.sleep(1)

    # Consume and verify
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    received = ""
    try:
        for message in consumer:
            received = message.value.decode("utf-8")
            break
        consumer.close()
    except:
        consumer.close()

    # Cleanup
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    admin.delete_topics([topic_name])
    admin.close()

    return received == message


def test_compression_zstd(bootstrap_servers: str) -> bool:
    """Test Zstd compression"""
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic

    topic_name = f"test-compression-zstd-{uuid.uuid4().hex[:8]}"
    message = "Zstd test message"

    # Create topic
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        )
    except:
        pass
    admin.close()
    time.sleep(1)

    # Produce with Zstd compression
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers, compression_type="zstd"
    )
    try:
        future = producer.send(topic_name, value=message.encode("utf-8"))
        record_metadata = future.get(timeout=10)
        print(f"Produced to topic {topic_name} at offset {record_metadata.offset}")
    finally:
        producer.close()

    time.sleep(1)

    # Consume and verify
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
    )
    received = ""
    try:
        for message in consumer:
            received = message.value.decode("utf-8")
            break
        consumer.close()
    except:
        consumer.close()

    # Cleanup
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    admin.delete_topics([topic_name])
    admin.close()

    return received == message


def test_compression_batch_compressed(bootstrap_servers: str) -> bool:
    """Test batch compression with multiple messages"""
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic

    topic_name = f"test-compression-batch-{uuid.uuid4().hex[:8]}"
    num_messages = 50

    # Create topic
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        )
    except:
        pass
    admin.close()
    time.sleep(1)

    # Produce multiple messages with Snappy compression
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers, compression_type="snappy"
    )
    try:
        for i in range(1, num_messages + 1):
            future = producer.send(
                topic_name, value=f"Compressed batch message {i}".encode("utf-8")
            )
            future.get(timeout=10)
            if i % 10 == 0:
                print(f"Produced {i}/{num_messages} messages")
    finally:
        producer.close()

    time.sleep(1)

    # Consume all messages
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
    )
    consumed_count = 0
    try:
        for message in consumer:
            consumed_count += 1
            assert "Compressed batch message" in message.value.decode("utf-8")
        consumer.close()
    except:
        consumer.close()

    # Cleanup
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    admin.delete_topics([topic_name])
    admin.close()

    return consumed_count == num_messages


def test_compression_mixed_types(bootstrap_servers: str) -> bool:
    """Test mixed compression types in same topic"""
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic

    topic_name = f"test-compression-mixed-{uuid.uuid4().hex[:8]}"

    # Create topic
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        )
    except:
        pass
    admin.close()
    time.sleep(1)

    # Produce with different compression types (each producer has different compression_type)
    # Snappy
    producer_snappy = KafkaProducer(
        bootstrap_servers=bootstrap_servers, compression_type="snappy"
    )
    try:
        future = producer_snappy.send(
            topic_name, value="Snappy message".encode("utf-8")
        )
        future.get(timeout=10)
        time.sleep(0.2)
    finally:
        producer_snappy.close()

    # Gzip
    producer_gzip = KafkaProducer(
        bootstrap_servers=bootstrap_servers, compression_type="gzip"
    )
    try:
        future = producer_gzip.send(topic_name, value="Gzip message".encode("utf-8"))
        future.get(timeout=10)
        time.sleep(0.2)
    finally:
        producer_gzip.close()

    # LZ4
    producer_lz4 = KafkaProducer(
        bootstrap_servers=bootstrap_servers, compression_type="lz4"
    )
    try:
        future = producer_lz4.send(topic_name, value="LZ4 message".encode("utf-8"))
        future.get(timeout=10)
        time.sleep(0.2)
    finally:
        producer_lz4.close()

    # None (uncompressed) - compression_type을 지정하지 않으면 기본값(압축 없음)
    producer_none = KafkaProducer(bootstrap_servers=bootstrap_servers)
    try:
        future = producer_none.send(
            topic_name, value="Uncompressed message".encode("utf-8")
        )
        future.get(timeout=10)
    finally:
        producer_none.close()

    print("Produced 4 messages with different compression types")

    time.sleep(1)

    # Consume and verify all messages
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
    )
    received_messages = []
    try:
        for message in consumer:
            received_messages.append(message.value.decode("utf-8"))
        consumer.close()
    except:
        consumer.close()

    # Cleanup
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    admin.delete_topics([topic_name])
    admin.close()

    has_snappy = "Snappy message" in received_messages
    has_gzip = "Gzip message" in received_messages
    has_lz4 = "LZ4 message" in received_messages
    has_uncompressed = "Uncompressed message" in received_messages

    print(
        f"Received messages: {len(received_messages)}, Snappy: {has_snappy}, Gzip: {has_gzip}, LZ4: {has_lz4}, None: {has_uncompressed}"
    )

    return has_snappy and has_gzip and has_lz4 and has_uncompressed


# ============================================
# Main
# ============================================


def main():
    parser = argparse.ArgumentParser(
        description="DataCore Kafka Python Compatibility Tests"
    )
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Bootstrap server address (default: localhost:9092)",
    )
    args = parser.parse_args()

    bootstrap_servers = args.bootstrap_server

    print("==========================================")
    print("  DataCore kafka-python Compatibility")
    print("==========================================")
    print()
    print(f"Bootstrap Server: {bootstrap_servers}")

    try:
        import kafka

        print(f"kafka-python version: {kafka.__version__}")
    except ImportError:
        print("\033[31mError:\033[0m kafka-python not installed")
        print("Install with: pip install kafka-python")
        sys.exit(1)

    # Run tests
    print()
    print("--- Admin Client Tests ---")
    run_test("Admin Client Operations", test_admin_client, bootstrap_servers)

    print()
    print("--- Producer Tests ---")
    run_test("Simple Producer", test_producer_simple, bootstrap_servers)
    run_test("Producer with Key", test_producer_with_key, bootstrap_servers)

    print()
    print("--- Consumer Tests ---")
    run_test("Basic Consumer", test_consumer_basic, bootstrap_servers)
    run_test("Consumer Group", test_consumer_group, bootstrap_servers)
    run_test("Offset Management", test_offset_management, bootstrap_servers)

    print()
    print("--- Compression Tests ---")
    run_test("Compression: Snappy", test_compression_snappy, bootstrap_servers)
    run_test("Compression: Gzip", test_compression_gzip, bootstrap_servers)
    run_test("Compression: LZ4", test_compression_lz4, bootstrap_servers)
    run_test("Compression: Zstd", test_compression_zstd, bootstrap_servers)
    run_test(
        "Compression: Batch Compressed",
        test_compression_batch_compressed,
        bootstrap_servers,
    )
    run_test(
        "Compression: Mixed Types", test_compression_mixed_types, bootstrap_servers
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
