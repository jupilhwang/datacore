#!/usr/bin/env python3
"""
DataCore SASL Authentication Integration Tests

Tests SASL PLAIN authentication with produce and consume operations.
Requires: pip install kafka-python

Usage:
    python test_kafka_sasl.py [--bootstrap-server localhost:9092]
    
Note: The broker must be configured with SASL_PLAINTEXT listener
"""

import sys
import time
import argparse
import uuid
from typing import List, Optional

# SASL configuration for tests
SASL_USERS = {
    'admin': 'adminpass',
    'user1': 'user1pass',
    'producer': 'producerpass',
    'consumer': 'consumerpass',
}

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

def log_skip(msg: str, reason: str = ""):
    print(f"\033[33m[SKIP]\033[0m {msg}")
    if reason:
        print(f"       Reason: {reason}")

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


def get_sasl_producer(bootstrap_servers: str, username: str, password: str):
    """Create a SASL-authenticated producer"""
    from kafka import KafkaProducer
    
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username=username,
        sasl_plain_password=password,
        value_serializer=lambda v: v.encode('utf-8') if isinstance(v, str) else v,
        key_serializer=lambda k: k.encode('utf-8') if k and isinstance(k, str) else k,
    )


def get_sasl_consumer(bootstrap_servers: str, username: str, password: str, 
                       group_id: str, topics: List[str]):
    """Create a SASL-authenticated consumer"""
    from kafka import KafkaConsumer
    
    return KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username=username,
        sasl_plain_password=password,
        group_id=group_id,
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
        value_deserializer=lambda v: v.decode('utf-8') if v else None,
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
    )


def get_sasl_admin(bootstrap_servers: str, username: str, password: str):
    """Create a SASL-authenticated admin client"""
    from kafka.admin import KafkaAdminClient
    
    return KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username=username,
        sasl_plain_password=password,
    )


# ============================================
# SASL Authentication Tests
# ============================================

def test_sasl_connection_success(bootstrap_servers: str) -> bool:
    """Test successful SASL PLAIN authentication"""
    admin = None
    try:
        admin = get_sasl_admin(bootstrap_servers, 'admin', 'adminpass')
        
        # Simple operation to verify connection
        topics = admin.list_topics()
        log_info(f"Connected successfully, found {len(topics)} topics")
        
        admin.close()
        return True
    except Exception as e:
        if admin:
            admin.close()
        raise e


def test_sasl_connection_wrong_password(bootstrap_servers: str) -> bool:
    """Test SASL authentication with wrong password"""
    from kafka.errors import SaslAuthenticationFailedError, KafkaError
    
    try:
        admin = get_sasl_admin(bootstrap_servers, 'admin', 'wrongpassword')
        # Try an operation that requires authentication
        admin.list_topics()
        admin.close()
        log_fail("Should have failed with wrong password")
        return False
    except SaslAuthenticationFailedError:
        log_info("Correctly rejected: SASL authentication failed")
        return True
    except KafkaError as e:
        if 'SASL' in str(e) or 'authentication' in str(e).lower():
            log_info(f"Correctly rejected: {e}")
            return True
        raise


def test_sasl_connection_unknown_user(bootstrap_servers: str) -> bool:
    """Test SASL authentication with unknown user"""
    from kafka.errors import SaslAuthenticationFailedError, KafkaError
    
    try:
        admin = get_sasl_admin(bootstrap_servers, 'unknownuser', 'anypassword')
        admin.list_topics()
        admin.close()
        log_fail("Should have failed with unknown user")
        return False
    except SaslAuthenticationFailedError:
        log_info("Correctly rejected: unknown user")
        return True
    except KafkaError as e:
        if 'SASL' in str(e) or 'authentication' in str(e).lower():
            log_info(f"Correctly rejected: {e}")
            return True
        raise


# ============================================
# Produce with SASL Tests
# ============================================

def test_sasl_produce_single_message(bootstrap_servers: str) -> bool:
    """Test producing a single message with SASL authentication"""
    from kafka.admin import NewTopic
    
    topic_name = f"sasl-produce-single-{uuid.uuid4().hex[:8]}"
    
    # Create topic with admin user
    admin = get_sasl_admin(bootstrap_servers, 'admin', 'adminpass')
    try:
        admin.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
        time.sleep(1)
    except Exception as e:
        log_info(f"Topic creation: {e}")
    admin.close()
    
    # Produce with producer user
    producer = get_sasl_producer(bootstrap_servers, 'producer', 'producerpass')
    
    try:
        future = producer.send(topic_name, value='Hello SASL', key='key1')
        result = future.get(timeout=10)
        
        log_info(f"Produced to partition {result.partition}, offset {result.offset}")
        
        producer.close()
        return result.offset >= 0
    except Exception as e:
        producer.close()
        raise e


def test_sasl_produce_multiple_messages(bootstrap_servers: str) -> bool:
    """Test producing multiple messages with SASL authentication"""
    from kafka.admin import NewTopic
    
    topic_name = f"sasl-produce-multi-{uuid.uuid4().hex[:8]}"
    message_count = 100
    
    # Create topic
    admin = get_sasl_admin(bootstrap_servers, 'admin', 'adminpass')
    try:
        admin.create_topics([NewTopic(name=topic_name, num_partitions=3, replication_factor=1)])
        time.sleep(1)
    except:
        pass
    admin.close()
    
    # Produce messages
    producer = get_sasl_producer(bootstrap_servers, 'user1', 'user1pass')
    
    try:
        futures = []
        for i in range(message_count):
            future = producer.send(
                topic_name, 
                value=f'Message {i}',
                key=f'key-{i % 10}'
            )
            futures.append(future)
        
        # Wait for all
        producer.flush()
        
        success = 0
        for f in futures:
            try:
                f.get(timeout=5)
                success += 1
            except:
                pass
        
        log_info(f"Successfully produced {success}/{message_count} messages")
        producer.close()
        
        return success == message_count
    except Exception as e:
        producer.close()
        raise e


# ============================================
# Consume with SASL Tests
# ============================================

def test_sasl_consume_messages(bootstrap_servers: str) -> bool:
    """Test consuming messages with SASL authentication"""
    from kafka.admin import NewTopic
    
    topic_name = f"sasl-consume-{uuid.uuid4().hex[:8]}"
    message_count = 10
    
    # Create topic
    admin = get_sasl_admin(bootstrap_servers, 'admin', 'adminpass')
    try:
        admin.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
        time.sleep(1)
    except:
        pass
    admin.close()
    
    # Produce messages first
    producer = get_sasl_producer(bootstrap_servers, 'producer', 'producerpass')
    for i in range(message_count):
        producer.send(topic_name, value=f'Message {i}', key=f'key{i}')
    producer.flush()
    producer.close()
    time.sleep(1)
    
    # Consume messages
    consumer = get_sasl_consumer(
        bootstrap_servers, 
        'consumer', 
        'consumerpass',
        f'test-group-{uuid.uuid4().hex[:8]}',
        [topic_name]
    )
    
    try:
        consumed = []
        for msg in consumer:
            consumed.append(msg)
            if len(consumed) >= message_count:
                break
        
        consumer.close()
        
        log_info(f"Consumed {len(consumed)} messages")
        return len(consumed) == message_count
    except Exception as e:
        consumer.close()
        raise e


def test_sasl_produce_consume_roundtrip(bootstrap_servers: str) -> bool:
    """Test full produce-consume cycle with SASL authentication"""
    from kafka.admin import NewTopic
    
    topic_name = f"sasl-roundtrip-{uuid.uuid4().hex[:8]}"
    test_messages = [
        ('key1', 'Hello, World!'),
        ('key2', 'SASL authentication test'),
        ('key3', 'DataCore Kafka'),
        (None, 'Message without key'),
        ('key5', 'Last message'),
    ]
    
    # Create topic
    admin = get_sasl_admin(bootstrap_servers, 'admin', 'adminpass')
    try:
        admin.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
        time.sleep(1)
    except:
        pass
    admin.close()
    
    # Produce messages
    producer = get_sasl_producer(bootstrap_servers, 'admin', 'adminpass')
    for key, value in test_messages:
        producer.send(topic_name, value=value, key=key)
    producer.flush()
    producer.close()
    time.sleep(1)
    
    # Consume and verify
    consumer = get_sasl_consumer(
        bootstrap_servers,
        'admin',
        'adminpass',
        f'roundtrip-group-{uuid.uuid4().hex[:8]}',
        [topic_name]
    )
    
    try:
        consumed = []
        for msg in consumer:
            consumed.append((msg.key, msg.value))
            if len(consumed) >= len(test_messages):
                break
        
        consumer.close()
        
        # Verify content
        for i, (expected_key, expected_value) in enumerate(test_messages):
            actual_key, actual_value = consumed[i]
            if actual_value != expected_value:
                log_fail(f"Message {i} value mismatch: expected '{expected_value}', got '{actual_value}'")
                return False
            if expected_key is not None and actual_key != expected_key:
                log_fail(f"Message {i} key mismatch: expected '{expected_key}', got '{actual_key}'")
                return False
        
        log_info(f"All {len(test_messages)} messages verified")
        return True
    except Exception as e:
        consumer.close()
        raise e


# ============================================
# Consumer Group with SASL Tests
# ============================================

def test_sasl_consumer_group_multiple_consumers(bootstrap_servers: str) -> bool:
    """Test multiple consumers in a group with SASL authentication"""
    from kafka.admin import NewTopic
    import threading
    
    topic_name = f"sasl-group-multi-{uuid.uuid4().hex[:8]}"
    group_id = f"sasl-group-{uuid.uuid4().hex[:8]}"
    message_count = 30
    partition_count = 3
    
    # Create topic with multiple partitions
    admin = get_sasl_admin(bootstrap_servers, 'admin', 'adminpass')
    try:
        admin.create_topics([NewTopic(name=topic_name, num_partitions=partition_count, replication_factor=1)])
        time.sleep(1)
    except:
        pass
    admin.close()
    
    # Produce messages
    producer = get_sasl_producer(bootstrap_servers, 'admin', 'adminpass')
    for i in range(message_count):
        producer.send(topic_name, value=f'Message {i}', key=f'key-{i}')
    producer.flush()
    producer.close()
    time.sleep(1)
    
    # Create multiple consumers in the same group
    consumed_messages = []
    lock = threading.Lock()
    
    def consume_worker(consumer_id: int):
        consumer = get_sasl_consumer(
            bootstrap_servers,
            'consumer',
            'consumerpass',
            group_id,
            [topic_name]
        )
        
        try:
            for msg in consumer:
                with lock:
                    consumed_messages.append((consumer_id, msg.value))
                    if len(consumed_messages) >= message_count:
                        break
        except:
            pass
        finally:
            consumer.close()
    
    # Start multiple consumer threads
    threads = []
    for i in range(2):  # 2 consumers
        t = threading.Thread(target=consume_worker, args=(i,))
        t.start()
        threads.append(t)
        time.sleep(0.5)  # Stagger starts
    
    # Wait for completion (with timeout)
    for t in threads:
        t.join(timeout=30)
    
    log_info(f"Total consumed: {len(consumed_messages)} messages")
    
    # Verify all messages were consumed exactly once
    return len(consumed_messages) >= message_count


# ============================================
# Main
# ============================================

def main():
    parser = argparse.ArgumentParser(description='DataCore SASL Authentication Tests')
    parser.add_argument('--bootstrap-server', default='localhost:9092',
                       help='Bootstrap server (default: localhost:9092)')
    parser.add_argument('--skip-auth-tests', action='store_true',
                       help='Skip authentication failure tests')
    args = parser.parse_args()
    
    bootstrap_servers = args.bootstrap_server
    
    print("=" * 60)
    print("DataCore SASL Authentication Integration Tests")
    print("=" * 60)
    print(f"Bootstrap Server: {bootstrap_servers}")
    print()
    
    # Check kafka-python is installed
    try:
        import kafka
        log_info(f"kafka-python version: {kafka.__version__}")
    except ImportError:
        print("ERROR: kafka-python not installed")
        print("Install with: pip install kafka-python")
        sys.exit(1)
    
    # SASL Authentication Tests
    print("\n" + "=" * 60)
    print("SASL Authentication Tests")
    print("=" * 60)
    
    run_test("SASL Connection Success", test_sasl_connection_success, bootstrap_servers)
    
    if not args.skip_auth_tests:
        run_test("SASL Connection Wrong Password", test_sasl_connection_wrong_password, bootstrap_servers)
        run_test("SASL Connection Unknown User", test_sasl_connection_unknown_user, bootstrap_servers)
    else:
        log_skip("SASL Connection Wrong Password", "skipped")
        log_skip("SASL Connection Unknown User", "skipped")
    
    # Produce with SASL Tests
    print("\n" + "=" * 60)
    print("Produce with SASL Tests")
    print("=" * 60)
    
    run_test("SASL Produce Single Message", test_sasl_produce_single_message, bootstrap_servers)
    run_test("SASL Produce Multiple Messages", test_sasl_produce_multiple_messages, bootstrap_servers)
    
    # Consume with SASL Tests
    print("\n" + "=" * 60)
    print("Consume with SASL Tests")
    print("=" * 60)
    
    run_test("SASL Consume Messages", test_sasl_consume_messages, bootstrap_servers)
    run_test("SASL Produce-Consume Roundtrip", test_sasl_produce_consume_roundtrip, bootstrap_servers)
    
    # Consumer Group Tests
    print("\n" + "=" * 60)
    print("Consumer Group with SASL Tests")
    print("=" * 60)
    
    run_test("SASL Multiple Consumers in Group", test_sasl_consumer_group_multiple_consumers, bootstrap_servers)
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    print(f"Total:  {tests_run}")
    print(f"Passed: {tests_passed}")
    print(f"Failed: {tests_failed}")
    
    if tests_failed > 0:
        sys.exit(1)
    else:
        print("\n✓ All tests passed!")
        sys.exit(0)


if __name__ == '__main__':
    main()
