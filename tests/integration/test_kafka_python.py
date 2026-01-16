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
        admin.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
    except:
        pass
    admin.close()
    time.sleep(1)
    
    # Produce messages
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks=1
    )
    
    try:
        for i in range(10):
            future = producer.send(topic_name, f"Message {i}".encode('utf-8'))
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
        admin.create_topics([NewTopic(name=topic_name, num_partitions=3, replication_factor=1)])
    except:
        pass
    admin.close()
    time.sleep(1)
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=str.encode,
        value_serializer=str.encode
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
        admin.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
    except:
        pass
    admin.close()
    time.sleep(1)
    
    # Produce messages
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    for msg in test_messages:
        producer.send(topic_name, msg.encode('utf-8')).get(timeout=10)
    producer.flush()
    producer.close()
    
    time.sleep(1)
    
    # Consume messages
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
        group_id=None  # Disable consumer group
    )
    
    received = []
    try:
        for message in consumer:
            received.append(message.value.decode('utf-8'))
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
        admin.create_topics([NewTopic(name=topic_name, num_partitions=3, replication_factor=1)])
    except:
        pass
    admin.close()
    time.sleep(1)
    
    # Produce messages
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    for i in range(10):
        producer.send(topic_name, f"Message-{i}".encode('utf-8')).get(timeout=10)
    producer.flush()
    producer.close()
    
    time.sleep(1)
    
    # Consume with consumer group
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
        group_id=group_id
    )
    
    received = []
    try:
        for message in consumer:
            received.append(message.value.decode('utf-8'))
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
        admin.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])
    except:
        pass
    admin.close()
    time.sleep(1)
    
    # Produce messages
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    for i in range(10):
        producer.send(topic_name, f"Message-{i}".encode('utf-8')).get(timeout=10)
    producer.flush()
    producer.close()
    
    time.sleep(1)
    
    # First consumer - read 5 messages and commit
    consumer1 = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
        group_id=group_id
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
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
        group_id=group_id
    )
    
    received = []
    try:
        for message in consumer2:
            received.append(message.value.decode('utf-8'))
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
# Main
# ============================================

def main():
    parser = argparse.ArgumentParser(description='DataCore Kafka Python Compatibility Tests')
    parser.add_argument('--bootstrap-server', default='localhost:9092',
                        help='Bootstrap server address (default: localhost:9092)')
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

if __name__ == '__main__':
    main()
