#!/usr/bin/env python3
"""
DataCore E2E Replication Test using Python kafka-python library
"""

import time
import subprocess
import signal
import sys
import os
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError

# Colors
GREEN = '\033[0;32m'
RED = '\033[0;31m'
YELLOW = '\033[1;33m'
NC = '\033[0m'

# Test counters
passed = 0
failed = 0

# Process handles
broker1_proc = None
broker2_proc = None

def log_info(msg):
    print(f"{GREEN}[INFO]{NC} {msg}")

def log_error(msg):
    print(f"{RED}[ERROR]{NC} {msg}")

def log_warn(msg):
    print(f"{YELLOW}[WARN]{NC} {msg}")

def check_result(test_name, success):
    global passed, failed
    if success:
        log_info(f"{test_name}: PASSED")
        passed += 1
    else:
        log_error(f"{test_name}: FAILED")
        failed += 1
    return success

def cleanup():
    global broker1_proc, broker2_proc
    log_info("Cleaning up...")
    
    if broker1_proc:
        broker1_proc.terminate()
        try:
            broker1_proc.wait(timeout=3)
        except:
            broker1_proc.kill()
    
    if broker2_proc:
        broker2_proc.terminate()
        try:
            broker2_proc.wait(timeout=3)
        except:
            broker2_proc.kill()
    
    # Kill any remaining processes
    os.system("pkill -f 'datacore broker' 2>/dev/null || true")
    time.sleep(2)

def main():
    global broker1_proc, broker2_proc
    
    print("=" * 50)
    print("DataCore E2E Replication Test (Python)")
    print("=" * 50)
    print()
    
    # Setup cleanup handler
    signal.signal(signal.SIGINT, lambda s, f: (cleanup(), sys.exit(1)))
    signal.signal(signal.SIGTERM, lambda s, f: (cleanup(), sys.exit(1)))
    
    try:
        # 1. Check binary
        log_info("Step 1: Checking binary...")
        if not os.path.exists("./bin/datacore"):
            log_error("Binary not found. Run 'make build' first.")
            sys.exit(1)
        check_result("Binary check", True)
        
        # 2. Start Broker 1
        log_info("Step 2: Starting Broker 1 (port 9092, replication 9093)...")
        broker1_proc = subprocess.Popen(
            ["./bin/datacore", "broker", "start", "--config=config-broker1-memory.toml"],
            stdout=open("broker1.log", "w"),
            stderr=subprocess.STDOUT
        )
        time.sleep(3)
        
        if broker1_proc.poll() is None:
            check_result("Broker 1 startup", True)
        else:
            log_error("Broker 1 failed to start")
            with open("broker1.log") as f:
                print(f.read())
            cleanup()
            sys.exit(1)
        
        # 3. Start Broker 2
        log_info("Step 3: Starting Broker 2 (port 9094, replication 9095)...")
        broker2_proc = subprocess.Popen(
            ["./bin/datacore", "broker", "start", "--config=config-broker2-memory.toml"],
            stdout=open("broker2.log", "w"),
            stderr=subprocess.STDOUT
        )
        time.sleep(3)
        
        if broker2_proc.poll() is None:
            check_result("Broker 2 startup", True)
        else:
            log_error("Broker 2 failed to start")
            with open("broker2.log") as f:
                print(f.read())
            cleanup()
            sys.exit(1)
        
        # 4. Create topic
        log_info("Step 4: Creating test topic 'test-replication'...")
        try:
            admin = KafkaAdminClient(bootstrap_servers='127.0.0.1:9092')
            topic = NewTopic(name='test-replication', num_partitions=1, replication_factor=1)
            admin.create_topics([topic])
            time.sleep(1)
            check_result("Topic creation", True)
        except Exception as e:
            log_warn(f"Topic creation: {e} (may already exist)")
        
        # 5. Send messages with acks=-1
        log_info("Step 5: Sending messages with acks=-1...")
        try:
            producer = KafkaProducer(
                bootstrap_servers='127.0.0.1:9092',
                acks='all',  # acks=-1
                retries=3,
                value_serializer=lambda v: v.encode('utf-8')
            )
            
            producer.send('test-replication', 'message1')
            producer.send('test-replication', 'message2')
            producer.flush()
            
            check_result("Message production", True)
            time.sleep(2)
        except Exception as e:
            log_error(f"Failed to send messages: {e}")
            check_result("Message production", False)
        
        # 6. Check logs for replication
        log_info("Step 6: Checking replication logs...")
        log_info("Broker 1 log (last 20 lines):")
        os.system("tail -20 broker1.log")
        
        print()
        log_info("Broker 2 log (last 20 lines):")
        os.system("tail -20 broker2.log")
        
        # Check for replication keywords
        with open("broker1.log") as f1, open("broker2.log") as f2:
            log1 = f1.read()
            log2 = f2.read()
            if "REPLICATE" in log1 or "replica" in log1 or "REPLICATE" in log2 or "replica" in log2:
                check_result("Replication evidence in logs", True)
            else:
                log_warn("No replication keywords found in logs")
        
        # 7. Consume messages
        log_info("Step 7: Consuming messages...")
        try:
            consumer = KafkaConsumer(
                'test-replication',
                bootstrap_servers='127.0.0.1:9092',
                auto_offset_reset='earliest',
                consumer_timeout_ms=5000,
                value_deserializer=lambda v: v.decode('utf-8')
            )
            
            messages = []
            for msg in consumer:
                messages.append(msg.value)
                if len(messages) >= 2:
                    break
            
            if 'message1' in messages and 'message2' in messages:
                check_result("Message consumption", True)
            else:
                log_error(f"Messages not found. Got: {messages}")
                check_result("Message consumption", False)
        except Exception as e:
            log_error(f"Failed to consume messages: {e}")
            check_result("Message consumption", False)
        
        # 8. Test broker crash scenario
        log_info("Step 8: Testing broker crash scenario...")
        log_info(f"Killing Broker 1 (PID: {broker1_proc.pid})...")
        broker1_proc.kill()
        broker1_proc.wait()
        time.sleep(2)
        
        # Send message to Broker 2
        log_info("Sending message to Broker 2...")
        try:
            producer2 = KafkaProducer(
                bootstrap_servers='127.0.0.1:9094',
                acks='all',
                retries=3,
                value_serializer=lambda v: v.encode('utf-8')
            )
            
            producer2.send('test-replication', 'message3-after-crash')
            producer2.flush()
            check_result("Message to Broker 2 after crash", True)
            time.sleep(2)
        except Exception as e:
            log_error(f"Failed to send to Broker 2: {e}")
            check_result("Message to Broker 2 after crash", False)
        
        # Consume from Broker 2
        log_info("Consuming from Broker 2...")
        try:
            consumer2 = KafkaConsumer(
                'test-replication',
                bootstrap_servers='127.0.0.1:9094',
                auto_offset_reset='earliest',
                consumer_timeout_ms=5000,
                value_deserializer=lambda v: v.decode('utf-8')
            )
            
            messages2 = []
            for msg in consumer2:
                messages2.append(msg.value)
                if len(messages2) >= 3:
                    break
            
            if 'message3-after-crash' in messages2:
                check_result("Message availability after crash", True)
            else:
                log_error(f"Message not available. Got: {messages2}")
                check_result("Message availability after crash", False)
        except Exception as e:
            log_error(f"Failed to consume from Broker 2: {e}")
            check_result("Message availability after crash", False)
        
        # Summary
        print()
        print("=" * 50)
        print("Test Summary")
        print("=" * 50)
        print(f"{GREEN}PASSED: {passed}{NC}")
        print(f"{RED}FAILED: {failed}{NC}")
        print()
        
        if failed == 0:
            log_info("All tests PASSED!")
            cleanup()
            sys.exit(0)
        else:
            log_error("Some tests FAILED!")
            cleanup()
            sys.exit(1)
    
    except Exception as e:
        log_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        cleanup()
        sys.exit(1)

if __name__ == "__main__":
    main()
