#!/usr/bin/env python3
"""
DataCore Socket-based Protocol Tests
Direct socket testing without external Kafka libraries.
"""

import socket
import struct
import time
import sys

# Kafka API Keys
API_VERSIONS = 18
METADATA = 3
PRODUCE = 0
FETCH = 1

def encode_string(s):
    """Encode nullable string (INT16 length + data)"""
    if s is None:
        return struct.pack('>h', -1)
    encoded = s.encode('utf-8')
    return struct.pack('>h', len(encoded)) + encoded

def encode_compact_string(s):
    """Encode compact string (UVARINT length+1 + data)"""
    encoded = s.encode('utf-8')
    return encode_uvarint(len(encoded) + 1) + encoded

def encode_uvarint(value):
    """Encode unsigned varint"""
    result = []
    while value > 0x7f:
        result.append((value & 0x7f) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)

def decode_uvarint(data, offset):
    """Decode unsigned varint, return (value, new_offset)"""
    result = 0
    shift = 0
    while True:
        byte = data[offset]
        result |= (byte & 0x7f) << shift
        offset += 1
        if not (byte & 0x80):
            break
        shift += 7
    return result, offset

class KafkaTestClient:
    def __init__(self, host='localhost', port=9092):
        self.host = host
        self.port = port
        self.correlation_id = 0
        self.sock = None
        
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(10)
        self.sock.connect((self.host, self.port))
        print(f"✓ Connected to {self.host}:{self.port}")
        
    def close(self):
        if self.sock:
            self.sock.close()
            
    def send_request(self, api_key, api_version, body):
        """Send request and receive response"""
        self.correlation_id += 1
        
        # Build header
        header = struct.pack('>hhih', 
            api_key, 
            api_version, 
            self.correlation_id,
            len(b'test-client')
        ) + b'test-client'
        
        # For flexible versions, add tagged_fields to header
        # ApiVersions is special - header is always non-flexible
        
        # Full message
        message = header + body
        
        # Send with size prefix
        self.sock.sendall(struct.pack('>i', len(message)) + message)
        
        # Receive response
        size_data = self.sock.recv(4)
        if len(size_data) < 4:
            raise Exception("Failed to receive response size")
        size = struct.unpack('>i', size_data)[0]
        
        response = b''
        while len(response) < size:
            chunk = self.sock.recv(size - len(response))
            if not chunk:
                break
            response += chunk
            
        return response

    def test_api_versions(self):
        """Test ApiVersions request (v0)"""
        print("\n=== Test: ApiVersions (v0) ===")
        
        try:
            response = self.send_request(API_VERSIONS, 0, b'')
            
            # Parse response: correlation_id (4) + error_code (2) + api_versions array
            corr_id = struct.unpack('>i', response[0:4])[0]
            error_code = struct.unpack('>h', response[4:6])[0]
            
            print(f"  Correlation ID: {corr_id}")
            print(f"  Error Code: {error_code}")
            
            if error_code == 0:
                # Parse array
                array_len = struct.unpack('>i', response[6:10])[0]
                print(f"  Supported APIs: {array_len}")
                
                offset = 10
                apis = []
                for _ in range(min(array_len, 5)):  # Show first 5
                    api_key = struct.unpack('>h', response[offset:offset+2])[0]
                    min_ver = struct.unpack('>h', response[offset+2:offset+4])[0]
                    max_ver = struct.unpack('>h', response[offset+4:offset+6])[0]
                    apis.append(f"API {api_key}: v{min_ver}-v{max_ver}")
                    offset += 6
                    
                for api in apis:
                    print(f"    {api}")
                if array_len > 5:
                    print(f"    ... and {array_len - 5} more")
                    
                print("  ✓ PASS: ApiVersions response valid")
                return True
            else:
                print(f"  ✗ FAIL: Error code {error_code}")
                return False
                
        except Exception as e:
            print(f"  ✗ FAIL: {e}")
            return False

    def test_metadata(self, version=9):
        """Test Metadata request"""
        print(f"\n=== Test: Metadata (v{version}) ===")
        
        try:
            # Build body based on version
            if version >= 9:
                # Flexible version
                body = b'\x01'  # compact nullable array = null (0 = null, 1 = empty)
                body += b'\x01'  # allow_auto_topic_creation = true
                if version >= 8:
                    body += b'\x00'  # include_topic_authorized_operations
                    body += b'\x00'  # include_cluster_authorized_operations
                body += b'\x00'  # tagged_fields
                
                # For flexible, add tagged_fields to header
                header = struct.pack('>hhih', 
                    METADATA, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client' + b'\x00'  # header tagged_fields
                
                message = header + body
                self.correlation_id += 1
                
                self.sock.sendall(struct.pack('>i', len(message)) + message)
            else:
                # Non-flexible
                body = struct.pack('>i', 0)  # empty topics array
                response = self.send_request(METADATA, version, body)
                
            # Receive response
            size_data = self.sock.recv(4)
            size = struct.unpack('>i', size_data)[0]
            response = b''
            while len(response) < size:
                chunk = self.sock.recv(size - len(response))
                if not chunk:
                    break
                response += chunk
                
            # Parse response header
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"  Correlation ID: {corr_id}")
            
            offset = 4
            if version >= 9:
                # Skip tagged_fields
                _, offset = decode_uvarint(response, offset)
                
            # Parse throttle_time_ms
            throttle = struct.unpack('>i', response[offset:offset+4])[0]
            print(f"  Throttle Time: {throttle}ms")
            offset += 4
            
            # Parse brokers array
            if version >= 9:
                brokers_len, offset = decode_uvarint(response, offset)
                brokers_len -= 1  # compact array encoding
            else:
                brokers_len = struct.unpack('>i', response[offset:offset+4])[0]
                offset += 4
                
            print(f"  Brokers: {brokers_len}")
            
            print("  ✓ PASS: Metadata response valid")
            return True
            
        except Exception as e:
            print(f"  ✗ FAIL: {e}")
            import traceback
            traceback.print_exc()
            return False

    def test_fetch(self, version=16):
        """Test Fetch request"""
        print(f"\n=== Test: Fetch (v{version}) ===")
        
        try:
            # For v15+, replica_id is REMOVED from body!
            body = b''
            
            if version < 15:
                body += struct.pack('>i', -1)  # replica_id
                
            body += struct.pack('>i', 500)   # max_wait_ms
            body += struct.pack('>i', 1)     # min_bytes
            
            if version >= 3:
                body += struct.pack('>i', 1048576)  # max_bytes
                
            if version >= 4:
                body += struct.pack('>b', 0)  # isolation_level
                
            if version >= 7:
                body += struct.pack('>i', 0)  # session_id
                body += struct.pack('>i', -1) # session_epoch
                
            # Topics array (empty for test)
            if version >= 12:
                body += b'\x01'  # compact array = empty (0+1=1)
            else:
                body += struct.pack('>i', 0)
                
            # Forgotten topics (v7+)
            if version >= 7:
                if version >= 12:
                    body += b'\x01'  # compact array = empty
                else:
                    body += struct.pack('>i', 0)
                    
            # Rack ID (v11+)
            if version >= 11:
                if version >= 12:
                    body += b'\x01'  # compact string = empty
                else:
                    body += struct.pack('>h', 0)
                    
            # Tagged fields (v12+ flexible)
            if version >= 12:
                body += b'\x00'
                
            # Build header
            if version >= 12:
                header = struct.pack('>hhih', 
                    FETCH, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client' + b'\x00'  # header tagged_fields
            else:
                header = struct.pack('>hhih', 
                    FETCH, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client'
                
            message = header + body
            self.correlation_id += 1
            
            self.sock.sendall(struct.pack('>i', len(message)) + message)
            
            # Receive response
            size_data = self.sock.recv(4)
            size = struct.unpack('>i', size_data)[0]
            response = b''
            while len(response) < size:
                chunk = self.sock.recv(size - len(response))
                if not chunk:
                    break
                response += chunk
                
            # Parse response header
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"  Correlation ID: {corr_id}")
            
            offset = 4
            if version >= 12:
                # Skip tagged_fields in header
                tag_count, offset = decode_uvarint(response, offset)
                
            # Parse throttle_time_ms
            throttle = struct.unpack('>i', response[offset:offset+4])[0]
            print(f"  Throttle Time: {throttle}ms")
            offset += 4
            
            # Error code (v7+)
            if version >= 7:
                error_code = struct.unpack('>h', response[offset:offset+2])[0]
                print(f"  Error Code: {error_code}")
                offset += 2
                
                # Session ID
                session_id = struct.unpack('>i', response[offset:offset+4])[0]
                print(f"  Session ID: {session_id}")
                offset += 4
                
            print("  ✓ PASS: Fetch response valid")
            return True
            
        except Exception as e:
            print(f"  ✗ FAIL: {e}")
            import traceback
            traceback.print_exc()
            return False


    def test_list_offsets(self, version=7):
        """Test ListOffsets request"""
        print(f"\n=== Test: ListOffsets (v{version}) ===")
        
        try:
            is_flexible = version >= 6
            body = b''
            
            # replica_id
            body += struct.pack('>i', -1)
            # isolation_level (v2+)
            if version >= 2:
                body += struct.pack('>b', 0)
            
            # Topics array (empty for test)
            if is_flexible:
                body += b'\x01'  # compact array = empty
            else:
                body += struct.pack('>i', 0)
            
            # Tagged fields (v6+ flexible)
            if is_flexible:
                body += b'\x00'
                
            # Build header
            if is_flexible:
                header = struct.pack('>hhih', 
                    2, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client' + b'\x00'
            else:
                header = struct.pack('>hhih', 
                    2, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client'
                
            message = header + body
            self.correlation_id += 1
            
            self.sock.sendall(struct.pack('>i', len(message)) + message)
            
            # Receive response
            size_data = self.sock.recv(4)
            size = struct.unpack('>i', size_data)[0]
            response = b''
            while len(response) < size:
                chunk = self.sock.recv(size - len(response))
                if not chunk:
                    break
                response += chunk
                
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"  Correlation ID: {corr_id}")
            
            offset = 4
            if is_flexible:
                _, offset = decode_uvarint(response, offset)
                
            throttle = struct.unpack('>i', response[offset:offset+4])[0]
            print(f"  Throttle Time: {throttle}ms")
            
            print("  ✓ PASS: ListOffsets response valid")
            return True
            
        except Exception as e:
            print(f"  ✗ FAIL: {e}")
            import traceback
            traceback.print_exc()
            return False

    def test_offset_commit(self, version=8):
        """Test OffsetCommit request"""
        print(f"\n=== Test: OffsetCommit (v{version}) ===")
        
        try:
            is_flexible = version >= 8
            body = b''
            
            # group_id
            if is_flexible:
                body += encode_compact_string('test-group')
            else:
                body += encode_string('test-group')
                
            # generation_id (v1+)
            if version >= 1:
                body += struct.pack('>i', -1)
                
            # member_id (v1+)
            if version >= 1:
                if is_flexible:
                    body += encode_compact_string('')
                else:
                    body += encode_string('')
                    
            # group_instance_id (v7+)
            if version >= 7:
                if is_flexible:
                    body += b'\x00'  # null compact nullable string
                else:
                    body += struct.pack('>h', -1)
                    
            # retention_time_ms (v2-v4)
            if version >= 2 and version <= 4:
                body += struct.pack('>q', -1)
                
            # Topics array
            if is_flexible:
                body += encode_uvarint(2)  # array len + 1 = 2 (1 topic)
                body += encode_compact_string('test-topic')
                body += encode_uvarint(2)  # partitions array len + 1
                body += struct.pack('>i', 0)  # partition_index
                body += struct.pack('>q', 100)  # committed_offset
                if version >= 6:
                    body += struct.pack('>i', -1)  # committed_leader_epoch
                if version >= 1 and version <= 4:
                    body += struct.pack('>q', -1)  # commit_timestamp
                body += b'\x00'  # partition metadata null
                body += b'\x00'  # partition tagged_fields
                body += b'\x00'  # topic tagged_fields
                body += b'\x00'  # request tagged_fields
            else:
                body += struct.pack('>i', 1)  # 1 topic
                body += encode_string('test-topic')
                body += struct.pack('>i', 1)  # 1 partition
                body += struct.pack('>i', 0)  # partition_index
                body += struct.pack('>q', 100)  # committed_offset
                if version >= 6:
                    body += struct.pack('>i', -1)  # committed_leader_epoch
                if version >= 1 and version <= 4:
                    body += struct.pack('>q', -1)  # commit_timestamp
                body += encode_string('')  # metadata
                
            # Build header
            if is_flexible:
                header = struct.pack('>hhih', 
                    8, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client' + b'\x00'
            else:
                header = struct.pack('>hhih', 
                    8, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client'
                
            message = header + body
            self.correlation_id += 1
            
            self.sock.sendall(struct.pack('>i', len(message)) + message)
            
            # Receive response
            size_data = self.sock.recv(4)
            size = struct.unpack('>i', size_data)[0]
            response = b''
            while len(response) < size:
                chunk = self.sock.recv(size - len(response))
                if not chunk:
                    break
                response += chunk
                
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"  Correlation ID: {corr_id}")
            
            offset = 4
            if is_flexible:
                _, offset = decode_uvarint(response, offset)
                
            throttle = struct.unpack('>i', response[offset:offset+4])[0]
            print(f"  Throttle Time: {throttle}ms")
            offset += 4
            
            # Topics array
            if is_flexible:
                topics_len, offset = decode_uvarint(response, offset)
                topics_len -= 1
            else:
                topics_len = struct.unpack('>i', response[offset:offset+4])[0]
                offset += 4
                
            print(f"  Topics: {topics_len}")
            
            print("  ✓ PASS: OffsetCommit response valid")
            return True
            
        except Exception as e:
            print(f"  ✗ FAIL: {e}")
            import traceback
            traceback.print_exc()
            return False

    def test_offset_fetch(self, version=8):
        """Test OffsetFetch request"""
        print(f"\n=== Test: OffsetFetch (v{version}) ===")
        
        try:
            is_flexible = version >= 6
            body = b''
            
            if version <= 7:
                # v0-7: single group format
                if is_flexible:
                    body += encode_compact_string('test-group')
                    body += encode_uvarint(2)  # topics array len + 1 = 2 (1 topic)
                    body += encode_compact_string('test-topic')
                    body += encode_uvarint(2)  # partitions array len + 1
                    body += struct.pack('>i', 0)  # partition_index
                    body += b'\x00'  # partition tagged_fields
                    body += b'\x00'  # topic tagged_fields
                else:
                    body += encode_string('test-group')
                    body += struct.pack('>i', 1)  # 1 topic
                    body += encode_string('test-topic')
                    body += struct.pack('>i', 1)  # 1 partition
                    body += struct.pack('>i', 0)
                    
                # require_stable (v7+)
                if version >= 7:
                    body += struct.pack('>?', False)
            else:
                # v8+: multi-group format
                body += encode_uvarint(2)  # groups array len + 1
                body += encode_compact_string('test-group')
                if version >= 9:
                    body += b'\x00'  # member_id null
                    body += struct.pack('>i', -1)  # member_epoch
                body += encode_uvarint(2)  # topics array len + 1
                body += encode_compact_string('test-topic')
                body += encode_uvarint(2)  # partitions array len + 1
                body += struct.pack('>i', 0)  # partition_index
                body += b'\x00'  # partition tagged_fields
                body += b'\x00'  # topic tagged_fields
                body += b'\x00'  # group tagged_fields
                body += struct.pack('>?', False)  # require_stable
                
            # Tagged fields
            if is_flexible:
                body += b'\x00'
                
            # Build header
            if is_flexible:
                header = struct.pack('>hhih', 
                    9, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client' + b'\x00'
            else:
                header = struct.pack('>hhih', 
                    9, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client'
                
            message = header + body
            self.correlation_id += 1
            
            self.sock.sendall(struct.pack('>i', len(message)) + message)
            
            # Receive response
            size_data = self.sock.recv(4)
            size = struct.unpack('>i', size_data)[0]
            response = b''
            while len(response) < size:
                chunk = self.sock.recv(size - len(response))
                if not chunk:
                    break
                response += chunk
                
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"  Correlation ID: {corr_id}")
            
            offset = 4
            if is_flexible:
                _, offset = decode_uvarint(response, offset)
                
            throttle = struct.unpack('>i', response[offset:offset+4])[0]
            print(f"  Throttle Time: {throttle}ms")
            
            print("  ✓ PASS: OffsetFetch response valid")
            return True
            
        except Exception as e:
            print(f"  ✗ FAIL: {e}")
            import traceback
            traceback.print_exc()
            return False

    def test_find_coordinator(self, version=4):
        """Test FindCoordinator request"""
        print(f"\n=== Test: FindCoordinator (v{version}) ===")
        
        try:
            is_flexible = version >= 3
            body = b''
            
            if version <= 3:
                # v0-3: single key format
                if is_flexible:
                    body += encode_compact_string('test-group')
                else:
                    body += encode_string('test-group')
                    
                # key_type (v1+): 0 = GROUP, 1 = TRANSACTION
                if version >= 1:
                    body += struct.pack('>b', 0)
            else:
                # v4+: still need key for compatibility, plus coordinator_keys array
                body += encode_compact_string('test-group')  # key
                body += struct.pack('>b', 0)  # key_type
                body += encode_uvarint(2)  # array len + 1 = 1 key
                body += encode_compact_string('test-group')
                body += b'\x00'  # key tagged_fields
                
            # Tagged fields
            if is_flexible:
                body += b'\x00'
                
            # Build header
            if is_flexible:
                header = struct.pack('>hhih', 
                    10, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client' + b'\x00'
            else:
                header = struct.pack('>hhih', 
                    10, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client'
                
            message = header + body
            self.correlation_id += 1
            
            self.sock.sendall(struct.pack('>i', len(message)) + message)
            
            # Receive response
            size_data = self.sock.recv(4)
            size = struct.unpack('>i', size_data)[0]
            print(f"  Response size: {size}")
            response = b''
            while len(response) < size:
                chunk = self.sock.recv(size - len(response))
                if not chunk:
                    break
                response += chunk
                
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"  Correlation ID: {corr_id}")
            
            offset = 4
            if is_flexible:
                tag_count, offset = decode_uvarint(response, offset)
                print(f"  Header tagged_fields count: {tag_count}")
                
            # v1+: throttle_time_ms
            if version >= 1:
                throttle = struct.unpack('>i', response[offset:offset+4])[0]
                print(f"  Throttle Time: {throttle}ms")
                offset += 4
            
            if version <= 3:
                error_code = struct.unpack('>h', response[offset:offset+2])[0]
                print(f"  Error Code: {error_code}")
            else:
                # v4+: coordinators array
                if is_flexible:
                    arr_len, offset = decode_uvarint(response, offset)
                    arr_len -= 1
                else:
                    arr_len = struct.unpack('>i', response[offset:offset+4])[0]
                    offset += 4
                print(f"  Coordinators: {arr_len}")
                
            print("  ✓ PASS: FindCoordinator response valid")
            return True
            
        except Exception as e:
            print(f"  ✗ FAIL: {e}")
            import traceback
            traceback.print_exc()
            return False

    def test_join_group(self, version=7):
        """Test JoinGroup request"""
        print(f"\n=== Test: JoinGroup (v{version}) ===")
        
        try:
            is_flexible = version >= 6
            body = b''
            
            # group_id
            if is_flexible:
                body += encode_compact_string('test-group')
            else:
                body += encode_string('test-group')
                
            # session_timeout_ms
            body += struct.pack('>i', 30000)
            
            # rebalance_timeout_ms (v1+)
            if version >= 1:
                body += struct.pack('>i', 60000)
                
            # member_id
            if is_flexible:
                body += encode_compact_string('')
            else:
                body += encode_string('')
                
            # group_instance_id (v5+)
            if version >= 5:
                if is_flexible:
                    body += b'\x00'  # null
                else:
                    body += struct.pack('>h', -1)
                    
            # protocol_type
            if is_flexible:
                body += encode_compact_string('consumer')
            else:
                body += encode_string('consumer')
                
            # protocols array
            if is_flexible:
                body += encode_uvarint(2)  # 1 protocol
                body += encode_compact_string('range')
                body += encode_uvarint(1)  # metadata bytes (empty)
                body += b'\x00'  # protocol tagged_fields
            else:
                body += struct.pack('>i', 1)  # 1 protocol
                body += encode_string('range')
                body += struct.pack('>i', 0)  # empty metadata
                
            # reason (v8+)
            if version >= 8:
                body += encode_compact_string('')
                
            # Tagged fields
            if is_flexible:
                body += b'\x00'
                
            # Build header
            if is_flexible:
                header = struct.pack('>hhih', 
                    11, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client' + b'\x00'
            else:
                header = struct.pack('>hhih', 
                    11, version, self.correlation_id + 1, len(b'test-client')
                ) + b'test-client'
                
            message = header + body
            self.correlation_id += 1
            
            self.sock.sendall(struct.pack('>i', len(message)) + message)
            
            # Receive response
            size_data = self.sock.recv(4)
            size = struct.unpack('>i', size_data)[0]
            response = b''
            while len(response) < size:
                chunk = self.sock.recv(size - len(response))
                if not chunk:
                    break
                response += chunk
                
            corr_id = struct.unpack('>i', response[0:4])[0]
            print(f"  Correlation ID: {corr_id}")
            
            offset = 4
            if is_flexible:
                _, offset = decode_uvarint(response, offset)
                
            throttle = struct.unpack('>i', response[offset:offset+4])[0]
            print(f"  Throttle Time: {throttle}ms")
            offset += 4
            
            error_code = struct.unpack('>h', response[offset:offset+2])[0]
            print(f"  Error Code: {error_code}")
            
            print("  ✓ PASS: JoinGroup response valid")
            return True
            
        except Exception as e:
            print(f"  ✗ FAIL: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    print("=" * 60)
    print("DataCore Kafka Protocol Socket Tests")
    print("=" * 60)
    
    client = KafkaTestClient()
    
    try:
        client.connect()
        
        passed = 0
        total = 0
        
        # Test ApiVersions
        total += 1
        if client.test_api_versions():
            passed += 1
            
        # Test Metadata v9 (flexible)
        total += 1
        if client.test_metadata(9):
            passed += 1
            
        # Test Fetch v16 (most recent, tests v15+ replica_id removal)
        total += 1
        if client.test_fetch(16):
            passed += 1
            
        # Test Fetch v12 (flexible, but still has replica_id)
        total += 1
        if client.test_fetch(12):
            passed += 1
            
        # Test ListOffsets v7
        total += 1
        if client.test_list_offsets(7):
            passed += 1
            
        # Test OffsetCommit v8 (flexible)
        total += 1
        if client.test_offset_commit(8):
            passed += 1
            
        # Test OffsetFetch v8 (flexible, multi-group)
        total += 1
        if client.test_offset_fetch(8):
            passed += 1
            
        # Test FindCoordinator v4 (flexible)
        total += 1
        if client.test_find_coordinator(4):
            passed += 1
            
        # Test JoinGroup v7 (flexible)
        total += 1
        if client.test_join_group(7):
            passed += 1
            
        print("\n" + "=" * 60)
        print(f"Results: {passed}/{total} tests passed")
        print("=" * 60)
        
        return 0 if passed == total else 1
        
    except Exception as e:
        print(f"Connection failed: {e}")
        return 1
    finally:
        client.close()


if __name__ == '__main__':
    sys.exit(main())
