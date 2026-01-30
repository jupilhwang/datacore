#!/usr/bin/env python3
"""Test FindCoordinator v4 request with proper v2 header format."""

import socket
import struct

def build_find_coordinator_v4_request():
    """Build a FindCoordinator v4 request with RequestHeader v2 (flexible format)."""
    # RequestHeader v2:
    # - api_key: i16
    # - api_version: i16
    # - correlation_id: i32
    # - client_id: NULLABLE_STRING (2-byte length prefix, NOT compact!)
    # - tagged_fields: varint count + fields
    #
    # FindCoordinatorRequest v4:
    # - key: COMPACT_STRING
    # - key_type: i8
    # (no trailing tagged fields in body)
    
    req_parts = []
    
    # Header
    req_parts.append(struct.pack('>h', 10))        # api_key = 10 (FindCoordinator)
    req_parts.append(struct.pack('>h', 4))         # api_version = 4
    req_parts.append(struct.pack('>i', 1))         # correlation_id = 1
    
    # client_id as nullable string (2-byte length, NOT compact!)
    # For "test-client" (11 bytes), we encode as length 11, then bytes
    client_id = b"test-client"
    req_parts.append(struct.pack('>h', len(client_id)))  # 2-byte length
    req_parts.append(client_id)
    
    # tagged_fields (0 fields = varint 0)
    req_parts.append(bytes([0]))
    
    # Body
    # key as compact string: "test-group" (10 bytes) -> varint 11
    key = b"test-group"
    req_parts.append(bytes([len(key) + 1]))        # varint: 11
    req_parts.append(key)
    
    # key_type: 0 (GROUP)
    req_parts.append(struct.pack('>b', 0))
    
    # For flexible versions, add trailing tagged_fields (varint 0)
    req_parts.append(bytes([0]))  # varint: 0 (no tagged fields)
    
    body = b''.join(req_parts)
    
    # Size prefix (4 bytes, big-endian)
    size = struct.pack('>i', len(body))
    
    return size + body

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    
    try:
        sock.connect(('localhost', 9092))
        print("[Connection] Connected to broker")
        
        # Build and send request
        request = build_find_coordinator_v4_request()
        print(f"[Request] Sending FindCoordinator v4 with proper RequestHeader v2")
        print(f"[Request] Total size: {len(request)} bytes (4 byte size + {len(request)-4} byte body)")
        print(f"[Request] Hex: {request.hex()}")
        
        sock.sendall(request)
        
        # Read response
        response_size_bytes = sock.recv(4)
        if len(response_size_bytes) < 4:
            print(f"[Error] Expected 4 bytes for size, got {len(response_size_bytes)}")
            return False
        
        response_size = struct.unpack('>i', response_size_bytes)[0]
        print(f"\n[Response] Size: {response_size} bytes")
        
        # Read response body
        response_body = b''
        remaining = response_size
        while remaining > 0:
            chunk = sock.recv(min(remaining, 4096))
            if not chunk:
                print(f"[Error] Connection closed before receiving full response")
                return False
            response_body += chunk
            remaining -= len(chunk)
        
        print(f"[Response] Received: {len(response_body)} bytes")
        print(f"[Response] Hex: {response_body.hex()}")
        
        # Parse response header
        if len(response_body) >= 4:
            corr_id = struct.unpack('>i', response_body[0:4])[0]
            print(f"[Response] Correlation ID: {corr_id}")
        
        if len(response_body) >= 5:
            # Next byte should be tagged fields count (0 for empty)
            tagged_count = response_body[4]
            print(f"[Response] Tagged fields count: {tagged_count}")
        
        print("\n✓ Successfully received FindCoordinator v4 response without BufferUnderflow!")
        return True
        
    except socket.timeout:
        print("[Error] Socket timeout - broker not responding")
        return False
    except Exception as e:
        print(f"[Error] {e}")
        return False
    finally:
        sock.close()

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
