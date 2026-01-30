#!/usr/bin/env python3
"""Capture and log the raw bytes of Metadata v12 request."""

import socket
import struct
import time

def log_metadata_v12_request():
    """Listen for Metadata request and log its bytes."""
    # This is a simple TCP server that logs what clients send
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('127.0.0.1', 19092))
    server.listen(1)
    
    print("[Server] Listening on 127.0.0.1:19092...")
    
    try:
        conn, addr = server.accept()
        print(f"[Connection] Accepted from {addr}")
        
        # Read first request (should be metadata)
        data = conn.recv(4096)
        
        if len(data) >= 8:
            # Parse size
            size = struct.unpack('>i', data[0:4])[0]
            print(f"[Request] Size prefix: {size}")
            print(f"[Request] Total data length: {len(data)}")
            print(f"[Request] Body length: {len(data) - 4}")
            print(f"[Request] Hex: {data.hex()}")
            print(f"[Request] Body hex: {data[4:].hex()}")
            
            # Parse header  
            pos = 4
            api_key = struct.unpack('>h', data[pos:pos+2])[0]
            pos += 2
            api_version = struct.unpack('>h', data[pos:pos+2])[0]
            pos += 2
            corr_id = struct.unpack('>i', data[pos:pos+4])[0]
            pos += 4
            
            print(f"\n[Header] api_key={api_key}, version={api_version}, correlation_id={corr_id}")
            
            # For flexible version, client_id should be compact nullable string
            # Read varint length
            byte = data[pos]
            pos += 1
            if byte & 0x80 == 0:
                length = byte
            else:
                print(f"[Warning] Multi-byte varint, first byte: 0x{byte:02x}")
                length = byte & 0x7f
            
            print(f"[Header] client_id length varint: 0x{byte:02x} ({length})")
            
            if length > 0:
                actual_len = length - 1
                client_id = data[pos:pos+actual_len].decode('utf-8', errors='replace')
                pos += actual_len
                print(f"[Header] client_id: '{client_id}'")
            
            # Read tagged fields (should be varint 0)
            tagged_byte = data[pos]
            pos += 1
            print(f"[Header] tagged_fields count: 0x{tagged_byte:02x}")
            
            print(f"\n[Body] Starting at position {pos}, remaining: {len(data) - pos} bytes")
            print(f"[Body] Hex from here: {data[pos:].hex()}")
            
            # Try to parse body manually
            # Should be: compact_array_len (topics), then for each topic...
            topic_count_byte = data[pos] if pos < len(data) else None
            if topic_count_byte is not None:
                print(f"[Body] First byte (likely topic count): 0x{topic_count_byte:02x} ({topic_count_byte})")
        
        conn.close()
    finally:
        server.close()

if __name__ == "__main__":
    log_metadata_v12_request()
