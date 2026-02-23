#!/usr/bin/env python3
"""
Test gRPC over HTTP/1.1
"""

import socket
import struct
import time


def send_http_grpc_request(host, port, path, data):
    """Send gRPC request over HTTP/1.1 POST"""
    # Create socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    # Encode gRPC frame (5-byte header + payload)
    frame = struct.pack(">BI", 0, len(data)) + data

    # Build HTTP request
    http_request = (
        f"POST {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Content-Type: application/octet-stream\r\n"
        f"Content-Length: {len(frame)}\r\n"
        f"Connection: keep-alive\r\n"
        f"\r\n"
    ).encode() + frame

    # Send request
    sock.sendall(http_request)

    # Read HTTP response headers
    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(1024)
        if not chunk:
            break
        response += chunk

    # Parse headers
    header_end = response.index(b"\r\n\r\n")
    headers = response[:header_end].decode()
    body_start = response[header_end + 4 :]

    print("HTTP Response Headers:")
    print(headers)
    print()

    # Read gRPC frame from body
    if len(body_start) >= 5:
        # Parse frame header
        compressed = body_start[0]
        length = struct.unpack(">I", body_start[1:5])[0]

        # Read remaining payload if needed
        payload = body_start[5:]
        while len(payload) < length:
            chunk = sock.recv(length - len(payload))
            if not chunk:
                break
            payload += chunk

        print(f"gRPC Frame: compressed={compressed}, length={length}")
        print(f"Payload: {payload[:100].hex()}")

        # Parse response
        if len(payload) > 0:
            resp_type = payload[0]
            print(f"Response Type: {resp_type}")

            if resp_type == 4:  # PONG
                timestamp = struct.unpack(">Q", payload[1:9])[0]
                print(f"✓ Pong received! Server timestamp: {timestamp}")
                return True

    sock.close()
    return False


def test_ping():
    """Test ping request"""
    print("=" * 60)
    print("Testing gRPC Ping over HTTP/1.1")
    print("=" * 60)

    # Encode ping request (type = 4)
    ping_data = struct.pack(">B", 4)

    # Send request
    success = send_http_grpc_request("localhost", 8080, "/v1/grpc", ping_data)

    if success:
        print("\n✓ Test passed!")
    else:
        print("\n✗ Test failed!")

    return success


if __name__ == "__main__":
    test_ping()
