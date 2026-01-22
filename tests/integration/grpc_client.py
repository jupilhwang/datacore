#!/usr/bin/env python3
"""
DataCore gRPC Client Library

Python client for DataCore's custom gRPC streaming protocol.
Implements binary protocol encoding/decoding for produce, consume, commit operations.
"""

import socket
import struct
import time
from typing import List, Dict, Optional, Tuple
from enum import IntEnum


class GrpcStreamRequestType(IntEnum):
    """gRPC stream request types"""

    PRODUCE = 0
    SUBSCRIBE = 1
    COMMIT = 2
    ACK = 3
    PING = 4


class GrpcStreamResponseType(IntEnum):
    """gRPC stream response types"""

    PRODUCE_ACK = 0
    MESSAGE = 1
    COMMIT_ACK = 2
    ERROR = 3
    PONG = 4


class GrpcError(Exception):
    """gRPC protocol error"""

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(f"gRPC Error {code}: {message}")


class GrpcRecord:
    """gRPC record structure"""

    def __init__(
        self,
        key: bytes = b"",
        value: bytes = b"",
        headers: Dict[str, bytes] = None,
        timestamp: int = 0,
    ):
        self.key = key
        self.value = value
        self.headers = headers or {}
        self.timestamp = timestamp or int(time.time() * 1000)

    def encode(self) -> bytes:
        """Encode record to binary format"""
        buf = bytearray()

        # Key length (4 bytes) + key
        buf.extend(struct.pack(">I", len(self.key)))
        buf.extend(self.key)

        # Value length (4 bytes) + value
        buf.extend(struct.pack(">I", len(self.value)))
        buf.extend(self.value)

        # Timestamp (8 bytes)
        buf.extend(struct.pack(">Q", self.timestamp))

        # Header count (4 bytes)
        buf.extend(struct.pack(">I", len(self.headers)))

        # Headers
        for k, v in self.headers.items():
            k_bytes = k.encode("utf-8")
            buf.extend(struct.pack(">H", len(k_bytes)))
            buf.extend(k_bytes)
            buf.extend(struct.pack(">H", len(v)))
            buf.extend(v)

        return bytes(buf)

    @staticmethod
    def decode(data: bytes) -> Tuple["GrpcRecord", int]:
        """Decode record from binary format, returns (record, bytes_consumed)"""
        pos = 0

        # Key length + key
        key_len = struct.unpack(">I", data[pos : pos + 4])[0]
        pos += 4
        key = data[pos : pos + key_len]
        pos += key_len

        # Value length + value
        val_len = struct.unpack(">I", data[pos : pos + 4])[0]
        pos += 4
        value = data[pos : pos + val_len]
        pos += val_len

        # Timestamp
        timestamp = struct.unpack(">Q", data[pos : pos + 8])[0]
        pos += 8

        # Header count
        header_count = struct.unpack(">I", data[pos : pos + 4])[0]
        pos += 4

        # Headers
        headers = {}
        for _ in range(header_count):
            k_len = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2
            k = data[pos : pos + k_len].decode("utf-8")
            pos += k_len

            v_len = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2
            v = data[pos : pos + v_len]
            pos += v_len

            headers[k] = v

        return GrpcRecord(key, value, headers, timestamp), pos


class GrpcClient:
    """Base gRPC client with connection management"""

    def __init__(self, host: str = "localhost", port: int = 9093):
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.connected = False

    def connect(self):
        """Establish connection to gRPC server"""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.connected = True

    def close(self):
        """Close connection"""
        if self.sock:
            self.sock.close()
            self.sock = None
        self.connected = False

    def send_frame(self, data: bytes, compressed: bool = False):
        """Send gRPC frame (5-byte header + payload)"""
        if not self.connected:
            raise RuntimeError("Not connected")

        # Frame header: compressed (1 byte) + length (4 bytes)
        header = struct.pack(">BI", 1 if compressed else 0, len(data))
        self.sock.sendall(header + data)

    def recv_frame(self, timeout: float = 10.0) -> bytes:
        """Receive gRPC frame"""
        if not self.connected:
            raise RuntimeError("Not connected")

        self.sock.settimeout(timeout)

        # Read 5-byte header
        header = self._recv_exact(5)
        compressed = header[0] == 1
        length = struct.unpack(">I", header[1:5])[0]

        # Read payload
        if length > 0:
            data = self._recv_exact(length)
        else:
            data = b""

        return data

    def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes"""
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Connection closed")
            buf.extend(chunk)
        return bytes(buf)

    def ping(self) -> int:
        """Send ping and receive pong, returns server timestamp"""
        # Encode ping request
        data = struct.pack(">B", GrpcStreamRequestType.PING)
        self.send_frame(data)

        # Receive pong response
        response = self.recv_frame()
        resp_type = response[0]

        if resp_type == GrpcStreamResponseType.PONG:
            timestamp = struct.unpack(">Q", response[1:9])[0]
            return timestamp
        elif resp_type == GrpcStreamResponseType.ERROR:
            code = struct.unpack(">i", response[1:5])[0]
            msg_len = struct.unpack(">H", response[5:7])[0]
            message = response[7 : 7 + msg_len].decode("utf-8")
            raise GrpcError(code, message)
        else:
            raise GrpcError(-1, f"Unexpected response type: {resp_type}")


class GrpcProducer(GrpcClient):
    """gRPC producer for sending messages"""

    def produce(
        self, topic: str, records: List[GrpcRecord], partition: Optional[int] = None
    ) -> Tuple[int, int]:
        """
        Produce records to topic.
        Returns (base_offset, record_count)
        """
        if not self.connected:
            self.connect()

        # Encode produce request
        buf = bytearray()
        buf.append(GrpcStreamRequestType.PRODUCE)

        # Topic length + topic
        topic_bytes = topic.encode("utf-8")
        buf.extend(struct.pack(">H", len(topic_bytes)))
        buf.extend(topic_bytes)

        # Partition (4 bytes, -1 for auto)
        buf.extend(struct.pack(">i", partition if partition is not None else -1))

        # Record count
        buf.extend(struct.pack(">I", len(records)))

        # Records
        for record in records:
            buf.extend(record.encode())

        # Send request
        self.send_frame(bytes(buf))

        # Receive response
        response = self.recv_frame()
        return self._parse_produce_response(response)

    def _parse_produce_response(self, data: bytes) -> Tuple[int, int]:
        """Parse produce response"""
        pos = 0
        resp_type = data[pos]
        pos += 1

        if resp_type == GrpcStreamResponseType.PRODUCE_ACK:
            # Topic length + topic
            topic_len = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2 + topic_len

            # Partition
            partition = struct.unpack(">i", data[pos : pos + 4])[0]
            pos += 4

            # Base offset
            base_offset = struct.unpack(">q", data[pos : pos + 8])[0]
            pos += 8

            # Record count
            record_count = struct.unpack(">I", data[pos : pos + 4])[0]
            pos += 4

            # Timestamp
            timestamp = struct.unpack(">Q", data[pos : pos + 8])[0]
            pos += 8

            # Error code
            error_code = struct.unpack(">i", data[pos : pos + 4])[0]
            pos += 4

            # Error message
            msg_len = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2
            error_msg = data[pos : pos + msg_len].decode("utf-8") if msg_len > 0 else ""

            if error_code != 0:
                raise GrpcError(error_code, error_msg)

            return base_offset, record_count

        elif resp_type == GrpcStreamResponseType.ERROR:
            code = struct.unpack(">i", data[pos : pos + 4])[0]
            pos += 4
            msg_len = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2
            message = data[pos : pos + msg_len].decode("utf-8")
            raise GrpcError(code, message)

        else:
            raise GrpcError(-1, f"Unexpected response type: {resp_type}")


class GrpcConsumer(GrpcClient):
    """gRPC consumer for receiving messages via streaming"""

    def __init__(
        self, host: str = "localhost", port: int = 9093, group_id: Optional[str] = None
    ):
        super().__init__(host, port)
        self.group_id = group_id
        self.subscriptions: List[
            Tuple[str, int, int]
        ] = []  # (topic, partition, offset)

    def subscribe(
        self,
        topic: str,
        partition: int = 0,
        offset: int = 0,
        max_records: int = 100,
        max_bytes: int = 1048576,
    ):
        """Subscribe to topic/partition for streaming"""
        if not self.connected:
            self.connect()

        # Encode subscribe request
        buf = bytearray()
        buf.append(GrpcStreamRequestType.SUBSCRIBE)

        # Topic length + topic
        topic_bytes = topic.encode("utf-8")
        buf.extend(struct.pack(">H", len(topic_bytes)))
        buf.extend(topic_bytes)

        # Partition
        buf.extend(struct.pack(">i", partition))

        # Offset
        buf.extend(struct.pack(">q", offset))

        # Max records
        buf.extend(struct.pack(">I", max_records))

        # Max bytes
        buf.extend(struct.pack(">I", max_bytes))

        # Send request
        self.send_frame(bytes(buf))

        # Receive initial response
        response = self.recv_frame()
        self._parse_subscribe_response(response)

        self.subscriptions.append((topic, partition, offset))

    def _parse_subscribe_response(self, data: bytes):
        """Parse subscribe response (initial ack)"""
        pos = 0
        resp_type = data[pos]
        pos += 1

        if resp_type == GrpcStreamResponseType.MESSAGE:
            # Initial response with starting offset
            return
        elif resp_type == GrpcStreamResponseType.ERROR:
            code = struct.unpack(">i", data[pos : pos + 4])[0]
            pos += 4
            msg_len = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2
            message = data[pos : pos + msg_len].decode("utf-8")
            raise GrpcError(code, message)

    def poll(self, timeout: float = 1.0) -> Optional[Tuple[str, int, int, GrpcRecord]]:
        """
        Poll for next message.
        Returns (topic, partition, offset, record) or None if timeout
        """
        if not self.connected:
            raise RuntimeError("Not connected. Call subscribe() first.")

        try:
            response = self.recv_frame(timeout)
            return self._parse_message_response(response)
        except socket.timeout:
            return None

    def _parse_message_response(
        self, data: bytes
    ) -> Optional[Tuple[str, int, int, GrpcRecord]]:
        """Parse message response"""
        pos = 0
        resp_type = data[pos]
        pos += 1

        if resp_type == GrpcStreamResponseType.MESSAGE:
            # Topic length + topic
            topic_len = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2
            topic = data[pos : pos + topic_len].decode("utf-8")
            pos += topic_len

            # Partition
            partition = struct.unpack(">i", data[pos : pos + 4])[0]
            pos += 4

            # Offset
            offset = struct.unpack(">q", data[pos : pos + 8])[0]
            pos += 8

            # Timestamp
            timestamp = struct.unpack(">Q", data[pos : pos + 8])[0]
            pos += 8

            # Key length + key
            key_len = struct.unpack(">I", data[pos : pos + 4])[0]
            pos += 4
            key = data[pos : pos + key_len]
            pos += key_len

            # Value length + value
            val_len = struct.unpack(">I", data[pos : pos + 4])[0]
            pos += 4
            value = data[pos : pos + val_len]
            pos += val_len

            # Header count
            header_count = struct.unpack(">I", data[pos : pos + 4])[0]
            pos += 4

            # Headers
            headers = {}
            for _ in range(header_count):
                k_len = struct.unpack(">H", data[pos : pos + 2])[0]
                pos += 2
                k = data[pos : pos + k_len].decode("utf-8")
                pos += k_len

                v_len = struct.unpack(">H", data[pos : pos + 2])[0]
                pos += 2
                v = data[pos : pos + v_len]
                pos += v_len

                headers[k] = v

            record = GrpcRecord(key, value, headers, timestamp)
            return (topic, partition, offset, record)

        elif resp_type == GrpcStreamResponseType.PONG:
            # Keepalive pong, ignore
            return None

        elif resp_type == GrpcStreamResponseType.ERROR:
            code = struct.unpack(">i", data[pos : pos + 4])[0]
            pos += 4
            msg_len = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2
            message = data[pos : pos + msg_len].decode("utf-8")
            raise GrpcError(code, message)

        return None

    def commit(self, offsets: List[Tuple[str, int, int]]):
        """
        Commit offsets for consumer group.
        offsets: List of (topic, partition, offset)
        """
        if not self.group_id:
            raise ValueError("group_id required for commit")

        if not self.connected:
            raise RuntimeError("Not connected")

        # Encode commit request
        buf = bytearray()
        buf.append(GrpcStreamRequestType.COMMIT)

        # Group ID length + group_id
        group_bytes = self.group_id.encode("utf-8")
        buf.extend(struct.pack(">H", len(group_bytes)))
        buf.extend(group_bytes)

        # Offset count
        buf.extend(struct.pack(">I", len(offsets)))

        # Offsets
        for topic, partition, offset in offsets:
            topic_bytes = topic.encode("utf-8")
            buf.extend(struct.pack(">H", len(topic_bytes)))
            buf.extend(topic_bytes)
            buf.extend(struct.pack(">i", partition))
            buf.extend(struct.pack(">q", offset))

        # Send request
        self.send_frame(bytes(buf))

        # Receive response
        response = self.recv_frame()
        self._parse_commit_response(response)

    def _parse_commit_response(self, data: bytes):
        """Parse commit response"""
        pos = 0
        resp_type = data[pos]
        pos += 1

        if resp_type == GrpcStreamResponseType.COMMIT_ACK:
            success = data[pos] == 1
            pos += 1
            msg_len = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2
            message = data[pos : pos + msg_len].decode("utf-8")

            if not success:
                raise GrpcError(-1, f"Commit failed: {message}")

        elif resp_type == GrpcStreamResponseType.ERROR:
            code = struct.unpack(">i", data[pos : pos + 4])[0]
            pos += 4
            msg_len = struct.unpack(">H", data[pos : pos + 2])[0]
            pos += 2
            message = data[pos : pos + msg_len].decode("utf-8")
            raise GrpcError(code, message)
