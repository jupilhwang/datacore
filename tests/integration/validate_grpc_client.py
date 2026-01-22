#!/usr/bin/env python3
"""
Quick validation test for gRPC client library.
Tests basic encoding/decoding without requiring a running broker.
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

from grpc_client import GrpcRecord


def test_record_encoding():
    """Test GrpcRecord encoding/decoding"""
    print("Testing GrpcRecord encoding/decoding...")

    # Create test record
    original = GrpcRecord(
        key=b"test-key",
        value=b"test-value",
        headers={"header1": b"value1", "header2": b"value2"},
        timestamp=1234567890,
    )

    # Encode
    encoded = original.encode()
    print(f"  Encoded size: {len(encoded)} bytes")

    # Decode
    decoded, bytes_consumed = GrpcRecord.decode(encoded)
    print(f"  Decoded {bytes_consumed} bytes")

    # Verify
    assert decoded.key == original.key, "Key mismatch"
    assert decoded.value == original.value, "Value mismatch"
    assert decoded.timestamp == original.timestamp, "Timestamp mismatch"
    assert decoded.headers == original.headers, "Headers mismatch"

    print("  ✓ Encoding/decoding works correctly")
    return True


def test_record_without_key():
    """Test record without key"""
    print("Testing record without key...")

    record = GrpcRecord(value=b"value-only")
    encoded = record.encode()
    decoded, _ = GrpcRecord.decode(encoded)

    assert decoded.key == b"", "Key should be empty"
    assert decoded.value == b"value-only", "Value mismatch"

    print("  ✓ Record without key works")
    return True


def test_record_with_empty_headers():
    """Test record with no headers"""
    print("Testing record with empty headers...")

    record = GrpcRecord(key=b"key", value=b"value", headers={})
    encoded = record.encode()
    decoded, _ = GrpcRecord.decode(encoded)

    assert len(decoded.headers) == 0, "Headers should be empty"

    print("  ✓ Record with empty headers works")
    return True


def main():
    print("=" * 50)
    print("  gRPC Client Library Validation")
    print("=" * 50)
    print()

    tests = [
        test_record_encoding,
        test_record_without_key,
        test_record_with_empty_headers,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  ✗ Test failed: {e}")
            import traceback

            traceback.print_exc()
            failed += 1
        print()

    print("=" * 50)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 50)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
