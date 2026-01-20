# DataCore Performance Benchmark Results

**Date:** Tue Jan 20 12:46:09 KST 2026
**Bootstrap Server:** localhost:9092
**Default Configuration:**
- Records: 100000
- Record Size: 1024 bytes
- Partitions: 3

## Test Results

### Producer Tests

| Test | Records/sec | MB/sec | Avg Latency (ms) | Max Latency (ms) |
|------|-------------|--------|------------------|------------------|
| baseline | 178571.4 | 174.39 | 82.81 | 184.00 |

### Consumer Tests

| Test | Records/sec | MB/sec | Fetch Time (ms) |
|------|-------------|--------|-----------------|

## Raw Results

All detailed results are available in the individual test files in this directory.

## Test Descriptions

1. **Baseline**: Single partition, default settings
2. **Record Sizes**: Various message sizes (100B - 10KB)
3. **Partition Scaling**: 1, 2, 4, 8 partitions
4. **Batching**: Different batch sizes and linger times
5. **ACK Modes**: acks=0 vs acks=1
6. **Sustained Throughput**: Rate-limited tests (10K, 50K msg/sec)
7. **Concurrent Consumers**: Multi-consumer testing

## Notes

- All tests use local broker (single node)
- Results may vary based on system resources
- For production benchmarks, run on dedicated hardware
