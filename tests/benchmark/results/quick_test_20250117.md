# DataCore Quick Performance Test Results

## Test Date: 2025-01-17

### Environment
- **OS**: macOS Darwin
- **Broker**: DataCore v0.1.0 (localhost:9092)
- **Client**: Confluent Kafka CLI 8.1.1

### Test Configuration
- **Topic**: perf-test-quick
- **Partitions**: 1
- **Replication Factor**: 1
- **Record Size**: 1024 bytes
- **Number of Records**: 10,000

---

## Producer Performance

| Metric | Value |
|--------|-------|
| Records Sent | 10,000 |
| Throughput | 35,211.3 records/sec |
| Bandwidth | 34.39 MB/sec |
| Avg Latency | 36.95 ms |
| Max Latency | 187.00 ms |
| P50 Latency | 37 ms |
| P95 Latency | 53 ms |
| P99 Latency | 54 ms |
| P99.9 Latency | 54 ms |

## Consumer Performance

| Metric | Value |
|--------|-------|
| Data Consumed | 9.77 MB |
| Throughput | 17.53 MB/sec |
| Messages/sec | 17,953.3 msg/sec |
| Rebalance Time | 189 ms |
| Fetch Time | 368 ms |
| Fetch Rate | 26.54 MB/sec |
| Fetch Messages | 27,173.9 msg/sec |

---

## Summary

✅ **Test Status**: PASSED

The DataCore broker successfully handled Kafka protocol messages with good performance:
- Producer throughput exceeds 35K msg/sec
- Consumer fetch rate exceeds 27K msg/sec
- Latency percentiles are consistent (P50-P99.9 within 17ms range)
