# Changelog

## [0.18.0] - 2026-01-21

### Added
- Complete Transaction API support for Exactly-Once Semantics (EOS)
- `AddOffsetsToTxn` (API Key 25) for adding consumer group offsets to transactions
- `TxnOffsetCommit` (API Key 28) for transactional offset commits
- Transaction validation in Produce handler to ensure transactional integrity
- `__consumer_offsets` partition tracking in transaction metadata

### Fixed
- Transaction validation in `TxnOffsetCommit` handler (validates producer ID, epoch, and state)
- `add_offsets_to_txn` now properly tracks `__consumer_offsets` partitions
- Produce handler now enforces strict transaction state validation (only `.ongoing` allowed)
- Produce handler now verifies partitions are added to transaction via `AddPartitionsToTxn`

### Changed
- Strengthened transactional produce validation to prevent invalid operations
- Improved error handling with specific error codes for transaction failures

## [0.17.0] - 2026-01-20

### Added
- Transaction Coordinator support for Exactly-Once Semantics (EOS).
- `InitProducerId` (API Key 22) enhancement for transactional producers.
- `AddPartitionsToTxn` (API Key 24) and `EndTxn` (API Key 26) APIs.
- In-memory transaction store and coordinator logic.

## [0.16.0] - 2026-01-20

### Added
- ACL Authorization support.
- `DescribeAcls` (API Key 29), `CreateAcls` (API Key 30), `DeleteAcls` (API Key 31) APIs.
- In-memory ACL manager for permission control.

## [0.15.0] - 2026-01-20

### Added
- SASL PLAIN authentication mechanism support.
- `SaslHandshake` (API Key 17) and `SaslAuthenticate` (API Key 36) APIs.
- In-memory user store for managing credentials.

## [0.14.0] - 2026-01-20

### Added
- S3 Range Request support in `S3StorageAdapter` for optimized segment reading.

### Changed
- Updated `fetch` operation to use HTTP Range headers when reading from the beginning of a log segment, reducing bandwidth usage and latency for small fetches.
