# Changelog

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
