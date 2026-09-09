## ADDED Requirements

### Requirement: Request ownership survives timeout and recovery boundaries
An expired sent WebSocket request without a response ID SHALL NOT allow late
frames to acquire a newer request's identity. Bridge reuse SHALL respect the
request's excluded accounts without disrupting unrelated owners. A drain
strategy SHALL fall back to an eligible request-scoped candidate when its
preferred budget-safe subset cannot serve.

#### Scenario: Expiry before response creation
- **WHEN** request A expires before its created event and a newer request waits
- **THEN** A's late lifecycle cannot finalize or settle the newer request

### Requirement: Retry and source lifecycle ownership is bounded
Terminal retries SHALL settle keyed reservations before applying one health
penalty for the failed attempt. Same-account retry and backoff SHALL honor the
absolute deadline. External source streams SHALL release eager leases even
before their first read; cancelled setup SHALL release its reservation.

#### Scenario: Initial heartbeat closes before source body starts
- **WHEN** the downstream closes after a heartbeat but before reading source data
- **THEN** the already acquired upstream lease is released exactly once
