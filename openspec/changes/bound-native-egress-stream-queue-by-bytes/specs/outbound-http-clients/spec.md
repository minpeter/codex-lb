# outbound-http-clients Delta

## ADDED Requirements

### Requirement: Native helper stream events are bounded per request by a byte budget

HTTP and WebSocket helper event queues MUST enforce a 32 MiB encoded-payload byte budget and a strict 4096-event cap. Payload accounting MUST charge base64 `data` string length or UTF-8 `text` bytes and MUST release bytes on dequeue, including overflow and generation-failure drains. Control events and failure objects without payload MUST cost zero bytes. A positive-size incoming event MUST be rejected when the nonempty queue's projected total exceeds the byte budget. One oversized event MUST be accepted on an empty queue. Zero-byte terminal events MUST remain admissible at or above the byte boundary when event capacity remains. Overflow MUST fail and cancel only the owning request with `consumer_backpressure`, without blocking siblings. The separate WebSocket consumer message queue MUST retain its 64-message cap. Helper protocol version, capabilities and line-size bounds MUST remain unchanged.

#### Scenario: Small chunk burst completes

- **GIVEN** 2000 small base64 chunk events fit the byte budget
- **WHEN** the full burst queues before body consumption
- **THEN** the complete body and terminal event are delivered without backpressure failure

#### Scenario: Exact encoded budget permits completion

- **GIVEN** 16 chunks each containing 2 MiB of base64 data are queued
- **WHEN** a zero-byte end event follows
- **THEN** the 24 MiB decoded body completes and queued bytes return to zero

#### Scenario: Stalled consumer is isolated

- **GIVEN** a consumer does not read while its helper emits 48 chunks of 1 MiB decoded data
- **WHEN** queued encoded bytes would exceed 32 MiB
- **THEN** only that request fails with consumer_backpressure and is cancelled
- **AND** its queued bytes are released and a healthy sibling still completes on the same helper
