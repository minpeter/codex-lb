# B state observer

## ADDED Requirements

### Requirement: State transitions are durable and bounded
The read-only B observer MUST persist sanitized state and emit only transitions.

#### Scenario: stable spike is suppressed
- **WHEN** two complete observations have the same spike state
- **THEN** the second observation emits no traffic event and preserves the incident state

#### Scenario: zero traffic is honest
- **WHEN** a five-minute observation has zero requests
- **THEN** success and error ratios are null and no healthy inference is emitted
