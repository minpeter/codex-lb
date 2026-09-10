## ADDED Requirements

### Requirement: Native reader dispatch yields to ready consumers

The native helper stdout reader SHALL provide a scheduler checkpoint after each successful dispatch to an owned request queue, even when buffered helper output makes the next read complete synchronously. The reader SHALL NOT wait for a stalled consumer to free queue capacity.

#### Scenario: Buffered chunks have a subscribed consumer
- **WHEN** two buffered helper chunks are dispatched to a queue with a ready consumer
- **THEN** the consumer can drain the first chunk before the second dispatch
- **AND** the existing event and payload-byte limits remain unchanged

#### Scenario: A stalled consumer exceeds its budget
- **WHEN** successful dispatches yield but a consumer still exceeds its bounded queue
- **THEN** only that request receives consumer_backpressure and its helper request is cancelled
- **AND** sibling request and generation ownership remain unchanged
