## ADDED Requirements

### Requirement: Keepalive finalization observes owned read tasks
The SSE keepalive wrapper SHALL retrieve the outcome of every owned pending
read task before closing its source, including a task that completed while
the wrapper was suspended at a heartbeat. Normal consumption SHALL continue
to propagate upstream failures, and unfinished reads SHALL retain existing
cancellation-deferring cleanup.

#### Scenario: EOF completes during a suspended heartbeat
- **WHEN** a read finishes with EOF before the downstream closes the wrapper
- **THEN** cleanup observes that read without an asyncio unhandled-task error

#### Scenario: Upstream exception completes during a suspended heartbeat
- **WHEN** a read fails after a heartbeat and the downstream closes the wrapper
- **THEN** cleanup observes the failure and closes the source
