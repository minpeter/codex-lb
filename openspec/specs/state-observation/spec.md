# State observation Specification

## Purpose
Provide read-only B operational snapshots and durable state transitions.

## Requirements
### Requirement: Explicit bounded read-only target
The observer MUST require an explicit container and HTTP(S) base URL without
credentials, query strings, or fragments. It MUST read the fixed SQLite database
with mode=ro, query_only, one transaction and a bounded query budget, and MUST
bound Docker logs to five minutes and 2000 lines and health reads to five seconds.

#### Scenario: Operator observes B
- **WHEN** the CLI runs with an explicit B target
- **THEN** it reads request and quota aggregates, bounded logs and readiness without provider calls or database writes

### Requirement: Honest traffic and capacity analysis
The observer MUST distinguish DB rows from unique stored request IDs, using the
last observed row per ID in the half-open five-minute window for outcome ratios.
It MUST return null ratios for no traffic. At least 20 requests MUST be required
to enter a spike at error ratio >=0.20 or recover at <=0.05. Empty or smaller
samples MUST NOT clear an incident. Capacity MUST be unknown unless all stored
active non-deleting accounts have fresh primary quota with positive duration,
future reset, and age <=900 seconds. Low capacity MUST mean remaining
percentage-point weight <=20, not runtime eligibility. Concentration MUST refer
to attributed DB rows and be high at top-account share >=0.80.

#### Scenario: Healthy, spike, duplicate, recovered
- **WHEN** four snapshots contain 20 requests with 20, 16, 16 and 19 successes
- **THEN** traffic states are healthy, spike, spike and recovered
- **AND** the third snapshot emits no duplicate traffic event

#### Scenario: Empty traffic and missing quota
- **WHEN** no requests and no fresh quota are observed
- **THEN** traffic is noTraffic, ratios are null and stored capacity is unknown

### Requirement: Sanitized durable transition history
The observer MUST persist every snapshot and resulting state in locked fsynced
0600 JSONL before emitting events. Restart MUST recover dedupe state, reject
cross-target/corrupt/non-monotonic history, and suppress an identical replay.
Events MUST use machine codes for traffic, quota, capacity, concentration,
no_accounts presence, novel exception classes, novel logged isolation changes,
external health and log-cap coverage transitions. Raw account/request identifiers,
headers, credentials, exception messages and health response bodies MUST NOT be
persisted. Isolation and exception signatures MUST be SHA-256 hashes.

#### Scenario: Restart during stable incident
- **WHEN** another CLI invocation appends an unchanged incident state
- **THEN** it records a snapshot without duplicate transition events

#### Scenario: Malformed readiness response
- **WHEN** readiness returns malformed or structurally invalid JSON
- **THEN** external health is down rather than causing an unhandled shape error
