## ADDED Requirements

### Requirement: Configurable overload isolation entry level
The proxy SHALL accept CODEX_LB_PROXY_OVERLOAD_ISOLATION_TRIP_LEVEL as an integer
from1 through5 with default3. Isolation SHALL begin at that qualifying trip
level and retain the configured duration. Zero duration SHALL disable isolation
regardless of entry level. Hard continuity and alternative-capacity rules SHALL
remain unchanged.

#### Scenario: Second-trip isolation
- **GIVEN** entry level2 and duration900 seconds
- **WHEN** an account reaches its second qualifying overload trip
- **THEN** it enters900-second isolation, while its first trip remains soft backoff

#### Scenario: Invalid or omitted entry level
- **GIVEN** the setting is omitted or outside1..5
- **WHEN** settings are loaded
- **THEN** omission uses3 and out-of-range values are rejected
