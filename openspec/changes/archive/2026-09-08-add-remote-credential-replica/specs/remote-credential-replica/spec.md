## ADDED Requirements

### Requirement: Dedicated remote credential authority
When remote credential mode is configured, the replica MUST obtain account credentials from the configured source dashboard API and MUST NOT exchange any refresh token with an OAuth provider. Native mode MUST preserve existing behavior.

#### Scenario: Forced refresh on the replica
- **GIVEN** a remotely managed account and an upstream authentication failure
- **WHEN** request recovery forces credential refresh
- **THEN** the replica fetches source credentials without attempting an OAuth refresh exchange
- **AND** source failure MUST NOT enable local refresh fallback

### Requirement: Source authentication and bounded synchronization
The replica MUST authenticate to the source as an authorized dashboard administrator, renew expired sessions through the configured authentication method, bound source requests and coalesce concurrent synchronization for the same account. Secrets MUST NOT appear in logs or public responses.

#### Scenario: Source session expires
- **GIVEN** a previously authenticated replica
- **WHEN** the source rejects the dashboard session
- **THEN** the replica may authenticate again once within the bounded synchronization attempt
- **AND** an authentication failure MUST be reported without exposing credentials

### Requirement: Source-owned account state
Successful synchronization MUST preserve source account identity and apply source pause, disable and removal state. Credential rotation MUST update the existing replica account rather than create duplicate accounts or override unrelated local request accounting.

#### Scenario: Source account is paused or removed
- **GIVEN** an account previously synchronized into the replica
- **WHEN** a successful source snapshot reports it paused or absent
- **THEN** the account is no longer eligible for new replica requests

### Requirement: Cached credentials survive source interruption
A failed or malformed source response MUST NOT be treated as an empty account pool and MUST NOT permanently revoke otherwise healthy accounts. The replica MAY continue using a cached access token while it remains usable. Unusable credentials without a usable replacement MUST fail within the request budget without OAuth refresh fallback.

#### Scenario: Source unavailable while cached token works
- **GIVEN** a valid cached access token
- **WHEN** the source cannot be reached during synchronization
- **THEN** existing cached account state is preserved
- **AND** normal inference can continue directly to the configured upstream

### Requirement: Lifecycle and cache consistency
Synchronization MUST own and release its HTTP client and tasks through application startup/shutdown. Committed credential or account-state changes MUST invalidate replica selection caches. Concurrent operations MUST NOT share an unsafe database session or let older responses overwrite newer synchronized credentials.

#### Scenario: Concurrent refresh callers
- **GIVEN** multiple callers requesting the same remote account credentials
- **WHEN** the source returns a rotated access token
- **THEN** callers observe the committed replacement without duplicate OAuth exchanges or stale cache reuse
