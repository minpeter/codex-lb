## ADDED Requirements

### Requirement: Eligible sibling preference for previsible overload
Before repeating a previsible overload attempt on the same account, the proxy
SHALL prefer an admitted alternative when the request is safely replayable and
the configured selector can choose an eligible sibling within its remaining
budget. The admitted selection and lease SHALL be consumed exactly once.

#### Scenario: Available sibling
- **GIVEN** a fresh account-neutral request whose first account rejects admission with overload
- **WHEN** the real selector admits a sibling within the remaining budget
- **THEN** the next attempt uses that sibling without another same-account backoff retry
- **AND** account health writes retain API-key settlement ordering

#### Scenario: No admitted sibling
- **GIVEN** overload on an account and no eligible alternative
- **WHEN** sibling selection cannot admit an account
- **THEN** the existing bounded same-account retry policy remains available

#### Scenario: Hard continuity or visible output
- **GIVEN** an account-bound request or a response with visible output
- **WHEN** overload occurs
- **THEN** sibling preference does not relax the existing replay or ownership boundary

### Requirement: Early soft-owner overload reroute
An established soft sticky owner in active overload backoff SHALL be eligible
for replacement before long isolation, only when the configured strategy admits
an overload-free alternative with the applicable caps and budget constraints.

#### Scenario: Early backoff with eligible sibling
- **GIVEN** a soft prompt-cache owner in first-level backoff and an eligible sibling
- **WHEN** a fresh admission uses that soft affinity
- **THEN** the selector rebinds the soft owner to the admitted sibling

#### Scenario: Preserve usable capacity
- **GIVEN** a lone account or an all-backed-off or strategy-ineligible alternative pool
- **WHEN** soft-owner rerouting is considered
- **THEN** the original usable owner is not discarded merely because of backoff
