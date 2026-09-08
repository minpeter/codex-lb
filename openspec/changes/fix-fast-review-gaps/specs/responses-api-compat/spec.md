## ADDED Requirements

### Requirement: Subscription hint eligibility is independent of account headers
An HTTP request sent through the selected subscription-account path SHALL
synthesize its routing hint even when the optional ChatGPT account-ID header
is absent or filtered as a legacy identity. Low-level clients SHALL remain
default-off for callers that do not establish subscription provenance.

#### Scenario: Priority subscription without account-ID header
- **WHEN** a selected subscription account has no usable account-ID header
- **THEN** the upstream priority body and model/priority routing hint agree

### Requirement: Benchmark request success is distinct from accounting completeness
A successfully completed response SHALL remain in E2E and first-output
statistics when optional accounting is missing. Missing accounting SHALL be
reported without invented zero values. A metric lacking required accounting
SHALL be unavailable rather than changing operational success into failure.

#### Scenario: Completed response omits cached-token details
- **WHEN** a response completes with visible text and no cached-token count
- **THEN** the request remains successful and its valid latency metrics count

### Requirement: Benchmark errors preserve safe supported codes
The benchmark SHALL classify supported top-level and nested Responses error
envelopes using the existing safe-code allowlist without retaining messages.

#### Scenario: Nested overload error
- **WHEN** an error envelope contains error.code equal to server_is_overloaded
- **THEN** it remains a failed request with that classified code
