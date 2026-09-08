## ADDED Requirements

### Requirement: Trusted Codex routing hint synthesis

Eligible account-bound ChatGPT/Codex backend requests MUST send
`x-codex-routing-hint: model=<model>;tier=<tier>` on outbound HTTP and
WebSocket requests, including HTTP fallback. The final normalized model and
effective service tier MUST determine the value; inbound hints MUST NOT be
forwarded. API-key, custom-provider and Guardian requests MUST NOT receive a
synthesized hint.

#### Scenario: Fast account request
- **WHEN** a ChatGPT account request uses model `gpt-6-astra` and tier `priority`
- **THEN** body tier remains `priority` and the outbound hint is
  `model=gpt-6-astra;tier=priority`

#### Scenario: Non-account route
- **WHEN** the route uses an API key, custom provider or Guardian endpoint
- **THEN** no synthesized routing hint is sent
