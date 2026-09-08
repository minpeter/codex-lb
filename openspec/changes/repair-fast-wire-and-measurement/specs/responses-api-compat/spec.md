## ADDED Requirements

### Requirement: Complete subscription routing hint propagation
Subscription-account Responses requests SHALL construct routing hints from
the final model and effective tier independently of downstream authentication.
Absent tier SHALL produce `model=<model>` without a tier component. HTTP,
persistent Responses WebSocket and WebSocket-to-HTTP fallback SHALL preserve
that signaling. Inbound hints SHALL be stripped. External model-source and
non-Responses realtime traffic SHALL NOT gain a synthesized hint.

#### Scenario: Standard request after WebSocket rejection
- **WHEN** a subscription-account request with no tier falls back to HTTP
- **THEN** its body has no tier and its hint contains the model only

#### Scenario: Fast WebSocket request from an authenticated B client
- **WHEN** a B API-key caller uses a ChatGPT account with effective priority
- **THEN** the Responses connection hint and response.create body use the final model and priority tier
