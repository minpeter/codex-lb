# Repair Fast wire coverage and measurement

## Why
Independent reviews found missing routing hints on persistent Responses
WebSocket and WebSocket-to-HTTP fallback, and an invented `tier=default`
component when the effective tier is absent. Previous benchmarks reused a
priority-enforcing key, so their control arm was not standard routing.

## What Changes
- Preserve subscription-account routing provenance through each transport.
- Match official model-only hint formatting when no effective tier exists.
- Preserve the hint on HTTP fallback and persistent Responses connections.
- Add buffered SSE measurement with separate visible output and reasoning,
  paired ordering, correct medians and explicit errors.

## Constraints
A and production API keys remain unchanged. B API-key authentication followed
by ChatGPT-account upstream routing is eligible; true external API-key/model
source routes are not. Keep remote no-refresh, source sync21600 and isolation2/900.
