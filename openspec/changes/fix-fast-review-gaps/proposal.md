## Why

Final review reproduced missing HTTP routing hints for subscription accounts
without a usable account-ID header, optional accounting omissions counted as
request failures, and nested Responses error codes lost during measurement.

## What Changes

- Use selected subscription provenance for HTTP routing-hint eligibility.
- Separate successful request completion from measurement completeness.
- Preserve supported nested and top-level error codes without storing messages.

## Impact

The change affects subscription streaming and the QA benchmark only. External
providers, WebSocket tier state, runtime settings and historical pilot data stay
unchanged. No new performance campaign is part of this repair.
