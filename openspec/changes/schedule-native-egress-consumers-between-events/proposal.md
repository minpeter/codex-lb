# Schedule native egress consumers between buffered events

## Why

Buffered helper stdout can keep readline from suspending, starving ready consumers and filling their bounded queues with a healthy response burst.

## What Changes

- Backport the Python reader checkpoint from upstream 9703ef9b (#2143) after every successful queue dispatch.
- Retain the legacy chunk protocol, queue byte/event budgets, overflow cancellation and generation ownership.
- Verify deterministic subscribed-consumer ordering and direct/routed Responses SSE, JSON and error boundaries.

## Impact

- Affected spec: outbound-http-clients.
- Affected code: app/core/clients/native_egress.py and native egress tests.
- No Rust framing migration, settings change or provider call.
