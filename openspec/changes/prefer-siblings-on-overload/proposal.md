# Prefer eligible sibling accounts on overload

## Why

Observed B traffic repeatedly receives upstream overload failures from a small
subset of accounts while sibling accounts succeed. Same-account retry batches
and soft affinity retained during early backoff prolong exposure to those
accounts. Existing isolation works, but only after three qualifying trips.

## What Changes

- Prefer an actually admitted sibling before another previsible overload retry.
- Retain and consume the selected replacement and its lease exactly once.
- Allow soft-owner rerouting during active overload backoff, not only isolation.
- Preserve hard continuity, available-capacity fallback, generic retry policy,
  settlement ordering and the existing overload observation semantics.

## Impact

Streaming retry and soft account selection only. No source-instance changes,
credential changes, database migrations, native transport changes or new settings.
