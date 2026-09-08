# Remote credential replica

## Why

A separately hosted routing instance should use accounts managed by another codex-lb installation without sharing its database or consuming its refresh tokens. This moves inference traffic off the account-management application while retaining that application as the credential authority.

## What Changes

- Add an opt-in, dedicated remote-credential mode using the source dashboard login, account listing and auth export APIs.
- Synchronize account identities, states and access credentials into the replica database; do not persist usable source refresh tokens.
- Route all local refresh attempts to remote credential synchronization, including forced and background refresh paths. Never fall back to an OAuth refresh exchange.
- Preserve usable cached credentials during source failures, deduplicate concurrent synchronization and invalidate selection caches after updates.
- Provide deployment documentation for separate database/key and protected source authentication. Native mode remains unchanged.

## Impact

Affected areas: account authentication and persistence, settings, application lifecycle, integration tests and deployment documentation. No source-instance changes, new database schema, frontend changes or upstream inference proxying through the source are required.
