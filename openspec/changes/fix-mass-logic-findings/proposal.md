## Why
The partitioned logic review identified fifteen reachable errors in request
ownership, retry budgets, credential validity, scope editing and persistence.
This change repairs those contracts without changing provider tier policy.

## What Changes
- Fence uncorrelated expired WebSocket work before releasing admission.
- Honor bridge account exclusions and drain-strategy fallback eligibility.
- Preserve single terminal health ownership, settlement order and deadlines.
- Close eager external-source streams and release cancelled setup reservations.
- Preserve pending account deletion and SQLite target/backup correctness.
- Bind sessions to current credentials and trust the original proxy peer.
- Preserve orphaned account restrictions and ignore stale OAuth callbacks.
- Build the corrected frontend into the B replica image rather than retaining
  the pinned base image UI.

## Impact
Runtime, backend and frontend logic and regression coverage change. Existing
benchmark modifications are unrelated and remain outside the repair commits.
Deployment is B-only; source A, production API keys, credential refresh policy,
21600-second source sync and overload isolation 2/900 remain unchanged.
