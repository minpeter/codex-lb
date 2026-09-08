# Configure overload isolation trip level

## Why
Operators need to tune when repeated overload enters longer isolation separately
from how long isolation lasts. A pool with healthy alternatives can benefit
from second-trip isolation without changing other installations' defaults.

## What Changes
- Add CODEX_LB_PROXY_OVERLOAD_ISOLATION_TRIP_LEVEL, default3, integer1..5.
- Use the selected trip level in the existing isolation policy.
- Preserve duration0 disabling, default1800 seconds, soft reroute and hard-owner rules.

## Impact
One settings field and policy wiring; no schema, credential or transport changes.
