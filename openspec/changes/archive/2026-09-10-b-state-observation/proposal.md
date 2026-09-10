# B state observation

## Why
Per-error chatter does not distinguish sustained incidents from stable errors.
Read-only observation needs durable transitions without claiming inference health
from infrastructure health or runtime eligibility from stored account status.

## What Changes
- Add an explicitly targeted, read-only five-minute observer CLI.
- Separate snapshot analysis from Docker, HTTP, and durable journal boundaries.
- Record snapshots and machine-coded transitions; recover state from history.

## Impact
Standalone operations script and tests only. No runtime, key, routing, or service
configuration changes. Parent owns standing monitor registration.
