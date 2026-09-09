## Why
The deployed SSE keepalive wrapper can close while its pending read has already
finished with EOF or an exception. Cleanup skips completed tasks, leaving their
exceptions unobserved and producing asyncio never-retrieved errors.

## What Changes
Retrieve completed pending read outcomes during wrapper finalization, preserving
normal iteration error propagation and existing cancellation-deferring cleanup.

## Impact
SSE keepalive cleanup only; no routing, credentials, or runtime settings change.
