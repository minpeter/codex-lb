## Implementation

- [x] Capture failing tests for remote credential source login, export and account synchronization.
- [x] Capture failing tests proving forced and background calls never exchange a refresh token in replica mode.
- [x] Implement the remote credential client, persistence and lifecycle integration.
- [x] Preserve normal-mode behavior and source-failure recovery.

## Verification

- [x] Pass unit and integration regressions, typing and lint checks.
- [x] Exercise a running replica against faithful source/upstream HTTP fixtures, including concurrency and unavailable-source scenarios.
- [x] Build the deployable image and document protected configuration.
- [x] Validate OpenSpec, sync the capability specification and archive only after verification.
