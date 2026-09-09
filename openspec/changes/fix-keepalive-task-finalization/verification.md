# Verification

- RED: two unhandled task exceptions reproduced deterministically, EOF and
  RuntimeError; cancellation and ordinary error propagation controls passed.
  Command: `uv run --no-sync pytest -q tests/unit/test_sse_keepalive_finalization.py`.
  Result: 2 failed, 2 passed in 0.55 seconds.
- GREEN: 85 SSE/shared-future/cancellation regressions passed in 4.10 seconds.
  Full ty, scoped Ruff, architecture and strict OpenSpec checks passed.
- Production change retrieves already-completed reads via the existing cleanup
  helper. It does not change EOF semantics, normal error propagation or the
  explicit cancellation of unfinished reads. AsyncGenerator return annotation
  describes the existing closeable runtime object without changing behavior.
- New race tests expire a waiter with zero timeout and await exact task-done
  signals before closing, rather than relying on wall-clock sleeps.
- The separate HTTP test drives an actual loopback connection, receives the
  heartbeat, releases upstream EOF and verifies clean closure with no loop
  exception. Its server and client use bounded context-manager teardown.
- Existing benchmark runner changes are unrelated and excluded from this fix.
- Final real HTTP and adjacent Responses regression run: 150 passed in 73.07
  seconds; scoped Ruff, ty and diff checks passed. Changed-file LSP diagnostics
  returned no errors. Ordinary upstream RuntimeError still propagates.
