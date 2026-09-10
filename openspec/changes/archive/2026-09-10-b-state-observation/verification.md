# Verification receipt: st_01a08980

All commands ran in post-recovery-monitor, using the existing codex-lb venv.
No production reads or writes, LLM requests, OAuth refreshes, credentials, host
restarts, deployment or standing monitor registration were performed.

## Preserved failures (chronological)
- Initial test-only run: collection exit 2, ModuleNotFoundError scripts.ops.
  This is explicitly NOT behavioral RED.
- Runnable draft: 12 failed, 8 passed. Behavioral failures included spike boundary
  (20 requests / 16 successes reported healthy), incident loss after noTraffic,
  duplicate health events, incorrect persistence shape, exception-message hashing.
  SQLite SQL quoting also failed. These were fixed, not suppressed.
- Native update patch failed an exact match; no change applied by that attempt.
- Source patch-construction Python failed syntax due nested triple quotes;
  corrected patch construction, no substitute file editor used for that attempt.
- Removing an existing scripts/__init__.py caused another import collection failure.
  Original file content was restored exactly; git diff for that file is empty.
- First unconfigured ty invocation could not locate the external venv. Explicit
  --python resolves the environment. LSP initially also reported missing pydantic;
  final LSP checks report no errors for both source and tests.
- Real HTTP fixture RED: 2 failed, 1 passed. HTTP200 [] and checks:null crashed
  with AttributeError; health boundary parsing corrected to report down.
- Precise mutation RED: replaced >= with > at spike entry. Two behavior tests
  failed (healthy-spike narrative and exact 20/16 boundary), four passed,
  17 deselected. Restored >= through native patch in finally. No mutation remains.
- Ruff import ordering and ty handler override signature failed after adding
  boundary tests; both corrected. One ruff --fix invocation was mistakenly used
  for import order; subsequent formatting/source changes used native patch engine.
- Initial OpenSpec validation failed for missing delta folder; proper delta
  requirements and scenarios were added. Strict change/spec validations pass.
- python -m build --version failed: module build absent in shared venv. This is
  a standalone script outside the application wheel; bytecode compilation of the
  real script plus compilation of the remote collector is the scoped build.

## Final GREEN and real-surface receipts
Command: .venv/bin/python -m pytest tests/unit/test_b_observer.py -q -s
(shared venv executable, cwd this isolated tree)

```text
....................{"cli_exits": [0, 0, 0, 0], "states": ["healthy", "spike", "spike", "recovered"], "http_gets": 4, "docker_commands": 8, "listener_closed": true, "worker_joined": true, "secret_absent": true}
.{"cli_exits": [0, 0, 0, 0], "states": ["healthy", "spike", "spike", "recovered"], "http_gets": 4, "docker_commands": 8, "listener_closed": true, "worker_joined": true, "secret_absent": true}
.{"cli_exits": [0, 0, 0, 0], "states": ["healthy", "spike", "spike", "recovered"], "http_gets": 4, "docker_commands": 8, "listener_closed": true, "worker_joined": true, "secret_absent": true}
.....
27 passed in 4.08s
```

Ruff check and format --check pass. ty check --python <shared-venv>/bin/python
passes. LSP errors: none in either Python file. Strict OpenSpec validation passes
for state-observation and b-state-observation. Standalone bytecode build and the
remote collector compile pass; temporary bytecode directory removed.

CLI fixtures execute real subprocesses against a Docker protocol executable and
real loopback HTTP, 4 observations per health-body scenario, no mocks of analyze
or persistence. Healthy -> spike -> stable spike -> recovered emits no duplicate
spike event. Malformed readiness bodies are down; well-formed database=ok is up.
Read-only SQLite tests execute the actual remote program against SQLite fixtures
and check unchanged database bytes, unique-request/row counts and quota boundaries.
No source-text assertions or timed sleeps are used in tests.

## Cleanup and handoff
Each fixture server listener closed, worker joined, and all subprocesses exited
as asserted by the JSON receipts. Pytest retains synthetic artifacts under its
per-run temporary directories as test evidence; no credentials are in them.
No standing process or Docker container was created. No original integration
repository or other worktree was edited. The existing scripts/__init__.py is
unchanged. Intended standing command and limitations are in
openspec/specs/state-observation/context.md. Parent must register the monitor.
