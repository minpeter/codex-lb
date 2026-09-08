# Verification

- HTTP behavioral RED: six missing wire-hint failures and five passing controls.
- HTTP GREEN: eleven real loopback tests, including absent and legacy IDs,
  default-off low-level clients, and external model-source exclusion.
- Benchmark RED reproduced missing accounting classified as operational errors
  and nested overload classified as other. Focused fixes passed six tests.
- Actual loopback CLI: completed response without cache accounting exits zero,
  has no errors, and records missing_terminal_cached_tokens as a measurement
  qualification. Nested overload exits one and preserves server_is_overloaded.
- Parent integrated run: 1138 passed in 17.02 seconds. Full ty, scoped Ruff,
  proxy architecture checks, strict OpenSpec validation and git diff check pass.
- LSP unavailable because basedpyright-langserver is not installed; repository
  ty checks passed instead. No dependencies or architecture limits changed.
- This repair does not establish realized upstream priority or a performance
  benefit. Historical pilot data is unchanged. No new live campaign, commit,
  push or deployment was performed for these three corrections.
