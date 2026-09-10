# Verify configurable benchmark models

## Why
The paired benchmark needs to measure explicitly selected models such as Sol
without requiring credentials for unselected default models. Historical
measurements must remain outside source commits and Docker build contexts.

## What Changes
- Accept an optional ordered, nonempty list of unique model names.
- Project keys and per-model effort onto that selection and propagate it through
  scheduling, payloads, journals and summaries, preserving omitted-model defaults.
- Verify validation with matching-key fixtures and precise mutation RED checks.
- Exclude only scripts/qa/artifacts from Git and Docker contexts.

## Scope
Benchmark CLI only. No provider calls, credential management, runtime policy,
production service changes, or claims about actual Fast performance.
