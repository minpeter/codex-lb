# Verification, 2026-09-10

Scope: isolated post-recovery/benchmark worktree. No provider requests, production
changes, key management, deployment, push or merge. Historical source artifacts
remain untouched; preservation/archive ownership remains with the parent.

- Five independent source mutations each produced one intended regression failure:
  empty, whitespace and duplicate model acceptance; schedule model propagation;
  summary model propagation. Each mutation was restored byte-for-byte.
- Focused benchmark, framing and real CLI tests: 963 passed.
- Ruff lint/format and ty with the explicitly supplied existing venv passed.
  Editor LSP cannot resolve aiohttp without that venv; this is not a clean LSP claim.
- Real CLI subprocess: ten rounds of synthetic Sol over ephemeral loopback HTTP,
  twenty successful Responses terminals, ten priority and ten omitted-tier requests.
  Checked payloads, headers, usage, ordered journal equality and redaction.
  Process reaped, listener closed and mode-0600 dummy config removed.
- Offline uv build produced an sdist and wheel. No frontend or production image
  build/deployment is claimed for this benchmark-only change.
- Git check-ignore confirms scripts/qa/artifacts exclusion. Dockerignore uses the
  same narrow directory exclusion; the benchmark source remains outside it.
- Strict change validation and all 59 existing OpenSpec specs passed.

External evidence root:
`/home/minpeter/github.com/minpeter-labs/.omo/evidence/post-recovery/benchmark/implementation`

Evidence includes red-*.log, focused-green-final.log, ty.log,
cli-run/test_real_cli_sol_campaign_pre0/{campaign.json,campaign.jsonl,requests.json,receipt.json},
and offline build distributions. These are verification artifacts, not live Fast
performance results. Existing broader historical failures are not superseded.
