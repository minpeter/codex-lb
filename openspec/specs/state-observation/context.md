# B observer operations

Run from the integrated repository with its Python environment:

```sh
.venv/bin/python scripts/ops/b_observer.py --container codex-lb-b \
  --base-url http://codex.minpeter.internal \
  --journal "$HOME/.local/state/codex-lb-b/observer/history.jsonl"
```

Parent owns actual standing registration; this lane starts no standing process.
Default interval is 300 seconds after each completed collection. `--interval`
changes scheduling only; the observation remains a trailing 300-second window.
`--once` performs one read. `--once --snapshot sanitized.json` uses the strict
Snapshot schema without Docker or HTTP; live/fixture histories have different
hashed target identities. Separate journals are required for different targets.

Snapshots use `[at-300s, at)` request bounds. Runtime collection duration adds
schedule drift, windows can overlap with short intervals and miss gaps after
outages: row/request deltas compare window counts, not cumulative ingress.
Late-persisted rows may be missed. Stored request IDs are not proven unique
client operations and last observed outcomes are not proven terminal outcomes.
Retries count as extra rows; no_accounts counts any matching DB row, not every
log mention. Readiness measures database infrastructure, not inference health.

Primary quota only is evaluated, conservatively: missing, stale, zero-duration
or reset-expired quota makes capacity unknown. Weights are remaining percentage
points, not tokens, money, or interchangeable entitlement. Additional/model quotas
and live selector eligibility are deliberately not inferred. Concentration is
descriptive, never a claim that priority caused failures or selection changes.

Isolation events hash the logged timestamp/account/level/duration tuple; no live
isolated-account count or expiry/release transition is invented. Exceptions dedupe
by hashed class name, not message or stack; novel same-class causes are not
separate events. Seen signatures and journal history are retained indefinitely.
Journal replay is linear in history size; archive only by explicit operator
handoff, preserving prior evidence. Corruption stops with observer.failed and
exit 2 rather than discarding or repairing evidence. Docker failures likewise
stop for supervisor attention; they do not manufacture an empty healthy snapshot.

Events are durable in each record before stdout output. A crash between fsync and
stdout can omit console output; replay the journal for authoritative evidence.
Log caps are marked, so absence of a parsed event is not proof of no event.
No credentials or custom request headers are accepted; redirects and proxies are
disabled for the health request. Use a trusted explicit B endpoint.

Contracts were extracted read-only from the parent evidence collect_snapshot.py,
snapshot.json and ops/README.md. No production fault, provider call, OAuth refresh,
service restart or configuration mutation was performed by this lane.
