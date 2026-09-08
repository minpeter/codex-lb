# Synthesize Codex Fast routing hints

## Why

Official Codex Fast requests use both the normalized `priority` body field and
the Codex-backend routing hint. B preserved the first but previously dropped
the second.

## What changes

Eligible account-bound ChatGPT/Codex backend HTTP and WebSocket requests now
synthesize `x-codex-routing-hint` from the final model and service tier.
Inbound values are never trusted. API-key, custom-provider, Guardian and
remote-credential refresh behavior remain unchanged.
