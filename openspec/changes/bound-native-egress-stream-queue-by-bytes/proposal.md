# Bound native helper queues by encoded payload bytes

## Why

The shared stdout reader currently fails an HTTP request after 64 small chunk events even when its complete burst is small. Backport only the Python queue portion of upstream #2173 (0ebf03f3), retaining the current chunk protocol and request ownership.

## What Changes

- Share a 32 MiB encoded-payload budget and 4096-event cap across HTTP and WebSocket helper event queues.
- Release charged bytes on dequeue, overflow drains and generation failure drains.
- Keep the independent WebSocket consumer queue at 64 messages and existing cancellation, protocol line bounds and reader scheduling unchanged.

## Impact

Only native_egress.py, matching tests and outbound-http-clients OpenSpec change. This is payload accounting, not an RSS ceiling: object overhead and a lone oversized event are outside the nominal budget. No settings, native protocol, Rust or routing changes.
