# Manage replica local routes

## Why
The recovered B subnet must continue to bypass the exit-node default without
deleting another operator rule for the same destination or hiding command errors.

## What Changes
- Ship the existing docker-lan-routes baseline with exact rule ownership.
- Add a read-only JSON preflight for Docker IPAM, loaded unit commands and routing.
- Exercise lifecycle in a disposable NET_ADMIN network namespace, never hostnet.

## Impact
Deployment artifacts and operator documentation only. Host installation is a
separate parent operation; no Compose, image, credential or Tailscale policy changes.
