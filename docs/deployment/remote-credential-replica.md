# Remote credential replica

This fork can run a dedicated routing replica whose account credentials come
from another codex-lb installation. The source remains the account manager and
OAuth refresh owner. The replica sends inference directly to the configured
upstream using its local access-token cache.

This is separate from the upstream shared-PostgreSQL replica mode. Each remote
credential replica has its own database and encryption key.

## Source requirements

The source must expose its dashboard password login, account list and
`POST /api/accounts/{account_id}/export/auth` APIs to the replica. An ordinary
proxy API key is insufficient: account export requires dashboard write access.
The supplied password must complete administrator login without an additional
interactive challenge.

Export returns the source's currently stored credentials. It does not force an
OAuth refresh. If the source has no usable replacement token, the replica cannot
repair that account itself.

## Configuration

Remote credential mode is opt-in:

```ini
CODEX_LB_REMOTE_CREDENTIAL_SOURCE_URL=https://codex.nekos.me
CODEX_LB_REMOTE_CREDENTIAL_SOURCE_PASSWORD_FILE=/run/secrets/source_password
CODEX_LB_REMOTE_CREDENTIAL_SOURCE_SYNC_INTERVAL_SECONDS=60
CODEX_LB_REMOTE_CREDENTIAL_SOURCE_TIMEOUT_SECONDS=8
```

Keep the password outside the repository in a protected file. The source URL is
the dashboard base URL, not the upstream inference URL. Do not point
`CODEX_LB_UPSTREAM_BASE_URL` at the source instance.

The replica must not exchange refresh tokens, including after an upstream 401
or a failed source request. The source export response includes refresh material;
the replica discards it rather than storing a usable refresh token.

Successful account snapshots propagate account state and removals. Source
transport or authentication failures are not empty snapshots: cached state
remains available while its access credentials remain usable.

## Internal deployment

The included `deploy/replica/compose.yaml` targets the workstation at
`10.10.10.10:2455`. It uses a separate persistent volume and mounts the source
password through a Docker Compose secret.

Build the unchanged frontend before building the replica overlay image:

```bash
cd frontend
bun install --frozen-lockfile
bun run build
cd ..

export CODEX_LB_REPLICA_ENV_FILE="$HOME/.config/codex-lb-b/replica.env"
export CODEX_LB_REPLICA_PASSWORD_FILE="$HOME/.config/codex-lb-b/source-password"
export CODEX_LB_REPLICA_REVISION="$(git rev-parse HEAD)"
export CODEX_LB_REPLICA_TAG="$CODEX_LB_REPLICA_REVISION"
docker compose -f deploy/replica/compose.yaml up -d --build
```

The overlay Dockerfile pins the upstream runtime image by digest, synchronizes
Python dependencies from the frozen lockfile, and replaces application source,
configuration, scripts and the built frontend. Rebuild the native runtime base
when updating the native egress implementation.

`deploy/replica/nginx.conf` provides the internal HTTP ingress with WebSocket
upgrade and unbuffered streaming. `deploy/replica/dnsmasq.conf` points
`codex.minpeter.internal` to the existing gateway at `10.10.10.2`. Validate nginx
and dnsmasq configuration before reloading their services.

Set a separate replica dashboard password and enable its own proxy API-key
authentication before admitting remote client traffic. Source dashboard
credentials must never be used as client API keys.

## Host route recovery and preflight

The replica host keeps Docker bridge traffic out of the Tailscale exit-node table with the checked-in `deploy/replica/docker-lan-routes.service`. The managed baseline is deliberately fixed to `172.17.0.0/16`, `172.18.0.0/16`, `172.19.0.0/16`, `172.28.0.0/16`, and `10.10.10.0/24`; it does not discover or autofix arbitrary Docker/private networks. Each owned exception is exactly priority `5199`, destination-specific, and `lookup main`. Unrelated rules for the same CIDR are preserved.

Install or update only after reviewing the host receipt (the parent operator owns `/etc`):

```bash
sudo install -o root -g root -m 0644 deploy/replica/docker-lan-routes.service /etc/systemd/system/docker-lan-routes.service
sudo systemctl daemon-reload
sudo systemctl enable --now docker-lan-routes.service
python3 deploy/replica/route_preflight.py codex-lb-b
```

Preflight is read-only. It inspects the selected container's Docker attachment and IPAM, loaded unit commands/status, IPv4 policy rules, and the selected replica route. It emits JSON and exits nonzero for subnet drift, a missing/duplicate/wrong-selector `5199` rule, a wrong bridge route, stale unit state, malformed command output, or command failure. A nonzero result requires operator investigation; it must not be repaired by adding a new private-CIDR exception or disabling Tailscale.

The lifecycle proof uses a disposable Docker network namespace with `NET_ADMIN`; it does not use host networking, restart Docker, restart the host, or restart Tailscale. Stop removes only priority `5199` `lookup main` rules and leaves other owners intact.

## Availability boundary

The replica reduces dependency on the source's inference-handling process. It
still depends on the source to supply new access credentials, and the same
upstream account quotas apply. It cannot promise uninterrupted service through
an outage that lasts beyond its cached credentials' usable lifetime.

The owning contract is
[remote-credential-replica](../../openspec/specs/remote-credential-replica/spec.md).
