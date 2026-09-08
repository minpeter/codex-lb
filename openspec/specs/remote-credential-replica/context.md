# Remote credential replica context

The remote mode addresses application-instance load, not additional upstream
quota. Account management and refresh ownership remain at the source; inference
and local request accounting happen on the replica.

The source's existing dashboard export endpoint is a stored-token snapshot, not
a refresh-on-demand endpoint. A replica can continue through a source outage
only while its cached access credentials remain usable. A source without a
usable replacement cannot be repaired by the replica.

This is a dedicated mode rather than a mixture of local and mirrored accounts.
It uses a separate local database and encryption key. Source refresh material
must not be persisted as usable credentials. Administrative password and API-key
authentication on the replica remain separate from the source login.

The source endpoint writes an audit event for each export. Routine synchronization
should use source token timestamps to avoid exporting every account on every
tick. Concurrent recovery callers should share bounded work rather than amplify
source load.

Example: `codex.nekos.me` manages accounts, while
`codex.minpeter.internal` reads that dashboard API and sends inference directly
to ChatGPT. Internal deployment details are in
[`docs/deployment/remote-credential-replica.md`](../../../docs/deployment/remote-credential-replica.md).
