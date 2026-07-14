# codex-worker-adapter

`codex-worker-adapter` is the Kubernetes worker entrypoint for one embedded
Codex app-server runtime. It leases one `CODEX_HOME` shard from the platform
control plane, invokes app-server through bounded in-process channels, and
exposes that connection to the platform over WebSocket.

The adapter is a member of the `codex-rs` Cargo workspace and can be built with
`cargo build -p codex-worker-adapter` from that directory.

## Runtime flow

1. Claim a home shard from the control plane.
2. Validate `homeShardId`, create `/codex-home/{homeShardId}`, and install it as
   the process `CODEX_HOME` before Codex helper dispatch and worker threads start.
3. Start an in-process app-server runtime with session source `worker-adapter`.
4. Serve `/rpc`, `/health/live`, and `/health/ready`.
5. Renew the lease. A fencing response, lease expiry, or repeated heartbeat
   failure stops the embedded runtime and makes the Pod unready.
6. Release the lease during a normal shutdown.

Only one `/rpc` WebSocket may be active because the embedded app-server client
is one logical JSON-RPC connection. If that connection drops, the platform
should reconnect and reconcile durable state with `thread/read`.

## Configuration

The only startup setting is available as a command-line flag or environment
variable.

| Environment variable | Required | Description |
| --- | --- | --- |
| `CONTROL_PLANE_URL` | yes | Java control-plane base URL |

Runtime conventions are fixed: the adapter listens on `0.0.0.0:8080`, expects
the shared home volume at `/codex-home`, renews every 10 seconds, and fences
locally after three failed renewals. The Pod IP is discovered from the local
interface selected to reach `CONTROL_PLANE_URL`; no Downward API environment
variable is required.

## Control-plane contract

All payloads use camelCase. The endpoints do not use bearer authentication and
must be protected by cluster NetworkPolicy or a service-mesh policy.

### Claim

`POST /api/v1/home-shards/claim`

```json
{
  "instanceIp": "10.0.0.12"
}
```

```json
{
  "homeShardId": "home-0001",
  "leaseToken": "opaque-token",
  "generation": 7,
  "leaseTtlSeconds": 30
}
```

### Renew and release

- `POST /api/v1/home-shards/{homeShardId}/renew`
- `POST /api/v1/home-shards/{homeShardId}/release`

Both use this body:

```json
{
  "leaseToken": "opaque-token",
  "generation": 7
}
```

A successful renew returns `2xx`. `409 Conflict`, `410 Gone`, or
`423 Locked` fences the adapter immediately. Other failures count against the
heartbeat threshold; the original lease TTL is also enforced locally.

## Worker endpoints

- `GET /health/live`: process liveness.
- `GET /health/ready`: `200` only while the shard lease and app-server are active.
- `GET /rpc`: WebSocket carrying one JSON-RPC object per text frame.

`/rpc` is intentionally unauthenticated and should only be reachable from the
platform gateway. Binary frames and invalid JSON close the connection. The
adapter terminates the external `initialize`/`initialized` handshake because
the in-process runtime is already initialized; all other supported app-server
requests, responses, and notifications are bridged without a process boundary.
