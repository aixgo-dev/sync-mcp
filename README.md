# sync-mcp

An [MCP](https://modelcontextprotocol.io) server that wraps the [Sync](https://github.com/aixgo-dev/sync) coordination service. It exposes Sync's sessions, thread messages and jobs as MCP tools, so any MCP client can use Sync without calling its HTTP API directly.

The server talks to Sync over HTTP with a bearer API key and speaks MCP over **stdio**. It is built on the [official Go SDK](https://github.com/modelcontextprotocol/go-sdk).

## Status

Initial implementation, tested against Sync `main` (API keys enforced, jobs routes mounted). Pre-1.0: tool names and arguments may still change.

## Tools

| Tool | Sync route | Purpose |
|---|---|---|
| `sync_health` | `GET /healthz` | Check that the server is reachable. Needs no key. |
| `sync_session_read` | `GET …/sessions/{session_id}` | Read a session. Returns the session and its `etag`. |
| `sync_session_write` | `PUT …/sessions/{session_id}` | Create or update a session with compare-and-swap. |
| `sync_message_append` | `POST …/threads/{thread_id}/messages` | Append a message to a thread. |
| `sync_message_list` | `GET …/threads/{thread_id}/messages` | List a thread's messages, with paging. |
| `sync_job_create` | `POST …/jobs` | Create a queued job. |
| `sync_job_get` | `GET …/jobs/{id}` | Read a job. Returns the job and its `etag`. |
| `sync_job_claim` | `POST …/jobs/{id}/claim` | Atomically claim a queued job for a worker. |

`…` stands for `/v1/orgs/{org}/projects/{project}`, which is always filled in from the server configuration (see [Tenant scope](#tenant-scope)).

### Arguments

| Tool | Arguments |
|---|---|
| `sync_health` | none |
| `sync_session_read` | `session_id` (required) |
| `sync_session_write` | `session_id`, `thread_id` (required); `status` (`open` or `closed`, default `open`), `worker_id`, `labels` (string map), `created_at`, `if_match` |
| `sync_message_append` | `thread_id`, `role` (`user`, `assistant`, `system`, `tool`, `agent`), `content` (required); `sender_actor_id`, `refs` (object) |
| `sync_message_list` | `thread_id` (required); `after_id`, `limit` (1–1000, server default 100) |
| `sync_job_create` | `kind` (`code_offload`, `command`, `schedule`, `custom`) (required); `payload` (any JSON) |
| `sync_job_get` | `job_id` (required) |
| `sync_job_claim` | `job_id`, `worker_id` (required) |

Identifiers (`session_id`, `thread_id`, `job_id`, `after_id`) may contain only letters, digits, `.`, `_` and `-`, which is what Sync accepts. Empty IDs, `.` and `..` are rejected before any request is made.

Every successful result is returned as structured content (with the same JSON as text for clients that ignore structured content). Results that carry a version include it as `etag`.

### Compare-and-swap for sessions

`sync_session_write` maps directly onto Sync's `If-Match` semantics:

- **Create:** omit `if_match`. Fails with a `409` tool error if the session already exists.
- **Update:** pass the `etag` from `sync_session_read` (or a previous write) as `if_match`. Fails with a `412` tool error if the session changed since that version. Re-read and retry.

An update replaces the whole session document, so include every field you want to keep. Sync stamps `created_at` itself unless you send one, so pass the value from `sync_session_read` to preserve it.

### Errors

Failures are returned as MCP tool results with `isError: true`, never as protocol errors, so a model can read the message and react. HTTP statuses from Sync are included with a short hint: `400` invalid request, `401` missing/invalid/revoked key, `403` key not allowed for this route or tenant, `404` not found, `409` conflict (already exists, or job already claimed), `412` stale `if_match`. Other statuses and network failures are reported with their status or cause.

## Build

Requires Go 1.25 or newer.

```bash
make build        # writes bin/sync-mcp
# or
go build -o bin/sync-mcp ./cmd/sync-mcp
```

## Configuration

All four settings are required. Each can be set by environment variable or flag; a flag overrides the variable.

| Environment variable | Flag | Description |
|---|---|---|
| `SYNC_BASE_URL` | `--base-url` | Base URL of the Sync server, e.g. `http://localhost:8080` or `https://sync.example.com`. |
| `SYNC_API_KEY` | `--api-key` | Sync API key (bearer token). |
| `SYNC_ORG` | `--org` | Organization ID every request is scoped to. |
| `SYNC_PROJECT` | `--project` | Project ID every request is scoped to. |
| – | `--timeout` | Per-request timeout for calls to Sync (default `15s`). |
| – | `--version` | Print the version and exit. |

Prefer environment variables for the API key: command-line flags are visible in process listings.

## Run

Sync authenticates with static keys of the form `org:project:token` (the server's `SYNC_API_KEYS`). To try the server against a local Sync:

```bash
# terminal 1: a local Sync with one key
SYNC_API_KEYS="org1:proj1:change-me" aixgo-sync serve

# terminal 2: the MCP server (stdio)
SYNC_BASE_URL=http://localhost:8080 \
SYNC_API_KEY=change-me \
SYNC_ORG=org1 \
SYNC_PROJECT=proj1 \
./bin/sync-mcp
```

MCP clients normally start the process themselves. Diagnostics go to stderr; stdout carries only the MCP protocol.

### Example MCP client configuration

For clients that use the common `mcpServers` JSON format:

```json
{
  "mcpServers": {
    "sync": {
      "command": "/path/to/sync-mcp",
      "env": {
        "SYNC_BASE_URL": "https://sync.example.com",
        "SYNC_API_KEY": "<your-sync-api-key>",
        "SYNC_ORG": "<org-id>",
        "SYNC_PROJECT": "<project-id>"
      }
    }
  }
}
```

## Security notes

### Tenant scope

The org and project are fixed by `SYNC_ORG` and `SYNC_PROJECT` when the server starts. No tool accepts an org or project argument, so a model using these tools cannot address another tenant, whatever it is told. This is a deliberate safety choice. Sync independently enforces that a key can only access its own org and project; the fixed scope here means a misconfigured or over-privileged key is still not steerable from the model side. To work in another project, run another instance with its own key.

### Credentials

- The API key stays inside the server process. It is never returned in tool results, never logged, and is redacted from any error text that would otherwise contain it.
- `sync_health` sends no `Authorization` header (`/healthz` is open).
- HTTP redirects are not followed, so the key can never be forwarded to another host.
- The server warns on stderr if the base URL is plain `http` to a non-loopback host. Use `https` for anything that crosses a network.
- Use a key with the narrowest scope your workload needs.

### Input handling

Path segments are validated against Sync's identifier charset and escaped with `url.PathEscape` before use. Responses are size-limited and every request has a timeout.

### Sender identity

Sync records `sender_actor_id` exactly as supplied in the request body. The `sender_actor_id` argument of `sync_message_append` is passed through as given, so treat it as a caller-supplied label, not as verified identity.

## Design notes

- **Transport:** stdio only. A network transport would need its own authentication in front of it, which is out of scope for this first version.
- **SDK:** the official `github.com/modelcontextprotocol/go-sdk` provides the protocol handling, schema inference from Go types, and in-memory transports for tests.

## Development

```bash
make check   # gofmt check, go vet, go test -race, go build
```

The unit tests run against an in-memory fake of the Sync API. An optional end-to-end test runs every tool against a real Sync binary:

```bash
# build aixgo-sync from github.com/aixgo-dev/sync, then:
SYNC_E2E_BINARY=/path/to/aixgo-sync go test ./internal/tools -run EndToEnd -v
```

## License

Apache-2.0, matching Sync. See [LICENSE](LICENSE).
