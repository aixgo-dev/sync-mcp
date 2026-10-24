# sync-mcp

An [MCP](https://modelcontextprotocol.io) server that wraps the [Sync](https://github.com/aixgo-dev/sync) service and exposes Sync's messaging, sessions and job APIs as MCP tools, so MCP clients such as Grok Bot can interact with Sync without calling its HTTP API directly.

## Status

Skeleton only. No code yet.

## Planned scope

- Messaging: post and list messages in a thread.
- Sessions: get and put sessions.
- Jobs: create, get and claim jobs.
- Credentials stay inside the server, so MCP clients see tool calls and never the Sync API keys.

## License

Apache-2.0, matching Sync.
