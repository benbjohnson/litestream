# Pending User-Facing Documentation

This file tracks open issues on [benbjohnson/litestream.io](https://github.com/benbjohnson/litestream.io) that need user-facing documentation for features recently added to Litestream.

## Open Issues

None right now. The litestream.io issues previously listed here (#240 through #250) are all closed and their docs have shipped.

Add a row when a new Litestream feature lands that still needs user-facing docs on the site:

| Issue | Title | Related Litestream PR | Category |
|-------|-------|-----------------------|----------|

## AI Documentation Status

Internal AI documentation (this repo) was tracked in [#1106](https://github.com/benbjohnson/litestream/issues/1106), now closed.

## Standalone MCP command (PR #1380)

Pending publication with [#1380](https://github.com/benbjohnson/litestream/pull/1380): update the MCP reference with the standalone command and transport selection.

`litestream mcp` runs without starting the replication daemon. Stdio is the default transport. Configure a local MCP client to launch `litestream` with arguments `mcp --config /path/to/litestream.yml`. Protocol messages use stdout and logs use stderr.

Use `litestream mcp --addr 127.0.0.1:3001 --config /path/to/litestream.yml` for Streamable HTTP. Keep remote access behind an authenticated proxy, SSH tunnel, or VPN. The existing `mcp-addr` YAML setting continues to enable HTTP MCP inside `litestream replicate`.

Both standalone transports stop on interrupt or termination signals. Existing tools still require the Litestream executable on PATH until the in-process execution change in #1377 is combined with this command.
