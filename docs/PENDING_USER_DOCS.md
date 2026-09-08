# Pending User-Facing Documentation

This file tracks open issues on [benbjohnson/litestream.io](https://github.com/benbjohnson/litestream.io) that need user-facing documentation for features recently added to Litestream.

## Open Issues

None right now. The litestream.io issues previously listed here (#240 through #250) are all closed and their docs have shipped.

Add a row when a new Litestream feature lands that still needs user-facing docs on the site:

| Issue | Title | Related Litestream PR | Category |
|-------|-------|-----------------------|----------|

## MCP HTTP Bearer-Token Authentication

User-facing documentation for #1384 must cover the optional `mcp-auth-token` setting and its environment variable expansion. Omitting the setting leaves the HTTP endpoint unauthenticated, while a configured token that expands to an empty value or contains whitespace or control characters is invalid. Because bearer tokens sent over plaintext can be intercepted, an authenticated TLS reverse proxy, SSH tunnel, VPN, or private network should remain the primary access control.

The token is a pre-shared secret that the operator configures on both the server and client; it is not OAuth, and Litestream provides no authorization discovery. Clients must explicitly send `Authorization: Bearer <token>`. MCP clients that expect automatic authorization discovery cannot connect without manual configuration, and the resulting 401 is expected rather than a bug. Full OAuth resource-server support—including RFC 9728 protected resource metadata, audience validation, and scope challenges—is possible future work and is not provided by this setting.

When integrating with the standalone MCP command in #1380, the HTTP command must load and validate `mcp-auth-token` from its selected config and pass it to `NewMCP`. Passing an empty token to resolve the constructor change would silently disable configured authentication. Stdio does not use HTTP bearer authentication.

## AI Documentation Status

Internal AI documentation (this repo) was tracked in [#1106](https://github.com/benbjohnson/litestream/issues/1106), now closed.
