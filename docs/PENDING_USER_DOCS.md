# Pending User-Facing Documentation

This file tracks open issues on [benbjohnson/litestream.io](https://github.com/benbjohnson/litestream.io) that need user-facing documentation for features recently added to Litestream.

## Open Issues

None right now. The litestream.io issues previously listed here (#240 through #250) are all closed and their docs have shipped.

Add a row when a new Litestream feature lands that still needs user-facing docs on the site:

| Issue | Title | Related Litestream PR | Category |
|-------|-------|-----------------------|----------|

## AI Documentation Status

Internal AI documentation (this repo) was tracked in [#1106](https://github.com/benbjohnson/litestream/issues/1106), now closed.

## MCP Restore Preview and Integrity Checks

The `litestream_restore_plan` tool in #1381 returns an ordered LTX restore plan without modifying its optional output path. Automatic and timestamp-based previews are unsupported when legacy v0.3.x backups are present, including mixed legacy/LTX replicas. An explicit `txid` selects an LTX preview. This restriction avoids presenting a plan that differs from automatic restore's legacy selection; full legacy preview support remains future work.

The `litestream_restore` tool accepts `integrity_check` values `none`, `quick`, and `full`. Failed integrity checks return a tool error and remove the failed restore output. User-facing website documentation is still pending for these MCP options.
