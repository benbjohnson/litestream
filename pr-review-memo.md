# PR Review Memo for Ben

Hi Ben,

Here's a summary of the PRs that need your review:

## Litestream Core

### [PR #746](https://github.com/benbjohnson/litestream/pull/746) - Global Replica Defaults
**Status:** Open, ready for review
**Summary:** Implements comprehensive global replica defaults to eliminate configuration duplication. Users can set global defaults (credentials, regions, intervals) that are inherited by all replicas while allowing per-replica overrides. Includes full backward compatibility and test coverage for all replica types.

### [PR #738](https://github.com/benbjohnson/litestream/pull/738) - Directory Replication
**Status:** Open, ready for review
**Summary:** Adds directory replication support for multi-tenant applications where each tenant has their own database. Scans directories for SQLite databases with configurable patterns, supports recursive scanning, and validates files via SQLite headers. Closes issue #42.

### [PR #731](https://github.com/benbjohnson/litestream/pull/731) - S3-Compatible Endpoints via URL Query
**Status:** Open, awaiting your review (ready for review label applied)
**Summary:** Enables custom S3-compatible storage endpoints (MinIO, Tigris, Wasabi) directly from command line using URL query parameters. No config file needed. Includes MinIO integration test in CI. Fixes #219.

### [PR #609](https://github.com/benbjohnson/litestream/pull/609) - SSH Host Key Verification
**Status:** Open, I've approved, awaiting your review
**Author:** tribut (external contributor)
**Summary:** Security fix that adds SSH host key verification for SFTP replicas to prevent MITM attacks. Backwards-compatible implementation that addresses GitHub security advisory GHSA-qpgw-j75c-j585.

## Documentation

### [PR #86](https://github.com/benbjohnson/litestream.io/pull/86) - S3 Custom Endpoint Documentation
**Status:** Open, ready for review
**Summary:** Documents the new custom S3 endpoint support from PR #731. Adds comprehensive "S3-Compatible Storage Providers" section with examples for MinIO, Tigris, Wasabi, and includes security guidance.

## Homebrew Formula

### [PR #3](https://github.com/benbjohnson/homebrew-litestream/pull/3) - Brew Services Support
**Status:** Open, ready for review
**Summary:** Adds launchd plist configuration to enable `brew services start/stop litestream` commands. Allows Litestream to run as a background service managed by Homebrew.

Let me know if you need any additional details on any of these PRs!
