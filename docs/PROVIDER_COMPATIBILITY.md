# Storage Provider Compatibility Guide

This document details S3-compatible storage provider compatibility with Litestream,
including known limitations, required configuration, and tested configurations.

## Overview

Litestream uses the AWS SDK v2 for S3-compatible storage backends. While most providers
implement the S3 API, there are important differences in behavior that can affect
Litestream's operation.

## Provider-Specific Configuration

### AWS S3 (Default)

**Status**: Fully supported (primary target)

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path
      region: us-east-1
```

**Notes**:

- The AWS SDK supplies its default checksum calculation and validation behavior for AWS endpoints.

### Cloudflare R2

**Status**: Supported with default configuration

**Configuration**:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path?endpoint=https://ACCOUNT_ID.r2.cloudflarestorage.com
      access-key-id: your-access-key-id
      secret-access-key: your-secret-access-key
```

**Automatic Defaults** (applied when R2 endpoint detected):

- `sign-payload=true` - Signed payloads required
- `concurrency=2` - Limits concurrent multipart upload parts
- Like other custom endpoints, R2 uses SDK checksums only where required. Litestream disables the optional checksum trailer that can trigger `aws-chunked` encoding.

Related issues: #948, #947, #940, #941

### Backblaze B2 (S3-Compatible API)

**Status**: Supported with configuration

**Known Limitations**:

- Requires signed payloads for all requests
- Specific authentication endpoint required

**Configuration**:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path?endpoint=https://s3.REGION.backblazeb2.com&sign-payload=true&force-path-style=true
      access-key-id: your-key-id
      secret-access-key: your-application-key
```

**Required Settings**:

- `sign-payload=true` - Required for B2 authentication
- `force-path-style=true` - Litestream's default when it detects the B2 endpoint
- Endpoint format: `https://s3.REGION.backblazeb2.com`

For buckets with Object Lock enabled, set `put-object-content-md5: true` on
the replica, or add `put-object-content-md5=true` to its URL. This opt-in setting
adds a signed `Content-MD5` header to single-part LTX uploads without enabling
`aws-chunked` encoding. It is disabled by default and is independent of
`require-content-md5`, which controls multi-object deletions.

The checksum is computed only for known-length, seekable single-part bodies,
including small streams buffered before upload. Known-length, seekable objects
exactly equal to `part-size` also use a single-part upload with this option.
Larger objects and streams that fill the part buffer use the multipart uploader;
this option does not add checksums to multipart requests or establish Object Lock
compatibility for those uploads.

Related issues: #918, #894, #1607

### DigitalOcean Spaces

**Status**: Supported with configuration

**Known Limitations**:

- Does not support `aws-chunked` content encoding
- Signature requirements differ from AWS

**Configuration**:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path?endpoint=https://REGION.digitaloceanspaces.com&force-path-style=false
      access-key-id: your-spaces-key
      secret-access-key: your-spaces-secret
```

**Notes**:

- Use virtual-hosted style paths (force-path-style=false)
- Litestream uses SDK checksums only where required for custom endpoints

Related issues: #943

### MinIO

**Status**: Fully supported

**Configuration**:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path?endpoint=https://your-minio-server:9000&force-path-style=true
      access-key-id: your-access-key
      secret-access-key: your-secret-key
```

**Notes**:

- Works well with default settings
- Force path style recommended for single-server deployments

### Garage

**Status**: Supported with configuration when an intermediary rewrites `Accept-Encoding`

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      type: s3
      bucket: bucket-name
      path: database-name
      endpoint: https://your-garage-server
      region: garage
      force-path-style: true
      sign-accept-encoding: false
      access-key-id: your-access-key
      secret-access-key: your-secret-key
```

`sign-accept-encoding` defaults to `true`. Set it to `false` when a reverse
proxy or other intermediary removes or rewrites `Accept-Encoding`, which would
otherwise invalidate the SigV4 signature. Litestream removes the header before
signing, although Go's HTTP transport may still add an unsigned header on the
wire.

Disabling this setting improves compatibility but removes `Accept-Encoding`
from SigV4 integrity protection, so changes to that header are no longer
detected during signature validation. Reported by @ahgraber in #1424.

### Scaleway Object Storage

**Status**: Supported with configuration

**Known Limitations**:

- `MissingContentLength` errors with streaming uploads
- Requires Content-Length header

**Configuration**:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path?endpoint=https://s3.REGION.scw.cloud&force-path-style=true
      access-key-id: your-access-key
      secret-access-key: your-secret-key
```

Related issues: #912

### Hetzner Object Storage

**Status**: Supported with configuration

**Known Limitations**:

- `InvalidArgument` errors with default AWS SDK settings
- Does not support `aws-chunked` content encoding
- Requires signed payloads for all requests

**Configuration**:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path?endpoint=https://REGION.your-objectstorage.com&force-path-style=true
      access-key-id: your-access-key
      secret-access-key: your-secret-key
```

### Filebase

**Status**: Supported with configuration

**Known Limitations**:

- Authentication failures with default SDK settings after SDK v2 migration

**Configuration**:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path?endpoint=https://s3.filebase.com&force-path-style=true
      access-key-id: your-access-key
      secret-access-key: your-secret-key
```

### Tigris

**Status**: Supported with configuration

**Configuration**:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path?endpoint=https://fly.storage.tigris.dev&force-path-style=true
      access-key-id: your-access-key
      secret-access-key: your-secret-key
```

### Supabase Storage (S3-Compatible API)

**Status**: Supported (auto-detected)

**Known Limitations**:

- Requires path-style URLs (`force-path-style=true`)
- No S3 versioning support

**Configuration**:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path?endpoint=https://PROJECT_REF.supabase.co/storage/v1/s3
      access-key-id: your-s3-access-key
      secret-access-key: your-s3-secret-key
```

**Automatic Defaults** (applied when Supabase endpoint detected):

- `sign-payload=true` - Signed payloads required
- `force-path-style=true` - Path-style URLs required

**Notes**:

- S3 access keys are generated in the Supabase dashboard under Storage > S3 Access Keys
- Endpoint format: `https://<PROJECT_REF>.supabase.co/storage/v1/s3`

Related issues: #1133

### Neon Object Storage

**Status**: Supported (auto-detected)

**Known Limitations**:

- Requires path-style URLs (`force-path-style=true`)
- Each Neon branch has its own storage endpoint, so the endpoint selects the branch Litestream writes to

**Configuration**:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name.BRANCH_ID.storage.CELL.REGION.CLOUD.neon.tech/path
      access-key-id: your-access-key-id
      secret-access-key: your-secret-access-key
```

Or pass the branch endpoint explicitly:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path?endpoint=https://BRANCH_ID.storage.CELL.REGION.CLOUD.neon.tech
      access-key-id: your-access-key-id
      secret-access-key: your-secret-access-key
```

**Automatic Defaults** (applied when a Neon storage endpoint is detected):

- `sign-payload=true` - Signed payloads
- `force-path-style=true` - Path-style URLs required
- `region` - Taken from the host when the bucket is prefixed to the branch endpoint

**Notes**:

- Endpoint format: `https://<BRANCH_ID>.storage.<CELL>.<REGION>.<CLOUD>.neon.tech`, for example `https://br-cool-darkness-a1b2c3d4.storage.c-6.us-east-2.aws.neon.tech`
- Create a bucket with `neon buckets create <name> --project-id <project> --branch <branch>`
- Create S3 credentials with `neon credentials create --project-id <project> --branch <branch> --scope storage:read --scope storage:write`. The credential's token ID (`nak_...`) is the access key ID and `s3_secret_access_key` is the secret.
- A child branch gets its own copy of the parent's buckets, so `litestream restore` against a child branch endpoint reads the replica as it was when the branch was created, and writes on either branch stay isolated.

### Wasabi

**Status**: Supported

**Configuration**:

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: s3://bucket-name/path?endpoint=https://s3.REGION.wasabisys.com
      access-key-id: your-access-key
      secret-access-key: your-secret-key
```

## Google Cloud Storage (GCS)

**Status**: Fully supported (native client)

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: gs://bucket-name/path
```

**Authentication**:

- Uses Application Default Credentials
- Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- Or use workload identity on GCP

### S3-Compatible XML API

**Status**: Supported with HMAC keys

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      type: s3
      bucket: bucket-name
      path: database-name
      endpoint: https://storage.googleapis.com
      region: us-east-1
      force-path-style: true
      access-key-id: your-hmac-access-key
      secret-access-key: your-hmac-secret
```

Litestream automatically excludes `Accept-Encoding` from SigV4 signatures for
Cloud Storage endpoints under the exact `googleapis.com` domain when `storage`
is an exact segment of the first hostname label. This covers global, regional,
locational, mTLS, and legacy upload and download endpoints while rejecting
external lookalike domains.

## Azure Blob Storage (ABS)

**Status**: Fully supported (native client)

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: abs://container-name/path
      account-name: your-account-name
      account-key: your-account-key
```

**Using SAS Token** (for granular container-level access):

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: abs://container-name/path
      account-name: your-account-name
      sas-token: "sv=2023-01-03&ss=b&srt=co&sp=rwdlacx..."
```

Or via environment variable: `LITESTREAM_AZURE_SAS_TOKEN`

**Alternative Authentication**:

- SAS token: `sas-token` config or `LITESTREAM_AZURE_SAS_TOKEN` env var
- Account key: `account-key` config or `LITESTREAM_AZURE_ACCOUNT_KEY` env var
- Managed identity on Azure (via DefaultAzureCredential)

**Authentication Priority**: SAS token > Account key > Default credential chain

## Alibaba Cloud OSS

**Status**: Supported (native client)

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: oss://bucket-name/path
      region: REGION
      endpoint: https://oss-REGION.aliyuncs.com
      access-key-id: your-access-key-id
      secret-access-key: your-access-key-secret
```

## SFTP

**Status**: Supported

```yaml
dbs:
  - path: /path/to/database.db
    replica:
      url: sftp://hostname/path
      user: username
      password: password  # or use key-path
```

IPv6 addresses in URLs must be enclosed in brackets, for example
`sftp://username@[2001:db8::1]/path`. Port 22 is used when omitted; specify a
different port after the closing bracket, such as
`sftp://username@[2001:db8::1]:2222/path`.

## Configuration Reference

### S3 Query Parameters

The aliases below apply to replica URLs used by the VFS and other URL factory callers. The CLI accepts the same S3 query options except that it reads `skipVerify` only in the URL and takes server-side encryption settings from YAML fields on the replica. Custom endpoints use SDK request and response checksums only where required. `require-content-md5` governs multi-object deletions separately.

| Parameter | Alias | Description | Default |
|-----------|-------|-------------|---------|
| `endpoint` | | Custom S3 endpoint URL | AWS S3 |
| `region` | | AWS region | Auto-detected |
| `forcePathStyle` | `force-path-style` | Use path-style URLs | `false` (auto for custom endpoints) |
| `skipVerify` | `skip-verify` | Skip TLS verification | `false` |
| `caFile` | `ca-file` | Path to PEM-encoded CA certificate bundle | None |
| `signPayload` | `sign-payload` | Sign request payloads | `true` |
| `signAcceptEncoding` | `sign-accept-encoding` | Include Accept-Encoding in SigV4 signatures | `true` |
| `requireContentMD5` | `require-content-md5` | Require Content-MD5 for multi-object deletions | `true` |
| `putObjectContentMD5` | `put-object-content-md5` | Add Content-MD5 to single-part LTX uploads | `false` |
| `concurrency` | | Multipart upload concurrency | `5` (`2` for R2/Tigris) |
| `partSize` | `part-size` | Multipart upload part size | `5242880` (5MB) (`16MiB` for Tigris) |
| `storageClass` | `storage-class` | Storage class for uploaded objects | None |
| `sseCustomerAlgorithm` | `sse-customer-algorithm` | SSE-C encryption algorithm | None |
| `sseCustomerKey` | `sse-customer-key` | SSE-C encryption key | None |
| `sseCustomerKeyMD5` | `sse-customer-key-md5` | SSE-C key MD5 checksum | None |
| `sseKmsKeyId` | `sse-kms-key-id` | KMS key for encryption | None |

### Provider Detection

Litestream automatically detects certain providers and applies appropriate defaults:

| Provider | Detection Pattern | Applied Settings |
|----------|-------------------|------------------|
| Hetzner | `*.your-objectstorage.com` | `sign-payload=true` |
| Cloudflare R2 | `*.r2.cloudflarestorage.com` | `sign-payload=true`, `concurrency=2` |
| Backblaze B2 | `*.backblazeb2.com` | `sign-payload=true`, `force-path-style=true` |
| DigitalOcean | `*.digitaloceanspaces.com` | `sign-payload=true` |
| Scaleway | `*.scw.cloud` | `sign-payload=true` |
| Filebase | `s3.filebase.com` | `sign-payload=true`, `force-path-style=true` |
| Tigris | `fly.storage.tigris.dev` or `t3.storage.dev` | `sign-payload=true`, `require-content-md5=false`, `concurrency=2`, `part-size=16MiB` |
| MinIO | host with port (not cloud provider) | `sign-payload=true`, `force-path-style=true` |
| Supabase | `*.supabase.co` | `sign-payload=true`, `force-path-style=true` |
| Neon | `*.storage.*.neon.tech` | `sign-payload=true`, `force-path-style=true` |
| Google Cloud Storage | Exact `googleapis.com` suffix with a first-label `storage` segment | Excludes `Accept-Encoding` from SigV4 signatures |

## Troubleshooting

### Common Errors

**`InvalidArgument: Unsupported content encoding: aws-chunked`**

- Provider doesn't support AWS SDK v2 chunked encoding
- Configure the provider's custom endpoint so Litestream disables optional SDK checksums and removes the checksum trailer

**`SignatureDoesNotMatch`**

- Try `sign-payload=true` in the URL
- Verify credentials are correct
- Check endpoint URL format

**`signed header accept-encoding is not present`**

- An intermediary removed or rewrote `Accept-Encoding` after request signing
- Set `sign-accept-encoding=false` in the replica URL or configuration
- This trades signature coverage of that header for intermediary compatibility

**`MissingContentLength`**

- Provider requires Content-Length header
- Litestream buffers small streaming uploads to send a known length. Larger streams use multipart upload.

**`Too many concurrent uploads` or timeout errors**

- Reduce concurrency: `?concurrency=2`
- Particularly important for Cloudflare R2

**`AccessDenied` or authentication failures**

- Verify credentials
- Check IAM/bucket permissions
- For B2, ensure `sign-payload=true`

### Debug Mode

Enable verbose logging to diagnose issues:

```bash
LOG_LEVEL=debug litestream replicate
```

Or in configuration:

```yaml
logging:
  level: debug
```

For AWS SDK diagnostics, set `LITESTREAM_S3_DEBUG=signing,retries`.

## Testing Your Configuration

Test connectivity without starting replication:

```bash
# List any existing backups
litestream ltx -level 9 's3://bucket/path?endpoint=https://storage.example.com'

# Perform a test restore (requires existing backup)
litestream restore -o /tmp/test.db 's3://bucket/path?endpoint=https://storage.example.com'
```

## Version Compatibility

- **Litestream v0.5.x**: AWS SDK v2, improved provider compatibility
- **Litestream v0.4.x**: AWS SDK v1, different authentication handling
- **Litestream v0.3.x**: Legacy format — v0.5.x can restore from v0.3.x backups via `ReplicaClientV3` interface

File and S3 replicas can restore from v0.3.x backups. When both backup formats are present, Litestream chooses based on the requested restore timestamp or the latest available backup. See [Replica client guide](REPLICA_CLIENT_GUIDE.md#replicaclientv3-interface-v03x-restore).

### Validation Interval

Litestream supports periodic validation of replica integrity:

```yaml
validation:
  interval: "5m"  # How often to validate; 0 disables
```

The store periodically checks TXID ordering, gaps, and overlaps within each replica's non-snapshot levels. See [`store.go`](../store.go) and [`replica.go`](../replica.go).

## Reporting Issues

When reporting provider compatibility issues, please include:

1. Provider name and region
2. Litestream version (`litestream version`)
3. Full error message
4. Configuration (with credentials redacted)
5. Whether the issue is with replication, restore, or both

File issues at: [GitHub Issues](https://github.com/benbjohnson/litestream/issues)
