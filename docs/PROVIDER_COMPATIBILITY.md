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
replicas:
  - url: s3://bucket-name/path
    region: us-east-1
```

**Notes**:

- No special configuration required
- All features fully supported
- Checksum validation enabled by default

### Cloudflare R2

**Status**: Supported with default configuration

**Known Limitations**:

- Strict concurrent upload limit (2-3 concurrent uploads max)
- Does not support `aws-chunked` content encoding
- Does not support request/response checksums

**Configuration**:

```yaml
replicas:
  - url: s3://bucket-name/path?endpoint=https://ACCOUNT_ID.r2.cloudflarestorage.com
    access-key-id: your-access-key-id
    secret-access-key: your-secret-access-key
```

**Automatic Defaults** (applied when R2 endpoint detected):

- `sign-payload=true` - Signed payloads required
- `concurrency=2` - Limits concurrent multipart upload parts
- Checksums disabled automatically

**Important**: The endpoint must use `https://` scheme for R2 detection to work.

Related issues: #948, #947, #940, #941

### Backblaze B2 (S3-Compatible API)

**Status**: Supported with configuration

**Known Limitations**:

- Requires signed payloads for all requests
- Specific authentication endpoint required

**Configuration**:

```yaml
replicas:
  - url: s3://bucket-name/path?endpoint=https://s3.REGION.backblazeb2.com&sign-payload=true&force-path-style=true
    access-key-id: your-key-id
    secret-access-key: your-application-key
```

**Required Settings**:

- `sign-payload=true` - Required for B2 authentication
- `force-path-style=true` - Required for bucket access
- Endpoint format: `https://s3.REGION.backblazeb2.com`

Related issues: #918, #894

### DigitalOcean Spaces

**Status**: Supported with configuration

**Known Limitations**:

- Does not support `aws-chunked` content encoding
- Signature requirements differ from AWS

**Configuration**:

```yaml
replicas:
  - url: s3://bucket-name/path?endpoint=https://REGION.digitaloceanspaces.com&force-path-style=false
    access-key-id: your-spaces-key
    secret-access-key: your-spaces-secret
```

**Notes**:

- Use virtual-hosted style paths (force-path-style=false)
- Checksum features disabled automatically for custom endpoints

Related issues: #943

### MinIO

**Status**: Fully supported

**Configuration**:

```yaml
replicas:
  - url: s3://bucket-name/path?endpoint=https://your-minio-server:9000&force-path-style=true
    access-key-id: your-access-key
    secret-access-key: your-secret-key
```

**Notes**:

- Works well with default settings
- Force path style recommended for single-server deployments

### Scaleway Object Storage

**Status**: Supported with configuration

**Known Limitations**:

- `MissingContentLength` errors with streaming uploads
- Requires Content-Length header

**Configuration**:

```yaml
replicas:
  - url: s3://bucket-name/path?endpoint=https://s3.REGION.scw.cloud&force-path-style=true
    access-key-id: your-access-key
    secret-access-key: your-secret-key
```

Related issues: #912

### Hetzner Object Storage

**Status**: Supported with configuration

**Known Limitations**:

- `InvalidArgument` errors with default AWS SDK settings
- Does not support `aws-chunked` content encoding

**Configuration**:

```yaml
replicas:
  - url: s3://bucket-name/path?endpoint=https://REGION.your-objectstorage.com&force-path-style=true
    access-key-id: your-access-key
    secret-access-key: your-secret-key
```

### Filebase

**Status**: Supported with configuration

**Known Limitations**:

- Authentication failures with default SDK settings after SDK v2 migration

**Configuration**:

```yaml
replicas:
  - url: s3://bucket-name/path?endpoint=https://s3.filebase.com&force-path-style=true
    access-key-id: your-access-key
    secret-access-key: your-secret-key
```

### Tigris

**Status**: Supported with configuration

**Configuration**:

```yaml
replicas:
  - url: s3://bucket-name/path?endpoint=https://fly.storage.tigris.dev&force-path-style=true
    access-key-id: your-access-key
    secret-access-key: your-secret-key
```

### Wasabi

**Status**: Supported

**Configuration**:

```yaml
replicas:
  - url: s3://bucket-name/path?endpoint=https://s3.REGION.wasabisys.com
    access-key-id: your-access-key
    secret-access-key: your-secret-key
```

## Google Cloud Storage (GCS)

**Status**: Fully supported (native client)

```yaml
replicas:
  - url: gcs://bucket-name/path
```

**Authentication**:

- Uses Application Default Credentials
- Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- Or use workload identity on GCP

## Azure Blob Storage (ABS)

**Status**: Fully supported (native client)

```yaml
replicas:
  - url: abs://container-name/path
    account-name: your-account-name
    account-key: your-account-key
```

**Using SAS Token** (for granular container-level access):

```yaml
replicas:
  - url: abs://container-name/path
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
replicas:
  - url: oss://bucket-name/path?endpoint=oss-REGION.aliyuncs.com
    access-key-id: your-access-key-id
    access-key-secret: your-access-key-secret
```

## SFTP

**Status**: Supported

```yaml
replicas:
  - url: sftp://hostname/path
    user: username
    password: password  # or use key-path
```

## Configuration Reference

### S3 Query Parameters

Parameters with an alias accept both camelCase and hyphenated forms
(e.g., `forcePathStyle` or `force-path-style`).

| Parameter | Alias | Description | Default |
|-----------|-------|-------------|---------|
| `endpoint` | | Custom S3 endpoint URL | AWS S3 |
| `region` | | AWS region | Auto-detected |
| `forcePathStyle` | `force-path-style` | Use path-style URLs | `false` (auto for custom endpoints) |
| `skipVerify` | `skip-verify` | Skip TLS verification | `false` |
| `signPayload` | `sign-payload` | Sign request payloads | `true` |
| `requireContentMD5` | `require-content-md5` | Require Content-MD5 header | `true` |
| `concurrency` | | Multipart upload concurrency | `5` |
| `partSize` | `part-size` | Multipart upload part size | `5MB` |
| `sseCustomerAlgorithm` | `sse-customer-algorithm` | SSE-C encryption algorithm | None |
| `sseCustomerKey` | `sse-customer-key` | SSE-C encryption key | None |
| `sseCustomerKeyMD5` | `sse-customer-key-md5` | SSE-C key MD5 checksum | None |
| `sseKmsKeyId` | `sse-kms-key-id` | KMS key for encryption | None |

### Provider Detection

Litestream automatically detects certain providers and applies appropriate defaults:

| Provider | Detection Pattern | Applied Settings |
|----------|-------------------|------------------|
| Cloudflare R2 | `*.r2.cloudflarestorage.com` | `sign-payload=true` |
| Backblaze B2 | `*.backblazeb2.com` | `sign-payload=true`, `force-path-style=true` |
| DigitalOcean | `*.digitaloceanspaces.com` | `sign-payload=true` |
| Scaleway | `*.scw.cloud` | `sign-payload=true` |
| Filebase | `s3.filebase.com` | `sign-payload=true`, `force-path-style=true` |
| Tigris | `*.tigris.dev` | `sign-payload=true`, `require-content-md5=false` |
| MinIO | host with port (not cloud provider) | `sign-payload=true`, `force-path-style=true` |

## Troubleshooting

### Common Errors

**`InvalidArgument: Unsupported content encoding: aws-chunked`**

- Provider doesn't support AWS SDK v2 chunked encoding
- Use a custom endpoint with automatic checksum disabling
- Or explicitly disable checksums

**`SignatureDoesNotMatch`**

- Try `sign-payload=true` in the URL
- Verify credentials are correct
- Check endpoint URL format

**`MissingContentLength`**

- Provider requires Content-Length header
- This is handled automatically for known providers

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
LITESTREAM_DEBUG=1 litestream replicate ...
```

Or in configuration:

```yaml
logging:
  level: debug
```

## Testing Your Configuration

Test connectivity without starting replication:

```bash
# List any existing backups
litestream snapshots s3://bucket/path?endpoint=...

# Perform a test restore (requires existing backup)
litestream restore -o /tmp/test.db s3://bucket/path?endpoint=...
```

## Version Compatibility

- **Litestream v0.5.x**: AWS SDK v2, improved provider compatibility
- **Litestream v0.4.x**: AWS SDK v1, different authentication handling
- **Litestream v0.3.x**: Legacy format, not compatible with v0.5.x restores

When upgrading from v0.3.x, be aware that v0.5.x uses a different backup format
and cannot restore backups created by v0.3.x. See the upgrade guide for migration
instructions.

## Reporting Issues

When reporting provider compatibility issues, please include:

1. Provider name and region
2. Litestream version (`litestream version`)
3. Full error message
4. Configuration (with credentials redacted)
5. Whether the issue is with replication, restore, or both

File issues at: [GitHub Issues](https://github.com/benbjohnson/litestream/issues)
