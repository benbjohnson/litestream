# Alibaba Cloud OSS Configuration

Replicate SQLite databases to Alibaba Cloud Object Storage Service (OSS).

## Quick Start

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: oss://my-bucket/backups/app
```

## URL Format

```
oss://BUCKET/PATH
```

## Authentication

### Environment Variables

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID=your-access-key
export ALIBABA_CLOUD_ACCESS_KEY_SECRET=your-secret-key
```

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: oss://my-bucket/backups/app
```

### Configuration File

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: oss://my-bucket/backups/app
      access-key-id: ${ALIBABA_CLOUD_ACCESS_KEY_ID}
      secret-access-key: ${ALIBABA_CLOUD_ACCESS_KEY_SECRET}
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `access-key-id` | - | Alibaba Cloud access key ID |
| `secret-access-key` | - | Alibaba Cloud access key secret |
| `endpoint` | - | Custom OSS endpoint |
| `region` | - | OSS region |

## Region Endpoints

Common OSS endpoints:

| Region | Endpoint |
|--------|----------|
| China (Hangzhou) | oss-cn-hangzhou.aliyuncs.com |
| China (Shanghai) | oss-cn-shanghai.aliyuncs.com |
| China (Beijing) | oss-cn-beijing.aliyuncs.com |
| Singapore | oss-ap-southeast-1.aliyuncs.com |
| US (Virginia) | oss-us-east-1.aliyuncs.com |
| Germany (Frankfurt) | oss-eu-central-1.aliyuncs.com |

## Examples

### Basic OSS Backup

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: oss://my-backup-bucket/databases/app
      access-key-id: ${ALIBABA_CLOUD_ACCESS_KEY_ID}
      secret-access-key: ${ALIBABA_CLOUD_ACCESS_KEY_SECRET}
```

### With Custom Endpoint

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: oss://my-bucket/app
      endpoint: oss-cn-shanghai.aliyuncs.com
      access-key-id: ${ALIBABA_CLOUD_ACCESS_KEY_ID}
      secret-access-key: ${ALIBABA_CLOUD_ACCESS_KEY_SECRET}
```

## Bucket Setup

### Create Bucket via Console

1. Go to Alibaba Cloud OSS Console
2. Click "Create Bucket"
3. Enter bucket name
4. Select region
5. Set storage class (Standard recommended)
6. Set access control (Private recommended)

### Create Bucket via CLI

```bash
aliyun oss mb oss://my-litestream-bucket --region cn-hangzhou
```

## Access Control

### RAM Policy

Create a RAM policy with minimum required permissions:

```json
{
  "Version": "1",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "oss:GetObject",
        "oss:PutObject",
        "oss:DeleteObject",
        "oss:ListObjects"
      ],
      "Resource": [
        "acs:oss:*:*:my-bucket",
        "acs:oss:*:*:my-bucket/*"
      ]
    }
  ]
}
```

## Troubleshooting

### "Access denied"
- Verify access key ID and secret are correct
- Check RAM user has required permissions
- Ensure bucket ACL allows access

### "Bucket not found"
- Verify bucket name is correct
- Check bucket exists in the expected region

### "Invalid endpoint"
- Ensure endpoint matches bucket's region
- Use internal endpoint if within Alibaba Cloud VPC

### Slow uploads
- Use endpoint in same region as your server
- Consider using internal endpoints within Alibaba Cloud
