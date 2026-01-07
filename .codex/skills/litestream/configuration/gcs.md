# Google Cloud Storage Configuration

Replicate SQLite databases to Google Cloud Storage (GCS).

## Quick Start

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: gs://my-bucket/backups/app
```

## URL Format

```
gs://BUCKET/PATH
```

## Authentication

GCS uses Google Cloud Application Default Credentials (ADC).

### Service Account Key (Recommended for Production)

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: gs://my-bucket/app-backup
```

### GCE Instance Service Account

On Google Compute Engine, uses the instance's service account automatically:

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: gs://my-bucket/app-backup
```

### User Credentials (Development)

```bash
gcloud auth application-default login
```

### Workload Identity (GKE)

Configure Workload Identity in GKE, then use without credentials:

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: gs://my-bucket/app-backup
```

## Configuration

GCS configuration is minimal - most settings come from ADC:

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: gs://my-bucket/backups/app
      sync-interval: 1s
```

## IAM Permissions

Required roles:
- `roles/storage.objectViewer` - Read LTX files
- `roles/storage.objectCreator` - Write LTX files
- `roles/storage.objectAdmin` - Delete old files (for retention)

Or custom IAM permissions:
```
storage.objects.get
storage.objects.create
storage.objects.delete
storage.objects.list
```

## Bucket Configuration

### Create Bucket

```bash
gsutil mb -l us-central1 gs://my-litestream-bucket
```

### Lifecycle Rules (Optional)

Auto-delete old files:

```bash
cat > lifecycle.json << EOF
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 30}
    }
  ]
}
EOF

gsutil lifecycle set lifecycle.json gs://my-bucket
```

### Versioning (Not Recommended)

Litestream manages its own versioning through LTX files. GCS versioning is not needed and will increase costs.

## Examples

### Basic GCS Backup

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: gs://my-company-backups/databases/app
```

### Multiple Databases to Same Bucket

```yaml
dbs:
  - path: /data/app1.db
    replica:
      url: gs://my-bucket/app1

  - path: /data/app2.db
    replica:
      url: gs://my-bucket/app2
```

### Cloud Run / Cloud Functions

Use the service's identity:

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: gs://my-bucket/app-backup
```

Ensure the service account has Storage Object Admin role.

## Troubleshooting

### "Permission denied"
- Verify service account has required IAM roles
- Check bucket-level permissions
- Ensure GOOGLE_APPLICATION_CREDENTIALS is set correctly

### "Bucket not found"
- Verify bucket name is correct
- Ensure bucket exists in the project

### "Invalid credentials"
- Re-run `gcloud auth application-default login`
- Check service account key file is valid JSON
- Verify key file path in GOOGLE_APPLICATION_CREDENTIALS

### Slow uploads from outside GCP
- Consider using a bucket in a region closer to your server
- GCS is optimized for Google Cloud workloads
