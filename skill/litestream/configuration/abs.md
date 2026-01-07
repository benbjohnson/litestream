# Azure Blob Storage Configuration

Replicate SQLite databases to Azure Blob Storage (ABS).

## Quick Start

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: abs://container@storageaccount/path
      account-name: ${AZURE_STORAGE_ACCOUNT}
      account-key: ${AZURE_STORAGE_KEY}
```

## URL Format

```
abs://CONTAINER@STORAGEACCOUNT/PATH
```

Where:
- `CONTAINER` - Azure blob container name
- `STORAGEACCOUNT` - Azure storage account name
- `PATH` - Path within the container

## Authentication

### Storage Account Key

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: abs://mycontainer@mystorageaccount/backups/app
      account-name: mystorageaccount
      account-key: ${AZURE_STORAGE_KEY}
```

### Environment Variables

```bash
export AZURE_STORAGE_ACCOUNT=mystorageaccount
export AZURE_STORAGE_KEY=base64encodedkey==
```

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: abs://mycontainer@mystorageaccount/backups/app
      account-name: ${AZURE_STORAGE_ACCOUNT}
      account-key: ${AZURE_STORAGE_KEY}
```

## Configuration Options

| Option | Description |
|--------|-------------|
| `account-name` | Azure storage account name |
| `account-key` | Storage account access key |

## Examples

### Basic Azure Backup

```yaml
dbs:
  - path: /data/app.db
    replica:
      url: abs://backups@mycompanystorage/databases/app
      account-name: mycompanystorage
      account-key: ${AZURE_STORAGE_KEY}
```

### Multiple Databases

```yaml
account-name: mycompanystorage
account-key: ${AZURE_STORAGE_KEY}

dbs:
  - path: /data/app1.db
    replica:
      url: abs://backups@mycompanystorage/app1

  - path: /data/app2.db
    replica:
      url: abs://backups@mycompanystorage/app2
```

## Azure Setup

### Create Storage Account

```bash
# Create resource group
az group create --name myresourcegroup --location eastus

# Create storage account
az storage account create \
  --name mystorageaccount \
  --resource-group myresourcegroup \
  --location eastus \
  --sku Standard_LRS

# Get storage account key
az storage account keys list \
  --account-name mystorageaccount \
  --resource-group myresourcegroup \
  --query '[0].value' -o tsv
```

### Create Container

```bash
az storage container create \
  --name backups \
  --account-name mystorageaccount \
  --account-key $AZURE_STORAGE_KEY
```

## Access Tiers

Azure Blob Storage offers different access tiers:
- **Hot**: Frequently accessed data (default)
- **Cool**: Infrequently accessed data
- **Archive**: Rarely accessed data

Litestream works best with Hot tier due to frequent small writes.

## Troubleshooting

### "Storage account not found"
- Verify storage account name is correct
- Check account exists in your Azure subscription

### "Authorization failed"
- Verify account key is correct and not expired
- Check container exists
- Ensure account key has write permissions

### "Container not found"
- Create the container before starting Litestream
- Verify container name in URL matches

### Slow performance
- Use a storage account in the same region as your server
- Consider Premium storage for high-frequency writes
