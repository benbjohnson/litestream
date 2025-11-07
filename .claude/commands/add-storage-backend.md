---
description: Create a new storage backend implementation
---

# Add Storage Backend Command

Create a new storage backend implementation for Litestream with all required components.

## Steps

1. **Create Package Directory**
   ```bash
   mkdir -p {{backend_name}}
   ```

2. **Implement ReplicaClient Interface**
   Create `{{backend_name}}/replica_client.go`:
   ```go
   package {{backend_name}}

   type ReplicaClient struct {
       // Configuration fields
   }

   func (c *ReplicaClient) Type() string {
       return "{{backend_name}}"
   }

   func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
       // List files at level
       // When useMetadata=true, fetch accurate timestamps from backend metadata
   }

   func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
       // Open file for reading
   }

   func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
       // Write file atomically
   }

   func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, files []*ltx.FileInfo) error {
       // Delete files
   }

   func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
       // Remove all files for replica
   }
   ```

3. **Add Configuration Parsing**
   Update `cmd/litestream/config.go`:
   ```go
   case "{{backend_name}}":
       client = &{{backend_name}}.ReplicaClient{
           // Parse config
       }
   ```

4. **Create Integration Tests**
   Create `{{backend_name}}/replica_client_test.go`:
   ```go
   func TestReplicaClient_{{backend_name}}(t *testing.T) {
       if !*integration || *backend != "{{backend_name}}" {
           t.Skip("{{backend_name}} integration test skipped")
       }
       // Test implementation
   }
   ```

5. **Add Documentation**
   Update README.md with configuration example:
   ```yaml
   replica:
     type: {{backend_name}}
     option1: value1
     option2: value2
   ```

## Key Requirements

- Handle eventual consistency
- Implement atomic writes (temp file + rename)
- Support partial reads (offset/size)
- Preserve CreatedAt timestamps in returned FileInfo
- Return proper error types (os.ErrNotExist)

## Testing

```bash
# Run integration tests
go test -v ./replica_client_test.go -integration {{backend_name}}

# Test with race detector
go test -race -v ./{{backend_name}}/...
```
