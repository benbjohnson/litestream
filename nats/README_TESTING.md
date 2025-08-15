# Testing NATS Replica Client

This document provides instructions for testing the NATS replica client implementation.

## Prerequisites

1. Install NATS server (or use Docker):
```bash
# Using Docker
docker run -p 4222:4222 -p 8222:8222 nats:latest -js

# Or install locally
brew install nats-server  # macOS
# or download from https://github.com/nats-io/nats-server/releases
```

2. Install NATS CLI tools:
```bash
brew install nats-io/nats-tools/nats  # macOS
# or download from https://github.com/nats-io/natscli/releases
```

## Manual Testing

### 1. Start NATS Server with JetStream

```bash
# Start NATS with JetStream enabled
nats-server -js
```

### 2. Create Object Store Bucket (Optional)

The NATS replica client will auto-create the bucket if it doesn't exist, but you can pre-create it:

```bash
# Create an object store bucket
nats object add litestream-backups --size=1G --replicas=3
```

### 3. Configure Litestream

Create a test configuration file (`litestream-test.yml`):

```yaml
dbs:
  - path: /tmp/test.db
    replica:
      type: nats
      url: nats://localhost:4222
      bucket: litestream-backups
      # Optional: Use authentication
      # username: myuser
      # password: mypassword
```

### 4. Run Litestream

```bash
# Create a test database
sqlite3 /tmp/test.db "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);"
sqlite3 /tmp/test.db "INSERT INTO test (value) VALUES ('test data');"

# Run litestream with the test config
./litestream replicate -config litestream-test.yml
```

### 5. Verify Replication

```bash
# List objects in the bucket
nats object ls litestream-backups

# Get info about the bucket
nats object info litestream-backups
```

### 6. Test Restoration

```bash
# Stop litestream
# Delete the original database
rm /tmp/test.db

# Restore from NATS
./litestream restore -config litestream-test.yml /tmp/test.db

# Verify the data
sqlite3 /tmp/test.db "SELECT * FROM test;"
```

## Integration Testing

Run the integration tests with a real NATS server:

```bash
# Start NATS server with JetStream
docker run -d -p 4222:4222 --name nats-test nats:latest -js

# Run integration tests
go test -v ./nats -integration nats \
  -nats-url="nats://localhost:4222" \
  -nats-bucket="test-bucket"

# Clean up
docker stop nats-test
docker rm nats-test
```

## Authentication Testing

### JWT/NKey Authentication

1. Generate credentials:
```bash
# Create an operator
nsc add operator TestOperator

# Create an account
nsc add account TestAccount

# Create a user
nsc add user TestUser

# Export the credentials
nsc generate creds -n TestUser > user.creds
```

2. Configure Litestream:
```yaml
dbs:
  - path: /tmp/test.db
    replica:
      type: nats
      url: nats://localhost:4222
      bucket: litestream-backups
      creds: /path/to/user.creds
```

### Username/Password Authentication

1. Create a NATS server config with auth:
```conf
# nats-auth.conf
jetstream: enabled
authorization {
  users = [
    {user: testuser, password: testpass}
  ]
}
```

2. Start NATS with config:
```bash
nats-server -c nats-auth.conf
```

3. Configure Litestream:
```yaml
dbs:
  - path: /tmp/test.db
    replica:
      type: nats
      url: nats://localhost:4222
      bucket: litestream-backups
      username: testuser
      password: testpass
```

## Performance Testing

For performance testing with larger databases:

```bash
# Create a larger test database
sqlite3 /tmp/large.db < large-dataset.sql

# Run litestream with metrics enabled
./litestream replicate -config litestream-test.yml -addr :9090

# Monitor metrics at http://localhost:9090/metrics
```

## Troubleshooting

1. **Connection Issues**: Ensure NATS server is running and JetStream is enabled
2. **Authentication Failures**: Verify credentials and permissions
3. **Bucket Issues**: Check bucket exists and has sufficient space/replicas
4. **Performance**: Monitor NATS server metrics and adjust bucket configuration as needed

## Notes

- The NATS replica client uses the LTX format for storing database snapshots and WAL segments
- Object Store provides automatic chunking for large files
- Bucket replication is handled by NATS JetStream, not Litestream
- TTL and retention should generally be managed by Litestream, not NATS bucket settings
