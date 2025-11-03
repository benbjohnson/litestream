Analyze LTX file issues in Litestream. This command helps diagnose problems with LTX files, including corruption, missing files, and consistency issues.

First, understand the context:
- What error messages are being reported?
- Which storage backend is being used?
- Are there any eventual consistency issues?

Then perform the analysis:

1. **Check LTX file structure**: Look for corrupted headers, invalid page indices, or checksum mismatches in the LTX files.

2. **Verify file continuity**: Ensure there are no gaps in the TXID sequence that could prevent restoration.

3. **Check compaction issues**: Look for problems during compaction that might corrupt files, especially with eventually consistent storage.

4. **Analyze page sequences**: Verify that page numbers are sequential and the lock page at 1GB is properly skipped.

5. **Review storage backend behavior**: Check if the storage backend has eventual consistency that might cause partial reads during compaction.

Key files to examine:
- `db.go`: WAL monitoring and LTX generation
- `replica_client.go`: Storage interface
- `store.go`: Compaction logic
- Backend-specific client in `s3/`, `gs/`, etc.

Common issues to look for:
- "nonsequential page numbers" errors (corrupted compaction)
- "EOF" errors (partial file reads)
- Missing TXID ranges (failed uploads)
- Lock page at 0x40000000 not being skipped

Use the testing harness to reproduce:
```bash
./bin/litestream-test validate -source-db test.db -replica-url [URL]
```
