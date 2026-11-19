---
description: Diagnose the Litestream Unix control socket.
---

Check `socket.enabled`, `socket.path`, and `socket.permissions` in the effective configuration. The socket defaults to disabled, with path `/var/run/litestream.sock` and mode `0600`. For a startup error, check the parent directory and the type of any existing path. `Server.Start` removes an existing socket and rejects an existing non-socket path.

```bash
curl --unix-socket /var/run/litestream.sock http://localhost/info
curl --unix-socket /var/run/litestream.sock http://localhost/list
curl --unix-socket /var/run/litestream.sock --get --data-urlencode 'path=/path/to/database.db' http://localhost/txid
curl --unix-socket /var/run/litestream.sock --get --data-urlencode 'path=/path/to/database.db' http://localhost/debug/sync-status
```

`/list` shows database paths, status, and the last successful replica sync when known. Use the exact database path it reports. `/debug/sync-status` reports sync diagnostics. If the socket is inaccessible, check the running process, configured socket path, directory permissions, and the mode and owner of the socket.

`server.go` defines the request and response types for `POST /start`, `/stop`, `/register`, `/unregister`, and `/sync`. Registration of an existing database returns HTTP 200 with status `already_registered`. Start, stop, and unregister responses include a TXID. Run state-changing requests only when the investigation calls for them and the operator has authorized the change.

For a goroutine dump, use `/debug/pprof/goroutine?debug=2`. For CPU and trace captures, use `/debug/pprof/profile` and `/debug/pprof/trace` and supply `seconds` to set the capture length. Use the heap endpoint for memory profiling.
