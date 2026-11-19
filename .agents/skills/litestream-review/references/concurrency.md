# Concurrency review

Check these contracts against the `golang.org/x/sync` version pinned in `go.mod`.

[errgroup](https://pkg.go.dev/golang.org/x/sync/errgroup): `WithContext` cancels on the first nonnil task error or when `Wait` returns. Do not reuse its context for work after `Wait`. `Go` blocks when a `SetLimit` bound is full; zero admits no new work. Never change the limit while tasks are active. Check nested submissions and whether blocking admission happens while holding a lock needed by an admitted worker.

[semaphore.Weighted](https://pkg.go.dev/golang.org/x/sync/semaphore): `Acquire` accepts a context and leaves the semaphore unchanged on failure. Release exactly the weight successfully acquired. Check weight bounds, cancellation, and whether admission occurs before unbounded goroutine creation. Trace ownership across early returns and shutdown.

[singleflight](https://pkg.go.dev/golang.org/x/sync/singleflight): duplicate callers share the result for a key; this is not a persistent cache. Keys must distinguish the database, version, and operation where relevant. `DoChan` returns a channel that is not closed. Do not range over it waiting for closure. Check whether a caller's cancellation incorrectly cancels work shared by other callers, and whether shared mutable results need protection.

For each new goroutine, identify who cancels it and waits for its exit. Check channel-close ownership, concurrent Start/Stop, lock order, and I/O under locks against the actual caller graph. Trace DB executor ownership, checkpoint coordination, Replica lifecycle, and Store shutdown rather than assuming a universal lock hierarchy. Race tests support observed interleavings; they do not prove deadlock freedom.
