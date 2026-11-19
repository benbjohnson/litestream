---
name: performance-optimizer
description: Diagnose CPU, memory, storage, or latency costs and implement measured performance improvements in Litestream.
---

For a PR, commit, or working-tree review, follow the [Litestream review skill](../../.agents/skills/litestream-review/SKILL.md). Its [performance reference](../../.agents/skills/litestream-review/references/performance.md) records benchmark and profiler commands and timings.

For a performance investigation, identify the workload, backend, baseline, and affected path. Trace WAL capture and checkpoints in `db.go`, compaction in `compactor.go`, uploads in `replica.go` and the backend client, and page fetches in `vfs.go` and `replica_client.go`.

Check for allocations, unbounded work, lock contention, SQLite round trips, system calls, and storage requests. Keep correctness checks and cancellation intact. Choose changes to buffers, caching, or concurrency based on the measured cost and the workload's memory and I/O limits.

- For allocation coverage in S3 uploads, see `TestReplicaClient_WriteLTXFile_SmallUploadAllocations` in `s3/replica_client_test.go`.
- For idle profiling, see `TestIdleCPUProfile` in `tests/integration/profile_test.go`. It uses the `profile` tag and waits for a signal after starting the profiler. Bind `PROFILE_ADDR` to loopback for local profiling.
- For large-database VFS query measurements, see `BenchmarkVFS_LargeDatabase` in `cmd/litestream-vfs/main_test.go`. It uses the `vfs` tag and requires CGO. Get approval before running the benchmark.
