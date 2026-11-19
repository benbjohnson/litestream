# Performance review and measurements

Inspect every diff for added allocations, lock contention, syscalls and fsyncs, SQLite round trips, and remote requests. Trace repeated work on WAL sync, checkpoint, compaction, and replica upload. Check whole-file buffering, growing page maps, list pagination, per-page requests, retry amplification, and I/O while holding locks. Separate measured regressions from hypotheses; correctness checks and durability must survive optimizations.

Before running any benchmark or profiling workload, give the user the exact command, scope, expected runtime, and resource requirements, then wait for approval. Approval to review or fix code is not approval to benchmark. If a runtime is unknown, say so and propose a bounded run. Compare baseline and candidate under the same workload and settings. Go's [diagnostics guide](https://go.dev/doc/diagnostics) explains the different CPU, heap, block, and mutex profiles; choose one that addresses the suspected cost.

## Existing VFS benchmark

`cmd/litestream-vfs/main_test.go` contains `BenchmarkVFS_LargeDatabase`. It requires the `vfs` tag, CGO, and a C compiler. It creates 20,000 rows with a local file replica, syncs and warms up the VFS, then measures `COUNT(*)` plus `SUM(LENGTH(value))`. It does not benchmark WAL capture or cloud uploads. Do not pass `-short`, which skips it.

```bash
CGO_ENABLED=1 go test -tags=vfs -run '^$' -bench '^BenchmarkVFS_LargeDatabase$' -benchmem -benchtime=1s -count=1 -timeout=3m ./cmd/litestream-vfs
```

Measured on 2026-11-19 at source revision `93eba41`, macOS arm64, Apple M4 Max, Go 1.27.3: 4.855 seconds test time and 57.93 seconds wall time including compilation. The run reported 277 iterations, 3,711,267 ns/op, 9,659 B/op, and 681 allocs/op. Debug logging was enabled by the existing test harness and a race-suite build ran concurrently. These are workload/runtime observations, not a regression threshold. Expect roughly a minute with compilation on that setup; other machines and cold caches differ. The three-minute test timeout does not bound compilation.

## Idle profiling test

`tests/integration/profile_test.go` provides `TestIdleCPUProfile` under the `profile` tag. It starts ten SQLite databases by default, uses temporary file replicas, seeds each once, and leaves the monitors idle. It serves standard pprof endpoints. Bind to loopback and select an unused port.

```bash
PROFILE_ADDR=127.0.0.1:6060 PROFILE_DB_COUNT=10 go test -tags=profile -run '^TestIdleCPUProfile$' -count=1 -timeout=0 -v ./tests/integration/
```

After startup, capture from a second terminal:

```bash
go tool pprof -top 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30'
```

The test does not finish automatically; stop it with Ctrl+C after capture. A precompiled test on the machine and revision described above reached readiness in 3.24 seconds, captured 30 seconds of CPU data, and exited successfully after SIGINT at 33.94 seconds total. Compilation is excluded. Explain the manual stop before requesting approval; do not describe it as a fixed-duration test.

There are no existing dedicated WAL-read or compaction benchmarks at this revision. Do not report nonexistent benchmark names as passing or add new benchmarks as part of an ordinary review. Use the relevant correctness tests from `docs/TESTING_GUIDE.md` and identify measurement gaps in the report.
