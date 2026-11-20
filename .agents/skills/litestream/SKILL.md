---
name: litestream
description: Implement, debug, and test Litestream SQLite replication. Use for WAL growth, checkpoint delays, replication lag, LTX corruption or compaction, storage backends, control socket failures, recovery tests, and performance investigations. For PR, commit, or local change reviews, use litestream-review.
---

# Litestream development

Work from a Litestream checkout. Read its `AGENTS.md`, `go.mod`, and contribution requirements before changing code. Resolve source paths such as `db.go` from that checkout's root. Resolve links below from this canonical skill directory, `.agents/skills/litestream/`, including when discovery uses a symlink.

For PR, commit, or working-tree reviews, follow [litestream-review](../litestream-review/SKILL.md). For implementation and debugging, load only the workflows relevant to the task, then read the source and tests they identify. A plan or diagnosis request does not authorize edits. Preserve unrelated changes and use disposable fixtures for state-changing checks.

Before benchmarks, profiling, or timing comparisons, follow the approval rule and runtime guidance in [performance](../litestream-review/references/performance.md). A request to investigate or optimize performance does not authorize running measurements.

## Choose a workflow

| Task | Workflow | Authoritative guide |
|------|----------|---------------------|
| WAL growth, checkpoints, transactions, or page boundaries | [WAL and checkpoints](references/wal-and-checkpoints.md) | [SQLite internals](../../../docs/SQLITE_INTERNALS.md) |
| LTX inspection, corruption, compaction, snapshots, or retention | [LTX and compaction](references/ltx-and-compaction.md) | [LTX format](../../../docs/LTX_FORMAT.md) |
| Add a storage backend or validate its contract | [Storage backends](references/replica-clients.md) | [Replica client guide](../../../docs/REPLICA_CLIENT_GUIDE.md) |
| Regression tests, race tests, integration suites, or VFS tests | [Test design and execution](references/testing.md) | [Testing guide](../../../docs/TESTING_GUIDE.md) |
| Replication lag, control socket failures, or recovery triage | [Runtime debugging](references/runtime-debugging.md) | [Architecture](../../../docs/ARCHITECTURE.md) |
| CPU, memory, latency, or storage costs | [Performance](../litestream-review/references/performance.md) | [Code patterns](../../../docs/PATTERNS.md) |

Trace a failure through capture, upload, compaction, and restore before choosing a fix. Preserve committed WAL boundaries, the lock-byte page exclusion, immutable LTX files, and continuous restore coverage. Check the configured provider's publication and consistency contract in the [provider guide](../../../docs/PROVIDER_COMPATIBILITY.md); do not assume all storage systems behave alike.

If delegating, give the worker the checkout path, exact task and scope, this skill's canonical path, and the relevant workflow path. Have it read those files before working. Use the host's available delegation mechanism; the workflows also work in a single agent session.

## Verify the result

Choose tests that reproduce the failure and exercise its recovery path. Report what ran, what skipped, and any remaining uncertainty. Follow [CONTRIBUTING.md](../../../CONTRIBUTING.md) for the applicable gates. For environment diagnosis, run `sh .agents/skills/litestream/scripts/validate-setup.sh` from the checkout root; it builds, vets, and runs short tests with the race detector.
