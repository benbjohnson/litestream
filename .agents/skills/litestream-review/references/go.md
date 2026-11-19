# Modern Go review

Read the language directive and selected toolchain in `go.mod` first. A newer installed compiler does not authorize newer language or library requirements.

Use [Go 1.21](https://go.dev/doc/go1.21) standard packages `slices`, `maps`, and `cmp` where they replace an older helper or a matching `golang.org/x/exp` API. Compare signatures and semantics before replacing imports. Keep unrelated modernization outside a correctness fix.

[Go 1.22](https://go.dev/doc/go1.22) gives newly declared loop variables per-iteration scope and supports ranging over integers. Check whether the loop declares or reuses a variable before diagnosing a capture bug. Prefer the clearer supported form when editing the loop.

[Go 1.25](https://go.dev/doc/go1.25) adds `sync.WaitGroup.Go` and `testing/synctest`. Consider them where their contracts fit; replacing an error-propagating group with a WaitGroup can lose failure handling.

[Go 1.26](https://go.dev/doc/go1.26) adds `errors.AsType`. Consider it for typed error extraction, while preserving wrapping and the distinction between modernc and mattn driver errors.

Treat these as review options bounded by the repository version, not a requirement to rewrite every older idiom. A modernization finding needs a concrete readability, correctness, or maintenance benefit.
