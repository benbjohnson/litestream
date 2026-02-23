# AI-Assisted Contribution Guide

This guide helps AI assistants (and humans using them) submit high-quality PRs to Litestream.

## TL;DR Checklist

Before submitting a PR:

- [ ] **Show your investigation** - Include logs, file patterns, or debug output proving the problem
- [ ] **Define scope clearly** - State what this PR does AND does not do
- [ ] **Include runnable test commands** - Not just descriptions, actual `go test` commands
- [ ] **Reference related issues/PRs** - Show awareness of related work
- [ ] **Error handling**: Does the code return errors to callers? Watch for `log.Printf(err)` followed by `continue` or `return nil` — this silently swallows failures.

## What Makes PRs Succeed

Analysis of recent PRs shows successful submissions share these patterns:

### 1. Investigation Artifacts

Show evidence, don't just describe the fix.

**Good:**

```markdown
## Problem
File patterns show excessive snapshot creation after checkpoint:
- 21:43 5.2G snapshot.ltx
- 21:47 5.2G snapshot.ltx (after checkpoint - should not trigger new snapshot)

Debug logs show `verify()` incorrectly detecting position mismatch...
```

**Bad:**

```markdown
## Problem
Snapshots are created too often. This PR fixes it.
```

### 2. Clear Scope Definition

Explicitly state boundaries.

**Good:**

```markdown
## Scope
This PR adds the lease client interface only.

**In scope:**
- LeaseClient interface definition
- Mock implementation for testing

**Not in scope (future PRs):**
- Integration with Store
- Distributed coordination logic
```

**Bad:**

```markdown
## Changes
Added leasing support and also fixed a checkpoint bug I noticed.
```

### 3. Runnable Test Commands

**Good:** Include actual commands that can be run:

```bash
# Unit tests
go test -race -v -run TestDB_CheckpointDoesNotTriggerSnapshot ./...

# Integration test with file backend
go test -v ./replica_client_test.go -integration file
```

**Bad:** Vague descriptions like "Manual testing with file backend" or "Verified it works"

### 4. Before/After Comparison

For behavior changes, show the difference:

**Good:**

```markdown
## Behavior Change

| Scenario | Before | After |
|----------|--------|-------|
| Checkpoint with no changes | Creates snapshot | No snapshot |
| Checkpoint with changes | Creates snapshot | Creates snapshot |
```

## Common Mistakes

### Scope Creep

**Problem:** Mixing unrelated changes in one PR.

**Example:** PR titled "Add lease client" also includes a fix for checkpoint timing.

**Fix:** Split into separate PRs. Reference them: "This PR adds the lease client. The checkpoint fix is in #XXX."

### Missing Root Cause Analysis

**Problem:** Implementing a fix without proving the problem exists.

**Example:** "Add exponential backoff" without showing what's filling disk.

**Fix:** Include investigation showing the actual cause before proposing solution.

### Vague Test Plans

**Problem:** "Tested manually" or "Verified it works."

**Fix:** Include exact commands:

```bash
go test -race -v -run TestSpecificFunction ./...
```

### No Integration Context

**Problem:** Large features without explaining how they fit.

**Fix:** For multi-PR work, explain the phases:

```markdown
This is Phase 1 of 3 for distributed leasing:
1. **This PR**: Lease client interface
2. Future: Store integration
3. Future: Distributed coordination
```

## PR Description Template

Use this structure for PR descriptions:

```text
## Summary
[1-2 sentences: what this PR does]

## Problem
[Evidence of the problem - logs, file patterns, user reports]

## Solution
[Brief explanation of the approach]

## Scope
**In scope:**
- [item]

**Not in scope:**
- [item]

## Test Plan
[Include actual go test commands here]

## Related
- Fixes #XXX
- Related to #YYY
```

## What We Accept

From [CONTRIBUTING.md](CONTRIBUTING.md):

- **Bug fixes** - Welcome, especially with evidence
- **Small improvements** - Performance, code cleanup
- **Documentation** - Always welcome
- **Features** - Discuss in issue first; large features typically implemented internally

## Resources

- [AGENTS.md](AGENTS.md) - Project overview and checklist
- [docs/PATTERNS.md](docs/PATTERNS.md) - Code patterns
- [CONTRIBUTING.md](CONTRIBUTING.md) - Contribution guidelines
