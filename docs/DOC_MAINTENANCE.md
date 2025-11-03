# Documentation Maintenance Guide

This guide ensures documentation stays synchronized with code changes and follows the principle-based approach established in PR #787.

## Philosophy: Principles Over Examples

**Key Insight**: Code examples become outdated quickly. Documentation should focus on **stable concepts** rather than **volatile implementations**.

### What to Document

✅ **DO Document**:

- **Architectural principles** (e.g., "DB layer handles database state")
- **Interface contracts** (what methods must do, not how they do it)
- **Design patterns** (atomic file operations, eventual consistency handling)
- **Critical edge cases** (1GB lock page, timestamp preservation)
- **"Why" not "what"** (rationale behind decisions)

❌ **DON'T Document**:

- Specific function implementations that change frequently
- Exact function names without referencing actual source
- Step-by-step code that duplicates the implementation
- Version-specific details that will quickly become stale

### Documentation Principles

1. **Abstractions over Details**: Document the concept, not the specific implementation
2. **Reference over Duplication**: Point to actual source files instead of copying code
3. **Patterns over Examples**: Describe the approach, let developers read the source
4. **Contracts over Implementations**: Define what must happen, not how

## When Code Changes, Update Docs

### Interface Changes

**Trigger**: Modifying `ReplicaClient` interface or any public interface

**Required Updates**:

1. Search for interface definitions in docs:

   ```bash
   rg "type ReplicaClient interface" docs/ CLAUDE.md AGENTS.md .claude/
   ```

2. Update interface signatures (don't forget parameters!)
3. Document new parameters with clear explanations of when/why to use them
4. Update all example calls to include new parameters

**Files to Check**:

- `AGENTS.md` - Interface definitions
- `docs/REPLICA_CLIENT_GUIDE.md` - Implementation guide
- `docs/TESTING_GUIDE.md` - Test examples
- `.claude/agents/replica-client-developer.md` - Agent knowledge
- `.claude/commands/add-storage-backend.md` - Backend templates
- `.claude/commands/validate-replica.md` - Validation commands

### New Features

**Trigger**: Adding new functionality, methods, or components

**Approach**:

1. **Don't rush to document** - Wait until the feature stabilizes
2. **Document the pattern**, not the implementation:
   - What problem does it solve?
   - What's the high-level approach?
   - What are the critical constraints?
3. **Reference the source**:
   - `See implementation in file.go:lines`
   - `Reference tests in file_test.go`

### Refactoring

**Trigger**: Moving or renaming functions, restructuring code

**Required Actions**:

1. **Search for references**:

   ```bash
   # Find function name references
   rg "functionName" docs/ CLAUDE.md AGENTS.md .claude/
   ```

2. **Update or remove**:
   - If it's a reference pointer (e.g., "See `DB.init()` in db.go:123"), update it
   - If it's a code example showing implementation, consider replacing with a pattern description

3. **Verify links**: Ensure all file:line references are still valid

## Documentation Update Checklist

Use this checklist when making code changes:

- [ ] **Search docs for affected code**:

  ```bash
  # Search for function names, types, or concepts
  rg "YourFunctionName" docs/ CLAUDE.md AGENTS.md .claude/
  ```

- [ ] **Update interface definitions** if signatures changed
- [ ] **Update examples** if they won't compile anymore
- [ ] **Convert brittle examples to patterns** if refactoring made them stale
- [ ] **Update file:line references** if code moved
- [ ] **Verify contracts still hold** (update if behavior changed)
- [ ] **Run markdownlint**:

  ```bash
  markdownlint --fix docs/ CLAUDE.md AGENTS.md .claude/
  ```

## Preventing Documentation Drift

### Pre-Commit Practices

1. **Search before committing**:

   ```bash
   git diff --name-only | xargs -I {} rg "basename {}" docs/
   ```

2. **Review doc references** in your PR description
3. **Test examples compile** (if they're meant to)

### Regular Audits

**Monthly**: Spot-check one documentation file against current codebase

**Questions to ask**:

- Do interface definitions match `replica_client.go`?
- Do code examples compile?
- Are file:line references accurate?
- Have we removed outdated examples?

### When in Doubt

**Rule**: Delete outdated documentation rather than let it mislead

- Stale examples cause compilation errors
- Outdated patterns cause architectural mistakes
- Incorrect references waste developer time

**Better**: A brief pattern description + reference to source than an outdated example

## Example: Good vs Bad Documentation Updates

### ❌ Bad: Copying Implementation

```markdown
### How to initialize DB

```go
func (db *DB) init() {
    db.mu.Lock()
    defer db.mu.Unlock()
    // ... 50 lines of code copied from db.go
}
\```
```

**Problem**: This will be outdated as soon as the implementation changes.

### ✅ Good: Documenting Pattern + Reference

```markdown
### DB Initialization Pattern

**Principle**: Database initialization must complete before replication starts.

**Pattern**:

1. Acquire exclusive lock (`mu.Lock()`)
2. Verify database state consistency
3. Initialize monitoring subsystems
4. Set up replication coordination

**Critical**: Use `Lock()` not `RLock()` as initialization modifies state.

**Reference Implementation**: See `DB.init()` in db.go:150-230
```

**Benefits**: Stays accurate even if implementation details change, focuses on the "why" and "what" rather than the "how".

## Tools and Commands

### Find Documentation References

```bash
# Find all code examples in documentation
rg "^```(go|golang)" docs/ CLAUDE.md AGENTS.md .claude/

# Find file:line references
rg "\.go:\d+" docs/ CLAUDE.md AGENTS.md .claude/

# Find interface definitions
rg "type .* interface" docs/ CLAUDE.md AGENTS.md .claude/
```

### Validate Markdown

```bash
# Lint all docs
markdownlint docs/ CLAUDE.md AGENTS.md .claude/

# Auto-fix issues
markdownlint --fix docs/ CLAUDE.md AGENTS.md .claude/
```

### Check for Broken References

```bash
# List all go files mentioned in docs
rg -o "[a-z_]+\.go:\d+" docs/ CLAUDE.md AGENTS.md | sort -u

# Verify they exist and line numbers are reasonable
```

## Resources

- **PR #787**: Original principle-based documentation refactor
- **Issue #805**: Context for why accurate documentation matters
- **INNOQ Best Practices**: <https://www.innoq.com/en/articles/2022/01/principles-of-technical-documentation/>
- **Google Style Guide**: <https://google.github.io/styleguide/docguide/best_practices.html>

## Questions?

When updating documentation, ask:

1. **Is this a stable concept or a volatile implementation?**
   - Stable → Document the principle
   - Volatile → Reference the source

2. **Will this stay accurate for 6+ months?**
   - Yes → Keep it
   - No → Replace with pattern description

3. **Does this explain WHY or just WHAT?**
   - WHY → Valuable documentation
   - WHAT → Code already shows this, just reference it

4. **Would a link to source code be better?**
   - Often, yes!
