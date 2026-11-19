# Documentation Maintenance Guide

Document contracts, architecture, configuration defaults, and edge cases. Link to source and tests for implementation details. Keep runnable examples short so they can be checked when the code changes.

## Find affected documentation

Search for the symbol, configuration key, or command that changed. Include contributor guides, agent instructions, and `.agents/skills/`. Review behavior belongs in [litestream-review](../.agents/skills/litestream-review/SKILL.md); implementation and debugging workflows belong in [litestream](../.agents/skills/litestream/SKILL.md). Keep architecture and API facts in `docs/` and link to them from workflows. `.claude/skills` is a discovery symlink to the shared skills, so it needs no separate content updates. Agent instructions live only in [AGENTS.md](../AGENTS.md).

```bash
rg --hidden -g '!.git' -g '!.local' -n -F -g '*.md' -- 'ReplicaClient' .
rg --hidden -g '!.git' -g '!.local' -n -F -g '*.md' -- 'shutdown-timeout' .
```

- For interface changes, check the definition, implementations, mocks, and callers. Update descriptions of arguments, return values, and error behavior where they changed. [`ReplicaClient`](../replica_client.go), its [implementation guide](REPLICA_CLIENT_GUIDE.md), and the [shared mock](../mock/replica_client.go) are starting points for storage changes.
- For behavior or configuration changes, trace the default, the validation, and where the value takes effect. Update user-facing and contributor documentation that describes the old behavior.
- For renames or moves, update links and replace stale snippets with a short explanation and a source link. Prefer a file link and symbol name to a line range that moves as the file changes.

## Check examples and links

Check CLI examples against command flags and help, and test examples against the declarations and build tags in the repository. For a test command, confirm it selects the intended test and check for skips. The [testing guide](TESTING_GUIDE.md) records how to run the common suites. The [commit workflow](../.github/workflows/commit.yml) shows CI prerequisites and exact selectors.

```bash
rg --hidden -g '!.git' -g '!.local' -n -g '*.md' '^```(go|golang)' .
rg --hidden -g '!.git' -g '!.local' -n -g '*.md' '\.go:[0-9]+' .
```

Check the paths of relative links from the file that contains them. Where a reference uses a heading anchor, check that the heading exists in the target file.

## Run the repository checks

The [pre-commit configuration](../.pre-commit-config.yaml) defines the checks used by the [CI lint job](../.github/workflows/commit.yml). Run it in the working checkout and inspect any edits the hooks make.

```bash
pre-commit run --all-files
```

If `markdownlint` is installed, it can check edited Markdown using [the repository configuration](../.markdownlint.json). Give it the paths you changed.

```bash
markdownlint docs/DOC_MAINTENANCE.md docs/TESTING_GUIDE.md
```
