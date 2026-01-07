#!/bin/bash
#
# sync-skills.sh - Syncs content from source skill to Codex skill directory
#
# Codex ignores symlinked directories, so we must copy files to maintain
# content parity between the Claude skill (skill/litestream/) and the
# Codex skill (.codex/skills/litestream/).
#
# Run this script after updating the source skill content.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SOURCE="$REPO_ROOT/skill/litestream"
DEST="$REPO_ROOT/.codex/skills/litestream"

if [ ! -d "$SOURCE" ]; then
    echo "Error: Source directory not found: $SOURCE"
    exit 1
fi

if [ ! -d "$DEST" ]; then
    echo "Error: Destination directory not found: $DEST"
    echo "Create .codex/skills/litestream/SKILL.md first."
    exit 1
fi

echo "Syncing skill content..."
echo "  Source: $SOURCE"
echo "  Dest:   $DEST"

for dir in concepts configuration commands operations deployment scripts; do
    if [ -d "$SOURCE/$dir" ]; then
        rm -rf "$DEST/$dir"
        cp -r "$SOURCE/$dir" "$DEST/$dir"
        echo "  Copied: $dir/"
    else
        echo "  Skipped (not found): $dir/"
    fi
done

echo ""
echo "Sync complete. Files copied to $DEST"
echo ""
echo "Note: SKILL.md is NOT synced (Codex uses different frontmatter)."
echo "Update .codex/skills/litestream/SKILL.md manually if needed."
