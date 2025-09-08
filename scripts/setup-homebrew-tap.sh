#!/bin/bash
set -e

echo "Setting up Homebrew tap repository for Litestream..."

REPO_NAME="homebrew-litestream"
GITHUB_USER="benbjohnson"

echo "This script will help you create the ${GITHUB_USER}/${REPO_NAME} repository."
echo ""
echo "Prerequisites:"
echo "1. GitHub CLI (gh) must be installed and authenticated"
echo "2. You must have permission to create repositories under ${GITHUB_USER}"
echo ""
read -p "Do you want to continue? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 1
fi

echo "Creating repository ${GITHUB_USER}/${REPO_NAME}..."
gh repo create ${GITHUB_USER}/${REPO_NAME} \
    --public \
    --description "Homebrew tap for Litestream" \
    --clone=false || echo "Repository may already exist, continuing..."

echo ""
echo "Cloning repository..."
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR"
gh repo clone ${GITHUB_USER}/${REPO_NAME} || git clone "https://github.com/${GITHUB_USER}/${REPO_NAME}.git"

cd ${REPO_NAME}

echo "Creating Formula directory..."
mkdir -p Formula

echo "Creating README..."
cat > README.md << 'EOF'
# Homebrew Tap for Litestream

This is the official Homebrew tap for [Litestream](https://github.com/benbjohnson/litestream).

## Installation

```bash
brew tap benbjohnson/litestream
brew install litestream
```

## Documentation

For more information about Litestream, visit:
- [GitHub Repository](https://github.com/benbjohnson/litestream)
- [Official Documentation](https://litestream.io)

## License

Apache License 2.0
EOF

echo "Creating initial Formula placeholder..."
cat > Formula/.gitkeep << 'EOF'
# This file ensures the Formula directory is tracked by git
# GoReleaser will automatically create and update formula files here
EOF

echo "Committing initial structure..."
git add .
git commit -m "Initial tap structure" || echo "Nothing to commit"

echo "Pushing to GitHub..."
git push origin main || git push origin master

echo ""
echo "✅ Homebrew tap repository setup complete!"
echo ""
echo "Repository: https://github.com/${GITHUB_USER}/${REPO_NAME}"
echo ""
echo "Next steps:"
echo "1. Create a GitHub Personal Access Token with 'repo' scope"
echo "2. Add it as HOMEBREW_TAP_GITHUB_TOKEN secret in the main repository"
echo "3. GoReleaser will automatically update the tap on each release"
