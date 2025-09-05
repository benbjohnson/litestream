# Litestream Release Process

This document describes the release process for Litestream using GoReleaser.

## Quick Start for Maintainers

To create a release after certificates are configured:

```bash
# Tag and push
git tag -a v0.3.14 -m "Release v0.3.14"
git push origin v0.3.14
```

The GitHub Actions workflow will handle everything else automatically.

## Overview

Litestream uses [GoReleaser](https://goreleaser.com/) to automate the release process, providing:

- Cross-platform binary builds (Linux, macOS, Windows)
- Automatic changelog generation
- Homebrew formula updates
- Debian/RPM package generation
- Binary signing (when certificates are configured)
- SBOM (Software Bill of Materials) generation

## Platform Support

### Officially Supported Platforms

- Linux (amd64, arm64, armv6, armv7)
- macOS (amd64, arm64)

### Unsupported Platforms

- **Windows (amd64, arm64)**: Binaries are provided for convenience but Windows is NOT an officially supported platform. Use at your own risk. Community contributions for Windows improvements are welcome.

## Prerequisites

### Required Tools

- [GoReleaser](https://goreleaser.com/install/) v2.0+
- [GitHub CLI](https://cli.github.com/) (for automated releases)
- Go 1.24+

### Optional Tools (for signing)

- [gon](https://github.com/mitchellh/gon) (macOS signing and notarization)
- signtool (Windows signing)

## Release Process

### 1. Prepare the Release

1. Ensure all changes are merged to main
2. Update CHANGELOG.md if needed (GoReleaser will auto-generate from commits)
3. Ensure all tests pass:

   ```bash
   go test -v ./...
   go vet ./...
   staticcheck ./...
   ```

### 2. Create a Release Tag

```bash
# Create and push a tag
git tag -a v0.3.14 -m "Release v0.3.14"
git push origin v0.3.14
```

The tag push will automatically trigger the GitHub Actions release workflow.

### 3. Manual Release (if needed)

If you need to run a release manually:

```bash
# Export GitHub token
export GITHUB_TOKEN="your-token-here"

# Run GoReleaser
goreleaser release --clean
```

### 4. Testing Releases

To test the release process without publishing:

```bash
# Create a snapshot release (doesn't publish)
goreleaser release --snapshot --clean

# Test a single platform build
goreleaser build --snapshot --clean --single-target
```

## GitHub Actions Workflow

The release workflow (`.github/workflows/release.yml`) is triggered automatically when:

- A tag matching `v*` is pushed
- Manually via workflow dispatch

The workflow:

1. Sets up the build environment
2. Runs GoReleaser to build all binaries
3. Creates GitHub release with artifacts
4. Updates Homebrew tap (if configured)
5. Signs binaries (if certificates are configured)

## Configuration Files

### `.goreleaser.yml`

Main GoReleaser configuration defining:

- Build targets and flags
- Archive formats
- Package formats (deb, rpm)
- Homebrew formula
- Release notes template

### `etc/gon-sign.hcl`

macOS signing configuration (requires Apple Developer certificates)

### `.github/workflows/release.yml`

GitHub Actions workflow for automated releases

## Setting Up Binary Signing

### macOS Signing - Detailed Instructions

#### Step 1: Get Apple Developer Account ($99/year)

1. Go to <https://developer.apple.com/programs/>
2. Click "Enroll" and follow the process
3. Use your existing Apple ID or create a new one
4. Complete identity verification (may take 24-48 hours)
5. Pay the $99 annual fee

#### Step 2: Create Developer ID Certificate

1. Once enrolled, go to <https://developer.apple.com/account>
2. Navigate to "Certificates, IDs & Profiles"
3. Click the "+" button to create a new certificate
4. Select "Developer ID Application" under "Software"
5. Follow the Certificate Signing Request (CSR) process:
   - Open Keychain Access on your Mac
   - Menu: Keychain Access → Certificate Assistant → Request a Certificate
   - Enter your email and name
   - Select "Saved to disk"
   - Save the CSR file
6. Upload the CSR file in the Apple Developer portal
7. Download the generated certificate
8. Double-click to install in Keychain Access

#### Step 3: Export Certificate for CI

1. Open Keychain Access
2. Find your "Developer ID Application: [Your Name]" certificate
3. Right-click and select "Export"
4. Save as .p12 format with a strong password
5. Convert to base64 for GitHub secrets:
   ```bash
   base64 -i certificate.p12 -o certificate_base64.txt
   ```

#### Step 4: Create App Store Connect API Key

1. Go to <https://appstoreconnect.apple.com/access/api>
2. Click the "+" button to generate a new API key
3. Name: "GoReleaser CI"
4. Access: "Developer" role
5. Download the .p8 file (IMPORTANT: Can only download once!)
6. Note these values:
   - Issuer ID (shown at the top of the API Keys page)
   - Key ID (shown in the key list)
7. Convert .p8 to base64:
   ```bash
   base64 -i AuthKey_XXXXX.p8 -o api_key_base64.txt
   ```

#### Step 5: Create App-Specific Password

1. Go to <https://appleid.apple.com/account/manage>
2. Sign in and go to "Security"
3. Under "App-Specific Passwords", click "Generate Password"
4. Label it "Litestream GoReleaser"
5. Save the generated password securely

#### Step 6: Configure GitHub Secrets

Go to GitHub repository Settings → Secrets and variables → Actions:

| Secret Name | How to Get It |
|------------|---------------|
| `MACOS_CERTIFICATE_P12` | Contents of certificate_base64.txt from Step 3 |
| `MACOS_CERTIFICATE_PASSWORD` | Password used when exporting .p12 in Step 3 |
| `APPLE_API_KEY_ID` | Key ID from Step 4 |
| `APPLE_API_ISSUER_ID` | Issuer ID from Step 4 |
| `APPLE_API_KEY_P8` | Contents of api_key_base64.txt from Step 4 |
| `AC_PASSWORD` | App-specific password from Step 5 |
| `APPLE_ID_USERNAME` | Your Apple ID email |
| `APPLE_TEAM_ID` | Find in Apple Developer account under Membership |
| `APPLE_DEVELOPER_ID` | Full certificate name (e.g., "Developer ID Application: Your Name (TEAMID)") |

#### Step 7: Enable in Workflow

Edit `.github/workflows/release.yml`:
- Find the `macos-sign` job
- Remove or change `if: ${{ false }}` to `if: true`

### Windows Signing (Optional - Unsupported Platform)

Since Windows is not officially supported, signing is optional.
If you choose to sign:

1. **Obtain Code Signing Certificate**
   - Purchase from DigiCert, Sectigo, or GlobalSign (~$200-500/year)
   - Or use Microsoft Trusted Signing (Azure-based)

2. **Configure GitHub Secrets**

   ```text
   WINDOWS_CERTIFICATE_PFX: Base64-encoded .pfx file
   WINDOWS_CERTIFICATE_PASSWORD: Certificate password
   ```

3. **Enable in workflow**
   - Remove `if: ${{ false }}` from windows-sign job in release.yml

## Homebrew Tap Setup (Required for macOS Distribution)

### Step 1: Create the Tap Repository

Run the provided script or manually create the repository:

```bash
./scripts/setup-homebrew-tap.sh
```

Or manually:
1. Create a new repository named `homebrew-litestream` under the `benbjohnson` account
2. Make it public
3. Add a README and Formula directory

### Step 2: Create GitHub Personal Access Token

1. Go to <https://github.com/settings/tokens/new>
2. Name: "Litestream Homebrew Tap"
3. Expiration: No expiration (or 1 year if you prefer)
4. Select scopes:
   - `repo` (Full control of private repositories)
   - This allows GoReleaser to push formula updates
5. Click "Generate token"
6. Copy the token immediately (won't be shown again)

### Step 3: Add Token to Repository Secrets

1. Go to Litestream repository settings
2. Navigate to Settings → Secrets and variables → Actions
3. Click "New repository secret"
4. Name: `HOMEBREW_TAP_GITHUB_TOKEN`
5. Value: Paste the token from Step 2

### Step 4: Test Installation

After the first release:

```bash
brew tap benbjohnson/litestream
brew install litestream
```

## Troubleshooting

### Common Issues

#### Build fails with "version: 0" error

- Ensure `.goreleaser.yml` starts with `version: 2`

#### Homebrew formula not updated

- Check HOMEBREW_TAP_GITHUB_TOKEN secret is set
- Verify tap repository exists and is accessible

#### macOS binary rejected by Gatekeeper

- Ensure signing certificates are valid
- Check notarization completed successfully
- Verify AC_PASSWORD is an app-specific password

#### Windows SmartScreen warning

- This is expected for unsigned binaries
- Consider signing if distributing widely (though platform is unsupported)

### Testing Local Builds

```bash
# Test specific platform
GOOS=linux GOARCH=arm64 goreleaser build --snapshot --clean --single-target

# Check configuration
goreleaser check

# Dry run (no upload)
goreleaser release --skip=publish --clean
```

## Migration from Manual Process

The old manual release process using Makefile targets and individual workflows has been replaced by GoReleaser. The following are deprecated:

- `make dist-linux`
- `make dist-macos`
- `.github/workflows/release.linux.yml`
- Manual gon invocation for macOS

## Support and Issues

For release process issues:

- Check GoReleaser documentation: <https://goreleaser.com/>
- File issues at: <https://github.com/benbjohnson/litestream/issues>

Remember: Windows binaries are provided as-is without official support.
