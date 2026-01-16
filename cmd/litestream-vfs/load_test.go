//go:build vfs
// +build vfs

package main_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestVFS_Load tests that the VFS extension can be loaded into sqlite3 without crashing.
// This is a critical test that catches issues like the ARM64 segfault caused by
// uninitialized sqlite3_api pointers.
func TestVFS_Load(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("skipping: test only runs on darwin or linux")
	}

	// Check if sqlite3 CLI is available
	sqlite3Path, err := exec.LookPath("sqlite3")
	if err != nil {
		t.Skip("skipping: sqlite3 CLI not found in PATH")
	}

	// Check if sqlite3 supports loading extensions
	if !sqlite3SupportsExtensions(t, sqlite3Path) {
		t.Skip("skipping: sqlite3 does not support loading extensions")
	}

	// Build the VFS extension
	extPath := buildVFSExtensionForLoad(t)

	// Try to load the extension - it should return an error about missing
	// LITESTREAM_REPLICA_URL, but it should NOT segfault.
	output, err := loadExtension(t, sqlite3Path, extPath)

	// The extension should load and return an error about missing env var.
	// A segfault would cause a different error (signal: segmentation fault).
	if err != nil {
		outputStr := string(output)

		// Check for segfault - this is what we're trying to prevent
		if strings.Contains(err.Error(), "signal:") ||
			strings.Contains(outputStr, "Segmentation fault") ||
			strings.Contains(outputStr, "SIGSEGV") {
			t.Fatalf("extension loading caused a crash: %v\nOutput: %s", err, outputStr)
		}

		// Expected error: missing LITESTREAM_REPLICA_URL
		if strings.Contains(outputStr, "LITESTREAM_REPLICA_URL") {
			t.Logf("extension loaded successfully (expected env var error): %s", outputStr)
			return
		}

		// Check for extension loading not supported
		if strings.Contains(outputStr, "not authorized") ||
			strings.Contains(outputStr, "unknown command") ||
			strings.Contains(outputStr, "symbol not found") ||
			strings.Contains(outputStr, "dlsym") {
			t.Skipf("skipping: sqlite3 cannot load extensions: %s", outputStr)
		}

		// Some other error - might be acceptable
		t.Logf("extension load returned error (not a crash): %v\nOutput: %s", err, outputStr)
	} else {
		t.Log("extension loaded successfully with no error")
	}
}

// TestVFS_LoadWithReplicaURL tests loading with a valid replica URL.
func TestVFS_LoadWithReplicaURL(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("skipping: test only runs on darwin or linux")
	}

	sqlite3Path, err := exec.LookPath("sqlite3")
	if err != nil {
		t.Skip("skipping: sqlite3 CLI not found in PATH")
	}

	if !sqlite3SupportsExtensions(t, sqlite3Path) {
		t.Skip("skipping: sqlite3 does not support loading extensions")
	}

	extPath := buildVFSExtensionForLoad(t)

	// Create a temporary directory for the replica
	replicaDir := t.TempDir()

	// Load extension with a file:// replica URL
	env := []string{
		"LITESTREAM_REPLICA_URL=file://" + replicaDir,
	}

	output, err := loadExtensionWithEnv(t, sqlite3Path, extPath, env)
	outputStr := string(output)

	// Check for segfault
	if err != nil {
		if strings.Contains(err.Error(), "signal:") ||
			strings.Contains(outputStr, "Segmentation fault") {
			t.Fatalf("extension loading caused a crash: %v\nOutput: %s", err, outputStr)
		}
	}

	// Extension should load - verify by running a simple query
	output, err = loadExtensionAndQuery(t, sqlite3Path, extPath, env, "SELECT 1;")
	if err != nil {
		outputStr = string(output)
		if strings.Contains(outputStr, "not authorized") ||
			strings.Contains(outputStr, "symbol not found") {
			t.Skipf("skipping: sqlite3 cannot load extensions: %s", outputStr)
		}
		// Don't fail on other errors - the VFS might not find data
		t.Logf("query returned error (not a crash): %v\nOutput: %s", err, outputStr)
	} else {
		if !strings.Contains(string(output), "1") {
			t.Errorf("expected output to contain '1', got: %s", output)
		}
	}
}

// sqlite3SupportsExtensions checks if the sqlite3 binary can load extensions.
func sqlite3SupportsExtensions(t *testing.T, sqlite3Path string) bool {
	t.Helper()

	// Try to run .load with a non-existent file - if extensions are disabled,
	// we'll get "unknown command" or "not authorized" error
	cmd := exec.Command(sqlite3Path, ":memory:", "-cmd", ".load /nonexistent/path")
	output, _ := cmd.CombinedOutput()
	outputStr := string(output)

	// These errors indicate extensions are disabled
	if strings.Contains(outputStr, "unknown command") ||
		strings.Contains(outputStr, "not authorized") {
		return false
	}

	return true
}

// loadExtension attempts to load the VFS extension into sqlite3.
func loadExtension(t *testing.T, sqlite3Path, extPath string) ([]byte, error) {
	t.Helper()

	cmd := exec.Command(sqlite3Path, ":memory:",
		"-cmd", ".load "+extPath+" sqlite3_litestreamvfs_init",
		"SELECT 'loaded';",
	)

	return cmd.CombinedOutput()
}

// loadExtensionWithEnv loads the extension with custom environment variables.
func loadExtensionWithEnv(t *testing.T, sqlite3Path, extPath string, env []string) ([]byte, error) {
	t.Helper()

	cmd := exec.Command(sqlite3Path, ":memory:",
		"-cmd", ".load "+extPath+" sqlite3_litestreamvfs_init",
		"SELECT 'loaded';",
	)
	cmd.Env = append(os.Environ(), env...)

	return cmd.CombinedOutput()
}

// loadExtensionAndQuery loads the extension and runs a query.
func loadExtensionAndQuery(t *testing.T, sqlite3Path, extPath string, env []string, query string) ([]byte, error) {
	t.Helper()

	cmd := exec.Command(sqlite3Path, ":memory:",
		"-cmd", ".load "+extPath+" sqlite3_litestreamvfs_init",
		query,
	)
	cmd.Env = append(os.Environ(), env...)

	return cmd.CombinedOutput()
}

// buildVFSExtensionForLoad builds the VFS extension and returns its path.
func buildVFSExtensionForLoad(t *testing.T) string {
	t.Helper()

	projectRoot := findProjectRootForLoad(t)

	// Determine the expected extension path based on OS and architecture
	var extPath string
	var makeTarget string

	switch runtime.GOOS {
	case "darwin":
		if runtime.GOARCH == "arm64" {
			makeTarget = "vfs-darwin-arm64"
			extPath = filepath.Join(projectRoot, "dist", "litestream-vfs-darwin-arm64.dylib")
		} else {
			makeTarget = "vfs-darwin-amd64"
			extPath = filepath.Join(projectRoot, "dist", "litestream-vfs-darwin-amd64.dylib")
		}
	case "linux":
		if runtime.GOARCH == "arm64" {
			makeTarget = "vfs-linux-arm64"
			extPath = filepath.Join(projectRoot, "dist", "litestream-vfs-linux-arm64.so")
		} else {
			makeTarget = "vfs-linux-amd64"
			extPath = filepath.Join(projectRoot, "dist", "litestream-vfs-linux-amd64.so")
		}
	default:
		t.Fatalf("unsupported OS: %s", runtime.GOOS)
	}

	// Check if extension already exists
	if _, err := os.Stat(extPath); err == nil {
		t.Logf("using existing VFS extension: %s", extPath)
		return extPath
	}

	// Build the extension
	t.Logf("building VFS extension with 'make %s'", makeTarget)

	cmd := exec.Command("make", makeTarget)
	cmd.Dir = projectRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to build VFS extension: %v", err)
	}

	// Verify it was created
	if _, err := os.Stat(extPath); err != nil {
		t.Fatalf("VFS extension not found after build: %s", extPath)
	}

	return extPath
}

// findProjectRootForLoad finds the project root directory.
func findProjectRootForLoad(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find project root (go.mod)")
		}
		dir = parent
	}
}
