//go:build vfs
// +build vfs

package main_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
	"github.com/stretchr/testify/require"
)

// TestHydration_E2E_SQLiteCLI tests hydration environment variables via the SQLite CLI.
// This test builds the VFS extension and uses the actual sqlite3 CLI to verify
// that LITESTREAM_HYDRATION_ENABLED and LITESTREAM_HYDRATION_PATH work correctly.
func TestHydration_E2E_SQLiteCLI(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("skipping: test only runs on darwin or linux")
	}

	// Check if sqlite3 CLI is available
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("skipping: sqlite3 CLI not found in PATH")
	}

	// Build the VFS extension
	extPath := buildVFSExtension(t)

	// Create a file replica with test data
	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)
	setupTestReplica(t, client)

	// Create a temp file for hydration output
	hydrationPath := filepath.Join(t.TempDir(), "hydrated.db")

	// Run sqlite3 with hydration enabled
	env := []string{
		"LITESTREAM_REPLICA_URL=file://" + replicaDir,
		"LITESTREAM_HYDRATION_ENABLED=true",
		"LITESTREAM_HYDRATION_PATH=" + hydrationPath,
		"LITESTREAM_LOG_LEVEL=DEBUG",
	}

	// Query via the VFS
	output := runSQLiteCLI(t, extPath, env, "SELECT name FROM users WHERE id = 1;")
	require.Contains(t, output, "Alice", "should read data via VFS")

	// Verify hydration file was created
	require.Eventually(t, func() bool {
		info, err := os.Stat(hydrationPath)
		return err == nil && info.Size() > 0
	}, 5*time.Second, 100*time.Millisecond, "hydration file should be created")
}

// TestHydration_E2E_SQLiteCLI_TempFile tests hydration without specifying a path (uses temp file).
func TestHydration_E2E_SQLiteCLI_TempFile(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("skipping: test only runs on darwin or linux")
	}

	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("skipping: sqlite3 CLI not found in PATH")
	}

	extPath := buildVFSExtension(t)

	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)
	setupTestReplica(t, client)

	// Run without LITESTREAM_HYDRATION_PATH - should use temp file
	env := []string{
		"LITESTREAM_REPLICA_URL=file://" + replicaDir,
		"LITESTREAM_HYDRATION_ENABLED=true",
		"LITESTREAM_LOG_LEVEL=DEBUG",
	}

	output := runSQLiteCLI(t, extPath, env, "SELECT COUNT(*) FROM users;")
	require.Contains(t, output, "1", "should read data via VFS with temp hydration file")
}

// TestHydration_E2E_SQLiteCLI_Disabled tests that hydration is disabled by default.
func TestHydration_E2E_SQLiteCLI_Disabled(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("skipping: test only runs on darwin or linux")
	}

	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("skipping: sqlite3 CLI not found in PATH")
	}

	extPath := buildVFSExtension(t)

	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)
	setupTestReplica(t, client)

	hydrationPath := filepath.Join(t.TempDir(), "should-not-exist.db")

	// Run without LITESTREAM_HYDRATION_ENABLED
	env := []string{
		"LITESTREAM_REPLICA_URL=file://" + replicaDir,
		"LITESTREAM_HYDRATION_PATH=" + hydrationPath,
		"LITESTREAM_LOG_LEVEL=DEBUG",
	}

	output := runSQLiteCLI(t, extPath, env, "SELECT name FROM users WHERE id = 1;")
	require.Contains(t, output, "Alice", "should still read data via VFS")

	// Hydration file should NOT be created when disabled
	_, err := os.Stat(hydrationPath)
	require.True(t, os.IsNotExist(err), "hydration file should not be created when disabled")
}

// TestHydration_E2E_SQLiteCLI_MultipleQueries tests that hydration persists across queries.
func TestHydration_E2E_SQLiteCLI_MultipleQueries(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("skipping: test only runs on darwin or linux")
	}

	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("skipping: sqlite3 CLI not found in PATH")
	}

	extPath := buildVFSExtension(t)

	replicaDir := t.TempDir()
	client := file.NewReplicaClient(replicaDir)
	setupTestReplicaWithMoreData(t, client)

	hydrationPath := filepath.Join(t.TempDir(), "hydrated.db")

	env := []string{
		"LITESTREAM_REPLICA_URL=file://" + replicaDir,
		"LITESTREAM_HYDRATION_ENABLED=true",
		"LITESTREAM_HYDRATION_PATH=" + hydrationPath,
		"LITESTREAM_LOG_LEVEL=DEBUG",
	}

	// Run multiple queries in single session
	queries := `
SELECT COUNT(*) FROM users;
SELECT name FROM users WHERE id = 1;
SELECT name FROM users WHERE id = 5;
`
	output := runSQLiteCLI(t, extPath, env, queries)
	require.Contains(t, output, "10", "should have 10 users")
	require.Contains(t, output, "Alice", "should find Alice")
	require.Contains(t, output, "User5", "should find User5")

	// Wait for hydration to complete
	require.Eventually(t, func() bool {
		info, err := os.Stat(hydrationPath)
		return err == nil && info.Size() > 0
	}, 5*time.Second, 100*time.Millisecond, "hydration file should be created")
}

// buildVFSExtension builds the VFS extension and returns its path.
func buildVFSExtension(t *testing.T) string {
	t.Helper()

	// Determine expected extension filename based on OS
	var extName string
	switch runtime.GOOS {
	case "darwin":
		extName = "litestream-vfs.dylib"
	case "linux":
		extName = "litestream-vfs.so"
	default:
		t.Fatalf("unsupported OS: %s", runtime.GOOS)
	}

	// Check if extension already exists in dist/
	projectRoot := findProjectRoot(t)
	extPath := filepath.Join(projectRoot, "dist", extName)

	if _, err := os.Stat(extPath); err == nil {
		return extPath
	}

	// Build the extension
	t.Logf("building VFS extension at %s", extPath)

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
	}

	cmd := exec.Command("make", makeTarget)
	cmd.Dir = projectRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to build VFS extension: %v", err)
	}

	return extPath
}

// findProjectRoot finds the project root directory.
func findProjectRoot(t *testing.T) string {
	t.Helper()

	// Start from current directory and walk up
	dir, err := os.Getwd()
	require.NoError(t, err)

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

// runSQLiteCLI runs the sqlite3 CLI with the VFS extension and returns output.
func runSQLiteCLI(t *testing.T, extPath string, env []string, query string) string {
	t.Helper()

	// Build command: sqlite3 :memory: -cmd ".load <ext>" "<query>"
	args := []string{
		":memory:",
		"-cmd", ".load " + extPath,
		query,
	}

	cmd := exec.Command("sqlite3", args...)
	cmd.Env = append(os.Environ(), env...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		outputStr := string(output)
		// Check for common extension loading failures
		if strings.Contains(outputStr, "Error: unknown command") ||
			strings.Contains(outputStr, "not authorized") ||
			strings.Contains(outputStr, "symbol not found") ||
			strings.Contains(outputStr, "dlsym") {
			t.Skipf("skipping: sqlite3 cannot load extensions (common on macOS): %s", outputStr)
		}
		t.Logf("sqlite3 output: %s", outputStr)
		t.Fatalf("sqlite3 command failed: %v", err)
	}

	return string(output)
}

// setupTestReplica creates a file replica with test data.
func setupTestReplica(t *testing.T, client litestream.ReplicaClient) {
	t.Helper()

	dbDir := t.TempDir()
	db := testingutil.NewDB(t, filepath.Join(dbDir, "source.db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.SyncInterval = 100 * time.Millisecond
	require.NoError(t, db.Open())

	sqldb := testingutil.MustOpenSQLDB(t, db.Path())

	_, err := sqldb.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)
	_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	require.NoError(t, err)

	waitForLTXFiles(t, client, 10*time.Second, db.MonitorInterval)

	require.NoError(t, db.Replica.Stop(false))
	testingutil.MustCloseSQLDB(t, sqldb)
	require.NoError(t, db.Close(context.Background()))
}

// setupTestReplicaWithMoreData creates a file replica with more test data.
func setupTestReplicaWithMoreData(t *testing.T, client litestream.ReplicaClient) {
	t.Helper()

	dbDir := t.TempDir()
	db := testingutil.NewDB(t, filepath.Join(dbDir, "source.db"))
	db.MonitorInterval = 100 * time.Millisecond
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = client
	db.Replica.SyncInterval = 100 * time.Millisecond
	require.NoError(t, db.Open())

	sqldb := testingutil.MustOpenSQLDB(t, db.Path())

	_, err := sqldb.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	require.NoError(t, err)

	// Insert 10 users
	_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	require.NoError(t, err)
	for i := 2; i <= 10; i++ {
		_, err = sqldb.Exec("INSERT INTO users (id, name) VALUES (?, ?)", i, fmt.Sprintf("User%d", i))
		require.NoError(t, err)
	}

	waitForLTXFiles(t, client, 10*time.Second, db.MonitorInterval)

	require.NoError(t, db.Replica.Stop(false))
	testingutil.MustCloseSQLDB(t, sqldb)
	require.NoError(t, db.Close(context.Background()))
}
