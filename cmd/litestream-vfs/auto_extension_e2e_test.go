//go:build vfs
// +build vfs

package main_test

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benbjohnson/litestream/file"
)

// TestAutoExtension_E2E_OrdinaryConnectionStillOpens loads the extension, reads
// the replica through the VFS, then opens an ordinary database in the same
// process. Loading registers an auto-extension that SQLite runs on every later
// sqlite3_open(), and its return value is that open's status: it must report
// SQLITE_OK for a connection that is not on the litestream VFS, or every other
// database the process opens fails with "automatic extension loading failed".
func TestAutoExtension_E2E_OrdinaryConnectionStillOpens(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("skipping: test only runs on darwin or linux")
	}

	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("skipping: sqlite3 CLI not found in PATH")
	}

	extPath := buildVFSExtension(t)

	replicaDir := t.TempDir()
	setupTestReplica(t, file.NewReplicaClient(replicaDir))

	// The entry point is named explicitly because SQLite otherwise derives it
	// from the file name, and the build outputs are arch-suffixed. Each .open is
	// a fresh sqlite3_open(), which is what runs the auto-extension.
	cmd := exec.Command("sqlite3", ":memory:",
		"-cmd", ".load "+extPath+" sqlite3_litestreamvfs_init",
		"-cmd", ".open file:replica.db?vfs=litestream",
		"-cmd", "SELECT 'vfs ok ' || litestream_txid() || ' ' || name FROM users;",
		"-cmd", ".open :memory:",
		"SELECT 'second connection ok';",
	)
	cmd.Env = append(os.Environ(), "LITESTREAM_REPLICA_URL=file://"+replicaDir)

	output, err := cmd.CombinedOutput()
	outputStr := string(output)
	// A shell that cannot load extensions says so on the .load and, without
	// -bail, carries on, so this cannot be gated on err.
	if strings.Contains(outputStr, "Error: unknown command") ||
		strings.Contains(outputStr, "not authorized") ||
		strings.Contains(outputStr, "symbol not found") ||
		strings.Contains(outputStr, "dlsym") {
		t.Skipf("skipping: sqlite3 cannot load extensions (common on macOS): %s", outputStr)
	}
	require.NoError(t, err, "sqlite3 output: %s", outputStr)
	require.Contains(t, outputStr, "vfs ok ", "the VFS connection should get the litestream functions and read the replica")
	require.Contains(t, outputStr, "Alice", "the VFS connection should read the replica")
	require.Contains(t, outputStr, "second connection ok", "an ordinary connection opened after the load should work")
}
