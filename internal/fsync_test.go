package internal_test

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/benbjohnson/litestream/internal"
)

func TestFsyncDir(t *testing.T) {
	t.Parallel()
	if err := internal.FsyncDir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestFsyncDir_NotExist(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory sync is a no-op on Windows")
	}
	t.Parallel()
	path := filepath.Join(t.TempDir(), "missing")
	if err := internal.FsyncDir(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected not-exist error, got: %v", err)
	}
}
