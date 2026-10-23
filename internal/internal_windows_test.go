package internal

import (
	"path/filepath"
	"testing"
)

func TestFsyncDir_Windows(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, path := range []string{dir, filepath.Join(dir, "missing")} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			if err := FsyncDir(path); err != nil {
				t.Fatalf("FsyncDir(%q): %v", path, err)
			}
		})
	}
}
