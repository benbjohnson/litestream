//go:build vfs
// +build vfs

package sqlite3vfs

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestRegisterVFS_FilenameURIParameters(t *testing.T) {
	vfsName := fmt.Sprintf("uri-capture-%s", strings.ReplaceAll(t.Name(), "/", "-"))
	vfs := &capturingVFS{}
	if err := RegisterVFS(vfsName, vfs); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", fmt.Sprintf("file:test.db?vfs=%s&poll_interval=5s&cache_size=128&_busy_timeout=10", vfsName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err == nil {
		t.Fatal("expected open failure")
	}

	got, ok := vfs.Filename()
	if !ok {
		t.Fatal("expected captured filename")
	}
	if got.String() != "test.db" {
		t.Fatalf("filename=%q, want %q", got.String(), "test.db")
	}
	if value, ok := got.URIParameter("poll_interval"); !ok || value != "5s" {
		t.Fatalf("poll_interval=%q, %v", value, ok)
	}
	if value, ok := got.URIParameter("cache_size"); !ok || value != "128" {
		t.Fatalf("cache_size=%q, %v", value, ok)
	}
	if value, ok := got.URIParameter("_busy_timeout"); !ok || value != "10" {
		t.Fatalf("_busy_timeout=%q, %v", value, ok)
	}
}

type capturingVFS struct {
	mu       sync.Mutex
	filename Filename
	captured bool
}

func (vfs *capturingVFS) Open(name string, flags OpenFlag) (File, OpenFlag, error) {
	return nil, flags, CantOpenError
}

func (vfs *capturingVFS) OpenFilename(name Filename, flags OpenFlag) (File, OpenFlag, error) {
	vfs.mu.Lock()
	if !vfs.captured {
		vfs.filename = name
		vfs.captured = true
	}
	vfs.mu.Unlock()
	return nil, flags, CantOpenError
}

func (vfs *capturingVFS) Delete(name string, dirSync bool) error {
	return nil
}

func (vfs *capturingVFS) Access(name string, flags AccessFlag) (bool, error) {
	return false, nil
}

func (vfs *capturingVFS) FullPathname(name string) string {
	return name
}

func (vfs *capturingVFS) Filename() (Filename, bool) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	return vfs.filename, vfs.captured
}
