//go:build vfs
// +build vfs

package litestream

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

// writeTestReplicaClient is a mock ReplicaClient for testing write functionality.
type writeTestReplicaClient struct {
	mu       sync.Mutex
	ltxFiles map[int][]*ltx.FileInfo // level -> files
	ltxData  map[string][]byte       // "level/minTXID-maxTXID" -> data
}

func newWriteTestReplicaClient() *writeTestReplicaClient {
	return &writeTestReplicaClient{
		ltxFiles: make(map[int][]*ltx.FileInfo),
		ltxData:  make(map[string][]byte),
	}
}

func (c *writeTestReplicaClient) Type() string { return "test" }

func (c *writeTestReplicaClient) Init(ctx context.Context) error { return nil }

func (c *writeTestReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var files []*ltx.FileInfo
	for _, f := range c.ltxFiles[level] {
		if f.MinTXID >= seek {
			files = append(files, f)
		}
	}
	return &writeTestFileIterator{files: files}, nil
}

func (c *writeTestReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := ltxKey(level, minTXID, maxTXID)
	data, ok := c.ltxData[key]
	if !ok {
		return nil, io.EOF
	}

	if offset > 0 || size > 0 {
		end := int64(len(data))
		if size > 0 && offset+size < end {
			end = offset + size
		}
		data = data[offset:end]
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (c *writeTestReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	key := ltxKey(level, minTXID, maxTXID)
	c.ltxData[key] = data

	info := &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		CreatedAt: time.Now(),
		Size:      int64(len(data)),
	}
	c.ltxFiles[level] = append(c.ltxFiles[level], info)

	return info, nil
}

func (c *writeTestReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	return nil
}

func (c *writeTestReplicaClient) DeleteAll(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ltxFiles = make(map[int][]*ltx.FileInfo)
	c.ltxData = make(map[string][]byte)
	return nil
}

func ltxKey(level int, minTXID, maxTXID ltx.TXID) string {
	return string(rune(level)) + "/" + minTXID.String() + "-" + maxTXID.String()
}

// writeTestFileIterator implements ltx.FileIterator for testing.
type writeTestFileIterator struct {
	files []*ltx.FileInfo
	index int
}

func (itr *writeTestFileIterator) Next() bool {
	if itr.index >= len(itr.files) {
		return false
	}
	itr.index++
	return true
}

func (itr *writeTestFileIterator) Item() *ltx.FileInfo {
	if itr.index == 0 || itr.index > len(itr.files) {
		return nil
	}
	return itr.files[itr.index-1]
}

func (itr *writeTestFileIterator) Close() error {
	return nil
}

func (itr *writeTestFileIterator) Err() error {
	return nil
}

// createTestLTXFile creates an LTX file with initial data for testing.
func createTestLTXFile(t *testing.T, client *writeTestReplicaClient, txid ltx.TXID, pageSize uint32, commit uint32, pages map[uint32][]byte) {
	t.Helper()

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}

	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    commit,
		MinTXID:   txid,
		MaxTXID:   txid,
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}

	// Sort page numbers to ensure proper encoding order (page 1 must be first for snapshots)
	pgnos := make([]uint32, 0, len(pages))
	for pgno := range pages {
		pgnos = append(pgnos, pgno)
	}
	sort.Slice(pgnos, func(i, j int) bool { return pgnos[i] < pgnos[j] })

	for _, pgno := range pgnos {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, pages[pgno]); err != nil {
			t.Fatal(err)
		}
	}

	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	client.mu.Lock()
	key := ltxKey(0, txid, txid)
	client.ltxData[key] = buf.Bytes()
	client.ltxFiles[0] = append(client.ltxFiles[0], &ltx.FileInfo{
		Level:     0,
		MinTXID:   txid,
		MaxTXID:   txid,
		CreatedAt: time.Now(),
		Size:      int64(buf.Len()),
	})
	client.mu.Unlock()
}

// setupWriteableVFSFile creates a VFSFile with write support enabled and a buffer file.
func setupWriteableVFSFile(t *testing.T, client *writeTestReplicaClient) *VFSFile {
	t.Helper()

	logger := slog.Default()
	f := NewVFSFile(client, "test.db", logger)
	f.writeEnabled = true
	f.dirty = make(map[uint32]int64)
	f.syncInterval = 0

	// Create a temporary buffer file
	tmpFile, err := os.CreateTemp("", "litestream-test-buffer-*")
	if err != nil {
		t.Fatal(err)
	}
	f.bufferFile = tmpFile
	f.bufferPath = tmpFile.Name()
	f.bufferNextOff = 0

	t.Cleanup(func() {
		if f.bufferFile != nil {
			f.bufferFile.Close()
		}
		os.Remove(f.bufferPath)
	})

	return f
}

func TestVFSFile_WriteEnabled(t *testing.T) {
	client := newWriteTestReplicaClient()

	// Create initial LTX file with page 1
	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	copy(initialPage, "initial data")
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	// Create VFSFile directly with write enabled
	tmpDir := t.TempDir()
	bufferPath := tmpDir + "/write-buffer"

	logger := slog.Default()
	f := NewVFSFile(client, "test.db", logger)
	f.writeEnabled = true
	f.dirty = make(map[uint32]int64)
	f.syncInterval = 0
	f.bufferPath = bufferPath

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if !f.writeEnabled {
		t.Error("expected writeEnabled to be true")
	}

	if f.dirty == nil {
		t.Error("expected dirty map to be initialized")
	}
}

func TestVFSFile_WriteAt(t *testing.T) {
	client := newWriteTestReplicaClient()

	// Create initial LTX file
	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	copy(initialPage, "initial data")
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	// Create VFSFile with write support
	f := setupWriteableVFSFile(t, client)

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Write some data at offset 100 (within page 1)
	writeData := []byte("hello world")
	n, err := f.WriteAt(writeData, 100)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(writeData) {
		t.Errorf("expected %d bytes written, got %d", len(writeData), n)
	}

	// Check dirty page exists
	if len(f.dirty) != 1 {
		t.Errorf("expected 1 dirty page, got %d", len(f.dirty))
	}
	if _, ok := f.dirty[1]; !ok {
		t.Error("expected page 1 to be dirty")
	}

	// Read back the written data
	readBuf := make([]byte, len(writeData))
	n, err = f.ReadAt(readBuf, 100)
	if err != nil {
		t.Fatal(err)
	}
	if string(readBuf) != string(writeData) {
		t.Errorf("expected %q, got %q", writeData, readBuf)
	}
}

func TestVFSFile_SyncToRemote(t *testing.T) {
	client := newWriteTestReplicaClient()

	// Create initial LTX file
	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	// Create VFSFile with write support
	f := setupWriteableVFSFile(t, client)

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Write data
	writeData := []byte("synced data")
	if _, err := f.WriteAt(writeData, 0); err != nil {
		t.Fatal(err)
	}

	// Sync to remote
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}

	// Check dirty pages are cleared
	if len(f.dirty) != 0 {
		t.Errorf("expected 0 dirty pages after sync, got %d", len(f.dirty))
	}

	// Check TXID advanced
	if f.expectedTXID != 2 {
		t.Errorf("expected TXID 2, got %d", f.expectedTXID)
	}

	// Check LTX file was written to client
	client.mu.Lock()
	if len(client.ltxFiles[0]) != 2 {
		t.Errorf("expected 2 LTX files, got %d", len(client.ltxFiles[0]))
	}
	client.mu.Unlock()
}

func TestVFSFile_ConflictDetection(t *testing.T) {
	client := newWriteTestReplicaClient()

	// Create initial LTX file
	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	// Create VFSFile with write support
	f := setupWriteableVFSFile(t, client)

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Write data
	if _, err := f.WriteAt([]byte("data"), 0); err != nil {
		t.Fatal(err)
	}

	// Simulate remote advancement (another writer)
	createTestLTXFile(t, client, 2, pageSize, 1, map[uint32][]byte{1: initialPage})

	// Try to sync - should fail with conflict
	err := f.Sync(0)
	if err == nil {
		t.Fatal("expected conflict error")
	}
	if err.Error() != "remote has newer transactions than expected: expected TXID 1 but remote has 2" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestVFSFile_TransactionTracking(t *testing.T) {
	client := newWriteTestReplicaClient()

	// Create initial LTX file
	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	// Create VFSFile with write support
	f := setupWriteableVFSFile(t, client)

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Acquire RESERVED lock (start transaction)
	if err := f.Lock(2); err != nil { // sqlite3vfs.LockReserved = 2
		t.Fatal(err)
	}

	if !f.inTransaction {
		t.Error("expected inTransaction to be true after RESERVED lock")
	}

	// Write data
	if _, err := f.WriteAt([]byte("tx data"), 0); err != nil {
		t.Fatal(err)
	}

	// Sync should be skipped during transaction
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	if len(f.dirty) == 0 {
		t.Error("expected dirty pages to remain during transaction")
	}

	// Release lock (end transaction)
	if err := f.Unlock(1); err != nil { // sqlite3vfs.LockShared = 1
		t.Fatal(err)
	}

	if f.inTransaction {
		t.Error("expected inTransaction to be false after unlock")
	}

	// Now sync should work
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}
	if len(f.dirty) != 0 {
		t.Error("expected dirty pages to be cleared after sync")
	}
}

func TestVFSFile_Truncate(t *testing.T) {
	client := newWriteTestReplicaClient()

	// Create initial LTX files with 2 pages
	pageSize := uint32(4096)
	page1 := make([]byte, pageSize)
	page2 := make([]byte, pageSize)
	createTestLTXFile(t, client, 1, pageSize, 2, map[uint32][]byte{1: page1, 2: page2})

	// Create VFSFile with write support
	f := setupWriteableVFSFile(t, client)

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Write to page 2
	if _, err := f.WriteAt([]byte("page2 data"), int64(pageSize)); err != nil {
		t.Fatal(err)
	}

	// Truncate to 1 page
	if err := f.Truncate(int64(pageSize)); err != nil {
		t.Fatal(err)
	}

	// Page 2 should no longer be dirty
	if _, ok := f.dirty[2]; ok {
		t.Error("expected page 2 to be removed from dirty pages")
	}

	// Commit should be 1
	if f.commit != 1 {
		t.Errorf("expected commit 1, got %d", f.commit)
	}
}

func TestVFSFile_WriteBuffer(t *testing.T) {
	client := newWriteTestReplicaClient()

	// Create initial LTX file
	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	copy(initialPage, "initial data")
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	// Create temp directory for buffer
	tmpDir := t.TempDir()
	bufferPath := tmpDir + "/.litestream-write-buffer"

	// Create VFSFile with write buffer
	logger := slog.Default()
	f := NewVFSFile(client, "test.db", logger)
	f.writeEnabled = true
	f.dirty = make(map[uint32]int64)
	f.syncInterval = 0
	f.bufferPath = bufferPath

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}

	// Write some data
	writeData := []byte("buffered data")
	if _, err := f.WriteAt(writeData, 0); err != nil {
		t.Fatal(err)
	}

	// Check buffer file exists and has content
	stat, err := os.Stat(bufferPath)
	if err != nil {
		t.Fatalf("buffer file should exist: %v", err)
	}
	if stat.Size() == 0 {
		t.Error("buffer file should not be empty")
	}

	// Don't call f.Close() - simulate a crash by just abandoning the file handle
	// Close just the buffer file directly to release the handle
	if f.bufferFile != nil {
		f.bufferFile.Close()
	}
	f.cancel() // Stop any goroutines

	// Verify buffer file still has content (simulating crash before sync)
	stat, err = os.Stat(bufferPath)
	if err != nil {
		t.Fatalf("buffer file should still exist after crash: %v", err)
	}
	if stat.Size() == 0 {
		t.Error("buffer file should still have content after crash")
	}
}

func TestVFSFile_WriteBufferDiscardedOnOpen(t *testing.T) {
	// Test that unsync'd buffer contents are discarded on open (no recovery)
	client := newWriteTestReplicaClient()

	// Create initial LTX file
	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	copy(initialPage, "initial data")
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	// Create temp directory for buffer
	tmpDir := t.TempDir()
	bufferPath := tmpDir + "/.litestream-write-buffer"

	// First: create a VFSFile and write some data
	logger := slog.Default()
	f1 := NewVFSFile(client, "test.db", logger)
	f1.writeEnabled = true
	f1.dirty = make(map[uint32]int64)
	f1.syncInterval = 0
	f1.bufferPath = bufferPath

	if err := f1.Open(); err != nil {
		t.Fatal(err)
	}

	// Write data (will be written to buffer)
	writeData := make([]byte, pageSize)
	copy(writeData, "unsync'd data that should be lost")
	if _, err := f1.WriteAt(writeData, 0); err != nil {
		t.Fatal(err)
	}

	// Simulate crash by abandoning the file handle without syncing
	if f1.bufferFile != nil {
		f1.bufferFile.Close()
	}
	f1.cancel()

	// Second: create a new VFSFile - buffer should be discarded
	f2 := NewVFSFile(client, "test.db", logger)
	f2.writeEnabled = true
	f2.dirty = make(map[uint32]int64)
	f2.syncInterval = 0
	f2.bufferPath = bufferPath

	if err := f2.Open(); err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	// Dirty pages should NOT be recovered - buffer is discarded on open
	if len(f2.dirty) != 0 {
		t.Errorf("expected 0 dirty pages (buffer should be discarded), got %d", len(f2.dirty))
	}

	// Reading should return original data from replica, not unsync'd data
	readBuf := make([]byte, pageSize)
	if _, err := f2.ReadAt(readBuf, 0); err != nil {
		t.Fatal(err)
	}
	if string(readBuf[:12]) != "initial data" {
		t.Errorf("expected 'initial data' (from replica), got %q", string(readBuf[:12]))
	}
}

func TestVFSFile_WriteBufferClearAfterSync(t *testing.T) {
	client := newWriteTestReplicaClient()

	// Create initial LTX file
	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	// Create temp directory for buffer
	tmpDir := t.TempDir()
	bufferPath := tmpDir + "/.litestream-write-buffer"

	// Create VFSFile with write buffer
	logger := slog.Default()
	f := NewVFSFile(client, "test.db", logger)
	f.writeEnabled = true
	f.dirty = make(map[uint32]int64)
	f.syncInterval = 0
	f.bufferPath = bufferPath

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Write data
	if _, err := f.WriteAt([]byte("sync test"), 0); err != nil {
		t.Fatal(err)
	}

	// Check buffer has content before sync
	stat, _ := os.Stat(bufferPath)
	if stat.Size() == 0 {
		t.Error("buffer should have content before sync")
	}

	// Sync to remote
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}

	// Check buffer is cleared after sync
	stat, _ = os.Stat(bufferPath)
	if stat.Size() != 0 {
		t.Errorf("buffer should be empty after sync, got size %d", stat.Size())
	}
}

func TestVFSFile_OpenFailsWithInvalidBufferPath(t *testing.T) {
	client := newWriteTestReplicaClient()

	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	logger := slog.Default()
	f := NewVFSFile(client, "test.db", logger)
	f.writeEnabled = true
	f.dirty = make(map[uint32]int64)
	f.syncInterval = 0
	f.bufferPath = "/nonexistent/path/that/cannot/be/created/buffer"

	err := f.Open()
	if err == nil {
		f.Close()
		t.Fatal("expected Open to fail with invalid buffer path")
	}
}

func TestVFSFile_BufferFileAlwaysCreatedWhenWriteEnabled(t *testing.T) {
	client := newWriteTestReplicaClient()

	pageSize := uint32(4096)
	initialPage := make([]byte, pageSize)
	createTestLTXFile(t, client, 1, pageSize, 1, map[uint32][]byte{1: initialPage})

	tmpDir := t.TempDir()
	bufferPath := tmpDir + "/write-buffer"

	logger := slog.Default()
	f := NewVFSFile(client, "test.db", logger)
	f.writeEnabled = true
	f.dirty = make(map[uint32]int64)
	f.syncInterval = 0
	f.bufferPath = bufferPath

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if f.bufferFile == nil {
		t.Fatal("bufferFile should never be nil when writeEnabled is true")
	}
}

func TestVFSFile_OpenNewDatabase(t *testing.T) {
	// Test opening a VFSFile with write mode enabled when no LTX files exist (new database)
	client := newWriteTestReplicaClient()
	// Note: No LTX files created - simulating a brand new database

	// Create temp directory for buffer
	tmpDir := t.TempDir()
	bufferPath := tmpDir + "/.litestream-write-buffer"

	// Create VFSFile with write support - no existing data
	logger := slog.Default()
	f := NewVFSFile(client, "new.db", logger)
	f.writeEnabled = true
	f.dirty = make(map[uint32]int64)
	f.syncInterval = 0
	f.bufferPath = bufferPath

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Verify it opened successfully as a new database
	if f.pageSize != DefaultPageSize {
		t.Errorf("expected page size %d, got %d", DefaultPageSize, f.pageSize)
	}

	if f.pos.TXID != 0 {
		t.Errorf("expected TXID 0 for new database, got %d", f.pos.TXID)
	}

	if f.expectedTXID != 0 {
		t.Errorf("expected expectedTXID 0, got %d", f.expectedTXID)
	}

	if f.pendingTXID != 1 {
		t.Errorf("expected pendingTXID 1, got %d", f.pendingTXID)
	}

	if f.commit != 0 {
		t.Errorf("expected commit 0 for new database, got %d", f.commit)
	}
}

func TestVFSFile_NewDatabase_ReadReturnsZeros(t *testing.T) {
	// Test that reading from a new database returns zeros
	client := newWriteTestReplicaClient()

	tmpDir := t.TempDir()
	bufferPath := tmpDir + "/.litestream-write-buffer"

	logger := slog.Default()
	f := NewVFSFile(client, "new.db", logger)
	f.writeEnabled = true
	f.dirty = make(map[uint32]int64)
	f.syncInterval = 0
	f.bufferPath = bufferPath

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Read page 1 - should return zeros for new database
	readBuf := make([]byte, 100)
	n, err := f.ReadAt(readBuf, 0)
	if err != nil {
		t.Fatalf("expected no error reading from new database, got: %v", err)
	}
	if n != len(readBuf) {
		t.Errorf("expected %d bytes, got %d", len(readBuf), n)
	}

	// Verify all zeros
	for i, b := range readBuf {
		if b != 0 {
			t.Errorf("expected zero at position %d, got %d", i, b)
			break
		}
	}
}

func TestVFSFile_NewDatabase_WriteAndSync(t *testing.T) {
	// Test writing to a new database and syncing to remote
	client := newWriteTestReplicaClient()

	tmpDir := t.TempDir()
	bufferPath := tmpDir + "/.litestream-write-buffer"

	logger := slog.Default()
	f := NewVFSFile(client, "new.db", logger)
	f.writeEnabled = true
	f.dirty = make(map[uint32]int64)
	f.syncInterval = 0
	f.bufferPath = bufferPath

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Write data to page 1
	writeData := []byte("new database content")
	n, err := f.WriteAt(writeData, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(writeData) {
		t.Errorf("expected %d bytes written, got %d", len(writeData), n)
	}

	// Verify dirty page exists
	if len(f.dirty) != 1 {
		t.Errorf("expected 1 dirty page, got %d", len(f.dirty))
	}

	// Sync to remote
	if err := f.Sync(0); err != nil {
		t.Fatal(err)
	}

	// Verify TXID advanced
	if f.expectedTXID != 1 {
		t.Errorf("expected expectedTXID 1 after sync, got %d", f.expectedTXID)
	}
	if f.pendingTXID != 2 {
		t.Errorf("expected pendingTXID 2 after sync, got %d", f.pendingTXID)
	}

	// Verify LTX file was written
	client.mu.Lock()
	if len(client.ltxFiles[0]) != 1 {
		t.Errorf("expected 1 LTX file after sync, got %d", len(client.ltxFiles[0]))
	}
	if len(client.ltxFiles[0]) > 0 {
		info := client.ltxFiles[0][0]
		if info.MinTXID != 1 || info.MaxTXID != 1 {
			t.Errorf("expected TXID 1, got min=%d max=%d", info.MinTXID, info.MaxTXID)
		}
	}
	client.mu.Unlock()
}

func TestVFSFile_NewDatabase_FileSize(t *testing.T) {
	// Test that FileSize returns 0 for a new empty database
	client := newWriteTestReplicaClient()

	tmpDir := t.TempDir()
	bufferPath := tmpDir + "/.litestream-write-buffer"

	logger := slog.Default()
	f := NewVFSFile(client, "new.db", logger)
	f.writeEnabled = true
	f.dirty = make(map[uint32]int64)
	f.syncInterval = 0
	f.bufferPath = bufferPath

	if err := f.Open(); err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// FileSize should be 0 for empty database
	size, err := f.FileSize()
	if err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		t.Errorf("expected size 0 for new database, got %d", size)
	}

	// Write a page
	data := make([]byte, DefaultPageSize)
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}

	// FileSize should now reflect the dirty page
	size, err = f.FileSize()
	if err != nil {
		t.Fatal(err)
	}
	if size != int64(DefaultPageSize) {
		t.Errorf("expected size %d after write, got %d", DefaultPageSize, size)
	}
}
