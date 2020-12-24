package litestream

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Replicator represents a method for replicating the snapshot & WAL data to
// a remote destination.
type Replicator interface {
	Name() string
	Type() string
	Snapshotting() bool
	Start(ctx context.Context)
	Stop()
}

var _ Replicator = (*FileReplicator)(nil)

// FileReplicator is a replicator that replicates a DB to a local file path.
type FileReplicator struct {
	db   *DB    // source database
	name string // replicator name, optional
	dst  string // destination path

	mu           sync.RWMutex
	wg           sync.WaitGroup
	snapshotting bool // if true, currently copying database

	ctx    context.Context
	cancel func()
}

// NewFileReplicator returns a new instance of FileReplicator.
func NewFileReplicator(db *DB, name, dst string) *FileReplicator {
	return &FileReplicator{
		db:     db,
		name:   name,
		dst:    dst,
		cancel: func() {},
	}
}

// Name returns the name of the replicator. Returns the type if no name set.
func (r *FileReplicator) Name() string {
	if r.name != "" {
		return r.name
	}
	return r.Type()
}

// Type returns the type of replicator.
func (r *FileReplicator) Type() string {
	return "file"
}

// SnapshotPath returns the path to a snapshot file.
func (r *FileReplicator) SnapshotPath(generation string, index int) string {
	return filepath.Join(r.dst, "generations", generation, "snapshots", fmt.Sprintf("%016x.snapshot", index))
}

// WALPath returns the path to a WAL file.
func (r *FileReplicator) WALPath(pos Pos) string {
	return filepath.Join(r.dst, "generations", pos.Generation, "wal", fmt.Sprintf("%016x.wal", pos.Index))
}

// Snapshotting returns true if replicator is current snapshotting.
func (r *FileReplicator) Snapshotting() bool {
	r.mu.RLock()
	defer r.mu.RLock()
	return r.snapshotting
}

// Start starts replication for a given generation.
func (r *FileReplicator) Start(ctx context.Context) {
	// Stop previous replication.
	r.Stop()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Set snapshotting state.
	r.snapshotting = true

	// Wrap context with cancelation.
	ctx, r.cancel = context.WithCancel(ctx)

	// Start goroutine to replicate data.
	r.wg.Add(1)
	go func() { defer r.wg.Done(); r.monitor(ctx) }()
}

// Stop cancels any outstanding replication and blocks until finished.
func (r *FileReplicator) Stop() {
	r.cancel()
	r.wg.Wait()
}

// monitor runs in a separate goroutine and continuously replicates the DB.
func (r *FileReplicator) monitor(ctx context.Context) {
	// Continuously check for new data to replicate.
	ch := make(chan struct{})
	close(ch)
	var notify <-chan struct{} = ch

	var pos Pos
	var err error
	for {
		select {
		case <-ctx.Done():
			return
		case <-notify:
		}

		// Fetch new notify channel before replicating data.
		notify = r.db.Notify()

		// Determine position, if necessary.
		if pos.IsZero() {
			if pos, err = r.pos(); err != nil {
				log.Printf("%s(%s): cannot determine position: %w", r.db.Path(), r.Name(), err)
				continue
			} else if pos.IsZero() {
				log.Printf("%s(%s): no generation, waiting for data", r.db.Path(), r.Name())
				continue
			}
		}

		// Synchronize the shadow wal into the replication directory.
		if pos, err = r.sync(ctx, pos); err != nil {
			log.Printf("%s(%s): sync error: %w", r.db.Path(), r.Name(), err)
			continue
		}
	}
}

// pos returns the position for the replicator for the current generation.
// Returns a zero value if there is no active generation.
func (r *FileReplicator) pos() (pos Pos, err error) {
	// Find the current generation from the DB. Return zero pos if no generation.
	generation, err := r.db.CurrentGeneration()
	if err != nil {
		return pos, err
	} else if generation == "" {
		return pos, nil // empty position
	}
	pos.Generation = generation

	// Find the max WAL file.
	walDir := filepath.Join(r.dst, "generations", generation, "wal")
	fis, err := ioutil.ReadDir(walDir)
	if os.IsNotExist(err) {
		return pos, nil // no replicated wal, start at beginning of generation
	} else if err != nil {
		return pos, err
	}

	index := -1
	for _, fi := range fis {
		if !strings.HasSuffix(fi.Name(), ".wal") {
			continue
		}

		if v, err := ParseWALFilename(filepath.Base(fi.Name())); err != nil {
			continue // invalid wal filename
		} else if index == -1 || v > index {
			index = v
		}
	}
	if index == -1 {
		return pos, nil // wal directory exists but no wal files, return beginning pos
	}
	pos.Index = index

	// Determine current offset.
	fi, err := os.Stat(filepath.Join(walDir, FormatWALFilename(pos.Index)))
	if err != nil {
		return pos, err
	}
	pos.Offset = fi.Size()

	return pos, nil
}

// snapshot copies the entire database to the replica path.
func (r *FileReplicator) snapshot(ctx context.Context, pos Pos) error {
	rd, err := os.Open(r.db.Path())
	if err != nil {
		return err
	}
	defer rd.Close()

	snapshotPath := r.SnapshotPath(pos.Generation, pos.Index)
	if err := os.MkdirAll(snapshotPath, 0700); err != nil {
		return err
	}

	w, err := os.Create(snapshotPath)
	if err != nil {
		return err
	}
	defer w.Close()

	if _, err := io.Copy(w, rd); err != nil {
		return err
	} else if err := w.Sync(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}

	r.mu.Lock()
	r.snapshotting = false
	r.mu.Unlock()

	return nil
}

func (r *FileReplicator) sync(ctx context.Context, pos Pos) (_ Pos, err error) {
	for {
		if pos, err = r.syncNext(ctx, pos); err == io.EOF {
			return pos, nil
		} else if err != nil {
			return pos, err
		}
	}
}

func (r *FileReplicator) syncNext(ctx context.Context, pos Pos) (_ Pos, err error) {
	rd, err := r.db.WALReader(pos)
	if err == io.EOF {
		return pos, err
	} else if err != nil {
		return pos, fmt.Errorf("wal reader: %w", err)
	}
	defer rd.Close()

	// Ensure parent directory exists for WAL file.
	filename := r.WALPath(rd.Pos())
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		return pos, err
	}

	// Create a temporary file to write into so we don't have partial writes.
	w, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return pos, err
	}
	defer w.Close()

	// Seek, copy & sync WAL contents.
	if _, err := w.Seek(rd.Pos().Offset, io.SeekStart); err != nil {
		return pos, err
	} else if _, err := io.Copy(w, rd); err != nil {
		return pos, err
	} else if err := w.Sync(); err != nil {
		return pos, err
	} else if err := w.Close(); err != nil {
		return pos, err
	}

	// Return ending position of the reader.
	return rd.Pos(), nil
}
