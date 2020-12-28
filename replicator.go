package litestream

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Replicator represents a method for replicating the snapshot & WAL data to
// a remote destination.
type Replicator interface {
	// The name of the replicator. Defaults to type if no name specified.
	Name() string

	// String identifier for the type of replicator ("file", "s3", etc).
	Type() string

	// Starts replicating in a background goroutine.
	Start(ctx context.Context)

	// Stops all replication processing. Blocks until processing stopped.
	Stop()

	// Returns a list of generation names for the replicator.
	Generations(ctx context.Context) ([]string, error)

	// Returns basic information about a generation including the number of
	// snapshot & WAL files as well as the time range covered.
	GenerationStats(ctx context.Context, generation string) (GenerationStats, error)

	// Returns the highest index for a snapshot within a generation that occurs
	// before timestamp. If timestamp is zero, returns the latest snapshot.
	SnapshotIndexAt(ctx context.Context, generation string, timestamp time.Time) (int, error)

	// Returns the highest index for a WAL file that occurs before timestamp.
	// If timestamp is zero, returns the highest WAL index.
	WALIndexAt(ctx context.Context, generation string, timestamp time.Time) (int, error)

	// Returns a reader for snapshot data at the given generation/index.
	SnapshotReader(ctx context.Context, generation string, index int) (io.ReadCloser, error)

	// Returns a reader for WAL data at the given position.
	WALReader(ctx context.Context, generation string, index int) (io.ReadCloser, error)
}

var _ Replicator = (*FileReplicator)(nil)

// FileReplicator is a replicator that replicates a DB to a local file path.
type FileReplicator struct {
	db   *DB    // source database
	name string // replicator name, optional
	dst  string // destination path

	// mu sync.RWMutex
	wg sync.WaitGroup

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

// SnapshotDir returns the path to a generation's snapshot directory.
func (r *FileReplicator) SnapshotDir(generation string) string {
	return filepath.Join(r.dst, "generations", generation, "snapshots")
}

// SnapshotPath returns the path to a snapshot file.
func (r *FileReplicator) SnapshotPath(generation string, index int) string {
	return filepath.Join(r.SnapshotDir(generation), fmt.Sprintf("%016x.snapshot.gz", index))
}

// WALDir returns the path to a generation's WAL directory
func (r *FileReplicator) WALDir(generation string) string {
	return filepath.Join(r.dst, "generations", generation, "wal")
}

// WALPath returns the path to a WAL file.
func (r *FileReplicator) WALPath(generation string, index int) string {
	return filepath.Join(r.WALDir(generation), fmt.Sprintf("%016x.wal", index))
}

// Generations returns a list of available generation names.
func (r *FileReplicator) Generations(ctx context.Context) ([]string, error) {
	fis, err := ioutil.ReadDir(filepath.Join(r.dst, "generations"))
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var generations []string
	for _, fi := range fis {
		if !IsGenerationName(fi.Name()) {
			continue
		} else if !fi.IsDir() {
			continue
		}
		generations = append(generations, fi.Name())
	}
	return generations, nil
}

// GenerationStats returns stats for a generation.
func (r *FileReplicator) GenerationStats(ctx context.Context, generation string) (stats GenerationStats, err error) {
	// Determine stats for all snapshots.
	n, min, max, err := r.snapshotStats(generation)
	if err != nil {
		return stats, err
	}
	stats.SnapshotN = n
	stats.CreatedAt, stats.UpdatedAt = min, max

	// Update stats if we have WAL files.
	n, min, max, err = r.walStats(generation)
	if err != nil {
		return stats, err
	} else if n == 0 {
		return stats, nil
	}

	stats.WALN = n
	if stats.CreatedAt.IsZero() || min.Before(stats.CreatedAt) {
		stats.CreatedAt = min
	}
	if stats.UpdatedAt.IsZero() || max.After(stats.UpdatedAt) {
		stats.UpdatedAt = max
	}
	return stats, nil
}

func (r *FileReplicator) snapshotStats(generation string) (n int, min, max time.Time, err error) {
	fis, err := ioutil.ReadDir(r.SnapshotDir(generation))
	if os.IsNotExist(err) {
		return n, min, max, nil
	} else if err != nil {
		return n, min, max, err
	}

	for _, fi := range fis {
		if !IsSnapshotPath(fi.Name()) {
			continue
		}
		modTime := fi.ModTime().UTC()

		n++
		if min.IsZero() || modTime.Before(min) {
			min = modTime
		}
		if max.IsZero() || modTime.After(max) {
			max = modTime
		}
	}
	return n, min, max, nil
}

func (r *FileReplicator) walStats(generation string) (n int, min, max time.Time, err error) {
	fis, err := ioutil.ReadDir(r.WALDir(generation))
	if os.IsNotExist(err) {
		return n, min, max, nil
	} else if err != nil {
		return n, min, max, err
	}

	for _, fi := range fis {
		if !IsWALPath(fi.Name()) {
			continue
		}
		modTime := fi.ModTime().UTC()

		n++
		if min.IsZero() || modTime.Before(min) {
			min = modTime
		}
		if max.IsZero() || modTime.After(max) {
			max = modTime
		}
	}
	return n, min, max, nil
}

type GenerationStats struct {
	SnapshotN int
	WALN      int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Start starts replication for a given generation.
func (r *FileReplicator) Start(ctx context.Context) {
	// Stop previous replication.
	r.Stop()

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
	// Clear old temporary files that my have been left from a crash.
	if err := removeTmpFiles(r.dst); err != nil {
		log.Printf("%s(%s): cannot remove tmp files: %w", r.db.Path(), r.Name(), err)
	}

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

		// If we have no replicated WALs, start from last index in shadow WAL.
		if pos.Index == 0 && pos.Offset == 0 {
			if pos.Index, err = r.db.CurrentShadowWALIndex(pos.Generation); err != nil {
				log.Printf("%s(%s): cannot determine latest shadow wal index: %s", r.db.Path(), r.Name(), err)
				continue
			}
		}

		// Synchronize the shadow wal into the replication directory.
		if pos, err = r.sync(ctx, pos); err != nil {
			log.Printf("%s(%s): sync error: %s", r.db.Path(), r.Name(), err)
			continue
		}

		// Gzip any old WAL files.
		if pos.Generation != "" {
			if err := r.compress(ctx, pos.Generation); err != nil {
				log.Printf("%s(%s): compress error: %s", r.db.Path(), r.Name(), err)
				continue
			}
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
	dir := r.WALDir(generation)
	fis, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		return pos, nil // no replicated wal, start at beginning of generation
	} else if err != nil {
		return pos, err
	}

	index := -1
	for _, fi := range fis {
		name := fi.Name()
		name = strings.TrimSuffix(name, ".gz")

		if !strings.HasSuffix(name, WALExt) {
			continue
		}

		if v, err := ParseWALFilename(filepath.Base(name)); err != nil {
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
	fi, err := os.Stat(filepath.Join(dir, FormatWALFilename(pos.Index)))
	if err != nil {
		return pos, err
	}
	pos.Offset = fi.Size()

	return pos, nil
}

// snapshot copies the entire database to the replica path.
func (r *FileReplicator) snapshot(ctx context.Context, generation string, index int) error {
	// Acquire a read lock on the database during snapshot to prevent checkpoints.
	tx, err := r.db.db.Begin()
	if err != nil {
		return err
	} else if _, err := tx.ExecContext(ctx, `SELECT COUNT(1) FROM _litestream_seq;`); err != nil {
		tx.Rollback()
		return err
	}
	defer tx.Rollback()

	// Ignore if we already have a snapshot for the given WAL index.
	snapshotPath := r.SnapshotPath(generation, index)
	if _, err := os.Stat(snapshotPath); err == nil {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(snapshotPath), 0700); err != nil {
		return err
	}

	return compressFile(r.db.Path(), snapshotPath)
}

// snapshotN returns the number of snapshots for a generation.
func (r *FileReplicator) snapshotN(generation string) (int, error) {
	fis, err := ioutil.ReadDir(r.SnapshotDir(generation))
	if os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	var n int
	for _, fi := range fis {
		if _, _, _, err := ParseSnapshotPath(fi.Name()); err == nil {
			n++
		}
	}
	return n, nil
}

func (r *FileReplicator) sync(ctx context.Context, pos Pos) (_ Pos, err error) {
	// Read all WAL files since the last position.
	for {
		if pos, err = r.syncNext(ctx, pos); err == io.EOF {
			return pos, nil
		} else if err != nil {
			return pos, err
		}
	}
}

func (r *FileReplicator) syncNext(ctx context.Context, pos Pos) (_ Pos, err error) {
	rd, err := r.db.ShadowWALReader(pos)
	if err == io.EOF {
		return pos, err
	} else if err != nil {
		return pos, fmt.Errorf("wal reader: %w", err)
	}
	defer rd.Close()

	// Create snapshot if no snapshots exist.
	if n, err := r.snapshotN(rd.Pos().Generation); err != nil {
		return pos, err
	} else if n == 0 {
		if err := r.snapshot(ctx, rd.Pos().Generation, rd.Pos().Index); err != nil {
			return pos, err
		}
	}

	// Ensure parent directory exists for WAL file.
	filename := r.WALPath(rd.Pos().Generation, rd.Pos().Index)
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

// compress gzips all WAL files before the current one.
func (r *FileReplicator) compress(ctx context.Context, generation string) error {
	dir := r.WALDir(generation)
	filenames, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil {
		return err
	} else if len(filenames) <= 1 {
		return nil // no uncompressed wal files or only one active file
	}

	// Ensure filenames are sorted & remove the last (active) WAL.
	sort.Strings(filenames)
	filenames = filenames[:len(filenames)-1]

	// Compress each file from oldest to newest.
	for _, filename := range filenames {
		select {
		case <-ctx.Done():
			return err
		default:
		}

		dst := filename + ".gz"
		if err := compressFile(filename, dst); err != nil {
			return err
		} else if err := os.Remove(filename); err != nil {
			return err
		}
	}

	return nil
}

// SnapsotIndexAt returns the highest index for a snapshot within a generation
// that occurs before timestamp. If timestamp is zero, returns the latest snapshot.
func (r *FileReplicator) SnapshotIndexAt(ctx context.Context, generation string, timestamp time.Time) (int, error) {
	fis, err := ioutil.ReadDir(r.SnapshotDir(generation))
	if os.IsNotExist(err) {
		return 0, ErrNoSnapshots
	} else if err != nil {
		return 0, err
	}

	index := -1
	var max time.Time
	for _, fi := range fis {
		// Read index from snapshot filename.
		idx, _, _, err := ParseSnapshotPath(fi.Name())
		if err != nil {
			continue // not a snapshot, skip
		} else if !timestamp.IsZero() && fi.ModTime().After(timestamp) {
			continue // after timestamp, skip
		}

		// Use snapshot if it newer.
		if max.IsZero() || fi.ModTime().After(max) {
			index, max = idx, fi.ModTime()
		}
	}

	if index == -1 {
		return 0, ErrNoSnapshots
	}
	return index, nil
}

// Returns the highest index for a WAL file that occurs before timestamp.
// If timestamp is zero, returns the highest WAL index.
func (r *FileReplicator) WALIndexAt(ctx context.Context, generation string, timestamp time.Time) (int, error) {
	fis, err := ioutil.ReadDir(r.WALDir(generation))
	if os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	index := -1
	for _, fi := range fis {
		// Read index from snapshot filename.
		idx, _, _, err := ParseWALPath(fi.Name())
		if err != nil {
			continue // not a snapshot, skip
		} else if !timestamp.IsZero() && fi.ModTime().After(timestamp) {
			continue // after timestamp, skip
		} else if idx < index {
			continue // earlier index, skip
		}

		index = idx
	}

	if index == -1 {
		return 0, nil
	}
	return index, nil
}

// SnapshotReader returns a reader for snapshot data at the given generation/index.
// Returns os.ErrNotExist if no matching index is found.
func (r *FileReplicator) SnapshotReader(ctx context.Context, generation string, index int) (io.ReadCloser, error) {
	dir := r.SnapshotDir(generation)
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, fi := range fis {
		// Parse index from snapshot filename. Skip if no match.
		idx, _, ext, err := ParseSnapshotPath(fi.Name())
		if err != nil || index != idx {
			continue
		}

		// Open & return the file handle if uncompressed.
		f, err := os.Open(filepath.Join(dir, fi.Name()))
		if err != nil {
			return nil, err
		} else if ext == ".snapshot" {
			return f, nil // not compressed, return as-is.
		}
		assert(ext == ".snapshot.gz", "invalid snapshot extension")

		// If compressed, wrap in a gzip reader and return with wrapper to
		// ensure that the underlying file is closed.
		r, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, err
		}
		return &gzipReadCloser{r: r, closer: f}, nil
	}
	return nil, os.ErrNotExist
}

// WALReader returns a reader for WAL data at the given index.
// Returns os.ErrNotExist if no matching index is found.
func (r *FileReplicator) WALReader(ctx context.Context, generation string, index int) (io.ReadCloser, error) {
	filename := r.WALPath(generation, index)

	// Attempt to read uncompressed file first.
	f, err := os.Open(filename)
	if err == nil {
		return f, nil // file exist, return
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	// Otherwise read the compressed file. Return error if file doesn't exist.
	f, err = os.Open(filename + ".gz")
	if err != nil {
		return nil, err
	}

	// If compressed, wrap in a gzip reader and return with wrapper to
	// ensure that the underlying file is closed.
	rd, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &gzipReadCloser{r: rd, closer: f}, nil
}

// compressFile compresses a file and replaces it with a new file with a .gz extension.
func compressFile(src, dst string) error {
	r, err := os.Open(src)
	if err != nil {
		return err
	}
	defer r.Close()

	w, err := os.Create(dst + ".tmp")
	if err != nil {
		return err
	}
	defer w.Close()

	gz := gzip.NewWriter(w)
	defer gz.Close()

	// Copy & compress file contents to temporary file.
	if _, err := io.Copy(gz, r); err != nil {
		return err
	} else if err := gz.Close(); err != nil {
		return err
	} else if err := w.Sync(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}

	// Move compressed file to final location.
	return os.Rename(dst+".tmp", dst)
}
