package litestream

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Replica represents a remote destination to replicate the database & WAL.
type Replica interface {
	// The name of the replica. Defaults to type if no name specified.
	Name() string

	// String identifier for the type of replica ("file", "s3", etc).
	Type() string

	// Starts replicating in a background goroutine.
	Start(ctx context.Context)

	// Stops all replication processing. Blocks until processing stopped.
	Stop()

	// Returns the last replication position.
	LastPos() Pos

	// Returns the computed position of the replica for a given generation.
	CalcPos(generation string) (Pos, error)

	// Returns a list of generation names for the replica.
	Generations(ctx context.Context) ([]string, error)

	// Returns basic information about a generation including the number of
	// snapshot & WAL files as well as the time range covered.
	GenerationStats(ctx context.Context, generation string) (GenerationStats, error)

	// Returns a list of available snapshots in the replica.
	Snapshots(ctx context.Context) ([]*SnapshotInfo, error)

	// Returns a list of available WAL files in the replica.
	WALs(ctx context.Context) ([]*WALInfo, error)

	// Returns the highest index for a snapshot within a generation that occurs
	// before timestamp. If timestamp is zero, returns the latest snapshot.
	SnapshotIndexAt(ctx context.Context, generation string, timestamp time.Time) (int, error)

	// Returns the highest index for a WAL file that occurs before timestamp.
	// If timestamp is zero, returns the highest WAL index.
	WALIndexAt(ctx context.Context, generation string, maxIndex int, timestamp time.Time) (int, error)

	// Returns a reader for snapshot data at the given generation/index.
	SnapshotReader(ctx context.Context, generation string, index int) (io.ReadCloser, error)

	// Returns a reader for WAL data at the given position.
	WALReader(ctx context.Context, generation string, index int) (io.ReadCloser, error)
}

// GenerationStats represents high level stats for a single generation.
type GenerationStats struct {
	// Count of snapshot & WAL files.
	SnapshotN int
	WALN      int

	// Time range for the earliest snapshot & latest WAL file update.
	CreatedAt time.Time
	UpdatedAt time.Time
}

var _ Replica = (*FileReplica)(nil)

// FileReplica is a replica that replicates a DB to a local file path.
type FileReplica struct {
	db   *DB    // source database
	name string // replica name, optional
	dst  string // destination path

	mu  sync.RWMutex
	pos Pos // last position

	wg     sync.WaitGroup
	ctx    context.Context
	cancel func()

	// If true, replica monitors database for changes automatically.
	// Set to false if replica is being used synchronously (such as in tests).
	MonitorEnabled bool
}

// NewFileReplica returns a new instance of FileReplica.
func NewFileReplica(db *DB, name, dst string) *FileReplica {
	return &FileReplica{
		db:     db,
		name:   name,
		dst:    dst,
		cancel: func() {},

		MonitorEnabled: true,
	}
}

// Name returns the name of the replica. Returns the type if no name set.
func (r *FileReplica) Name() string {
	if r.name != "" {
		return r.name
	}
	return r.Type()
}

// Type returns the type of replica.
func (r *FileReplica) Type() string {
	return "file"
}

// LastPos returns the last successfully replicated position.
func (r *FileReplica) LastPos() Pos {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pos
}

// SnapshotDir returns the path to a generation's snapshot directory.
func (r *FileReplica) SnapshotDir(generation string) string {
	return filepath.Join(r.dst, "generations", generation, "snapshots")
}

// SnapshotPath returns the path to a snapshot file.
func (r *FileReplica) SnapshotPath(generation string, index int) string {
	return filepath.Join(r.SnapshotDir(generation), fmt.Sprintf("%016x.snapshot.gz", index))
}

// MaxSnapshotIndex returns the highest index for the snapshots.
func (r *FileReplica) MaxSnapshotIndex(generation string) (int, error) {
	fis, err := ioutil.ReadDir(r.SnapshotDir(generation))
	if err != nil {
		return 0, err
	}

	index := -1
	for _, fi := range fis {
		if idx, _, _, err := ParseSnapshotPath(fi.Name()); err != nil {
			continue
		} else if index == -1 || idx > index {
			index = idx
		}
	}
	if index == -1 {
		return 0, fmt.Errorf("no snapshots found")
	}
	return index, nil
}

// WALDir returns the path to a generation's WAL directory
func (r *FileReplica) WALDir(generation string) string {
	return filepath.Join(r.dst, "generations", generation, "wal")
}

// WALSubdir returns the directory used for grouping WAL files.
func (r *FileReplica) WALSubdir(generation string, index int) string {
	return filepath.Join(r.WALDir(generation), fmt.Sprintf("%016x", uint64(index)&walDirMask))
}

// WALSubdirNames returns a list of all WAL subdirectory group names.
func (r *FileReplica) WALSubdirNames(generation string) ([]string, error) {
	fis, err := ioutil.ReadDir(r.WALDir(generation))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	var names []string
	for _, fi := range fis {
		if _, err := strconv.ParseUint(fi.Name(), 16, 64); err != nil {
			continue
		}
		names = append(names, fi.Name())
	}
	return names, nil
}

// MaxWALSubdirName returns the highest WAL subdirectory group name.
func (r *FileReplica) MaxWALSubdirName(generation string) (string, error) {
	fis, err := ioutil.ReadDir(r.WALDir(generation))
	if err != nil {
		return "", err
	}

	var name string
	for _, fi := range fis {
		if _, err := strconv.ParseUint(fi.Name(), 16, 64); err != nil {
			continue
		} else if name == "" || fi.Name() > name {
			name = fi.Name()
		}
	}
	if name == "" {
		return "", os.ErrNotExist
	}
	return name, nil
}

// WALPath returns the path to a WAL file.
func (r *FileReplica) WALPath(generation string, index int) string {
	return filepath.Join(r.WALSubdir(generation, index), fmt.Sprintf("%016x.wal", index))
}

// Generations returns a list of available generation names.
func (r *FileReplica) Generations(ctx context.Context) ([]string, error) {
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
func (r *FileReplica) GenerationStats(ctx context.Context, generation string) (stats GenerationStats, err error) {
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

func (r *FileReplica) snapshotStats(generation string) (n int, min, max time.Time, err error) {
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

func (r *FileReplica) walStats(generation string) (n int, min, max time.Time, err error) {
	names, err := r.WALSubdirNames(generation)
	if err != nil {
		return n, min, max, err
	}

	for _, name := range names {
		fis, err := ioutil.ReadDir(filepath.Join(r.WALDir(generation), name))
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
	}
	return n, min, max, nil
}

// Snapshots returns a list of available snapshots in the replica.
func (r *FileReplica) Snapshots(ctx context.Context) ([]*SnapshotInfo, error) {
	generations, err := r.Generations(ctx)
	if err != nil {
		return nil, err
	}

	var infos []*SnapshotInfo
	for _, generation := range generations {
		fis, err := ioutil.ReadDir(r.SnapshotDir(generation))
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, err
		}

		for _, fi := range fis {
			index, _, _, err := ParseSnapshotPath(fi.Name())
			if err != nil {
				continue
			}

			// TODO: Add schedule name to snapshot info.

			infos = append(infos, &SnapshotInfo{
				Name:       fi.Name(),
				Replica:    r.Name(),
				Generation: generation,
				Index:      index,
				Size:       fi.Size(),
				CreatedAt:  fi.ModTime().UTC(),
			})
		}
	}

	return infos, nil
}

// WALs returns a list of available WAL files in the replica.
func (r *FileReplica) WALs(ctx context.Context) ([]*WALInfo, error) {
	generations, err := r.Generations(ctx)
	if err != nil {
		return nil, err
	}

	var infos []*WALInfo
	for _, generation := range generations {
		// Find a list of all directory groups.
		dir := r.WALDir(generation)
		subfis, err := ioutil.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return nil, err
		}

		// Iterate over WAL group subdirectories.
		for _, subfi := range subfis {
			if !subfi.IsDir() {
				continue
			}

			// Find a list of all WAL files in the group.
			fis, err := ioutil.ReadDir(filepath.Join(dir, subfi.Name()))
			if os.IsNotExist(err) {
				continue
			} else if err != nil {
				return nil, err
			}

			// Iterate over each WAL file.
			for _, fi := range fis {
				index, offset, _, err := ParseWALPath(fi.Name())
				if err != nil {
					continue
				}

				infos = append(infos, &WALInfo{
					Name:       fi.Name(),
					Replica:    r.Name(),
					Generation: generation,
					Index:      index,
					Offset:     offset,
					Size:       fi.Size(),
					CreatedAt:  fi.ModTime().UTC(),
				})
			}
		}
	}

	return infos, nil
}

// Start starts replication for a given generation.
func (r *FileReplica) Start(ctx context.Context) {
	// Ignore if replica is being used sychronously.
	if !r.MonitorEnabled {
		return
	}

	// Stop previous replication.
	r.Stop()

	// Wrap context with cancelation.
	ctx, r.cancel = context.WithCancel(ctx)

	// Start goroutine to replicate data.
	r.wg.Add(1)
	go func() { defer r.wg.Done(); r.monitor(ctx) }()
}

// Stop cancels any outstanding replication and blocks until finished.
func (r *FileReplica) Stop() {
	r.cancel()
	r.wg.Wait()
}

// monitor runs in a separate goroutine and continuously replicates the DB.
func (r *FileReplica) monitor(ctx context.Context) {
	// Clear old temporary files that my have been left from a crash.
	if err := removeTmpFiles(r.dst); err != nil {
		log.Printf("%s(%s): cannot remove tmp files: %s", r.db.Path(), r.Name(), err)
	}

	// Continuously check for new data to replicate.
	ch := make(chan struct{})
	close(ch)
	var notify <-chan struct{} = ch

	for {
		select {
		case <-ctx.Done():
			return
		case <-notify:
		}

		// Fetch new notify channel before replicating data.
		notify = r.db.Notify()

		// Synchronize the shadow wal into the replication directory.
		if err := r.Sync(ctx); err != nil {
			log.Printf("%s(%s): sync error: %s", r.db.Path(), r.Name(), err)
			continue
		}
	}
}

// CalcPos returns the position for the replica for the current generation.
// Returns a zero value if there is no active generation.
func (r *FileReplica) CalcPos(generation string) (pos Pos, err error) {
	pos.Generation = generation

	// Find maximum snapshot index.
	if pos.Index, err = r.MaxSnapshotIndex(generation); err != nil {
		return Pos{}, err
	}

	// Find highest WAL subdirectory group.
	subdir, err := r.MaxWALSubdirName(generation)
	if os.IsNotExist(err) {
		return pos, nil // no replicated wal, start at snapshot index
	} else if err != nil {
		return Pos{}, err
	}

	// Find the max WAL file within WAL group.
	fis, err := ioutil.ReadDir(filepath.Join(r.WALDir(generation), subdir))
	if os.IsNotExist(err) {
		return pos, nil // no replicated wal, start at snapshot index.
	} else if err != nil {
		return Pos{}, err
	}

	index := -1
	for _, fi := range fis {
		if idx, _, _, err := ParseWALPath(fi.Name()); err != nil {
			continue // invalid wal filename
		} else if index == -1 || idx > index {
			index = idx
		}
	}
	if index == -1 {
		return pos, nil // wal directory exists but no wal files, return snapshot position
	}
	pos.Index = index

	// Determine current offset.
	fi, err := os.Stat(r.WALPath(pos.Generation, pos.Index))
	if err != nil {
		return Pos{}, err
	}
	pos.Offset = fi.Size()

	return pos, nil
}

// snapshot copies the entire database to the replica path.
func (r *FileReplica) snapshot(ctx context.Context, generation string, index int) error {
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
func (r *FileReplica) snapshotN(generation string) (int, error) {
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

func (r *FileReplica) Sync(ctx context.Context) (err error) {
	// Find current position of database.
	dpos, err := r.db.Pos()
	if err != nil {
		return fmt.Errorf("cannot determine current generation: %w", err)
	} else if dpos.IsZero() {
		return fmt.Errorf("no generation, waiting for data")
	}
	generation := dpos.Generation

	// Create snapshot if no snapshots exist for generation.
	if n, err := r.snapshotN(generation); err != nil {
		return err
	} else if n == 0 {
		if err := r.snapshot(ctx, generation, dpos.Index); err != nil {
			return err
		}
	}

	// Determine position, if necessary.
	if r.LastPos().IsZero() {
		pos, err := r.CalcPos(generation)
		if err != nil {
			return fmt.Errorf("cannot determine replica position: %s", err)
		}

		r.mu.Lock()
		r.pos = pos
		r.mu.Unlock()
	}

	// Read all WAL files since the last position.
	for {
		if err = r.syncWAL(ctx); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	// Gzip any old WAL files.
	if generation != "" {
		if err := r.compress(ctx, generation); err != nil {
			return fmt.Errorf("cannot compress: %s", err)
		}
	}

	return nil
}

func (r *FileReplica) syncWAL(ctx context.Context) (err error) {
	rd, err := r.db.ShadowWALReader(r.LastPos())
	if err == io.EOF {
		return err
	} else if err != nil {
		return fmt.Errorf("wal reader: %w", err)
	}
	defer rd.Close()

	// Ensure parent directory exists for WAL file.
	filename := r.WALPath(rd.Pos().Generation, rd.Pos().Index)
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		return err
	}

	// Create a temporary file to write into so we don't have partial writes.
	w, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer w.Close()

	// Seek, copy & sync WAL contents.
	if _, err := w.Seek(rd.Pos().Offset, io.SeekStart); err != nil {
		return err
	} else if _, err := io.Copy(w, rd); err != nil {
		return err
	} else if err := w.Sync(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}

	// Save last replicated position.
	r.mu.Lock()
	r.pos = rd.Pos()
	r.mu.Unlock()

	return nil
}

// compress gzips all WAL files before the current one.
func (r *FileReplica) compress(ctx context.Context, generation string) error {
	filenames, err := filepath.Glob(filepath.Join(r.WALDir(generation), "**/*.wal"))
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
func (r *FileReplica) SnapshotIndexAt(ctx context.Context, generation string, timestamp time.Time) (int, error) {
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

// Returns the highest index for a WAL file that occurs before maxIndex & timestamp.
// If timestamp is zero, returns the highest WAL index.
func (r *FileReplica) WALIndexAt(ctx context.Context, generation string, maxIndex int, timestamp time.Time) (int, error) {
	names, err := r.WALSubdirNames(generation)
	if err != nil {
		return 0, err
	}

	// TODO: Optimize to only read the last group if no timestamp specified.
	// TODO: Perform binary search to find correct timestamp.

	var index int
	for _, name := range names {
		fis, err := ioutil.ReadDir(filepath.Join(r.WALDir(generation), name))
		if os.IsNotExist(err) {
			return 0, nil
		} else if err != nil {
			return 0, err
		}

		for _, fi := range fis {
			// Read index from snapshot filename.
			idx, _, _, err := ParseWALPath(fi.Name())
			if err != nil {
				continue // not a snapshot, skip
			} else if !timestamp.IsZero() && fi.ModTime().After(timestamp) {
				continue // after timestamp, skip
			} else if idx > maxIndex {
				continue // after timestamp, skip
			} else if idx < index {
				continue // earlier index, skip
			}

			index = idx
		}
	}

	// If max index is specified but not found, return an error.
	if maxIndex != math.MaxInt64 && index != maxIndex {
		return index, fmt.Errorf("unable to locate index %d in generation %q, highest index was %d", maxIndex, generation, index)
	}

	return index, nil
}

// SnapshotReader returns a reader for snapshot data at the given generation/index.
// Returns os.ErrNotExist if no matching index is found.
func (r *FileReplica) SnapshotReader(ctx context.Context, generation string, index int) (io.ReadCloser, error) {
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
func (r *FileReplica) WALReader(ctx context.Context, generation string, index int) (io.ReadCloser, error) {
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

// walDirMask is a mask used to group 64K wal files into a directory.
const (
	walDirFileN = 0x10000
	walDirMask  = uint64(0xFFFFFFFFFFFFFFFF ^ (walDirFileN - 1))
)
