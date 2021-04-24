package litestream

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/benbjohnson/litestream/internal"
	"github.com/pierrec/lz4/v4"
	"github.com/prometheus/client_golang/prometheus"
)

// Replica represents a remote destination to replicate the database & WAL.
type Replica interface {
	// The name of the replica. Defaults to type if no name specified.
	Name() string

	// String identifier for the type of replica ("file", "s3", etc).
	Type() string

	// The parent database.
	DB() *DB

	// Starts replicating in a background goroutine.
	Start(ctx context.Context) error

	// Stops all replication processing. Blocks until processing stopped.
	Stop(hard bool) error

	// Performs a backup of outstanding WAL frames to the replica.
	Sync(ctx context.Context) error

	// Returns the last replication position.
	LastPos() Pos

	// Returns the computed position of the replica for a given generation.
	CalcPos(ctx context.Context, generation string) (Pos, error)

	// Returns a list of generation names for the replica.
	Generations(ctx context.Context) ([]string, error)

	// Returns basic information about a generation including the number of
	// snapshot & WAL files as well as the time range covered.
	GenerationStats(ctx context.Context, generation string) (GenerationStats, error)

	// Returns a list of available snapshots in the replica.
	Snapshots(ctx context.Context) ([]*SnapshotInfo, error)

	// Returns a list of available WAL files in the replica.
	WALs(ctx context.Context) ([]*WALInfo, error)

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

// Default file replica settings.
const (
	DefaultRetention              = 24 * time.Hour
	DefaultRetentionCheckInterval = 1 * time.Hour
)

var _ Replica = (*FileReplica)(nil)

// FileReplica is a replica that replicates a DB to a local file path.
type FileReplica struct {
	db   *DB    // source database
	name string // replica name, optional
	dst  string // destination path

	mu  sync.RWMutex
	pos Pos // last position

	muf sync.Mutex
	f   *os.File // long-running file descriptor to avoid non-OFD lock issues

	wg     sync.WaitGroup
	cancel func()

	snapshotTotalGauge prometheus.Gauge
	walBytesCounter    prometheus.Counter
	walIndexGauge      prometheus.Gauge
	walOffsetGauge     prometheus.Gauge

	// Frequency to create new snapshots.
	SnapshotInterval time.Duration

	// Time to keep snapshots and related WAL files.
	// Database is snapshotted after interval, if needed, and older WAL files are discarded.
	Retention time.Duration

	// Time between checks for retention.
	RetentionCheckInterval time.Duration

	// Time between validation checks.
	ValidationInterval time.Duration

	// If true, replica monitors database for changes automatically.
	// Set to false if replica is being used synchronously (such as in tests).
	MonitorEnabled bool
}

// NewFileReplica returns a new instance of FileReplica.
func NewFileReplica(db *DB, name, dst string) *FileReplica {
	r := &FileReplica{
		db:     db,
		name:   name,
		dst:    dst,
		cancel: func() {},

		Retention:              DefaultRetention,
		RetentionCheckInterval: DefaultRetentionCheckInterval,
		MonitorEnabled:         true,
	}

	var dbPath string
	if db != nil {
		dbPath = db.Path()
	}
	r.snapshotTotalGauge = internal.ReplicaSnapshotTotalGaugeVec.WithLabelValues(dbPath, r.Name())
	r.walBytesCounter = internal.ReplicaWALBytesCounterVec.WithLabelValues(dbPath, r.Name())
	r.walIndexGauge = internal.ReplicaWALIndexGaugeVec.WithLabelValues(dbPath, r.Name())
	r.walOffsetGauge = internal.ReplicaWALOffsetGaugeVec.WithLabelValues(dbPath, r.Name())

	return r
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

// DB returns the parent database reference.
func (r *FileReplica) DB() *DB {
	return r.db
}

// Path returns the path the replica was initialized with.
func (r *FileReplica) Path() string {
	return r.dst
}

// LastPos returns the last successfully replicated position.
func (r *FileReplica) LastPos() Pos {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pos
}

// GenerationDir returns the path to a generation's root directory.
func (r *FileReplica) GenerationDir(generation string) string {
	return filepath.Join(r.dst, "generations", generation)
}

// SnapshotDir returns the path to a generation's snapshot directory.
func (r *FileReplica) SnapshotDir(generation string) string {
	return filepath.Join(r.GenerationDir(generation), "snapshots")
}

// SnapshotPath returns the path to a snapshot file.
func (r *FileReplica) SnapshotPath(generation string, index int) string {
	return filepath.Join(r.SnapshotDir(generation), fmt.Sprintf("%08x.snapshot.lz4", index))
}

// MaxSnapshotIndex returns the highest index for the snapshots.
func (r *FileReplica) MaxSnapshotIndex(generation string) (int, error) {
	fis, err := ioutil.ReadDir(r.SnapshotDir(generation))
	if err != nil {
		return 0, err
	}

	index := -1
	for _, fi := range fis {
		if idx, _, err := ParseSnapshotPath(fi.Name()); err != nil {
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
	return filepath.Join(r.GenerationDir(generation), "wal")
}

// WALPath returns the path to a WAL file.
func (r *FileReplica) WALPath(generation string, index int) string {
	return filepath.Join(r.WALDir(generation), fmt.Sprintf("%08x.wal", index))
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
			index, _, err := ParseSnapshotPath(fi.Name())
			if err != nil {
				continue
			}

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
		// Find a list of all WAL files.
		fis, err := ioutil.ReadDir(r.WALDir(generation))
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

	return infos, nil
}

// Start starts replication for a given generation.
func (r *FileReplica) Start(ctx context.Context) (err error) {
	// Ignore if replica is being used sychronously.
	if !r.MonitorEnabled {
		return nil
	}

	// Stop previous replication.
	r.Stop(false)

	// Wrap context with cancelation.
	ctx, r.cancel = context.WithCancel(ctx)

	// Start goroutine to replicate data.
	r.wg.Add(4)
	go func() { defer r.wg.Done(); r.monitor(ctx) }()
	go func() { defer r.wg.Done(); r.retainer(ctx) }()
	go func() { defer r.wg.Done(); r.snapshotter(ctx) }()
	go func() { defer r.wg.Done(); r.validator(ctx) }()

	return nil
}

// Stop cancels any outstanding replication and blocks until finished.
//
// Performing a hard stop will close the DB file descriptor which could release
// locks on per-process locks. Hard stops should only be performed when
// stopping the entire process.
func (r *FileReplica) Stop(hard bool) (err error) {
	r.cancel()
	r.wg.Wait()

	r.muf.Lock()
	defer r.muf.Unlock()
	if hard && r.f != nil {
		if e := r.f.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// monitor runs in a separate goroutine and continuously replicates the DB.
func (r *FileReplica) monitor(ctx context.Context) {
	// Clear old temporary files that my have been left from a crash.
	if err := removeTmpFiles(r.dst); err != nil {
		log.Printf("%s(%s): monitor: cannot remove tmp files: %s", r.db.Path(), r.Name(), err)
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
			log.Printf("%s(%s): monitor error: %s", r.db.Path(), r.Name(), err)
			continue
		}
	}
}

// retainer runs in a separate goroutine and handles retention.
func (r *FileReplica) retainer(ctx context.Context) {
	// Disable retention enforcement if retention period is non-positive.
	if r.Retention <= 0 {
		return
	}

	// Ensure check interval is not longer than retention period.
	checkInterval := r.RetentionCheckInterval
	if checkInterval > r.Retention {
		checkInterval = r.Retention
	}

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.EnforceRetention(ctx); err != nil {
				log.Printf("%s(%s): retainer error: %s", r.db.Path(), r.Name(), err)
				continue
			}
		}
	}
}

// snapshotter runs in a separate goroutine and handles snapshotting.
func (r *FileReplica) snapshotter(ctx context.Context) {
	if r.SnapshotInterval <= 0 {
		return
	}

	ticker := time.NewTicker(r.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.Snapshot(ctx); err != nil && err != ErrNoGeneration {
				log.Printf("%s(%s): snapshotter error: %s", r.db.Path(), r.Name(), err)
				continue
			}
		}
	}
}

// validator runs in a separate goroutine and handles periodic validation.
func (r *FileReplica) validator(ctx context.Context) {
	// Initialize counters since validation occurs infrequently.
	for _, status := range []string{"ok", "error"} {
		internal.ReplicaValidationTotalCounterVec.WithLabelValues(r.db.Path(), r.Name(), status).Add(0)
	}

	// Exit validation if interval is not set.
	if r.ValidationInterval <= 0 {
		return
	}

	ticker := time.NewTicker(r.ValidationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := ValidateReplica(ctx, r); err != nil {
				log.Printf("%s(%s): validation error: %s", r.db.Path(), r.Name(), err)
				continue
			}
		}
	}
}

// CalcPos returns the position for the replica for the current generation.
// Returns a zero value if there is no active generation.
func (r *FileReplica) CalcPos(ctx context.Context, generation string) (pos Pos, err error) {
	pos.Generation = generation

	// Find maximum snapshot index.
	if pos.Index, err = r.MaxSnapshotIndex(generation); err != nil {
		return Pos{}, err
	}

	// Find the max WAL file within WAL.
	fis, err := ioutil.ReadDir(r.WALDir(generation))
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

// Snapshot copies the entire database to the replica path.
func (r *FileReplica) Snapshot(ctx context.Context) error {
	// Find current position of database.
	pos, err := r.db.Pos()
	if err != nil {
		return fmt.Errorf("cannot determine current db generation: %w", err)
	} else if pos.IsZero() {
		return ErrNoGeneration
	}
	return r.snapshot(ctx, pos.Generation, pos.Index)
}

// snapshot copies the entire database to the replica path.
func (r *FileReplica) snapshot(ctx context.Context, generation string, index int) error {
	r.muf.Lock()
	defer r.muf.Unlock()

	// Issue a passive checkpoint to flush any pages to disk before snapshotting.
	if _, err := r.db.db.ExecContext(ctx, `PRAGMA wal_checkpoint(PASSIVE);`); err != nil {
		return fmt.Errorf("pre-snapshot checkpoint: %w", err)
	}

	// Acquire a read lock on the database during snapshot to prevent checkpoints.
	tx, err := r.db.db.Begin()
	if err != nil {
		return err
	} else if _, err := tx.ExecContext(ctx, `SELECT COUNT(1) FROM _litestream_seq;`); err != nil {
		_ = tx.Rollback()
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Ignore if we already have a snapshot for the given WAL index.
	snapshotPath := r.SnapshotPath(generation, index)
	if _, err := os.Stat(snapshotPath); err == nil {
		return nil
	}

	startTime := time.Now()

	if err := mkdirAll(filepath.Dir(snapshotPath), r.db.dirmode, r.db.diruid, r.db.dirgid); err != nil {
		return err
	}

	// Open db file descriptor, if not already open.
	if r.f == nil {
		if r.f, err = os.Open(r.db.Path()); err != nil {
			return err
		}
	}

	if _, err := r.f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	fi, err := r.f.Stat()
	if err != nil {
		return err
	}

	w, err := createFile(snapshotPath+".tmp", fi.Mode(), r.db.uid, r.db.gid)
	if err != nil {
		return err
	}
	defer w.Close()

	zr := lz4.NewWriter(w)
	defer zr.Close()

	// Copy & compress file contents to temporary file.
	if _, err := io.Copy(zr, r.f); err != nil {
		return err
	} else if err := zr.Close(); err != nil {
		return err
	} else if err := w.Sync(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}

	// Move compressed file to final location.
	if err := os.Rename(snapshotPath+".tmp", snapshotPath); err != nil {
		return err
	}

	log.Printf("%s(%s): snapshot: creating %s/%08x t=%s", r.db.Path(), r.Name(), generation, index, time.Since(startTime).Truncate(time.Millisecond))
	return nil
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
		if _, _, err := ParseSnapshotPath(fi.Name()); err == nil {
			n++
		}
	}
	return n, nil
}

// Sync replays data from the shadow WAL into the file replica.
func (r *FileReplica) Sync(ctx context.Context) (err error) {
	// Clear last position if if an error occurs during sync.
	defer func() {
		if err != nil {
			r.mu.Lock()
			r.pos = Pos{}
			r.mu.Unlock()
		}
	}()

	// Find current position of database.
	dpos, err := r.db.Pos()
	if err != nil {
		return fmt.Errorf("cannot determine current generation: %w", err)
	} else if dpos.IsZero() {
		return fmt.Errorf("no generation, waiting for data")
	}
	generation := dpos.Generation

	Tracef("%s(%s): replica sync: db.pos=%s", r.db.Path(), r.Name(), dpos)

	// Create snapshot if no snapshots exist for generation.
	if n, err := r.snapshotN(generation); err != nil {
		return err
	} else if n == 0 {
		if err := r.snapshot(ctx, generation, dpos.Index); err != nil {
			return err
		}
		r.snapshotTotalGauge.Set(1.0)
	} else {
		r.snapshotTotalGauge.Set(float64(n))
	}

	// Determine position, if necessary.
	if r.LastPos().Generation != generation {
		pos, err := r.CalcPos(ctx, generation)
		if err != nil {
			return fmt.Errorf("cannot determine replica position: %s", err)
		}

		Tracef("%s(%s): replica sync: calc new pos: %s", r.db.Path(), r.Name(), pos)
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

	// Compress any old WAL files.
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
	if err := mkdirAll(filepath.Dir(filename), r.db.dirmode, r.db.diruid, r.db.dirgid); err != nil {
		return err
	}

	w, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, r.db.mode)
	if err != nil {
		return err
	}
	defer w.Close()

	_ = os.Chown(filename, r.db.uid, r.db.gid)

	// Seek, copy & sync WAL contents.
	if _, err := w.Seek(rd.Pos().Offset, io.SeekStart); err != nil {
		return err
	}

	// Copy header if at offset zero.
	var psalt uint64 // previous salt value
	if pos := rd.Pos(); pos.Offset == 0 {
		buf := make([]byte, WALHeaderSize)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return err
		}

		psalt = binary.BigEndian.Uint64(buf[16:24])

		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		r.walBytesCounter.Add(float64(n))
	}

	// Copy frames.
	for {
		pos := rd.Pos()
		assert(pos.Offset == frameAlign(pos.Offset, r.db.pageSize), "shadow wal reader not frame aligned")

		buf := make([]byte, WALFrameHeaderSize+r.db.pageSize)
		if _, err := io.ReadFull(rd, buf); err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Verify salt matches the previous frame/header read.
		salt := binary.BigEndian.Uint64(buf[8:16])
		if psalt != 0 && psalt != salt {
			return fmt.Errorf("replica salt mismatch: %s", filepath.Base(filename))
		}
		psalt = salt

		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		r.walBytesCounter.Add(float64(n))
	}

	if err := w.Sync(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}

	// Save last replicated position.
	r.mu.Lock()
	r.pos = rd.Pos()
	r.mu.Unlock()

	// Track current position
	r.walIndexGauge.Set(float64(rd.Pos().Index))
	r.walOffsetGauge.Set(float64(rd.Pos().Offset))

	return nil
}

// compress compresses all WAL files before the current one.
func (r *FileReplica) compress(ctx context.Context, generation string) error {
	filenames, err := filepath.Glob(filepath.Join(r.WALDir(generation), "*.wal"))
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

		dst := filename + ".lz4"
		if err := compressWALFile(filename, dst, r.db.uid, r.db.gid); err != nil {
			return err
		} else if err := os.Remove(filename); err != nil {
			return err
		}
	}

	return nil
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
		idx, ext, err := ParseSnapshotPath(fi.Name())
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
		assert(ext == ".snapshot.lz4", "invalid snapshot extension")

		// If compressed, wrap in an lz4 reader and return with wrapper to
		// ensure that the underlying file is closed.
		return internal.NewReadCloser(lz4.NewReader(f), f), nil
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
	f, err = os.Open(filename + ".lz4")
	if err != nil {
		return nil, err
	}

	// If compressed, wrap in an lz4 reader and return with wrapper to
	// ensure that the underlying file is closed.
	return internal.NewReadCloser(lz4.NewReader(f), f), nil
}

// EnforceRetention forces a new snapshot once the retention interval has passed.
// Older snapshots and WAL files are then removed.
func (r *FileReplica) EnforceRetention(ctx context.Context) (err error) {
	// Find current position of database.
	pos, err := r.db.Pos()
	if err != nil {
		return fmt.Errorf("cannot determine current generation: %w", err)
	} else if pos.IsZero() {
		return fmt.Errorf("no generation, waiting for data")
	}

	// Obtain list of snapshots that are within the retention period.
	snapshots, err := r.Snapshots(ctx)
	if err != nil {
		return fmt.Errorf("cannot obtain snapshot list: %w", err)
	}
	snapshots = FilterSnapshotsAfter(snapshots, time.Now().Add(-r.Retention))

	// If no retained snapshots exist, create a new snapshot.
	if len(snapshots) == 0 {
		if err := r.snapshot(ctx, pos.Generation, pos.Index); err != nil {
			return fmt.Errorf("cannot snapshot: %w", err)
		}
		snapshots = append(snapshots, &SnapshotInfo{Generation: pos.Generation, Index: pos.Index})
	}

	// Loop over generations and delete unretained snapshots & WAL files.
	generations, err := r.Generations(ctx)
	if err != nil {
		return fmt.Errorf("cannot obtain generations: %w", err)
	}
	for _, generation := range generations {
		// Find earliest retained snapshot for this generation.
		snapshot := FindMinSnapshotByGeneration(snapshots, generation)

		// Delete generations if it has no snapshots being retained.
		if snapshot == nil {
			log.Printf("%s(%s): retainer: deleting generation %q has no retained snapshots, deleting", r.db.Path(), r.Name(), generation)
			if err := os.RemoveAll(r.GenerationDir(generation)); err != nil {
				return fmt.Errorf("cannot delete generation %q dir: %w", generation, err)
			}
			continue
		}

		// Otherwise delete all snapshots & WAL files before a lowest retained index.
		if err := r.deleteGenerationSnapshotsBefore(ctx, generation, snapshot.Index); err != nil {
			return fmt.Errorf("cannot delete generation %q snapshots before index %d: %w", generation, snapshot.Index, err)
		} else if err := r.deleteGenerationWALBefore(ctx, generation, snapshot.Index); err != nil {
			return fmt.Errorf("cannot delete generation %q wal before index %d: %w", generation, snapshot.Index, err)
		}
	}

	return nil
}

// deleteGenerationSnapshotsBefore deletes snapshot before a given index.
func (r *FileReplica) deleteGenerationSnapshotsBefore(ctx context.Context, generation string, index int) (err error) {
	dir := r.SnapshotDir(generation)

	fis, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	var n int
	for _, fi := range fis {
		idx, _, err := ParseSnapshotPath(fi.Name())
		if err != nil {
			continue
		} else if idx >= index {
			continue
		}

		if err := os.Remove(filepath.Join(dir, fi.Name())); err != nil {
			return err
		}
		n++
	}
	if n > 0 {
		log.Printf("%s(%s): retainer: deleting snapshots before %s/%08x; n=%d", r.db.Path(), r.Name(), generation, index, n)
	}

	return nil
}

// deleteGenerationWALBefore deletes WAL files before a given index.
func (r *FileReplica) deleteGenerationWALBefore(ctx context.Context, generation string, index int) (err error) {
	dir := r.WALDir(generation)

	fis, err := ioutil.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	var n int
	for _, fi := range fis {
		idx, _, _, err := ParseWALPath(fi.Name())
		if err != nil {
			continue
		} else if idx >= index {
			continue
		}

		if err := os.Remove(filepath.Join(dir, fi.Name())); err != nil {
			return err
		}
		n++
	}
	if n > 0 {
		log.Printf("%s(%s): retainer: deleting wal files before %s/%08x n=%d", r.db.Path(), r.Name(), generation, index, n)
	}

	return nil
}

// SnapshotIndexAt returns the highest index for a snapshot within a generation
// that occurs before timestamp. If timestamp is zero, returns the latest snapshot.
func SnapshotIndexAt(ctx context.Context, r Replica, generation string, timestamp time.Time) (int, error) {
	snapshots, err := r.Snapshots(ctx)
	if err != nil {
		return 0, err
	} else if len(snapshots) == 0 {
		return 0, ErrNoSnapshots
	}

	snapshotIndex := -1
	var max time.Time
	for _, snapshot := range snapshots {
		if snapshot.Generation != generation {
			continue // generation mismatch, skip
		} else if !timestamp.IsZero() && snapshot.CreatedAt.After(timestamp) {
			continue // after timestamp, skip
		}

		// Use snapshot if it newer.
		if max.IsZero() || snapshot.CreatedAt.After(max) {
			snapshotIndex, max = snapshot.Index, snapshot.CreatedAt
		}
	}

	if snapshotIndex == -1 {
		return 0, ErrNoSnapshots
	}
	return snapshotIndex, nil
}

// SnapshotIndexbyIndex returns the highest index for a snapshot within a generation
// that occurs before a given index. If index is MaxInt32, returns the latest snapshot.
func SnapshotIndexByIndex(ctx context.Context, r Replica, generation string, index int) (int, error) {
	snapshots, err := r.Snapshots(ctx)
	if err != nil {
		return 0, err
	} else if len(snapshots) == 0 {
		return 0, ErrNoSnapshots
	}

	snapshotIndex := -1
	for _, snapshot := range snapshots {
		if index < math.MaxInt32 && snapshot.Index > index {
			continue // after index, skip
		}

		// Use snapshot if it newer.
		if snapshotIndex == -1 || snapshotIndex >= snapshotIndex {
			snapshotIndex = snapshot.Index
		}
	}

	if snapshotIndex == -1 {
		return 0, ErrNoSnapshots
	}
	return snapshotIndex, nil
}

// WALIndexAt returns the highest index for a WAL file that occurs before
// maxIndex & timestamp. If timestamp is zero, returns the highest WAL index.
// Returns -1 if no WAL found and MaxInt32 specified.
func WALIndexAt(ctx context.Context, r Replica, generation string, maxIndex int, timestamp time.Time) (int, error) {
	wals, err := r.WALs(ctx)
	if err != nil {
		return 0, err
	}

	index := -1
	for _, wal := range wals {
		if wal.Generation != generation {
			continue
		}

		if !timestamp.IsZero() && wal.CreatedAt.After(timestamp) {
			continue // after timestamp, skip
		} else if wal.Index > maxIndex {
			continue // after max index, skip
		} else if wal.Index < index {
			continue // earlier index, skip
		}

		index = wal.Index
	}

	// If max index is specified but not found, return an error.
	if maxIndex != math.MaxInt32 && index != maxIndex {
		return index, fmt.Errorf("unable to locate index %d in generation %q, highest index was %d", maxIndex, generation, index)
	}
	return index, nil
}

// compressWALFile compresses a file and replaces it with a new file with a .lz4 extension.
// Do not use this on database files because of issues with non-OFD locks.
func compressWALFile(src, dst string, uid, gid int) error {
	r, err := os.Open(src)
	if err != nil {
		return err
	}
	defer r.Close()

	fi, err := r.Stat()
	if err != nil {
		return err
	}

	w, err := createFile(dst+".tmp", fi.Mode(), uid, gid)
	if err != nil {
		return err
	}
	defer w.Close()

	zr := lz4.NewWriter(w)
	defer zr.Close()

	// Copy & compress file contents to temporary file.
	if _, err := io.Copy(zr, r); err != nil {
		return err
	} else if err := zr.Close(); err != nil {
		return err
	} else if err := w.Sync(); err != nil {
		return err
	} else if err := w.Close(); err != nil {
		return err
	}

	// Move compressed file to final location.
	return os.Rename(dst+".tmp", dst)
}

// ValidateReplica restores the most recent data from a replica and validates
// that the resulting database matches the current database.
func ValidateReplica(ctx context.Context, r Replica) error {
	db := r.DB()

	// Restore replica to a temporary directory.
	tmpdir, err := ioutil.TempDir("", "*-litestream")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpdir)

	// Compute checksum of primary database under lock. This prevents a
	// sync from occurring and the database will not be written.
	chksum0, pos, err := db.CRC64(ctx)
	if err != nil {
		return fmt.Errorf("cannot compute checksum: %w", err)
	}

	// Wait until replica catches up to position.
	if err := waitForReplica(ctx, r, pos); err != nil {
		return fmt.Errorf("cannot wait for replica: %w", err)
	}

	restorePath := filepath.Join(tmpdir, "replica")
	if err := RestoreReplica(ctx, r, RestoreOptions{
		OutputPath:  restorePath,
		ReplicaName: r.Name(),
		Generation:  pos.Generation,
		Index:       pos.Index - 1,
		Logger:      log.New(os.Stderr, "", 0),
	}); err != nil {
		return fmt.Errorf("cannot restore: %w", err)
	}

	// Open file handle for restored database.
	// NOTE: This open is ok as the restored database is not managed by litestream.
	f, err := os.Open(restorePath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Read entire file into checksum.
	h := crc64.New(crc64.MakeTable(crc64.ISO))
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	chksum1 := h.Sum64()

	status := "ok"
	mismatch := chksum0 != chksum1
	if mismatch {
		status = "mismatch"
	}
	log.Printf("%s(%s): validator: status=%s db=%016x replica=%016x pos=%s", db.Path(), r.Name(), status, chksum0, chksum1, pos)

	// Validate checksums match.
	if mismatch {
		internal.ReplicaValidationTotalCounterVec.WithLabelValues(db.Path(), r.Name(), "error").Inc()
		return ErrChecksumMismatch
	}

	internal.ReplicaValidationTotalCounterVec.WithLabelValues(db.Path(), r.Name(), "ok").Inc()

	if err := os.RemoveAll(tmpdir); err != nil {
		return fmt.Errorf("cannot remove temporary validation directory: %w", err)
	}
	return nil
}

// waitForReplica blocks until replica reaches at least the given position.
func waitForReplica(ctx context.Context, r Replica, pos Pos) error {
	db := r.DB()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	timer := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	once := make(chan struct{}, 1)
	once <- struct{}{}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("replica wait exceeded timeout")
		case <-ticker.C:
		case <-once: // immediate on first check
		}

		// Obtain current position of replica, check if past target position.
		curr, err := r.CalcPos(ctx, pos.Generation)
		if err != nil {
			log.Printf("%s(%s): validator: cannot obtain replica position: %s", db.Path(), r.Name(), err)
			continue
		}

		// Exit if the generation has changed while waiting as there will be
		// no further progress on the old generation.
		if curr.Generation != pos.Generation {
			return fmt.Errorf("generation changed")
		}

		ready := true
		if curr.Index < pos.Index {
			ready = false
		} else if curr.Index == pos.Index && curr.Offset < pos.Offset {
			ready = false
		}

		// If not ready, restart loop.
		if !ready {
			continue
		}

		// Current position at or after target position.
		return nil
	}
}
