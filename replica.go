package litestream

import (
	"context"
	"fmt"
	"hash/crc64"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"filippo.io/age"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/superfly/ltx"
)

// Default replica settings.
const (
	DefaultSyncInterval           = 1 * time.Second
	DefaultRetention              = 24 * time.Hour
	DefaultRetentionCheckInterval = 1 * time.Hour
)

// Replica connects a database to a replication destination via a ReplicaClient.
// The replica manages periodic synchronization and maintaining the current
// replica position.
type Replica struct {
	db   *DB
	name string

	mu  sync.RWMutex
	pos ltx.Pos // current replicated position

	muf sync.Mutex
	f   *os.File // long-running file descriptor to avoid non-OFD lock issues

	wg     sync.WaitGroup
	cancel func()

	// Client used to connect to the remote replica.
	Client ReplicaClient

	// Time between syncs with the shadow WAL.
	SyncInterval time.Duration

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

	// Encryption identities and recipients
	AgeIdentities []age.Identity
	AgeRecipients []age.Recipient
}

func NewReplica(db *DB, name string) *Replica {
	r := &Replica{
		db:     db,
		name:   name,
		cancel: func() {},

		SyncInterval:           DefaultSyncInterval,
		Retention:              DefaultRetention,
		RetentionCheckInterval: DefaultRetentionCheckInterval,
		MonitorEnabled:         true,
	}

	return r
}

// Name returns the name of the replica.
func (r *Replica) Name() string {
	if r.name == "" && r.Client != nil {
		return r.Client.Type()
	}
	return r.name
}

// Logger returns the DB sub-logger for this replica.
func (r *Replica) Logger() *slog.Logger {
	logger := slog.Default()
	if r.db != nil {
		logger = r.db.Logger
	}
	return logger.With("replica", r.Name())
}

// DB returns a reference to the database the replica is attached to, if any.
func (r *Replica) DB() *DB { return r.db }

// Starts replicating in a background goroutine.
func (r *Replica) Start(ctx context.Context) error {
	// Ignore if replica is being used sychronously.
	if !r.MonitorEnabled {
		return nil
	}

	// Stop previous replication.
	r.Stop(false)

	// Wrap context with cancelation.
	ctx, r.cancel = context.WithCancel(ctx)

	// Start goroutine to replicate data.
	r.wg.Add(3)
	go func() { defer r.wg.Done(); r.monitor(ctx) }()
	go func() { defer r.wg.Done(); r.retainer(ctx) }()
	go func() { defer r.wg.Done(); r.validator(ctx) }()

	return nil
}

// Stop cancels any outstanding replication and blocks until finished.
//
// Performing a hard stop will close the DB file descriptor which could release
// locks on per-process locks. Hard stops should only be performed when
// stopping the entire process.
func (r *Replica) Stop(hard bool) (err error) {
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

// Sync copies new WAL frames from the shadow WAL to the replica client.
func (r *Replica) Sync(ctx context.Context) (err error) {
	// Clear last position if if an error occurs during sync.
	defer func() {
		if err != nil {
			r.mu.Lock()
			r.pos = ltx.Pos{}
			r.mu.Unlock()
		}
	}()

	// Calculate current replica position, if unknown.
	if r.Pos().IsZero() {
		pos, err := r.calcPos(ctx)
		if err != nil {
			return fmt.Errorf("calc pos: %w", err)
		}
		r.SetPos(pos)
	}

	// Find current position of database.
	dpos, err := r.db.Pos()
	if err != nil {
		return fmt.Errorf("cannot determine current position: %w", err)
	} else if dpos.IsZero() {
		return fmt.Errorf("no position, waiting for data")
	}

	r.Logger().Debug("replica sync", "txid", dpos.TXID.String())

	// Replicate all L0 LTX files since last replica position.
	for txID := r.Pos().TXID + 1; txID <= dpos.TXID; txID = r.Pos().TXID + 1 {
		if err = r.uploadLTXFile(ctx, 0, txID, txID); err != nil {
			return err
		}
		r.SetPos(ltx.Pos{TXID: txID})
	}

	return nil
}

func (r *Replica) uploadLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (err error) {
	filename := r.db.LTXPath(level, minTXID, maxTXID)
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := r.Client.WriteLTXFile(ctx, level, minTXID, maxTXID, f); err != nil {
		return fmt.Errorf("write ltx file: %w", err)
	}
	r.Logger().Debug("ltx file uploaded", "filename", filename, "minTXID", minTXID, "maxTXID", maxTXID)

	// Track current position
	//replicaWALIndexGaugeVec.WithLabelValues(r.db.Path(), r.Name()).Set(float64(rd.Pos().Index))
	//replicaWALOffsetGaugeVec.WithLabelValues(r.db.Path(), r.Name()).Set(float64(rd.Pos().Offset))

	return nil
}

// calcPos returns the last position saved to the replica for level 0.
func (r *Replica) calcPos(ctx context.Context) (pos ltx.Pos, err error) {
	_, maxTXID, err := r.maxLTXFile(ctx, 0)
	if err != nil {
		return pos, fmt.Errorf("max ltx file: %w", err)
	}
	return ltx.Pos{TXID: maxTXID}, nil
}

// maxLTXFile returns the min & max TXID of the last LTX file for a given level.
func (r *Replica) maxLTXFile(ctx context.Context, level int) (minTXID, maxTXID ltx.TXID, err error) {
	itr, err := r.Client.LTXFiles(ctx, level)
	if err != nil {
		return 0, 0, err
	}
	defer itr.Close()

	for itr.Next() {
		info := itr.Item()
		if info.MaxTXID > maxTXID {
			minTXID, maxTXID = info.MinTXID, info.MaxTXID
		}
	}
	return minTXID, maxTXID, itr.Close()
}

// Pos returns the current replicated position.
// Returns a zero value if the current position cannot be determined.
func (r *Replica) Pos() ltx.Pos {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pos
}

// SetPos sets the current replicated position.
func (r *Replica) SetPos(pos ltx.Pos) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.pos = pos
}

// EnforceRetention forces a new snapshot once the retention interval has passed.
// Older snapshots and WAL files are then removed.
func (r *Replica) EnforceRetention(ctx context.Context) (err error) {
	panic("TODO(ltx): Re-implement after multi-level compaction")

	/*
		// Obtain list of snapshots that are within the retention period.
		snapshots, err := r.Snapshots(ctx)
		if err != nil {
			return fmt.Errorf("snapshots: %w", err)
		}
		retained := FilterSnapshotsAfter(snapshots, time.Now().Add(-r.Retention))

		// If no retained snapshots exist, create a new snapshot.
		if len(retained) == 0 {
			snapshot, err := r.Snapshot(ctx)
			if err != nil {
				return fmt.Errorf("snapshot: %w", err)
			}
			retained = append(retained, snapshot)
		}

		// Delete unretained snapshots & WAL files.
		snapshot := FindMinSnapshot(retained)

		// Otherwise remove all earlier snapshots & WAL segments.
		if err := r.deleteSnapshotsBeforeIndex(ctx, snapshot.Index); err != nil {
			return fmt.Errorf("delete snapshots before index: %w", err)
		} else if err := r.deleteWALSegmentsBeforeIndex(ctx, snapshot.Index); err != nil {
			return fmt.Errorf("delete wal segments before index: %w", err)
		}

		return nil
	*/
}

/*
func (r *Replica) deleteBeforeTXID(ctx context.Context, level int, txID ltx.TXID) error {
	itr, err := r.Client.LTXFiles(ctx, level)
	if err != nil {
		return fmt.Errorf("fetch ltx files: %w", err)
	}
	defer itr.Close()

	var a []*ltx.FileInfo
	for itr.Next() {
		info := itr.Item()
		if info.MinTXID >= txID {
			continue
		}
		a = append(a, info)
	}
	if err := itr.Close(); err != nil {
		return err
	}

	if len(a) == 0 {
		return nil
	}

	if err := r.Client.DeleteLTXFiles(ctx, a); err != nil {
		return fmt.Errorf("delete wal segments: %w", err)
	}

	r.Logger().Info("ltx files deleted before",
		slog.Int("level", level),
		slog.String("txID", txID.String()),
		slog.Int("n", len(a)))

	return nil
}
*/

// monitor runs in a separate goroutine and continuously replicates the DB.
func (r *Replica) monitor(ctx context.Context) {
	ticker := time.NewTicker(r.SyncInterval)
	defer ticker.Stop()

	// Continuously check for new data to replicate.
	ch := make(chan struct{})
	close(ch)
	var notify <-chan struct{} = ch

	for initial := true; ; initial = false {
		// Enforce a minimum time between synchronization.
		if !initial {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}

		// Wait for changes to the database.
		select {
		case <-ctx.Done():
			return
		case <-notify:
		}

		// Fetch new notify channel before replicating data.
		notify = r.db.Notify()

		// Synchronize the shadow wal into the replication directory.
		if err := r.Sync(ctx); err != nil {
			r.Logger().Error("monitor error", "error", err)
			continue
		}
	}
}

// retainer runs in a separate goroutine and handles retention.
func (r *Replica) retainer(ctx context.Context) {
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
				r.Logger().Error("retainer error", "error", err)
				continue
			}
		}
	}
}

// validator runs in a separate goroutine and handles periodic validation.
func (r *Replica) validator(ctx context.Context) {
	// Initialize counters since validation occurs infrequently.
	for _, status := range []string{"ok", "error"} {
		replicaValidationTotalCounterVec.WithLabelValues(r.db.Path(), r.Name(), status).Add(0)
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
			if err := r.Validate(ctx); err != nil {
				r.Logger().Error("validation error", "error", err)
				continue
			}
		}
	}
}

// Validate restores the most recent data from a replica and validates
// that the resulting database matches the current database.
func (r *Replica) Validate(ctx context.Context) error {
	db := r.DB()

	// Restore replica to a temporary directory.
	tmpdir, err := os.MkdirTemp("", "*-litestream")
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
	if err := r.waitForReplica(ctx, pos); err != nil {
		return fmt.Errorf("cannot wait for replica: %w", err)
	}

	restorePath := filepath.Join(tmpdir, "replica")
	if err := r.Restore(ctx, RestoreOptions{
		OutputPath:  restorePath,
		ReplicaName: r.Name(),
		TXID:        pos.TXID - 1,
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
	r.Logger().Info("validator", "status", status, "db", fmt.Sprintf("%016x", chksum0), "replica", fmt.Sprintf("%016x", chksum1), "position", pos.String())

	// Validate checksums match.
	if mismatch {
		replicaValidationTotalCounterVec.WithLabelValues(r.db.Path(), r.Name(), "error").Inc()
		return ErrChecksumMismatch
	}

	replicaValidationTotalCounterVec.WithLabelValues(r.db.Path(), r.Name(), "ok").Inc()

	if err := os.RemoveAll(tmpdir); err != nil {
		return fmt.Errorf("cannot remove temporary validation directory: %w", err)
	}
	return nil
}

// waitForReplica blocks until replica reaches at least the given position.
func (r *Replica) waitForReplica(ctx context.Context, pos ltx.Pos) error {
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
		curr := r.Pos()
		if curr.IsZero() {
			r.Logger().Info("validator: no replica position available")
			continue
		}

		ready := true
		if curr.TXID < pos.TXID {
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

// CreatedAt returns the earliest creation time of any LTX file.
// Returns zero time if no LTX files exist.
func (r *Replica) CreatedAt(ctx context.Context) (time.Time, error) {
	var min time.Time

	itr, err := r.Client.LTXFiles(ctx, 0)
	if err != nil {
		return min, err
	}
	defer itr.Close()

	if itr.Next() {
		min = itr.Item().CreatedAt
	}
	return min, itr.Close()
}

// TimeBounds returns the creation time & last updated time.
// Returns zero time if LTX files exist.
func (r *Replica) TimeBounds(ctx context.Context) (createdAt, updatedAt time.Time, err error) {
	itr, err := r.Client.LTXFiles(ctx, 0)
	if err != nil {
		return createdAt, updatedAt, err
	}
	defer itr.Close()

	for itr.Next() {
		info := itr.Item()
		if createdAt.IsZero() || info.CreatedAt.Before(createdAt) {
			createdAt = info.CreatedAt
		}
		if updatedAt.IsZero() || info.CreatedAt.After(updatedAt) {
			updatedAt = info.CreatedAt
		}
	}
	return createdAt, updatedAt, itr.Close()
}

// CalcRestoreTarget returns a target time restore from.
func (r *Replica) CalcRestoreTarget(ctx context.Context, opt RestoreOptions) (updatedAt time.Time, err error) {
	// Determine the replicated time bounds.
	createdAt, updatedAt, err := r.TimeBounds(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("created at: %w", err)
	}

	// Skip if it does not contain timestamp.
	if !opt.Timestamp.IsZero() {
		if opt.Timestamp.Before(createdAt) || opt.Timestamp.After(updatedAt) {
			return time.Time{}, fmt.Errorf("timestamp does not exist")
		}
	}

	return updatedAt, nil
}

// Replica restores the database from a replica based on the options given.
// This method will restore into opt.OutputPath, if specified, or into the
// DB's original database path. It can optionally restore from a specific
// replica or it will automatically choose the best one. Finally,
// a timestamp can be specified to restore the database to a specific
// point-in-time.
func (r *Replica) Restore(ctx context.Context, opt RestoreOptions) (err error) {
	// Validate options.
	if opt.OutputPath == "" {
		return fmt.Errorf("output path required")
	} else if opt.TXID != 0 && !opt.Timestamp.IsZero() {
		return fmt.Errorf("cannot specify index & timestamp to restore")
	}

	// Ensure output path does not already exist.
	if _, err := os.Stat(opt.OutputPath); err == nil {
		return fmt.Errorf("cannot restore, output path already exists: %s", opt.OutputPath)
	} else if !os.IsNotExist(err) {
		return err
	}

	// Fetch every LTX file from TXID 1 to target TXID or timestamp.
	itr, err := r.Client.LTXFiles(ctx, 0)
	if err != nil {
		return fmt.Errorf("cannot list ltx files: %w", err)
	}

	var rdrs []io.Reader
	defer func() {
		for _, rd := range rdrs {
			_ = rd.(io.Closer).Close()
		}
	}()

	for itr.Next() {
		info := itr.Item()
		if opt.TXID > info.MinTXID {
			break
		}

		// TODO(ltx): Check that timestamp is within bounds, if specified

		r.Logger().Debug("opening ltx file for restore", "level", info.Level, "min", info.MinTXID, "max", info.MaxTXID)

		// Add file to be compacted.
		f, err := r.Client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID)
		if err != nil {
			return fmt.Errorf("open ltx file: %w", err)
		}
		rdrs = append(rdrs, f)
	}

	if len(rdrs) == 0 {
		return fmt.Errorf("no matching backup files available")
	}

	// Output to temp file & atomically rename.
	tmpOutputPath := opt.OutputPath + ".tmp"
	r.Logger().Debug("compacting into database", "path", tmpOutputPath, "n", len(rdrs))

	f, err := os.Create(tmpOutputPath)
	if err != nil {
		return fmt.Errorf("create temp database path: %w", err)
	}
	defer func() { _ = f.Close() }()

	pr, pw := io.Pipe()

	go func() {
		c := ltx.NewCompactor(pw, rdrs)
		c.HeaderFlags = ltx.HeaderFlagNoChecksum | ltx.HeaderFlagCompressLZ4
		_ = pw.CloseWithError(c.Compact(ctx))
	}()

	dec := ltx.NewDecoder(pr)
	if err := dec.DecodeDatabaseTo(f); err != nil {
		return fmt.Errorf("decode database: %w", err)
	}

	if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	// Copy file to final location.
	r.Logger().Debug("renaming database from temporary location")
	if err := os.Rename(tmpOutputPath, opt.OutputPath); err != nil {
		return err
	}

	return nil
}

// Replica metrics.
var (
	replicaValidationTotalCounterVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "litestream",
		Subsystem: "replica",
		Name:      "validation_total",
		Help:      "The number of validations performed",
	}, []string{"db", "name", "status"})
)
