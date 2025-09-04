package litestream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/superfly/ltx"
)

var (
	// ErrNoCompaction is returned when no new files are available from the previous level.
	ErrNoCompaction = errors.New("no compaction")

	// ErrCompactionTooEarly is returned when a compaction is attempted too soon
	// since the last compaction time. This is used to prevent frequent
	// re-compaction when restarting the process.
	ErrCompactionTooEarly = errors.New("compaction too early")

	// ErrTxNotAvailable is returned when a transaction does not exist.
	ErrTxNotAvailable = errors.New("transaction not available")
)

// Store defaults
const (
	DefaultSnapshotInterval  = 24 * time.Hour
	DefaultSnapshotRetention = 24 * time.Hour

	DefaultRetention              = 24 * time.Hour
	DefaultRetentionCheckInterval = 1 * time.Hour
)

// Store represents the top-level container for databases.
//
// It manages async background tasks like compactions so that the system
// is not overloaded by too many concurrent tasks.
type Store struct {
	mu     sync.Mutex
	dbs    []*DB
	levels CompactionLevels

	wg     sync.WaitGroup
	ctx    context.Context
	cancel func()

	// The frequency of snapshots.
	SnapshotInterval time.Duration
	// The duration of time that snapshots are kept before being deleted.
	SnapshotRetention time.Duration

	// If true, compaction is run in the background according to compaction levels.
	CompactionMonitorEnabled bool
}

func NewStore(dbs []*DB, levels CompactionLevels) *Store {
	s := &Store{
		dbs:    dbs,
		levels: levels,

		SnapshotInterval:         DefaultSnapshotInterval,
		SnapshotRetention:        DefaultSnapshotRetention,
		CompactionMonitorEnabled: true,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s
}

func (s *Store) Open(ctx context.Context) error {
	if err := s.levels.Validate(); err != nil {
		return err
	}

	for _, db := range s.dbs {
		if err := db.Open(); err != nil {
			return err
		}
	}

	// Start monitors for compactions & snapshots.
	if s.CompactionMonitorEnabled {
		// Start compaction monitors for all levels except L0.
		for _, lvl := range s.levels {
			lvl := lvl
			if lvl.Level == 0 {
				continue
			}

			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.monitorCompactionLevel(s.ctx, lvl)
			}()
		}

		// Start snapshot monitor for snapshots.
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.monitorCompactionLevel(s.ctx, s.SnapshotLevel())
		}()
	}

	return nil
}

func (s *Store) Close(ctx context.Context) (err error) {
	for _, db := range s.dbs {
		if e := db.Close(ctx); e != nil && err == nil {
			err = e
		}
	}

	// Cancel and wait for background tasks to complete.
	s.cancel()
	s.wg.Wait()

	return err
}

func (s *Store) DBs() []*DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.dbs)
}

// SnapshotLevel returns a pseudo compaction level based on snapshot settings.
func (s *Store) SnapshotLevel() *CompactionLevel {
	return &CompactionLevel{
		Level:    SnapshotLevel,
		Interval: s.SnapshotInterval,
	}
}

func (s *Store) monitorCompactionLevel(ctx context.Context, lvl *CompactionLevel) {
	slog.Info("starting compaction monitor", "level", lvl.Level, "interval", lvl.Interval)

	// Start first compaction immediately to check for any missed compactions from shutdown
	timer := time.NewTimer(time.Nanosecond)

LOOP:
	for {
		select {
		case <-ctx.Done():
			timer.Stop()
			break LOOP

		case <-timer.C:
			// Reset timer before we start compactions so we don't delay it
			// from long compactions.
			timer = time.NewTimer(time.Until(lvl.NextCompactionAt(time.Now())))

			for _, db := range s.DBs() {
				// First attempt to compact the database.
				if _, err := s.CompactDB(ctx, db, lvl); errors.Is(err, ErrNoCompaction) {
					slog.Debug("no compaction", "level", lvl.Level, "path", db.Path())
					continue
				} else if errors.Is(err, ErrCompactionTooEarly) {
					slog.Debug("recently compacted, skipping", "level", lvl.Level, "path", db.Path())
					continue
				} else if err != nil {
					// Don't log context cancellation errors during shutdown
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						slog.Error("compaction failed", "level", lvl.Level, "error", err)
					}
					time.Sleep(1 * time.Second) // wait so we don't rack up S3 charges
				}

				// Each time we snapshot, clean up everything before the oldest snapshot.
				if lvl.Level == SnapshotLevel {
					if err := s.EnforceSnapshotRetention(ctx, db); err != nil {
						// Don't log context cancellation errors during shutdown
						if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
							slog.Error("retention enforcement failed", "error", err)
						}
						time.Sleep(1 * time.Second) // wait so we don't rack up S3 charges
					}
				}
			}
		}
	}
}

// CompactDB performs a compaction or snapshot for a given database on a single destination level.
// This function will only proceed if a compaction has not occurred before the last compaction time.
func (s *Store) CompactDB(ctx context.Context, db *DB, lvl *CompactionLevel) (*ltx.FileInfo, error) {
	dstLevel := lvl.Level

	// Ensure we are not re-compacting before the most recent compaction time.
	prevCompactionAt := lvl.PrevCompactionAt(time.Now())
	dstInfo, err := db.MaxLTXFileInfo(ctx, dstLevel)
	if err != nil {
		return nil, fmt.Errorf("fetch dst level info: %w", err)
	} else if dstInfo.CreatedAt.After(prevCompactionAt) {
		return nil, ErrCompactionTooEarly
	}

	// Shortcut if this is a snapshot since we are not pulling from a previous level.
	if dstLevel == SnapshotLevel {
		info, err := db.Snapshot(ctx)
		if err != nil {
			return info, err
		}
		slog.InfoContext(ctx, "snapshot complete", "txid", info.MaxTXID.String(), "size", info.Size)
		return info, nil
	}

	// Fetch latest LTX files for both the source & destination so we can see if we need to make progress.
	srcLevel := s.levels.PrevLevel(dstLevel)
	srcInfo, err := db.MaxLTXFileInfo(ctx, srcLevel)
	if err != nil {
		return nil, fmt.Errorf("fetch src level info: %w", err)
	}

	// Skip if there are no new files to compact.
	if srcInfo.MaxTXID <= dstInfo.MinTXID {
		return nil, ErrNoCompaction
	}

	info, err := db.Compact(ctx, dstLevel)
	if err != nil {
		return info, err
	}

	slog.InfoContext(ctx, "compaction complete",
		"level", dstLevel,
		slog.Group("txid",
			"min", info.MinTXID.String(),
			"max", info.MaxTXID.String(),
		),
		"size", info.Size,
	)

	return info, nil
}

// EnforceSnapshotRetention removes old snapshots by timestamp and then
// cleans up all lower levels based on minimum snapshot TXID.
func (s *Store) EnforceSnapshotRetention(ctx context.Context, db *DB) error {
	// Enforce retention for the snapshot level.
	minSnapshotTXID, err := db.EnforceSnapshotRetention(ctx, time.Now().Add(-s.SnapshotRetention))
	if err != nil {
		return fmt.Errorf("enforce snapshot retention: %w", err)
	}

	// We should also enforce retention for L0 on the same schedule as L1.
	for _, lvl := range s.levels {
		// Skip L0 since it is enforced on a more frequent basis.
		if lvl.Level == 0 {
			continue
		}

		if err := db.EnforceRetentionByTXID(ctx, lvl.Level, minSnapshotTXID); err != nil {
			return fmt.Errorf("enforce L%d retention: %w", lvl.Level, err)
		}
	}

	return nil
}
