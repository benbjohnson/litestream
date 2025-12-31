//go:build vfs
// +build vfs

package litestream

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/superfly/ltx"
)

// HydrationState represents the current state of the hydration process.
type HydrationState int

const (
	HydrationNotStarted HydrationState = iota
	HydrationInProgress
	HydrationCompleted
	HydrationFailed
)

func (s HydrationState) String() string {
	switch s {
	case HydrationNotStarted:
		return "not_started"
	case HydrationInProgress:
		return "in_progress"
	case HydrationCompleted:
		return "completed"
	case HydrationFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// HydrationStatus provides a snapshot of hydration progress.
type HydrationStatus struct {
	State          HydrationState
	Err            error
	StartTime      time.Time
	CompleteTime   time.Time
	PagesCompleted uint32
	TotalPages     uint32
}

// IsComplete returns true if hydration finished (success or failure).
func (s HydrationStatus) IsComplete() bool {
	return s.State == HydrationCompleted || s.State == HydrationFailed
}

// Progress returns completion percentage (0.0 to 1.0).
func (s HydrationStatus) Progress() float64 {
	if s.TotalPages == 0 {
		return 0.0
	}
	return float64(s.PagesCompleted) / float64(s.TotalPages)
}

// Hydrator manages background database restoration to local disk.
// It performs a full restore of the database from replica storage,
// allowing VFS reads to serve pages from local disk instead of remote storage.
type Hydrator struct {
	mu sync.RWMutex

	// Configuration
	client     ReplicaClient
	dbPath     string // Database path (for logging/identification)
	outputPath string // Path to .hydrating file
	logger     *slog.Logger

	// State
	state        HydrationState
	err          error // Error that occurred during hydration
	startTime    time.Time
	completeTime time.Time

	// Compactor reference for progress tracking
	compactor *ltx.Compactor

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	done   chan struct{} // Closed when hydration completes/fails
}

// NewHydrator creates a new hydrator for a database path.
func NewHydrator(client ReplicaClient, dbPath string, logger *slog.Logger) *Hydrator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Hydrator{
		client:     client,
		dbPath:     dbPath,
		outputPath: dbPath + ".hydrating",
		logger:     logger.With("component", "hydrator", "db", dbPath),
		state:      HydrationNotStarted,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
}

// Start begins the background hydration process.
// Returns immediately; use Wait() or Status() to track progress.
func (h *Hydrator) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.state != HydrationNotStarted {
		return fmt.Errorf("hydration already started")
	}

	h.state = HydrationInProgress
	h.startTime = time.Now()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer close(h.done)
		h.run()
	}()

	return nil
}

// run executes the hydration process (runs in background goroutine).
func (h *Hydrator) run() {
	h.logger.Info("starting database hydration", "output", h.outputPath)

	// Clean up on exit
	defer func() {
		h.mu.Lock()
		h.completeTime = time.Now()

		if h.err != nil {
			h.state = HydrationFailed
			h.logger.Error("hydration failed", "error", h.err, "duration", h.completeTime.Sub(h.startTime))

			// Delete .hydrating file on failure
			if err := os.Remove(h.outputPath); err != nil && !os.IsNotExist(err) {
				h.logger.Error("failed to clean up hydrating file", "error", err)
			}
		} else {
			h.state = HydrationCompleted
			h.logger.Info("hydration completed", "duration", h.completeTime.Sub(h.startTime))
		}
		h.mu.Unlock()
	}()

	// Perform restoration
	if err := h.restore(); err != nil {
		h.mu.Lock()
		h.err = err
		h.mu.Unlock()
		return
	}

	// Atomic rename to remove .hydrating extension
	finalPath := strings.TrimSuffix(h.outputPath, ".hydrating")
	h.logger.Debug("renaming hydrated file", "from", h.outputPath, "to", finalPath)

	if err := os.Rename(h.outputPath, finalPath); err != nil {
		h.mu.Lock()
		h.err = fmt.Errorf("atomic rename failed: %w", err)
		h.mu.Unlock()
		return
	}
}

// restore performs the actual database restoration.
func (h *Hydrator) restore() error {
	// Calculate restore plan
	infos, err := CalcRestorePlan(h.ctx, h.client, 0, time.Time{}, h.logger)
	if err != nil {
		return fmt.Errorf("calc restore plan: %w", err)
	}

	h.logger.Debug("restore plan calculated", "files", len(infos))

	// Open all LTX files
	rdrs := make([]io.Reader, 0, len(infos))
	defer func() {
		for _, rd := range rdrs {
			if closer, ok := rd.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	}()

	for _, info := range infos {
		if info.Size < ltx.HeaderSize {
			return fmt.Errorf("invalid ltx file: level=%d min=%s max=%s size=%d",
				info.Level, info.MinTXID, info.MaxTXID, info.Size)
		}

		rc, err := h.client.OpenLTXFile(h.ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
		if err != nil {
			return fmt.Errorf("open ltx file: %w", err)
		}
		rdrs = append(rdrs, rc)
	}

	// Create output file
	f, err := os.Create(h.outputPath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()

	// Set up compaction pipeline
	pr, pw := io.Pipe()

	// Compactor goroutine
	compactorErrCh := make(chan error, 1)
	go func() {
		compactor, err := ltx.NewCompactor(pw, rdrs)
		if err != nil {
			compactorErrCh <- fmt.Errorf("new compactor: %w", err)
			pw.CloseWithError(err)
			return
		}

		compactor.HeaderFlags = ltx.HeaderFlagNoChecksum

		// Store reference so Status() can query it directly
		h.mu.Lock()
		h.compactor = compactor
		h.mu.Unlock()

		err = compactor.Compact(h.ctx)
		compactorErrCh <- err
		pw.CloseWithError(err)
	}()

	// Decode to output file
	dec := ltx.NewDecoder(pr)
	if err := dec.DecodeDatabaseTo(f); err != nil {
		return fmt.Errorf("decode database: %w", err)
	}

	// Sync to disk
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	// Wait for compactor to finish
	if err := <-compactorErrCh; err != nil {
		return fmt.Errorf("compact: %w", err)
	}

	return nil
}

// Status returns the current hydration status.
// Safe to call concurrently.
func (h *Hydrator) Status() HydrationStatus {
	h.mu.RLock()
	state := h.state
	err := h.err
	startTime := h.startTime
	completeTime := h.completeTime
	compactor := h.compactor
	h.mu.RUnlock()

	var compactorStatus ltx.CompactorStatus
	if compactor != nil {
		compactorStatus = compactor.Status()
	}

	return HydrationStatus{
		State:          state,
		Err:            err,
		StartTime:      startTime,
		CompleteTime:   completeTime,
		PagesCompleted: compactorStatus.N,
		TotalPages:     compactorStatus.Total,
	}
}

// Wait blocks until hydration completes or context is cancelled.
func (h *Hydrator) Wait(ctx context.Context) error {
	select {
	case <-h.done:
		h.mu.RLock()
		err := h.err
		h.mu.RUnlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop cancels the hydration process and waits for cleanup.
func (h *Hydrator) Stop() error {
	h.cancel()
	h.wg.Wait()

	h.mu.RLock()
	err := h.err
	h.mu.RUnlock()

	return err
}

// IsPageHydrated returns true if the given page number is available locally.
// This allows VFSFile to optimize by reading from local disk instead of remote.
// Note: This returns true for pages 1 to N where N is the last page written
// by the compactor (in page number order).
func (h *Hydrator) IsPageHydrated(pgno uint32) bool {
	h.mu.RLock()
	compactor := h.compactor
	h.mu.RUnlock()

	if compactor == nil {
		return false
	}
	return pgno <= compactor.Status().N
}

// OutputPath returns the path to the hydrating file.
func (h *Hydrator) OutputPath() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.outputPath
}

// Done returns a channel that is closed when hydration completes or fails.
func (h *Hydrator) Done() <-chan struct{} {
	return h.done
}
