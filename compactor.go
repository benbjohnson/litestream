package litestream

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/superfly/ltx"
)

// Compactor handles compaction and retention for LTX files.
// It operates solely through the ReplicaClient interface, making it
// suitable for both DB (with local file caching) and VFS (remote-only).
type Compactor struct {
	client ReplicaClient
	logger *slog.Logger

	// VerifyCompaction enables post-compaction TXID consistency verification.
	// When enabled, verifies that files at the destination level have
	// contiguous TXID ranges after each compaction. Disabled by default.
	VerifyCompaction bool

	// LocalFileOpener optionally opens a local LTX file for compaction.
	// If nil or returns os.ErrNotExist, falls back to remote.
	// This is used by DB to prefer local files over remote for consistency.
	LocalFileOpener func(level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error)

	// LocalFileDeleter optionally deletes local LTX files after retention.
	// If nil, only remote files are deleted.
	LocalFileDeleter func(level int, minTXID, maxTXID ltx.TXID) error

	// CacheGetter optionally retrieves cached MaxLTXFileInfo for a level.
	// If nil, max file info is always fetched from remote.
	CacheGetter func(level int) (*ltx.FileInfo, bool)

	// CacheSetter optionally stores MaxLTXFileInfo for a level.
	// If nil, max file info is not cached.
	CacheSetter func(level int, info *ltx.FileInfo)
}

// NewCompactor creates a new Compactor with the given client and logger.
func NewCompactor(client ReplicaClient, logger *slog.Logger) *Compactor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Compactor{
		client: client,
		logger: logger,
	}
}

// Client returns the underlying ReplicaClient.
func (c *Compactor) Client() ReplicaClient {
	return c.client
}

// SetClient updates the ReplicaClient.
// This is used by DB when the Replica is assigned after construction.
func (c *Compactor) SetClient(client ReplicaClient) {
	c.client = client
}

// MaxLTXFileInfo returns metadata for the last LTX file in a level.
// Uses cache if available, otherwise fetches from remote.
func (c *Compactor) MaxLTXFileInfo(ctx context.Context, level int) (ltx.FileInfo, error) {
	if c.CacheGetter != nil {
		if info, ok := c.CacheGetter(level); ok {
			return *info, nil
		}
	}

	itr, err := c.client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return ltx.FileInfo{}, err
	}
	defer itr.Close()

	var info ltx.FileInfo
	for itr.Next() {
		item := itr.Item()
		if item.MaxTXID > info.MaxTXID {
			info = *item
		}
	}

	if c.CacheSetter != nil && info.MaxTXID > 0 {
		c.CacheSetter(level, &info)
	}

	return info, itr.Close()
}

// Compact compacts source level files into the destination level.
// Returns ErrNoCompaction if there are no files to compact.
func (c *Compactor) Compact(ctx context.Context, dstLevel int) (*ltx.FileInfo, error) {
	srcLevel := dstLevel - 1

	prevMaxInfo, err := c.MaxLTXFileInfo(ctx, dstLevel)
	if err != nil {
		return nil, fmt.Errorf("cannot determine max ltx file for destination level: %w", err)
	}
	seekTXID := prevMaxInfo.MaxTXID + 1

	itr, err := c.client.LTXFiles(ctx, srcLevel, seekTXID, false)
	if err != nil {
		return nil, fmt.Errorf("source ltx files after %s: %w", seekTXID, err)
	}
	defer itr.Close()

	var rdrs []io.Reader
	defer func() {
		for _, rd := range rdrs {
			if closer, ok := rd.(io.Closer); ok {
				_ = closer.Close()
			}
		}
	}()

	var minTXID, maxTXID ltx.TXID
	for itr.Next() {
		info := itr.Item()

		if minTXID == 0 || info.MinTXID < minTXID {
			minTXID = info.MinTXID
		}
		if maxTXID == 0 || info.MaxTXID > maxTXID {
			maxTXID = info.MaxTXID
		}

		if c.LocalFileOpener != nil {
			if f, err := c.LocalFileOpener(srcLevel, info.MinTXID, info.MaxTXID); err == nil {
				rdrs = append(rdrs, f)
				continue
			} else if !os.IsNotExist(err) {
				return nil, fmt.Errorf("open local ltx file: %w", err)
			}
		}

		f, err := c.client.OpenLTXFile(ctx, info.Level, info.MinTXID, info.MaxTXID, 0, 0)
		if err != nil {
			return nil, fmt.Errorf("open ltx file: %w", err)
		}
		rdrs = append(rdrs, f)
	}
	if len(rdrs) == 0 {
		return nil, ErrNoCompaction
	}

	pr, pw := io.Pipe()
	go func() {
		comp, err := ltx.NewCompactor(pw, rdrs)
		if err != nil {
			pw.CloseWithError(fmt.Errorf("new ltx compactor: %w", err))
			return
		}
		comp.HeaderFlags = ltx.HeaderFlagNoChecksum
		_ = pw.CloseWithError(comp.Compact(ctx))
	}()

	info, err := c.client.WriteLTXFile(ctx, dstLevel, minTXID, maxTXID, pr)
	if err != nil {
		return nil, fmt.Errorf("write ltx file: %w", err)
	}

	if c.CacheSetter != nil {
		c.CacheSetter(dstLevel, info)
	}

	// Verify level consistency if enabled
	if c.VerifyCompaction {
		if err := c.VerifyLevelConsistency(ctx, dstLevel); err != nil {
			c.logger.Warn("post-compaction verification failed",
				"level", dstLevel,
				"error", err)
		}
	}

	return info, nil
}

// VerifyLevelConsistency checks that LTX files at the given level have
// contiguous TXID ranges (prevMaxTXID + 1 == currMinTXID for consecutive files).
// Returns an error describing any gaps or overlaps found.
func (c *Compactor) VerifyLevelConsistency(ctx context.Context, level int) error {
	itr, err := c.client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return fmt.Errorf("fetch ltx files: %w", err)
	}
	defer itr.Close()

	var prevInfo *ltx.FileInfo
	for itr.Next() {
		info := itr.Item()

		// Skip first file - nothing to compare against
		if prevInfo == nil {
			prevInfo = info
			continue
		}

		// Check for TXID contiguity: prev.MaxTXID + 1 should equal curr.MinTXID
		expectedMinTXID := prevInfo.MaxTXID + 1
		if info.MinTXID != expectedMinTXID {
			if info.MinTXID > expectedMinTXID {
				return fmt.Errorf("TXID gap detected: prev.MaxTXID=%s, next.MinTXID=%s (expected %s)",
					prevInfo.MaxTXID, info.MinTXID, expectedMinTXID)
			}
			return fmt.Errorf("TXID overlap detected: prev.MaxTXID=%s, next.MinTXID=%s",
				prevInfo.MaxTXID, info.MinTXID)
		}

		prevInfo = info
	}

	if err := itr.Close(); err != nil {
		return fmt.Errorf("close iterator: %w", err)
	}

	return nil
}

// EnforceSnapshotRetention enforces retention of snapshot level files by timestamp.
// Files older than the retention duration are deleted (except the newest is always kept).
// Returns the minimum snapshot TXID still retained (useful for cascading retention to lower levels).
func (c *Compactor) EnforceSnapshotRetention(ctx context.Context, retention time.Duration) (ltx.TXID, error) {
	timestamp := time.Now().Add(-retention)
	c.logger.Debug("enforcing snapshot retention", "timestamp", timestamp)

	itr, err := c.client.LTXFiles(ctx, SnapshotLevel, 0, false)
	if err != nil {
		return 0, fmt.Errorf("fetch ltx files: %w", err)
	}
	defer itr.Close()

	var deleted []*ltx.FileInfo
	var lastInfo *ltx.FileInfo
	var minSnapshotTXID ltx.TXID

	for itr.Next() {
		info := itr.Item()
		lastInfo = info

		if info.CreatedAt.Before(timestamp) {
			deleted = append(deleted, info)
			continue
		}

		if minSnapshotTXID == 0 || info.MaxTXID < minSnapshotTXID {
			minSnapshotTXID = info.MaxTXID
		}
	}

	if len(deleted) > 0 && deleted[len(deleted)-1] == lastInfo {
		deleted = deleted[:len(deleted)-1]
	}

	if err := c.client.DeleteLTXFiles(ctx, deleted); err != nil {
		return 0, fmt.Errorf("remove ltx files: %w", err)
	}

	if c.LocalFileDeleter != nil {
		for _, info := range deleted {
			c.logger.Debug("deleting local ltx file",
				"level", SnapshotLevel,
				"minTXID", info.MinTXID,
				"maxTXID", info.MaxTXID)
			if err := c.LocalFileDeleter(SnapshotLevel, info.MinTXID, info.MaxTXID); err != nil {
				c.logger.Error("failed to remove local ltx file", "error", err)
			}
		}
	}

	return minSnapshotTXID, nil
}

// EnforceRetentionByTXID deletes files at the given level with maxTXID below the target.
// Always keeps at least one file.
func (c *Compactor) EnforceRetentionByTXID(ctx context.Context, level int, txID ltx.TXID) error {
	c.logger.Debug("enforcing retention", "level", level, "txid", txID)

	itr, err := c.client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return fmt.Errorf("fetch ltx files: %w", err)
	}
	defer itr.Close()

	var deleted []*ltx.FileInfo
	var lastInfo *ltx.FileInfo
	for itr.Next() {
		info := itr.Item()
		lastInfo = info

		if info.MaxTXID < txID {
			deleted = append(deleted, info)
			continue
		}
	}

	if len(deleted) > 0 && deleted[len(deleted)-1] == lastInfo {
		deleted = deleted[:len(deleted)-1]
	}

	if err := c.client.DeleteLTXFiles(ctx, deleted); err != nil {
		return fmt.Errorf("remove ltx files: %w", err)
	}

	if c.LocalFileDeleter != nil {
		for _, info := range deleted {
			c.logger.Debug("deleting local ltx file",
				"level", level,
				"minTXID", info.MinTXID,
				"maxTXID", info.MaxTXID)
			if err := c.LocalFileDeleter(level, info.MinTXID, info.MaxTXID); err != nil {
				c.logger.Error("failed to remove local ltx file", "error", err)
			}
		}
	}

	return nil
}

// EnforceL0Retention retains L0 files based on L1 compaction progress and time.
// Files are only deleted if they have been compacted into L1 AND are older than retention.
// This ensures contiguous L0 coverage for VFS reads.
func (c *Compactor) EnforceL0Retention(ctx context.Context, retention time.Duration) error {
	if retention <= 0 {
		return nil
	}

	c.logger.Debug("enforcing l0 retention", "retention", retention)

	itr, err := c.client.LTXFiles(ctx, 1, 0, false)
	if err != nil {
		return fmt.Errorf("fetch l1 files: %w", err)
	}
	var maxL1TXID ltx.TXID
	for itr.Next() {
		info := itr.Item()
		if info.MaxTXID > maxL1TXID {
			maxL1TXID = info.MaxTXID
		}
	}
	if err := itr.Close(); err != nil {
		return fmt.Errorf("close l1 iterator: %w", err)
	}
	if maxL1TXID == 0 {
		return nil
	}

	threshold := time.Now().Add(-retention)
	itr, err = c.client.LTXFiles(ctx, 0, 0, false)
	if err != nil {
		return fmt.Errorf("fetch l0 files: %w", err)
	}
	defer itr.Close()

	var (
		deleted      []*ltx.FileInfo
		lastInfo     *ltx.FileInfo
		processedAll = true
	)
	for itr.Next() {
		info := itr.Item()
		lastInfo = info

		createdAt := info.CreatedAt
		if createdAt.IsZero() {
			createdAt = threshold
		}

		if createdAt.After(threshold) {
			processedAll = false
			break
		}

		if info.MaxTXID <= maxL1TXID {
			deleted = append(deleted, info)
		}
	}

	if processedAll && len(deleted) > 0 && lastInfo != nil && deleted[len(deleted)-1] == lastInfo {
		deleted = deleted[:len(deleted)-1]
	}

	if len(deleted) == 0 {
		return nil
	}

	if err := c.client.DeleteLTXFiles(ctx, deleted); err != nil {
		return fmt.Errorf("remove expired l0 files: %w", err)
	}

	if c.LocalFileDeleter != nil {
		for _, info := range deleted {
			c.logger.Debug("deleting expired local l0 file",
				"minTXID", info.MinTXID,
				"maxTXID", info.MaxTXID)
			if err := c.LocalFileDeleter(0, info.MinTXID, info.MaxTXID); err != nil {
				c.logger.Error("failed to remove local l0 file", "error", err)
			}
		}
	}

	c.logger.Info("l0 retention enforced", "deleted_count", len(deleted), "max_l1_txid", maxL1TXID)

	return nil
}
