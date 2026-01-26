package litestream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/superfly/ltx"
)

// RetryConfig configures retry behavior for RetryReplicaClient.
type RetryConfig struct {
	// InitialDelay is the delay before the first retry.
	// Default: 1 second
	InitialDelay time.Duration

	// MaxDelay is the maximum delay between retries.
	// Default: 30 seconds
	MaxDelay time.Duration

	// MaxRetries is the maximum number of retry attempts.
	// 0 means no retries, -1 means infinite retries.
	// Default: 5
	MaxRetries int

	// Logger for retry attempts. If nil, uses slog.Default().
	Logger *slog.Logger
}

// DefaultRetryConfig returns a RetryConfig with sensible defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		MaxRetries:   5,
	}
}

// RetryReplicaClient wraps a ReplicaClient and adds retry with exponential
// backoff for read operations (LTXFiles, OpenLTXFile).
// Write operations are not retried as they may cause duplicate data.
type RetryReplicaClient struct {
	client ReplicaClient
	config RetryConfig
}

// NewRetryReplicaClient creates a new RetryReplicaClient wrapping the given client.
func NewRetryReplicaClient(client ReplicaClient, config RetryConfig) *RetryReplicaClient {
	// Apply defaults for zero values
	if config.InitialDelay == 0 {
		config.InitialDelay = 1 * time.Second
	}
	if config.MaxDelay == 0 {
		config.MaxDelay = 30 * time.Second
	}
	return &RetryReplicaClient{
		client: client,
		config: config,
	}
}

// Type returns the underlying client's type.
func (c *RetryReplicaClient) Type() string {
	return c.client.Type()
}

// Init initializes the underlying client.
func (c *RetryReplicaClient) Init(ctx context.Context) error {
	return c.client.Init(ctx)
}

// logger returns the configured logger or the default logger.
func (c *RetryReplicaClient) logger() *slog.Logger {
	if c.config.Logger != nil {
		return c.config.Logger
	}
	return slog.Default()
}

// shouldRetry returns true if the error is retryable.
func shouldRetry(err error) bool {
	// Don't retry on "not found" - it's a legitimate response
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	// Don't retry on context errors
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return true
}

// LTXFiles returns an iterator with retry on transient errors.
// Does NOT retry on os.ErrNotExist.
func (c *RetryReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	var lastErr error
	delay := c.config.InitialDelay

	for attempt := 0; c.config.MaxRetries < 0 || attempt <= c.config.MaxRetries; attempt++ {
		// Check context before attempt
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		itr, err := c.client.LTXFiles(ctx, level, seek, useMetadata)
		if err == nil {
			return itr, nil
		}

		if !shouldRetry(err) {
			return nil, err
		}

		lastErr = err

		// Log retry attempt
		c.logger().Warn("LTXFiles failed, retrying",
			"attempt", attempt+1,
			"max_retries", c.config.MaxRetries,
			"delay", delay,
			"error", err)

		// Wait before retry with context cancellation support
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}

		// Exponential backoff
		delay *= 2
		if delay > c.config.MaxDelay {
			delay = c.config.MaxDelay
		}
	}

	return nil, lastErr
}

// OpenLTXFile opens an LTX file with retry on transient errors.
// Does NOT retry on os.ErrNotExist.
func (c *RetryReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	var lastErr error
	delay := c.config.InitialDelay

	for attempt := 0; c.config.MaxRetries < 0 || attempt <= c.config.MaxRetries; attempt++ {
		// Check context before attempt
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		rc, err := c.client.OpenLTXFile(ctx, level, minTXID, maxTXID, offset, size)
		if err == nil {
			return rc, nil
		}

		if !shouldRetry(err) {
			return nil, err
		}

		lastErr = err

		// Log retry attempt
		c.logger().Warn("OpenLTXFile failed, retrying",
			"attempt", attempt+1,
			"max_retries", c.config.MaxRetries,
			"delay", delay,
			"level", level,
			"minTXID", minTXID,
			"maxTXID", maxTXID,
			"error", err)

		// Wait before retry with context cancellation support
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}

		// Exponential backoff
		delay *= 2
		if delay > c.config.MaxDelay {
			delay = c.config.MaxDelay
		}
	}

	return nil, lastErr
}

// WriteLTXFile delegates to the underlying client without retry.
// Write operations should not be retried to avoid duplicate writes.
func (c *RetryReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	return c.client.WriteLTXFile(ctx, level, minTXID, maxTXID, r)
}

// DeleteLTXFiles delegates to the underlying client without retry.
func (c *RetryReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	return c.client.DeleteLTXFiles(ctx, a)
}

// DeleteAll delegates to the underlying client without retry.
func (c *RetryReplicaClient) DeleteAll(ctx context.Context) error {
	return c.client.DeleteAll(ctx)
}

// Unwrap returns the underlying ReplicaClient.
func (c *RetryReplicaClient) Unwrap() ReplicaClient {
	return c.client
}
