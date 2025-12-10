package nats

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

func init() {
	litestream.RegisterReplicaClientFactory("nats", NewReplicaClientFromURL)
}

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "nats"

// HeaderKeyTimestamp is the header key for storing LTX file timestamps in NATS object headers.
const HeaderKeyTimestamp = "Litestream-Timestamp"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files to NATS JetStream Object Store.
type ReplicaClient struct {
	mu     sync.Mutex
	logger *slog.Logger

	// NATS connection and JetStream context
	nc          *nats.Conn
	js          jetstream.JetStream
	objectStore jetstream.ObjectStore

	// Configuration
	URL        string   // NATS server URL
	BucketName string   // Object store bucket name
	Path       string   // Base path for LTX files within the bucket
	JWT        string   // JWT token for authentication
	Seed       string   // Seed for JWT authentication
	Creds      string   // Credentials file path
	NKey       string   // NKey for authentication
	Username   string   // Username for authentication
	Password   string   // Password for authentication
	Token      string   // Token for authentication
	TLS        bool     // Enable TLS
	RootCAs    []string // Root CA certificates
	ClientCert string   // Client certificate file path
	ClientKey  string   // Client key file path

	// Note: Bucket configuration (replicas, storage, TTL, etc.) should be
	// managed externally via NATS CLI or API, not by Litestream

	// Connection options
	MaxReconnects    int                          // Maximum reconnection attempts (-1 for unlimited)
	ReconnectWait    time.Duration                // Wait time between reconnection attempts
	ReconnectJitter  time.Duration                // Random jitter for reconnection
	Timeout          time.Duration                // Connection timeout
	PingInterval     time.Duration                // Ping interval
	MaxPingsOut      int                          // Maximum number of pings without response
	ReconnectBufSize int                          // Reconnection buffer size
	UserJWT          func() (string, error)       // JWT callback
	SigCB            func([]byte) ([]byte, error) // Signature callback
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{
		logger:           slog.Default().WithGroup(ReplicaClientType),
		MaxReconnects:    -1, // Unlimited
		ReconnectWait:    2 * time.Second,
		Timeout:          10 * time.Second,
		PingInterval:     2 * time.Minute,
		MaxPingsOut:      2,
		ReconnectBufSize: 8 * 1024 * 1024, // 8MB
	}
}

// NewReplicaClientFromURL creates a new ReplicaClient from URL components.
// This is used by the replica client factory registration.
// URL format: nats://[user:pass@]host[:port]/bucket
func NewReplicaClientFromURL(scheme, host, urlPath string, query url.Values, userinfo *url.Userinfo) (litestream.ReplicaClient, error) {
	client := NewReplicaClient()

	// Reconstruct URL without bucket path
	if host != "" {
		client.URL = fmt.Sprintf("nats://%s", host)
	}

	// Extract credentials from userinfo if present
	if userinfo != nil {
		client.Username = userinfo.Username()
		client.Password, _ = userinfo.Password()
	}

	// Extract bucket name from path
	bucket := strings.Trim(urlPath, "/")
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for nats replica URL")
	}
	client.BucketName = bucket

	return client, nil
}

// Type returns "nats" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the connection to NATS JetStream. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nc != nil {
		return nil
	}

	if err := c.connect(ctx); err != nil {
		return fmt.Errorf("nats: failed to connect: %w", err)
	}

	if err := c.initObjectStore(ctx); err != nil {
		return fmt.Errorf("nats: failed to initialize object store: %w", err)
	}

	return nil
}

// connect establishes a connection to NATS server with proper configuration.
func (c *ReplicaClient) connect(_ context.Context) error {
	opts := []nats.Option{
		nats.MaxReconnects(c.MaxReconnects),
		nats.ReconnectWait(c.ReconnectWait),
		nats.ReconnectJitter(c.ReconnectJitter, c.ReconnectJitter*2),
		nats.Timeout(c.Timeout),
		nats.PingInterval(c.PingInterval),
		nats.MaxPingsOutstanding(c.MaxPingsOut),
		nats.ReconnectBufSize(c.ReconnectBufSize),
	}

	// Authentication options
	switch {
	case c.JWT != "" && c.Seed != "":
		opts = append(opts, nats.UserJWTAndSeed(c.JWT, c.Seed))
	case c.Creds != "":
		opts = append(opts, nats.UserCredentials(c.Creds))
	case c.NKey != "":
		opts = append(opts, nats.Nkey(c.NKey, c.SigCB))
	case c.Username != "" && c.Password != "":
		opts = append(opts, nats.UserInfo(c.Username, c.Password))
	case c.Token != "":
		opts = append(opts, nats.Token(c.Token))
	}
	// JWT callback
	if c.UserJWT != nil {
		opts = append(opts, nats.UserJWT(c.UserJWT, c.SigCB))
	}

	// TLS configuration
	if c.ClientCert != "" && c.ClientKey != "" {
		opts = append(opts, nats.ClientCert(c.ClientCert, c.ClientKey))
	}

	if len(c.RootCAs) > 0 {
		opts = append(opts, nats.RootCAs(c.RootCAs...))
	}

	// Note: NATS Connect doesn't directly support context cancellation during connection
	// The context parameter is preserved for potential future use

	url := c.URL
	if url == "" {
		url = nats.DefaultURL
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS server: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("failed to create JetStream context: %w", err)
	}

	c.nc = nc
	c.js = js
	return nil
}

// initObjectStore retrieves the existing object store bucket.
// The bucket must be pre-created using the NATS CLI or API.
func (c *ReplicaClient) initObjectStore(ctx context.Context) error {
	if c.BucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	// Get existing object store - do not auto-create
	objectStore, err := c.js.ObjectStore(ctx, c.BucketName)
	if err != nil {
		return fmt.Errorf("failed to access object store bucket %q (bucket must be created beforehand): %w", c.BucketName, err)
	}

	c.objectStore = objectStore
	return nil
}

// Close closes the NATS connection.
func (c *ReplicaClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nc != nil {
		c.nc.Close()
		c.nc = nil
		c.js = nil
		c.objectStore = nil
	}
	return nil
}

// ltxPath returns the object path for an LTX file.
func (c *ReplicaClient) ltxPath(level int, minTXID, maxTXID ltx.TXID) string {
	return litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)
}

// parseLTXPath parses an LTX object path and returns level, minTXID, and maxTXID.
func (c *ReplicaClient) parseLTXPath(objPath string) (level int, minTXID, maxTXID ltx.TXID, err error) {
	// Remove the base path prefix if present
	if c.Path != "" && strings.HasPrefix(objPath, c.Path+"/") {
		objPath = strings.TrimPrefix(objPath, c.Path+"/")
	}

	// Expected format: "ltx/<level>/<minTXID>-<maxTXID>.ltx"
	parts := strings.Split(objPath, "/")
	if len(parts) < 3 || parts[0] != "ltx" {
		return 0, 0, 0, fmt.Errorf("invalid ltx path: %s", objPath)
	}

	// Parse level
	if level, err = strconv.Atoi(parts[1]); err != nil {
		return 0, 0, 0, fmt.Errorf("invalid level in path %s: %w", objPath, err)
	}

	// Parse filename (minTXID-maxTXID.ltx)
	filename := parts[2]
	minTXIDVal, maxTXIDVal, err := ltx.ParseFilename(filename)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid filename in path %s: %w", objPath, err)
	}

	return level, minTXIDVal, maxTXIDVal, nil
}

// LTXFiles returns an iterator of all LTX files on the replica for a given level.
// NATS always uses accurate timestamps from headers since they're included in LIST operations at zero cost.
// The useMetadata parameter is ignored.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	// List all objects in the store
	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()
	objectList, err := c.objectStore.List(ctx)
	if err != nil {
		// NATS returns "no objects found" when bucket is empty, treat as empty list
		if strings.Contains(err.Error(), "no objects found") {
			objectList = nil // Empty list
		} else {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
	}

	prefix := litestream.LTXLevelDir(c.Path, level) + "/"
	fileInfos := make([]*ltx.FileInfo, 0, len(objectList))

	for _, objInfo := range objectList {
		// Filter by level prefix
		if !strings.HasPrefix(objInfo.Name, prefix) {
			continue
		}

		fileLevel, minTXID, maxTXID, err := c.parseLTXPath(objInfo.Name)
		if err != nil {
			continue // Skip invalid paths
		}

		if fileLevel != level {
			continue
		}

		// Apply seek filter
		if minTXID < seek {
			continue
		}

		// Always use accurate timestamp from headers since it's zero-cost
		// NATS includes headers in LIST operations, so no extra API call needed
		createdAt := objInfo.ModTime
		if objInfo.Headers != nil {
			if values, ok := objInfo.Headers[HeaderKeyTimestamp]; ok && len(values) > 0 {
				if parsed, err := time.Parse(time.RFC3339Nano, values[0]); err == nil {
					createdAt = parsed
				}
			}
		}

		fileInfos = append(fileInfos, &ltx.FileInfo{
			Level:     fileLevel,
			MinTXID:   minTXID,
			MaxTXID:   maxTXID,
			Size:      int64(objInfo.Size),
			CreatedAt: createdAt,
		})
	}

	// Sort by minTXID
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].MinTXID < fileInfos[j].MinTXID
	})

	return &ltxFileIterator{files: fileInfos, index: -1}, nil
}

// OpenLTXFile returns a reader that contains an LTX file at a given TXID range.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	objectPath := c.ltxPath(level, minTXID, maxTXID)

	objectResult, err := c.objectStore.Get(ctx, objectPath)
	if err != nil {
		if isNotFoundError(err) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("failed to get object %s: %w", objectPath, err)
	}

	// Record metrics
	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	// Note: We can't get the size from NATS object reader directly, so we skip bytes counter

	// If offset is non-zero then discard the beginning bytes.
	if offset > 0 {
		if _, err := io.CopyN(io.Discard, objectResult, offset); err != nil {
			objectResult.Close()
			return nil, fmt.Errorf("failed to discard offset bytes: %w", err)
		}
	}

	// If size is non-zero then limit the reader to the size.
	if size > 0 {
		return internal.LimitReadCloser(objectResult, size), nil
	}

	return objectResult, nil
}

// WriteLTXFile writes an LTX file to the replica.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	objectPath := c.ltxPath(level, minTXID, maxTXID)

	// Use TeeReader to peek at LTX header while preserving data for upload
	var buf bytes.Buffer
	teeReader := io.TeeReader(r, &buf)

	// Extract timestamp from LTX header
	hdr, _, err := ltx.PeekHeader(teeReader)
	if err != nil {
		return nil, fmt.Errorf("extract timestamp from LTX header: %w", err)
	}
	timestamp := time.UnixMilli(hdr.Timestamp).UTC()

	// Combine buffered data with rest of reader
	rc := internal.NewReadCounter(io.MultiReader(&buf, r))

	// Store timestamp in NATS object headers for accurate timestamp retrieval
	objectInfo, err := c.objectStore.Put(ctx, jetstream.ObjectMeta{
		Name: objectPath,
		Headers: map[string][]string{
			HeaderKeyTimestamp: {timestamp.Format(time.RFC3339Nano)},
		},
	}, rc)
	if err != nil {
		return nil, fmt.Errorf("failed to put object %s: %w", objectPath, err)
	}

	// Record metrics
	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(objectInfo.Size))

	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      int64(objectInfo.Size),
		CreatedAt: timestamp,
	}, nil
}

// DeleteLTXFiles deletes one or more LTX files.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	for _, fileInfo := range a {
		objectPath := c.ltxPath(fileInfo.Level, fileInfo.MinTXID, fileInfo.MaxTXID)

		c.logger.Debug("deleting ltx file", "level", fileInfo.Level, "minTXID", fileInfo.MinTXID, "maxTXID", fileInfo.MaxTXID, "path", objectPath)

		if err := c.objectStore.Delete(ctx, objectPath); err != nil {
			if !isNotFoundError(err) {
				return fmt.Errorf("failed to delete object %s: %w", objectPath, err)
			}
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

// DeleteAll deletes all files in the object store.
func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	// List all objects in the bucket
	objectList, err := c.objectStore.List(ctx)
	if err != nil {
		// NATS returns "no objects found" when bucket is empty, treat as empty list
		if strings.Contains(err.Error(), "no objects found") {
			objectList = nil // Empty list, nothing to delete
		} else {
			return fmt.Errorf("failed to list all objects: %w", err)
		}
	}

	for _, objInfo := range objectList {
		if err := c.objectStore.Delete(ctx, objInfo.Name); err != nil {
			if !isNotFoundError(err) {
				return fmt.Errorf("failed to delete object %s: %w", objInfo.Name, err)
			}
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

// isNotFoundError checks if the error is a "not found" error.
func isNotFoundError(err error) bool {
	return err != nil && (errors.Is(err, jetstream.ErrObjectNotFound) || strings.Contains(err.Error(), "not found"))
}

// ltxFileIterator implements ltx.FileIterator for NATS object store.
type ltxFileIterator struct {
	files []*ltx.FileInfo
	index int
	err   error
}

// Next advances the iterator to the next file.
func (itr *ltxFileIterator) Next() bool {
	itr.index++
	return itr.index < len(itr.files)
}

// Item returns the current file info.
func (itr *ltxFileIterator) Item() *ltx.FileInfo {
	if itr.index < 0 || itr.index >= len(itr.files) {
		return nil
	}
	return itr.files[itr.index]
}

// Err returns any error that occurred during iteration.
func (itr *ltxFileIterator) Err() error {
	return itr.err
}

// Close closes the iterator.
func (itr *ltxFileIterator) Close() error {
	return nil
}
