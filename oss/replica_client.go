package oss

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

func init() {
	litestream.RegisterReplicaClientFactory("oss", NewReplicaClientFromURL)
}

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "oss"

// MetadataKeyTimestamp is the metadata key for storing LTX file timestamps in OSS.
// Note: OSS SDK automatically adds "x-oss-meta-" prefix when setting metadata.
const MetadataKeyTimestamp = "litestream-timestamp"

// MaxKeys is the number of keys OSS can operate on per batch.
const MaxKeys = 1000

// DefaultRegion is the region used if one is not specified.
const DefaultRegion = "cn-hangzhou"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files to Alibaba Cloud OSS.
type ReplicaClient struct {
	mu       sync.Mutex
	client   *oss.Client
	uploader *oss.Uploader
	logger   *slog.Logger

	// Alibaba Cloud authentication keys.
	AccessKeyID     string
	AccessKeySecret string

	// OSS bucket information
	Region   string
	Bucket   string
	Path     string
	Endpoint string

	// Upload configuration
	PartSize    int64 // Part size for multipart uploads (default: 5MB)
	Concurrency int   // Number of concurrent parts to upload (default: 3)
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{
		logger: slog.Default().WithGroup(ReplicaClientType),
	}
}

// NewReplicaClientFromURL creates a new ReplicaClient from URL components.
// This is used by the replica client factory registration.
// URL format: oss://bucket[.oss-region.aliyuncs.com]/path
func NewReplicaClientFromURL(scheme, host, urlPath string, query url.Values, userinfo *url.Userinfo) (litestream.ReplicaClient, error) {
	client := NewReplicaClient()

	bucket, region, _ := ParseHost(host)
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for oss replica URL")
	}

	client.Bucket = bucket
	client.Region = region
	client.Path = urlPath

	return client, nil
}

// Type returns "oss" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the connection to OSS. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		return nil
	}

	// Validate required configuration
	if c.Bucket == "" {
		return fmt.Errorf("oss: bucket name is required")
	}

	// Use default region if not specified
	region := c.Region
	if region == "" {
		region = DefaultRegion
	}

	// Build configuration
	cfg := oss.LoadDefaultConfig()

	// Configure credentials
	if c.AccessKeyID != "" && c.AccessKeySecret != "" {
		cfg = cfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.AccessKeySecret),
		)
	} else {
		// Use environment variable credentials provider
		cfg = cfg.WithCredentialsProvider(
			credentials.NewEnvironmentVariableCredentialsProvider(),
		)
	}

	// Configure region
	cfg = cfg.WithRegion(region)

	// Configure custom endpoint if specified
	if c.Endpoint != "" {
		endpoint := c.Endpoint
		// Add scheme if not present
		if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
			endpoint = "https://" + endpoint
		}
		cfg = cfg.WithEndpoint(endpoint)
	}

	// Create OSS client
	c.client = oss.NewClient(cfg)

	// Create uploader with configurable part size and concurrency
	uploaderOpts := []func(*oss.UploaderOptions){}
	if c.PartSize > 0 {
		uploaderOpts = append(uploaderOpts, func(o *oss.UploaderOptions) {
			o.PartSize = c.PartSize
		})
	}
	if c.Concurrency > 0 {
		uploaderOpts = append(uploaderOpts, func(o *oss.UploaderOptions) {
			o.ParallelNum = c.Concurrency
		})
	}
	c.uploader = c.client.NewUploader(uploaderOpts...)

	return nil
}

// LTXFiles returns an iterator over all LTX files on the replica for the given level.
// When useMetadata is true, fetches accurate timestamps from OSS metadata via HeadObject.
// When false, uses fast LastModified timestamps from LIST operation.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	return newFileIterator(ctx, c, level, seek, useMetadata), nil
}

// OpenLTXFile returns a reader for an LTX file.
// Returns os.ErrNotExist if no matching file is found.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	// Build the key from the file info
	filename := ltx.FormatFilename(minTXID, maxTXID)
	key := c.ltxPath(level, filename)

	request := &oss.GetObjectRequest{
		Bucket: oss.Ptr(c.Bucket),
		Key:    oss.Ptr(key),
	}

	// Set range header if offset is specified
	if size > 0 {
		request.RangeBehavior = oss.Ptr("standard")
		request.Range = oss.Ptr(fmt.Sprintf("bytes=%d-%d", offset, offset+size-1))
	} else if offset > 0 {
		request.RangeBehavior = oss.Ptr("standard")
		request.Range = oss.Ptr(fmt.Sprintf("bytes=%d-", offset))
	}

	result, err := c.client.GetObject(ctx, request)
	if err != nil {
		if isNotExists(err) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("oss: get object %s: %w", key, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	return result.Body, nil
}

// WriteLTXFile writes an LTX file to the replica.
// Extracts timestamp from LTX header and stores it in OSS metadata to preserve original creation time.
// Uses multipart upload for large files via the uploader.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

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

	filename := ltx.FormatFilename(minTXID, maxTXID)
	key := c.ltxPath(level, filename)

	// Store timestamp in OSS metadata for accurate timestamp retrieval
	metadata := map[string]string{
		MetadataKeyTimestamp: timestamp.Format(time.RFC3339Nano),
	}

	// Use uploader for automatic multipart handling (files >5GB)
	result, err := c.uploader.UploadFrom(ctx, &oss.PutObjectRequest{
		Bucket:   oss.Ptr(c.Bucket),
		Key:      oss.Ptr(key),
		Metadata: metadata,
	}, rc)
	if err != nil {
		return nil, fmt.Errorf("oss: upload to %s: %w", key, err)
	}

	// Build file info from the uploaded file
	info := &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      rc.N(),
		CreatedAt: timestamp,
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(rc.N()))

	// ETag indicates successful upload
	if result.ETag == nil || *result.ETag == "" {
		return nil, fmt.Errorf("oss: upload failed: no ETag returned")
	}

	return info, nil
}

// DeleteLTXFiles deletes one or more LTX files.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	if len(a) == 0 {
		return nil
	}

	// Convert file infos to object identifiers
	objects := make([]oss.DeleteObject, 0, len(a))
	for _, info := range a {
		filename := ltx.FormatFilename(info.MinTXID, info.MaxTXID)
		key := c.ltxPath(info.Level, filename)
		objects = append(objects, oss.DeleteObject{Key: oss.Ptr(key)})

		c.logger.Debug("deleting ltx file", "level", info.Level, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "key", key)
	}

	// Delete in batches
	for len(objects) > 0 {
		n := min(len(objects), MaxKeys)
		batch := objects[:n]

		request := &oss.DeleteMultipleObjectsRequest{
			Bucket:  oss.Ptr(c.Bucket),
			Objects: batch,
		}

		out, err := c.client.DeleteMultipleObjects(ctx, request)
		if err != nil {
			return fmt.Errorf("oss: delete batch of %d objects: %w", n, err)
		} else if err := deleteResultError(batch, out); err != nil {
			return err
		}

		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

		objects = objects[n:]
	}

	return nil
}

// DeleteAll deletes all files.
func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	var objects []oss.DeleteObject

	// Create paginator for listing objects
	prefix := c.Path + "/"
	paginator := c.client.NewListObjectsV2Paginator(&oss.ListObjectsV2Request{
		Bucket: oss.Ptr(c.Bucket),
		Prefix: oss.Ptr(prefix),
	})

	// Iterate through all pages
	for paginator.HasNext() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("oss: list objects page: %w", err)
		}

		// Collect object identifiers
		for _, obj := range page.Contents {
			if obj.Key != nil {
				objects = append(objects, oss.DeleteObject{Key: obj.Key})
			}
		}
	}

	// Delete all collected objects in batches
	for len(objects) > 0 {
		n := min(len(objects), MaxKeys)
		batch := objects[:n]

		request := &oss.DeleteMultipleObjectsRequest{
			Bucket:  oss.Ptr(c.Bucket),
			Objects: batch,
		}

		out, err := c.client.DeleteMultipleObjects(ctx, request)
		if err != nil {
			return fmt.Errorf("oss: delete all batch of %d objects: %w", n, err)
		} else if err := deleteResultError(batch, out); err != nil {
			return err
		}

		objects = objects[n:]
	}

	return nil
}

// ltxPath returns the full path to an LTX file.
func (c *ReplicaClient) ltxPath(level int, filename string) string {
	return c.Path + "/" + fmt.Sprintf("%04x/%s", level, filename)
}

// fileIterator represents an iterator over LTX files in OSS.
type fileIterator struct {
	ctx         context.Context
	cancel      context.CancelFunc
	client      *ReplicaClient
	level       int
	seek        ltx.TXID
	useMetadata bool // When true, fetch accurate timestamps from metadata

	paginator   *oss.ListObjectsV2Paginator
	page        *oss.ListObjectsV2Result
	pageIndex   int
	initialized bool

	closed bool
	err    error
	info   *ltx.FileInfo
}

func newFileIterator(ctx context.Context, client *ReplicaClient, level int, seek ltx.TXID, useMetadata bool) *fileIterator {
	ctx, cancel := context.WithCancel(ctx)

	itr := &fileIterator{
		ctx:         ctx,
		cancel:      cancel,
		client:      client,
		level:       level,
		seek:        seek,
		useMetadata: useMetadata,
	}

	return itr
}

// initPaginator initializes the paginator lazily.
func (itr *fileIterator) initPaginator() {
	if itr.initialized {
		return
	}
	itr.initialized = true

	// Create paginator for listing objects with level prefix
	prefix := itr.client.ltxPath(itr.level, "")
	itr.paginator = itr.client.client.NewListObjectsV2Paginator(&oss.ListObjectsV2Request{
		Bucket: oss.Ptr(itr.client.Bucket),
		Prefix: oss.Ptr(prefix),
	})
}

// Close stops iteration.
func (itr *fileIterator) Close() (err error) {
	itr.closed = true
	itr.cancel()
	return nil
}

// Next returns the next file. Returns false when no more files are available.
func (itr *fileIterator) Next() bool {
	if itr.closed || itr.err != nil {
		return false
	}

	// Initialize paginator on first call
	itr.initPaginator()

	// Process objects until we find a valid LTX file
	for {
		// Load next page if needed
		if itr.page == nil || itr.pageIndex >= len(itr.page.Contents) {
			if !itr.paginator.HasNext() {
				return false
			}

			var err error
			itr.page, err = itr.paginator.NextPage(itr.ctx)
			if err != nil {
				itr.err = err
				return false
			}
			itr.pageIndex = 0
		}

		// Process current object
		if itr.pageIndex < len(itr.page.Contents) {
			obj := itr.page.Contents[itr.pageIndex]
			itr.pageIndex++

			if obj.Key == nil {
				continue
			}

			// Extract file info from key
			key := path.Base(*obj.Key)
			minTXID, maxTXID, err := ltx.ParseFilename(key)
			if err != nil {
				continue // Skip non-LTX files
			}

			// Build file info
			info := &ltx.FileInfo{
				Level:   itr.level,
				MinTXID: minTXID,
				MaxTXID: maxTXID,
			}

			// Skip if below seek TXID
			if info.MinTXID < itr.seek {
				continue
			}

			// Set file info
			info.Size = obj.Size

			// Use fast LastModified timestamp by default
			var createdAt time.Time
			if obj.LastModified != nil {
				createdAt = obj.LastModified.UTC()
			} else {
				createdAt = time.Now().UTC()
			}

			// Only fetch accurate timestamp from metadata when requested (timestamp-based restore)
			if itr.useMetadata {
				head, err := itr.client.client.HeadObject(itr.ctx, &oss.HeadObjectRequest{
					Bucket: oss.Ptr(itr.client.Bucket),
					Key:    obj.Key,
				})
				if err != nil {
					itr.err = fmt.Errorf("fetch object metadata: %w", err)
					return false
				}

				if head.Metadata != nil {
					if ts, ok := head.Metadata[MetadataKeyTimestamp]; ok {
						if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
							createdAt = parsed
						} else {
							itr.err = fmt.Errorf("parse timestamp from metadata: %w", err)
							return false
						}
					}
				}
			}

			info.CreatedAt = createdAt
			itr.info = info
			return true
		}
	}
}

// Item returns the metadata for the current file.
func (itr *fileIterator) Item() *ltx.FileInfo {
	return itr.info
}

// Err returns any error that occurred during iteration.
func (itr *fileIterator) Err() error {
	return itr.err
}

// ParseURL parses an OSS URL into its host and path parts.
func ParseURL(s string) (bucket, region, key string, err error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", "", "", err
	}

	if u.Scheme != "oss" {
		return "", "", "", fmt.Errorf("oss: invalid url scheme")
	}

	// Parse host to extract bucket and region
	bucket, region, _ = ParseHost(u.Host)
	if bucket == "" {
		bucket = u.Host
	}

	key = strings.TrimPrefix(u.Path, "/")
	return bucket, region, key, nil
}

// ParseHost parses the host/endpoint for an OSS storage system.
// Supports formats like:
//   - bucket.oss-cn-hangzhou.aliyuncs.com
//   - bucket.oss-cn-hangzhou-internal.aliyuncs.com
//   - bucket (just bucket name)
func ParseHost(host string) (bucket, region, endpoint string) {
	// Check for internal OSS URL format first (more specific pattern)
	if a := ossInternalRegex.FindStringSubmatch(host); len(a) > 1 {
		bucket = a[1]
		if len(a) > 2 && a[2] != "" {
			region = a[2]
		}
		return bucket, region, ""
	}

	// Check for standard OSS URL format
	if a := ossRegex.FindStringSubmatch(host); len(a) > 1 {
		bucket = a[1]
		if len(a) > 2 && a[2] != "" {
			region = a[2]
		}
		return bucket, region, ""
	}

	// For other hosts, assume it's just the bucket name
	return host, "", ""
}

var (
	// oss-cn-hangzhou.aliyuncs.com or bucket.oss-cn-hangzhou.aliyuncs.com
	ossRegex = regexp.MustCompile(`^(?:([^.]+)\.)?oss-([^.]+)\.aliyuncs\.com$`)
	// oss-cn-hangzhou-internal.aliyuncs.com or bucket.oss-cn-hangzhou-internal.aliyuncs.com
	// Uses non-greedy .+? to correctly extract region without -internal suffix
	ossInternalRegex = regexp.MustCompile(`^(?:([^.]+)\.)?oss-(.+?)-internal\.aliyuncs\.com$`)
)

func isNotExists(err error) bool {
	var serviceErr *oss.ServiceError
	if errors.As(err, &serviceErr) {
		return serviceErr.Code == "NoSuchKey"
	}
	return false
}

// deleteResultError checks if all requested objects were deleted.
// OSS SDK doesn't have explicit per-object error reporting like S3, so we verify
// all requested keys appear in the deleted list.
func deleteResultError(requested []oss.DeleteObject, out *oss.DeleteMultipleObjectsResult) error {
	if out == nil {
		return nil
	}

	// Build set of deleted keys for quick lookup
	deleted := make(map[string]struct{}, len(out.DeletedObjects))
	for _, obj := range out.DeletedObjects {
		if obj.Key != nil {
			deleted[*obj.Key] = struct{}{}
		}
	}

	// Check that all requested keys were deleted
	var failed []string
	for _, obj := range requested {
		if obj.Key == nil {
			continue
		}
		if _, ok := deleted[*obj.Key]; !ok {
			failed = append(failed, *obj.Key)
		}
	}

	if len(failed) == 0 {
		return nil
	}

	// Build error message listing failed keys
	var b strings.Builder
	b.WriteString("oss: failed to delete files:")
	for _, key := range failed {
		fmt.Fprintf(&b, "\n%s", key)
	}
	return errors.New(b.String())
}
