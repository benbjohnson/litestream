package webdav

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/studio-b12/gowebdav"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

func init() {
	litestream.RegisterReplicaClientFactory("webdav", NewReplicaClientFromURL)
	litestream.RegisterReplicaClientFactory("webdavs", NewReplicaClientFromURL)
}

const ReplicaClientType = "webdav"

const (
	DefaultTimeout = 30 * time.Second
)

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

type ReplicaClient struct {
	mu     sync.Mutex
	client *gowebdav.Client
	logger *slog.Logger

	URL      string
	Username string
	Password string
	Path     string
	Timeout  time.Duration
}

func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{
		logger:  slog.Default().WithGroup(ReplicaClientType),
		Timeout: DefaultTimeout,
	}
}

// NewReplicaClientFromURL creates a new ReplicaClient from URL components.
// This is used by the replica client factory registration.
// URL format: webdav://[user[:password]@]host[:port]/path or webdavs://... (for HTTPS)
func NewReplicaClientFromURL(scheme, host, urlPath string, query url.Values, userinfo *url.Userinfo) (litestream.ReplicaClient, error) {
	client := NewReplicaClient()

	// Determine HTTP or HTTPS based on scheme
	httpScheme := "http"
	if scheme == "webdavs" {
		httpScheme = "https"
	}

	// Extract credentials from userinfo
	if userinfo != nil {
		client.Username = userinfo.Username()
		client.Password, _ = userinfo.Password()
	}

	if host == "" {
		return nil, fmt.Errorf("host required for webdav replica URL")
	}

	client.URL = fmt.Sprintf("%s://%s", httpScheme, host)
	client.Path = urlPath

	return client, nil
}

func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

func (c *ReplicaClient) Init(ctx context.Context) error {
	_, err := c.init(ctx)
	return err
}

// init initializes the connection and returns the WebDAV client.
func (c *ReplicaClient) init(ctx context.Context) (_ *gowebdav.Client, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		return c.client, nil
	}

	if c.URL == "" {
		return nil, fmt.Errorf("webdav url required")
	}

	c.client = gowebdav.NewClient(c.URL, c.Username, c.Password)

	c.client.SetTimeout(c.Timeout)

	if err := c.client.Connect(); err != nil {
		c.client = nil
		return nil, fmt.Errorf("webdav: cannot connect to server: %w", err)
	}

	return c.client, nil
}

func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	client, err := c.init(ctx)
	if err != nil {
		return err
	}

	if err := client.RemoveAll(c.Path); err != nil && !os.IsNotExist(err) && !gowebdav.IsErrNotFound(err) {
		return fmt.Errorf("webdav: cannot delete path %q: %w", c.Path, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

	return nil
}

func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, _ bool) (_ ltx.FileIterator, err error) {
	client, err := c.init(ctx)
	if err != nil {
		return nil, err
	}

	dir := litestream.LTXLevelDir(c.Path, level)
	files, err := client.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) || gowebdav.IsErrNotFound(err) {
			return ltx.NewFileInfoSliceIterator(nil), nil
		}
		return nil, fmt.Errorf("webdav: cannot read directory %q: %w", dir, err)
	}

	infos := make([]*ltx.FileInfo, 0, len(files))
	for _, fi := range files {
		if fi.IsDir() {
			continue
		}

		minTXID, maxTXID, err := ltx.ParseFilename(path.Base(fi.Name()))
		if err != nil {
			continue
		} else if minTXID < seek {
			continue
		}

		infos = append(infos, &ltx.FileInfo{
			Level:     level,
			MinTXID:   minTXID,
			MaxTXID:   maxTXID,
			Size:      fi.Size(),
			CreatedAt: fi.ModTime().UTC(),
		})
	}

	sort.Slice(infos, func(i, j int) bool {
		if infos[i].MinTXID != infos[j].MinTXID {
			return infos[i].MinTXID < infos[j].MinTXID
		}
		return infos[i].MaxTXID < infos[j].MaxTXID
	})

	return ltx.NewFileInfoSliceIterator(infos), nil
}

// WriteLTXFile writes an LTX file to the WebDAV server.
//
// WebDAV Upload Strategy - Temp File Approach:
//
// Unlike other replica backends (S3, SFTP, NATS, ABS) which stream directly using
// internal.NewReadCounter, WebDAV requires a different approach due to library and
// protocol constraints:
//
// 1. gowebdav Library Limitations:
//
//   - WriteStream() buffers entire payload in memory for non-seekable readers
//
//   - WriteStreamWithLength() requires both content-length AND seekable reader
//
//   - No native support for HTTP chunked transfer encoding
//
//     2. Server Compatibility Issues:
//     Research shows HTTP chunked transfer encoding with WebDAV is unreliable:
//
//   - Nginx + FastCGI: Discards request body → 0-byte files (silent data loss)
//
//   - Lighttpd: Returns HTTP 411 (Length Required), rejects chunked requests
//
//   - Apache + FastCGI: Request body never arrives at application
//
//   - Only Apache + mod_php handles chunked encoding reliably (~30-40% of deployments)
//
// 3. LTX Header Requirement:
//   - Must peek at LTX header to extract timestamp before upload
//   - Peeking consumes data, making the reader non-seekable
//   - Cannot calculate content-length without fully reading stream
//
// Solution: Stage to temporary file
//
// To ensure universal compatibility and prevent silent data loss:
//  1. Extract timestamp from LTX header (required for file metadata)
//  2. Stream full contents to temporary file on disk
//  3. Seek back to start of temp file (now seekable + known size)
//  4. Upload using WriteStreamWithLength() with Content-Length header
//  5. Clean up temp file
//
// Trade-offs:
//   - Universal compatibility with all WebDAV server configurations
//   - No risk of silent data loss or failed uploads
//   - Predictable, reliable behavior
//   - Additional disk I/O overhead
//   - Requires local disk space proportional to LTX file size
//   - Diverges from streaming pattern used by other backends
//
// References:
//   - https://github.com/studio-b12/gowebdav/issues/35 (chunked encoding issues)
//   - https://github.com/nextcloud/server/issues/7995 (0-byte file bug)
//   - https://evertpot.com/260/ (WebDAV chunked encoding compatibility)
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (info *ltx.FileInfo, err error) {
	client, err := c.init(ctx)
	if err != nil {
		return nil, err
	}

	filename := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)

	var buf bytes.Buffer
	teeReader := io.TeeReader(rd, &buf)

	hdr, _, err := ltx.PeekHeader(teeReader)
	if err != nil {
		return nil, fmt.Errorf("extract timestamp from LTX header: %w", err)
	}
	timestamp := time.UnixMilli(hdr.Timestamp).UTC()

	// Stage to temporary file to get seekable reader with known size.
	// This ensures compatibility with all WebDAV servers and avoids the
	// unreliable chunked transfer encoding that causes silent data loss
	// on common configurations (Nginx+FastCGI, Lighttpd, Apache+FastCGI).
	tmpFile, err := os.CreateTemp("", "litestream-webdav-*.ltx")
	if err != nil {
		return nil, fmt.Errorf("webdav: cannot create temp file: %w", err)
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	fullReader := io.MultiReader(&buf, rd)

	size, err := io.Copy(tmpFile, fullReader)
	if err != nil {
		return nil, fmt.Errorf("webdav: cannot copy to temp file: %w", err)
	}

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("webdav: cannot seek temp file: %w", err)
	}

	if err := client.MkdirAll(path.Dir(filename), 0755); err != nil {
		return nil, fmt.Errorf("webdav: cannot create parent directory %q: %w", path.Dir(filename), err)
	}

	// Upload with Content-Length header using seekable temp file.
	// WriteStreamWithLength requires both a seekable reader and known size,
	// which we now have from the temp file. This avoids chunked encoding
	// and ensures reliable uploads across all WebDAV server configurations.
	if err := client.WriteStreamWithLength(filename, tmpFile, size, 0644); err != nil {
		return nil, fmt.Errorf("webdav: cannot write file %q: %w", filename, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(size))

	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      size,
		CreatedAt: timestamp,
	}, nil
}

func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (_ io.ReadCloser, err error) {
	client, err := c.init(ctx)
	if err != nil {
		return nil, err
	}

	filename := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	if size > 0 {
		rc, err := client.ReadStreamRange(filename, offset, size)
		if err != nil {
			if os.IsNotExist(err) || gowebdav.IsErrNotFound(err) {
				return nil, os.ErrNotExist
			}
			return nil, fmt.Errorf("webdav: cannot read file %q: %w", filename, err)
		}
		return internal.LimitReadCloser(rc, size), nil
	}

	if offset > 0 {
		rc, err := client.ReadStream(filename)
		if err != nil {
			if os.IsNotExist(err) || gowebdav.IsErrNotFound(err) {
				return nil, os.ErrNotExist
			}
			return nil, fmt.Errorf("webdav: cannot read file %q: %w", filename, err)
		}

		if _, err := io.CopyN(io.Discard, rc, offset); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				_ = rc.Close()
				return io.NopCloser(bytes.NewReader(nil)), nil
			}
			_ = rc.Close()
			return nil, fmt.Errorf("webdav: cannot skip offset in file %q: %w", filename, err)
		}

		return rc, nil
	}

	rc, err := client.ReadStream(filename)
	if err != nil {
		if os.IsNotExist(err) || gowebdav.IsErrNotFound(err) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("webdav: cannot read file %q: %w", filename, err)
	}
	return rc, nil
}

func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	client, err := c.init(ctx)
	if err != nil {
		return err
	}

	for _, info := range a {
		filename := litestream.LTXFilePath(c.Path, info.Level, info.MinTXID, info.MaxTXID)

		c.logger.Debug("deleting ltx file", "level", info.Level, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "path", filename)

		if err := client.Remove(filename); err != nil && !os.IsNotExist(err) && !gowebdav.IsErrNotFound(err) {
			return fmt.Errorf("webdav: cannot delete ltx file %q: %w", filename, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}
