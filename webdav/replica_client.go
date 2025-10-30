package webdav

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
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

func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

func (c *ReplicaClient) Init(ctx context.Context) (_ *gowebdav.Client, err error) {
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
	client, err := c.Init(ctx)
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
	client, err := c.Init(ctx)
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

func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (info *ltx.FileInfo, err error) {
	client, err := c.Init(ctx)
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
	client, err := c.Init(ctx)
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
	client, err := c.Init(ctx)
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
