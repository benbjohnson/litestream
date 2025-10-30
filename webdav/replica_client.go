package webdav

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

	httpClient := &http.Client{
		Timeout: c.Timeout,
	}
	c.client.SetTransport(httpClient.Transport)

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

	if err := client.RemoveAll(c.Path); err != nil && !os.IsNotExist(err) {
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
		if os.IsNotExist(err) {
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

	fullReader := io.MultiReader(&buf, rd)

	if err := client.MkdirAll(path.Dir(filename), 0755); err != nil {
		return nil, fmt.Errorf("webdav: cannot create parent directory %q: %w", path.Dir(filename), err)
	}

	data, err := io.ReadAll(fullReader)
	if err != nil {
		return nil, fmt.Errorf("webdav: cannot read data: %w", err)
	}

	if err := client.Write(filename, data, 0644); err != nil {
		return nil, fmt.Errorf("webdav: cannot write file %q: %w", filename, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(len(data)))

	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      int64(len(data)),
		CreatedAt: timestamp,
	}, nil
}

func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (_ io.ReadCloser, err error) {
	client, err := c.Init(ctx)
	if err != nil {
		return nil, err
	}

	filename := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)

	data, err := client.Read(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("webdav: cannot read file %q: %w", filename, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	if offset > 0 {
		if offset >= int64(len(data)) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		data = data[offset:]
	}

	if size > 0 && size < int64(len(data)) {
		data = data[:size]
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	client, err := c.Init(ctx)
	if err != nil {
		return err
	}

	for _, info := range a {
		filename := litestream.LTXFilePath(c.Path, info.Level, info.MinTXID, info.MaxTXID)

		c.logger.Debug("deleting ltx file", "level", info.Level, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "path", filename)

		if err := client.Remove(filename); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("webdav: cannot delete ltx file %q: %w", filename, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}
