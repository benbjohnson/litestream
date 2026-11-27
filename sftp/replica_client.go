package sftp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"github.com/superfly/ltx"
	"golang.org/x/crypto/ssh"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

func init() {
	litestream.RegisterReplicaClientFactory("sftp", NewReplicaClientFromURL)
}

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "sftp"

// Default settings for replica client.
const (
	DefaultDialTimeout = 30 * time.Second
)

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient stores and retrieves LTX files over SFTP.
type ReplicaClient struct {
	mu     sync.Mutex
	conn   *replicaConn
	logger *slog.Logger

	// SFTP connection info
	Host        string
	User        string
	Password    string
	Path        string
	KeyPath     string
	HostKey     string
	DialTimeout time.Duration

	// ConcurrentWrites enables concurrent writes for better performance.
	// Note: This makes resuming failed transfers unsafe.
	ConcurrentWrites bool
}

type replicaConn struct {
	sftp *sftp.Client
	ssh  *ssh.Client
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{
		logger:           slog.Default().WithGroup(ReplicaClientType),
		DialTimeout:      DefaultDialTimeout,
		ConcurrentWrites: true, // Default to true for better performance
	}
}

func (c *ReplicaClient) SetLogger(logger *slog.Logger) {
	c.logger = logger.WithGroup(ReplicaClientType)
}

// NewReplicaClientFromURL creates a new ReplicaClient from URL components.
// This is used by the replica client factory registration.
// URL format: sftp://[user[:password]@]host[:port]/path
func NewReplicaClientFromURL(scheme, host, urlPath string, query url.Values, userinfo *url.Userinfo) (litestream.ReplicaClient, error) {
	client := NewReplicaClient()

	// Extract credentials from userinfo
	if userinfo != nil {
		client.User = userinfo.Username()
		client.Password, _ = userinfo.Password()
	}

	client.Host = host
	client.Path = urlPath

	if client.Host == "" {
		return nil, fmt.Errorf("host required for sftp replica URL")
	}
	if client.User == "" {
		return nil, fmt.Errorf("user required for sftp replica URL")
	}

	return client, nil
}

// Type returns "sftp" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the connection to SFTP. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) error {
	_, err := c.getConn(ctx)
	return err
}

// getConn returns the cached connection or opens one.
func (c *ReplicaClient) getConn(ctx context.Context) (*replicaConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		return c.conn, nil
	}

	if c.User == "" {
		return nil, fmt.Errorf("sftp user required")
	}

	// Build SSH configuration & auth methods
	var hostkey ssh.HostKeyCallback
	if c.HostKey != "" {
		pubkey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(c.HostKey))
		if err != nil {
			return nil, fmt.Errorf("cannot parse sftp host key: %w", err)
		}
		hostkey = ssh.FixedHostKey(pubkey)
	} else {
		slog.Warn("sftp host key not verified", "host", c.Host)
		hostkey = ssh.InsecureIgnoreHostKey()
	}
	config := &ssh.ClientConfig{
		User:            c.User,
		HostKeyCallback: hostkey,
		BannerCallback:  ssh.BannerDisplayStderr(),
	}
	if c.Password != "" {
		config.Auth = append(config.Auth, ssh.Password(c.Password))
	}

	if c.KeyPath != "" {
		buf, err := os.ReadFile(c.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("sftp: cannot read sftp key path: %w", err)
		}

		signer, err := ssh.ParsePrivateKey(buf)
		if err != nil {
			return nil, fmt.Errorf("sftp: cannot parse sftp key path: %w", err)
		}
		config.Auth = append(config.Auth, ssh.PublicKeys(signer))
	}

	// Connect via SSH.
	sshClient, err := ssh.Dial("tcp", c.sshAddr(), config)
	if err != nil {
		return nil, err
	}

	// Wrap connection with an SFTP client.
	// Configure options based on client settings
	opts := []sftp.ClientOption{}
	if c.ConcurrentWrites {
		opts = append(opts, sftp.UseConcurrentWrites(true))
	}

	sftpClient, err := sftp.NewClient(sshClient, opts...)
	if err != nil {
		sshClient.Close()
		return nil, err
	}

	c.conn = &replicaConn{sftp: sftpClient, ssh: sshClient}
	return c.conn, nil
}

// sshAddr returns the SSH address, adding the standard port when omitted.
func (c *ReplicaClient) sshAddr() string {
	if _, _, err := net.SplitHostPort(c.Host); err == nil {
		return c.Host
	}
	host := c.Host
	if inner, ok := strings.CutPrefix(host, "["); ok {
		if inner, ok := strings.CutSuffix(inner, "]"); ok {
			host = inner
		}
	}
	return net.JoinHostPort(host, "22")
}

// DeleteAll deletes all LTX files.
func (c *ReplicaClient) DeleteAll(ctx context.Context) (err error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return err
	}
	defer func() { c.discardConnIfLost(conn, err) }()

	var dirs []string
	walker := conn.sftp.Walk(c.Path)
	for walker.Step() {
		if err := walker.Err(); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return fmt.Errorf("sftp: cannot walk path %q: %w", walker.Path(), err)
		}
		if walker.Stat().IsDir() {
			dirs = append(dirs, walker.Path())
			continue
		}

		if err := conn.sftp.Remove(walker.Path()); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("sftp: cannot delete file %q: %w", walker.Path(), err)
		}

		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	// Remove directories in reverse order after they have been emptied.
	for i := len(dirs) - 1; i >= 0; i-- {
		filename := dirs[i]
		if err := conn.sftp.RemoveDirectory(filename); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("sftp: cannot delete directory %q: %w", filename, err)
		}
	}

	// log.Printf("%s(%s): retainer: deleting all", r.db.Path(), r.Name())

	return nil
}

// LTXFiles returns an iterator over all available LTX files for a level.
// SFTP uses file ModTime for timestamps, which is set via Chtimes() to preserve original timestamp.
// The useMetadata parameter is ignored since ModTime always contains the accurate timestamp.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, _ bool) (_ ltx.FileIterator, err error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { c.discardConnIfLost(conn, err) }()

	dir := litestream.LTXLevelDir(c.Path, level)
	fis, err := conn.sftp.ReadDir(dir)
	if os.IsNotExist(err) {
		return ltx.NewFileInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, err
	}

	// Iterate over every file and convert to metadata.
	infos := make([]*ltx.FileInfo, 0, len(fis))
	for _, fi := range fis {
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
			CreatedAt: fi.ModTime().UTC(), // ModTime contains accurate timestamp from Chtimes()
		})
	}

	return ltx.NewFileInfoSliceIterator(infos), nil
}

// WriteLTXFile writes a LTX file from r into a remote file.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (info *ltx.FileInfo, err error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { c.discardConnIfLost(conn, err) }()

	filename := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)

	hdr, src, err := ltx.PeekHeader(r)
	if err != nil {
		return nil, fmt.Errorf("extract timestamp from LTX header: %w", err)
	}
	timestamp := time.UnixMilli(hdr.Timestamp).UTC()

	if err := conn.sftp.MkdirAll(path.Dir(filename)); err != nil {
		return nil, fmt.Errorf("sftp: cannot make parent snapshot directory %q: %w", path.Dir(filename), err)
	}

	tmpFilename := fmt.Sprintf("%s.%d.%d.tmp", filename, os.Getpid(), time.Now().UnixNano())
	f, err := conn.sftp.OpenFile(tmpFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return nil, fmt.Errorf("sftp: cannot open temporary snapshot file for writing: %w", err)
	}
	defer func() {
		if f != nil {
			err = errors.Join(err, f.Close())
		}
		if err != nil {
			if removeErr := conn.sftp.Remove(tmpFilename); removeErr != nil && !os.IsNotExist(removeErr) {
				err = errors.Join(err, fmt.Errorf("sftp: cannot remove temporary ltx file %q: %w", tmpFilename, removeErr))
			}
		}
	}()

	// Compaction streams its output through an io.Pipe in small writes, and
	// pkg/sftp cannot tell the size of such a reader: io.Copy then sends every
	// read as its own request and waits for the reply before the next one, so
	// over a link with tens of milliseconds of latency a snapshot of a large
	// database takes days. Fill each read up to the SFTP packet size and, when
	// concurrent writes are enabled, keep several packets in flight.
	upload := fillReader{src}
	var n int64
	if c.ConcurrentWrites {
		n, err = f.ReadFromWithConcurrency(upload, 0)
	} else {
		n, err = io.Copy(f, upload)
	}
	if err != nil {
		return nil, err
	}

	// Flush the data to stable storage before the rename so a server crash
	// cannot leave an empty file at the final path. Servers that do not
	// advertise fsync@openssh.com are left to their own write-back policy.
	if _, ok := conn.sftp.HasExtension("fsync@openssh.com"); ok {
		if err := f.Sync(); err != nil {
			return nil, fmt.Errorf("sftp: cannot sync temporary ltx file %q: %w", tmpFilename, err)
		}
	}

	if err := f.Close(); err != nil {
		return nil, err
	}
	f = nil

	if err := conn.sftp.Chtimes(tmpFilename, timestamp, timestamp); err != nil {
		return nil, fmt.Errorf("sftp: cannot set file timestamps: %w", err)
	}

	if err := conn.sftp.Rename(tmpFilename, filename); err != nil {
		return nil, fmt.Errorf("sftp: cannot rename temporary ltx file %q to %q: %w", tmpFilename, filename, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(n))

	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      n,
		CreatedAt: timestamp,
	}, nil
}

// OpenLTXFile returns a reader for an LTX file.
// Returns os.ErrNotExist if no matching position is found.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (_ io.ReadCloser, err error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { c.discardConnIfLost(conn, err) }()

	filename := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)
	file, err := conn.sftp.OpenFile(filename, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	f := &ltxFile{File: file, ctx: ctx, client: c, conn: conn}
	f.stopOnCancel = context.AfterFunc(ctx, f.discardConnIfActive)

	if offset > 0 {
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			return nil, errors.Join(err, f.Close())
		}
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	// The LTX decoder reads a few bytes at a time, such as the 6-byte header
	// before each page, and pkg/sftp sends every read that fits in a packet as
	// its own request, so each would cost a round trip. Read through a buffer
	// instead. The limit sits beneath the buffer so that read-ahead stays
	// within the requested range.
	var r io.Reader = f
	remaining := size
	if size > 0 {
		r = io.LimitReader(r, size)
	} else {
		info, err := f.Stat()
		if ctx.Err() != nil {
			return nil, errors.Join(ctx.Err(), f.Close())
		}
		if err != nil {
			slog.DebugContext(ctx, "cannot size SFTP read buffer", "path", filename, "error", err)
			remaining = sftpPacketSize
		} else {
			remaining = max(0, info.Size()-max(offset, 0))
		}
	}
	bufSize := int(min(remaining, maxReadBufferSize))
	return &bufferedLTXFile{file: f, reader: bufio.NewReaderSize(r, bufSize)}, nil
}

// maxReadBufferSize is the read-ahead applied to an opened LTX file. It spans
// several SFTP packets so that pkg/sftp can request them concurrently.
const maxReadBufferSize = 256 * 1024
const sftpPacketSize = 32 * 1024

type bufferedLTXFile struct {
	file   *ltxFile
	reader *bufio.Reader
}

func (f *bufferedLTXFile) Read(p []byte) (int, error) {
	if err := f.file.ctx.Err(); err != nil {
		return 0, err
	}
	return f.reader.Read(p)
}

func (f *bufferedLTXFile) WriteTo(w io.Writer) (int64, error) {
	if err := f.file.ctx.Err(); err != nil {
		return 0, err
	}
	return f.reader.WriteTo(w)
}

func (f *bufferedLTXFile) Close() error {
	return f.file.Close()
}

// DeleteLTXFiles deletes LTX files with at the given positions.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) (err error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return err
	}
	defer func() { c.discardConnIfLost(conn, err) }()

	for _, info := range a {
		filename := litestream.LTXFilePath(c.Path, info.Level, info.MinTXID, info.MaxTXID)

		c.logger.Debug("deleting ltx file", "level", info.Level, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "path", filename)

		if err := conn.sftp.Remove(filename); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("sftp: cannot delete ltx file %q: %w", filename, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

// Cleanup deletes path & directories after empty.
func (c *ReplicaClient) Cleanup(ctx context.Context) (err error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return err
	}
	defer func() { c.discardConnIfLost(conn, err) }()

	if err := conn.sftp.RemoveDirectory(c.Path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("sftp: cannot delete path: %w", err)
	}
	return nil
}

func (c *ReplicaClient) discardConnIfLost(conn *replicaConn, err error) {
	if errors.Is(err, sftp.ErrSSHFxConnectionLost) {
		c.discardConn(conn)
	}
}

func (c *ReplicaClient) discardConn(conn *replicaConn) {
	c.mu.Lock()
	if c.conn != conn {
		c.mu.Unlock()
		return
	}
	c.conn = nil
	c.mu.Unlock()

	if conn.ssh != nil {
		_ = conn.ssh.Close()
	}
	_ = conn.sftp.Close()
}

type ltxFile struct {
	*sftp.File
	ctx          context.Context
	client       *ReplicaClient
	conn         *replicaConn
	stopOnCancel func() bool

	mu       sync.Mutex
	inFlight int
}

func (f *ltxFile) beginIO() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.ctx.Err(); err != nil {
		return err
	}
	f.inFlight++
	return nil
}

func (f *ltxFile) endIO() {
	f.mu.Lock()
	f.inFlight--
	f.mu.Unlock()
}

func (f *ltxFile) discardConnIfActive() {
	f.mu.Lock()
	active := f.inFlight > 0
	f.mu.Unlock()
	if active {
		f.client.discardConn(f.conn)
	}
}

func (f *ltxFile) Stat() (os.FileInfo, error) {
	if err := f.beginIO(); err != nil {
		return nil, err
	}
	defer f.endIO()
	return f.File.Stat()
}

func (f *ltxFile) Read(p []byte) (int, error) {
	if err := f.beginIO(); err != nil {
		return 0, err
	}
	defer f.endIO()
	if len(p) > sftpPacketSize {
		p = p[:len(p)-len(p)%sftpPacketSize]
	}
	return f.File.Read(p)
}

func (f *ltxFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.beginIO(); err != nil {
		return 0, err
	}
	defer f.endIO()
	return f.File.ReadAt(p, off)
}

func (f *ltxFile) WriteTo(w io.Writer) (int64, error) {
	if err := f.beginIO(); err != nil {
		return 0, err
	}
	defer f.endIO()
	return f.File.WriteTo(w)
}

func (f *ltxFile) Close() error {
	if f.beginIO() == nil {
		defer f.endIO()
	}
	err := f.File.Close()
	f.stopOnCancel()
	f.client.discardConnIfLost(f.conn, err)
	return err
}

// fillReader fills every read completely, except at the end of the stream,
// so that each SFTP write request carries a full packet.
type fillReader struct {
	r io.Reader
}

func (c fillReader) Read(p []byte) (int, error) {
	var n int
	for n < len(p) {
		nn, err := c.r.Read(p[n:])
		n += nn
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
