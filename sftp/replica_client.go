package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"github.com/pkg/sftp"
	"github.com/superfly/ltx"
	"golang.org/x/crypto/ssh"
)

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "sftp"

// Default settings for replica client.
const (
	DefaultDialTimeout = 30 * time.Second
)

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files over SFTP.
type ReplicaClient struct {
	mu         sync.Mutex
	sshClient  *ssh.Client
	sftpClient *sftp.Client

	// SFTP connection info
	Host        string
	User        string
	Password    string
	Path        string
	KeyPath     string
	DialTimeout time.Duration
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{
		DialTimeout: DefaultDialTimeout,
	}
}

// Type returns "gcs" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the connection to GCS. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) (_ *sftp.Client, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sftpClient != nil {
		return c.sftpClient, nil
	}

	if c.User == "" {
		return nil, fmt.Errorf("sftp user required")
	}

	// Build SSH configuration & auth methods
	config := &ssh.ClientConfig{
		User:            c.User,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		BannerCallback:  ssh.BannerDisplayStderr(),
	}
	if c.Password != "" {
		config.Auth = append(config.Auth, ssh.Password(c.Password))
	}

	if c.KeyPath != "" {
		buf, err := os.ReadFile(c.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("cannot read sftp key path: %w", err)
		}

		signer, err := ssh.ParsePrivateKey(buf)
		if err != nil {
			return nil, fmt.Errorf("cannot parse sftp key path: %w", err)
		}
		config.Auth = append(config.Auth, ssh.PublicKeys(signer))
	}

	// Append standard port, if necessary.
	host := c.Host
	if _, _, err := net.SplitHostPort(c.Host); err != nil {
		host = net.JoinHostPort(c.Host, "22")
	}

	// Connect via SSH.
	if c.sshClient, err = ssh.Dial("tcp", host, config); err != nil {
		return nil, err
	}

	// Wrap connection with an SFTP client.
	if c.sftpClient, err = sftp.NewClient(c.sshClient); err != nil {
		c.sshClient.Close()
		c.sshClient = nil
		return nil, err
	}

	return c.sftpClient, nil
}

// DeleteAll deletes all LTX files.
func (c *ReplicaClient) DeleteAll(ctx context.Context) (err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return err
	}

	var dirs []string
	walker := sftpClient.Walk(c.Path)
	for walker.Step() {
		if err := walker.Err(); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return fmt.Errorf("cannot walk path %q: %w", walker.Path(), err)
		}
		if walker.Stat().IsDir() {
			dirs = append(dirs, walker.Path())
			continue
		}

		if err := sftpClient.Remove(walker.Path()); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("cannot delete file %q: %w", walker.Path(), err)
		}

		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	// Remove directories in reverse order after they have been emptied.
	for i := len(dirs) - 1; i >= 0; i-- {
		filename := dirs[i]
		if err := sftpClient.RemoveDirectory(filename); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("cannot delete directory %q: %w", filename, err)
		}
	}

	// log.Printf("%s(%s): retainer: deleting all", r.db.Path(), r.Name())

	return nil
}

// LTXFiles returns an iterator over all available LTX files for a level.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int) (_ ltx.FileIterator, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	}

	dir := litestream.LTXLevelDir(c.Path, level)
	fis, err := sftpClient.ReadDir(dir)
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
		}

		infos = append(infos, &ltx.FileInfo{
			Level:     level,
			MinTXID:   minTXID,
			MaxTXID:   maxTXID,
			Size:      fi.Size(),
			CreatedAt: fi.ModTime().UTC(),
		})
	}

	return ltx.NewFileInfoSliceIterator(infos), nil
}

// WriteLTXFile writes a LTX file from rd into a remote file.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (info *ltx.FileInfo, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	}

	filename := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)
	startTime := time.Now()

	if err := sftpClient.MkdirAll(path.Dir(filename)); err != nil {
		return nil, fmt.Errorf("cannot make parent snapshot directory %q: %w", path.Dir(filename), err)
	}

	f, err := sftpClient.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return nil, fmt.Errorf("cannot open snapshot file for writing: %w", err)
	}
	defer f.Close()

	n, err := io.Copy(f, rd)
	if err != nil {
		return nil, err
	} else if err := f.Close(); err != nil {
		return nil, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(n))

	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      n,
		CreatedAt: startTime.UTC(),
	}, nil
}

// OpenLTXFile returns a reader for an LTX file.
// Returns os.ErrNotExist if no matching position is found.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (_ io.ReadCloser, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	}

	filename := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)
	f, err := sftpClient.Open(filename)
	if err != nil {
		return nil, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	return f, nil
}

// DeleteLTXFiles deletes LTX files with at the given positions.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) (err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return err
	}

	for _, info := range a {
		filename := litestream.LTXFilePath(c.Path, info.Level, info.MinTXID, info.MaxTXID)
		if err := sftpClient.Remove(filename); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("cannot delete ltx file %q: %w", filename, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

// Cleanup deletes path & directories after empty.
func (c *ReplicaClient) Cleanup(ctx context.Context) (err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return err
	}

	if err := sftpClient.RemoveDirectory(c.Path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cannot delete path: %w", err)
	}
	return nil
}

// resetOnConnError closes & clears the client if a connection error occurs.
func (c *ReplicaClient) resetOnConnError(err error) {
	if !errors.Is(err, sftp.ErrSSHFxConnectionLost) {
		return
	}

	if c.sftpClient != nil {
		c.sftpClient.Close()
		c.sftpClient = nil
	}
	if c.sshClient != nil {
		c.sshClient.Close()
		c.sshClient = nil
	}
}
