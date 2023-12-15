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
	"golang.org/x/crypto/ssh"
)

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "sftp"

// Default settings for replica client.
const (
	DefaultDialTimeout = 30 * time.Second
)

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing snapshots & WAL segments to disk.
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

// Generations returns a list of available generation names.
func (c *ReplicaClient) Generations(ctx context.Context) (_ []string, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	}

	fis, err := sftpClient.ReadDir(litestream.GenerationsPath(c.Path))
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	var generations []string
	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}

		name := path.Base(fi.Name())
		if !litestream.IsGenerationName(name) {
			continue
		}
		generations = append(generations, name)
	}

	return generations, nil
}

// DeleteGeneration deletes all snapshots & WAL segments within a generation.
func (c *ReplicaClient) DeleteGeneration(ctx context.Context, generation string) (err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return err
	}

	dir, err := litestream.GenerationPath(c.Path, generation)
	if err != nil {
		return fmt.Errorf("cannot determine generation path: %w", err)
	}

	var dirs []string
	walker := sftpClient.Walk(dir)
	for walker.Step() {
		if err := walker.Err(); err != nil {
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

	// log.Printf("%s(%s): retainer: deleting generation: %s", r.db.Path(), r.Name(), generation)

	return nil
}

// Snapshots returns an iterator over all available snapshots for a generation.
func (c *ReplicaClient) Snapshots(ctx context.Context, generation string) (_ litestream.SnapshotIterator, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	}

	dir, err := litestream.SnapshotsPath(c.Path, generation)
	if err != nil {
		return nil, fmt.Errorf("cannot determine snapshots path: %w", err)
	}

	fis, err := sftpClient.ReadDir(dir)
	if os.IsNotExist(err) {
		return litestream.NewSnapshotInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, err
	}

	// Iterate over every file and convert to metadata.
	infos := make([]litestream.SnapshotInfo, 0, len(fis))
	for _, fi := range fis {
		// Parse index from filename.
		index, err := litestream.ParseSnapshotPath(path.Base(fi.Name()))
		if err != nil {
			continue
		}

		infos = append(infos, litestream.SnapshotInfo{
			Generation: generation,
			Index:      index,
			Size:       fi.Size(),
			CreatedAt:  fi.ModTime().UTC(),
		})
	}

	return litestream.NewSnapshotInfoSliceIterator(infos), nil
}

// WriteSnapshot writes LZ4 compressed data from rd to the object storage.
func (c *ReplicaClient) WriteSnapshot(ctx context.Context, generation string, index int, rd io.Reader) (info litestream.SnapshotInfo, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return info, err
	}

	filename, err := litestream.SnapshotPath(c.Path, generation, index)
	if err != nil {
		return info, fmt.Errorf("cannot determine snapshot path: %w", err)
	}
	startTime := time.Now()

	if err := sftpClient.MkdirAll(path.Dir(filename)); err != nil {
		return info, fmt.Errorf("cannot make parent wal segment directory %q: %w", path.Dir(filename), err)
	}

	f, err := sftpClient.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return info, fmt.Errorf("cannot open snapshot file for writing: %w", err)
	}
	defer f.Close()

	n, err := io.Copy(f, rd)
	if err != nil {
		return info, err
	} else if err := f.Close(); err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(n))

	// log.Printf("%s(%s): snapshot: creating %s/%08x t=%s", r.db.Path(), r.Name(), generation, index, time.Since(startTime).Truncate(time.Millisecond))

	return litestream.SnapshotInfo{
		Generation: generation,
		Index:      index,
		Size:       n,
		CreatedAt:  startTime.UTC(),
	}, nil
}

// SnapshotReader returns a reader for snapshot data at the given generation/index.
func (c *ReplicaClient) SnapshotReader(ctx context.Context, generation string, index int) (_ io.ReadCloser, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	}

	filename, err := litestream.SnapshotPath(c.Path, generation, index)
	if err != nil {
		return nil, fmt.Errorf("cannot determine snapshot path: %w", err)
	}

	f, err := sftpClient.Open(filename)
	if err != nil {
		return nil, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	return f, nil
}

// DeleteSnapshot deletes a snapshot with the given generation & index.
func (c *ReplicaClient) DeleteSnapshot(ctx context.Context, generation string, index int) (err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return err
	}

	filename, err := litestream.SnapshotPath(c.Path, generation, index)
	if err != nil {
		return fmt.Errorf("cannot determine snapshot path: %w", err)
	}

	if err := sftpClient.Remove(filename); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cannot delete snapshot %q: %w", filename, err)
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	return nil
}

// WALSegments returns an iterator over all available WAL files for a generation.
func (c *ReplicaClient) WALSegments(ctx context.Context, generation string) (_ litestream.WALSegmentIterator, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	}

	dir, err := litestream.WALPath(c.Path, generation)
	if err != nil {
		return nil, fmt.Errorf("cannot determine wal path: %w", err)
	}

	fis, err := sftpClient.ReadDir(dir)
	if os.IsNotExist(err) {
		return litestream.NewWALSegmentInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, err
	}

	// Iterate over every file and convert to metadata.
	infos := make([]litestream.WALSegmentInfo, 0, len(fis))
	for _, fi := range fis {
		index, offset, err := litestream.ParseWALSegmentPath(path.Base(fi.Name()))
		if err != nil {
			continue
		}

		infos = append(infos, litestream.WALSegmentInfo{
			Generation: generation,
			Index:      index,
			Offset:     offset,
			Size:       fi.Size(),
			CreatedAt:  fi.ModTime().UTC(),
		})
	}

	return litestream.NewWALSegmentInfoSliceIterator(infos), nil
}

// WriteWALSegment writes LZ4 compressed data from rd into a file on disk.
func (c *ReplicaClient) WriteWALSegment(ctx context.Context, pos litestream.Pos, rd io.Reader) (info litestream.WALSegmentInfo, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return info, err
	}

	filename, err := litestream.WALSegmentPath(c.Path, pos.Generation, pos.Index, pos.Offset)
	if err != nil {
		return info, fmt.Errorf("cannot determine wal segment path: %w", err)
	}
	startTime := time.Now()

	if err := sftpClient.MkdirAll(path.Dir(filename)); err != nil {
		return info, fmt.Errorf("cannot make parent snapshot directory %q: %w", path.Dir(filename), err)
	}

	f, err := sftpClient.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return info, fmt.Errorf("cannot open snapshot file for writing: %w", err)
	}
	defer f.Close()

	n, err := io.Copy(f, rd)
	if err != nil {
		return info, err
	} else if err := f.Close(); err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(n))

	return litestream.WALSegmentInfo{
		Generation: pos.Generation,
		Index:      pos.Index,
		Offset:     pos.Offset,
		Size:       n,
		CreatedAt:  startTime.UTC(),
	}, nil
}

// WALSegmentReader returns a reader for a section of WAL data at the given index.
// Returns os.ErrNotExist if no matching index/offset is found.
func (c *ReplicaClient) WALSegmentReader(ctx context.Context, pos litestream.Pos) (_ io.ReadCloser, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return nil, err
	}

	filename, err := litestream.WALSegmentPath(c.Path, pos.Generation, pos.Index, pos.Offset)
	if err != nil {
		return nil, fmt.Errorf("cannot determine wal segment path: %w", err)
	}

	f, err := sftpClient.Open(filename)
	if err != nil {
		return nil, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()

	return f, nil
}

// DeleteWALSegments deletes WAL segments with at the given positions.
func (c *ReplicaClient) DeleteWALSegments(ctx context.Context, a []litestream.Pos) (err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return err
	}

	for _, pos := range a {
		filename, err := litestream.WALSegmentPath(c.Path, pos.Generation, pos.Index, pos.Offset)
		if err != nil {
			return fmt.Errorf("cannot determine wal segment path: %w", err)
		}

		if err := sftpClient.Remove(filename); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("cannot delete wal segment %q: %w", filename, err)
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()
	}

	return nil
}

// Cleanup deletes path & generations directories after empty.
func (c *ReplicaClient) Cleanup(ctx context.Context) (err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return err
	}

	if err := sftpClient.RemoveDirectory(litestream.GenerationsPath(c.Path)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cannot delete generations path: %w", err)
	} else if err := sftpClient.RemoveDirectory(c.Path); err != nil && !os.IsNotExist(err) {
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
