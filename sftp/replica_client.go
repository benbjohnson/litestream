package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"sort"
	"strings"
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
	HostKeyPath string
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
		User:           c.User,
		BannerCallback: ssh.BannerDisplayStderr(),
	}
	if c.Password != "" {
		config.Auth = append(config.Auth, ssh.Password(c.Password))
	}

	if c.HostKeyPath == "" {
		config.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	} else {
		buf, err := os.ReadFile(c.HostKeyPath)
		if err != nil {
			return nil, fmt.Errorf("cannot read sftp host key path: %w", err)
		}

		key, _, _, _, err := ssh.ParseAuthorizedKey(buf)
		if err != nil {
			return nil, fmt.Errorf("cannot parse sftp host key path: path=%s len=%d err=%w", c.HostKeyPath, len(buf), err)
		}
		config.HostKeyCallback = ssh.FixedHostKey(key)
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

	fis, err := sftpClient.ReadDir(path.Join(c.Path, "generations"))
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

	sort.Strings(generations)

	return generations, nil
}

// DeleteGeneration deletes all snapshots & WAL segments within a generation.
func (c *ReplicaClient) DeleteGeneration(ctx context.Context, generation string) (err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return err
	} else if generation == "" {
		return fmt.Errorf("generation required")
	}

	dir := path.Join(c.Path, "generations", generation)

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
	} else if generation == "" {
		return nil, fmt.Errorf("generation required")
	}

	dir := path.Join(c.Path, "generations", generation, "snapshots")

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
		index, err := internal.ParseSnapshotPath(path.Base(fi.Name()))
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

	sort.Sort(litestream.SnapshotInfoSlice(infos))

	return litestream.NewSnapshotInfoSliceIterator(infos), nil
}

// WriteSnapshot writes LZ4 compressed data from rd to the object storage.
func (c *ReplicaClient) WriteSnapshot(ctx context.Context, generation string, index int, rd io.Reader) (info litestream.SnapshotInfo, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return info, err
	} else if generation == "" {
		return info, fmt.Errorf("generation required")
	}

	filename := path.Join(c.Path, "generations", generation, "snapshots", litestream.FormatIndex(index)+".snapshot.lz4")
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
	} else if generation == "" {
		return nil, fmt.Errorf("generation required")
	}

	filename := path.Join(c.Path, "generations", generation, "snapshots", litestream.FormatIndex(index)+".snapshot.lz4")

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
	} else if generation == "" {
		return fmt.Errorf("generation required")
	}

	filename := path.Join(c.Path, "generations", generation, "snapshots", litestream.FormatIndex(index)+".snapshot.lz4")

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
	} else if generation == "" {
		return nil, fmt.Errorf("generation required")
	}

	dir := path.Join(c.Path, "generations", generation, "wal")

	fis, err := sftpClient.ReadDir(dir)
	if os.IsNotExist(err) {
		return litestream.NewWALSegmentInfoSliceIterator(nil), nil
	} else if err != nil {
		return nil, err
	}

	// Iterate over every file and convert to metadata.
	indexes := make([]int, 0, len(fis))
	for _, fi := range fis {
		index, err := litestream.ParseIndex(fi.Name())
		if err != nil || !fi.IsDir() {
			continue
		}
		indexes = append(indexes, index)
	}

	sort.Ints(indexes)

	return newWALSegmentIterator(ctx, c, dir, generation, indexes), nil
}

// WriteWALSegment writes LZ4 compressed data from rd into a file on disk.
func (c *ReplicaClient) WriteWALSegment(ctx context.Context, pos litestream.Pos, rd io.Reader) (info litestream.WALSegmentInfo, err error) {
	defer func() { c.resetOnConnError(err) }()

	sftpClient, err := c.Init(ctx)
	if err != nil {
		return info, err
	} else if pos.Generation == "" {
		return info, fmt.Errorf("generation required")
	}

	filename := path.Join(c.Path, "generations", pos.Generation, "wal", litestream.FormatIndex(pos.Index), litestream.FormatOffset(pos.Offset)+".wal.lz4")
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
	} else if pos.Generation == "" {
		return nil, fmt.Errorf("generation required")
	}

	filename := path.Join(c.Path, "generations", pos.Generation, "wal", litestream.FormatIndex(pos.Index), litestream.FormatOffset(pos.Offset)+".wal.lz4")

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
		if pos.Generation == "" {
			return fmt.Errorf("generation required")
		}

		filename := path.Join(c.Path, "generations", pos.Generation, "wal", litestream.FormatIndex(pos.Index), litestream.FormatOffset(pos.Offset)+".wal.lz4")

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

	if err := sftpClient.RemoveDirectory(path.Join(c.Path, "generations")); err != nil && !os.IsNotExist(err) {
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

type walSegmentIterator struct {
	ctx        context.Context
	client     *ReplicaClient
	dir        string
	generation string
	indexes    []int

	infos []litestream.WALSegmentInfo
	err   error
}

func newWALSegmentIterator(ctx context.Context, client *ReplicaClient, dir, generation string, indexes []int) *walSegmentIterator {
	return &walSegmentIterator{
		ctx:        ctx,
		client:     client,
		dir:        dir,
		generation: generation,
		indexes:    indexes,
	}
}

func (itr *walSegmentIterator) Close() (err error) {
	return itr.err
}

func (itr *walSegmentIterator) Next() bool {
	sftpClient, err := itr.client.Init(itr.ctx)
	if err != nil {
		itr.err = err
		return false
	}

	// Exit if an error has already occurred.
	if itr.err != nil {
		return false
	}

	for {
		// Move to the next segment in cache, if available.
		if len(itr.infos) > 1 {
			itr.infos = itr.infos[1:]
			return true
		}
		itr.infos = itr.infos[:0] // otherwise clear infos

		// Move to the next index unless this is the first time initializing.
		if itr.infos != nil && len(itr.indexes) > 0 {
			itr.indexes = itr.indexes[1:]
		}

		// If no indexes remain, stop iteration.
		if len(itr.indexes) == 0 {
			return false
		}

		// Read segments into a cache for the current index.
		index := itr.indexes[0]
		fis, err := sftpClient.ReadDir(path.Join(itr.dir, litestream.FormatIndex(index)))
		if err != nil {
			itr.err = err
			return false
		}

		for _, fi := range fis {
			filename := path.Base(fi.Name())
			if fi.IsDir() {
				continue
			}

			offset, err := litestream.ParseOffset(strings.TrimSuffix(filename, ".wal.lz4"))
			if err != nil {
				continue
			}

			itr.infos = append(itr.infos, litestream.WALSegmentInfo{
				Generation: itr.generation,
				Index:      index,
				Offset:     offset,
				Size:       fi.Size(),
				CreatedAt:  fi.ModTime().UTC(),
			})
		}

		if len(itr.infos) > 0 {
			return true
		}
	}
}

func (itr *walSegmentIterator) Err() error { return itr.err }

func (itr *walSegmentIterator) WALSegment() litestream.WALSegmentInfo {
	if len(itr.infos) == 0 {
		return litestream.WALSegmentInfo{}
	}
	return itr.infos[0]
}
