package sftp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/superfly/ltx"
	"golang.org/x/crypto/ssh"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

const (
	fsyncExt = "fsync@openssh.com"

	// SSH_FXP_VERSION reply (with its length prefix) for protocol version 3
	// that advertises the fsync extension.
	versionReply = "\x00\x00\x00\x1f\x02\x00\x00\x00\x03\x00\x00\x00\x11" + fsyncExt + "\x00\x00\x00\x011"
)

func TestReplicaClient_SSHAddr(t *testing.T) {
	for _, tt := range []struct {
		host string
		want string
	}{
		{host: "example.com", want: "example.com:22"},
		{host: "example.com:2222", want: "example.com:2222"},
		{host: "127.0.0.1", want: "127.0.0.1:22"},
		{host: "127.0.0.1:2222", want: "127.0.0.1:2222"},
		{host: "::1", want: "[::1]:22"},
		{host: "[::1]", want: "[::1]:22"},
		{host: "[::1]:2222", want: "[::1]:2222"},
		{host: "[fe80::1%eth0]", want: "[fe80::1%eth0]:22"},
		{host: "[fe80::1%eth0]:2222", want: "[fe80::1%eth0]:2222"},
	} {
		t.Run(tt.host, func(t *testing.T) {
			c := NewReplicaClient()
			c.Host = tt.host
			if got := c.sshAddr(); got != tt.want {
				t.Fatalf("sshAddr()=%q, want %q", got, tt.want)
			}
		})
	}
}

func TestReplicaClient_WriteLTXFile_Fsync(t *testing.T) {
	hdr := ltx.Header{Version: ltx.Version, PageSize: 4096, Commit: 1, MinTXID: 1, MaxTXID: 1, Timestamp: time.Now().UnixMilli()}
	data, _ := hdr.MarshalBinary()

	t.Run("BeforeRename", func(t *testing.T) {
		c, requests := newFsyncClient(t, 0)
		if _, err := c.WriteLTXFile(context.Background(), 1, 1, 1, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		if got, want := requests(), []string{"open", "fsync", "rename"}; !slices.Equal(got, want) {
			t.Fatalf("requests=%v, want %v", got, want)
		}
	})

	t.Run("SyncError", func(t *testing.T) {
		c, _ := newFsyncClient(t, 4) // SSH_FX_FAILURE
		_, err := c.WriteLTXFile(context.Background(), 1, 1, 1, bytes.NewReader(data))
		if err == nil || !strings.Contains(err.Error(), "cannot sync temporary ltx file") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// newFsyncClient returns a client for an in-process pkg/sftp server, which
// does not implement fsync@openssh.com. A proxy in between advertises the
// extension, answers fsync requests with the given status code, and records
// open, fsync and rename requests for the returned function.
func newFsyncClient(t *testing.T, status byte) (*ReplicaClient, func() []string) {
	t.Helper()

	clientConn, clientProxy := net.Pipe()
	serverProxy, serverConn := net.Pipe()
	server, err := sftp.NewServer(serverConn)
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve()
	go io.Copy(clientProxy, serverProxy)

	// Forward client requests to the server. The client waits for each reply, so
	// the proxy's replies never interleave with the server's, and the recorded
	// requests are ordered before the call returns without a lock.
	var reqs []string
	go func() {
		defer serverProxy.Close()
		for {
			pkt, err := readPacket(clientProxy) // length, type, request id, body
			if err != nil {
				return
			}

			var reply []byte
			switch pkt[4] {
			case 1: // SSH_FXP_INIT
				reply = []byte(versionReply)
			case 3: // SSH_FXP_OPEN
				reqs = append(reqs, "open")
			case 18: // SSH_FXP_RENAME
				reqs = append(reqs, "rename")
			case 200: // SSH_FXP_EXTENDED
				if bytes.Contains(pkt, []byte(fsyncExt)) {
					reqs = append(reqs, "fsync")
					// SSH_FXP_STATUS with the request id, code, empty message and language tag.
					reply = append(append([]byte{0, 0, 0, 17, 101}, pkt[5:9]...), 0, 0, 0, status, 0, 0, 0, 0, 0, 0, 0, 0)
				}
			}

			if reply != nil {
				_, err = clientProxy.Write(reply)
			} else {
				_, err = serverProxy.Write(pkt)
			}
			if err != nil {
				return
			}
		}
	}()

	sftpClient, err := sftp.NewClientPipe(clientConn, clientConn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sftpClient.Close() })

	c := NewReplicaClient()
	c.Path = t.TempDir()
	c.conn = &replicaConn{sftp: sftpClient}
	return c, func() []string { return reqs }
}

// readPacket reads one SFTP packet, including its length prefix.
func readPacket(r io.Reader) ([]byte, error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, err
	}
	pkt := append(n[:], make([]byte, binary.BigEndian.Uint32(n[:]))...)
	_, err := io.ReadFull(r, pkt[4:])
	return pkt, err
}

// Compaction hands WriteLTXFile an io.Pipe fed by many small writes. The upload
// must still go out in full packets: a request per small write costs a round
// trip each, which makes large uploads over a real link crawl.
func TestReplicaClient_WriteLTXFile_FullPackets(t *testing.T) {
	for _, concurrent := range []bool{true, false} {
		t.Run(fmt.Sprintf("ConcurrentWrites=%t", concurrent), func(t *testing.T) {
			server := newRecordingServer(t, concurrent)

			c := NewReplicaClient()
			c.Path = "/replica"
			c.ConcurrentWrites = concurrent
			c.conn = &replicaConn{sftp: server.client}

			data := testLTXFile(t, 3*sftpPacketSize+1234)
			pr, pw := io.Pipe()
			go func() {
				for rest := data; len(rest) > 0; {
					n := min(37, len(rest))
					if _, err := pw.Write(rest[:n]); err != nil {
						return
					}
					rest = rest[n:]
				}
				_ = pw.Close()
			}()

			info, err := c.WriteLTXFile(context.Background(), 1, 1, 1, pr)
			if err != nil {
				t.Fatal(err)
			} else if info.Size != int64(len(data)) {
				t.Fatalf("size=%d, want %d", info.Size, len(data))
			}

			f, err := server.client.Open(litestream.LTXFilePath(c.Path, 1, 1, 1))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			uploaded, err := io.ReadAll(f)
			if err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(uploaded, data) {
				t.Fatalf("uploaded %d bytes differ from the %d bytes written", len(uploaded), len(data))
			}

			server.mu.Lock()
			pipelined := server.pipelined
			server.mu.Unlock()
			if concurrent && !pipelined {
				t.Fatal("second write was not sent before the first write response")
			}
			limit := (len(data) + sftpPacketSize - 1) / sftpPacketSize
			if n := server.writeRequests(); n > limit {
				t.Fatalf("upload took %d write requests, want at most %d", n, limit)
			}
		})
	}
}

func TestFillReader(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 5000)
	pr, pw := io.Pipe()
	go func() {
		for rest := data; len(rest) > 0; {
			n := min(37, len(rest))
			if _, err := pw.Write(rest[:n]); err != nil {
				return
			}
			rest = rest[n:]
		}
		_ = pw.Close()
	}()

	r := fillReader{pr}
	buf := make([]byte, sftpPacketSize)
	var got []byte
	var sizes []int
	for {
		n, err := r.Read(buf)
		got = append(got, buf[:n]...)
		sizes = append(sizes, n)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}

	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, want %d", len(got), len(data))
	}
	if want := []int{sftpPacketSize, sftpPacketSize, len(data) - 2*sftpPacketSize}; fmt.Sprint(sizes) != fmt.Sprint(want) {
		t.Fatalf("read sizes=%v, want %v", sizes, want)
	}
}

// testLTXFile returns size bytes that start with a valid LTX snapshot header,
// which is all WriteLTXFile reads before streaming the rest as is.
func testLTXFile(t *testing.T, size int) []byte {
	t.Helper()
	hdr := ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   1,
		MaxTXID:   1,
		Timestamp: time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
	}
	b, err := hdr.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, size-len(b))
	if _, err := rand.Read(body); err != nil {
		t.Fatal(err)
	}
	return append(b, body...)
}

// recordingServer is an in-memory SFTP server that counts the read and write
// requests it receives.
type recordingServer struct {
	client *sftp.Client

	mu         sync.Mutex
	reads      int
	maxReadEnd uint64
	closes     int
	writes     int
	writeErr   error
	closeErr   error
	pipelined  bool
}

func newRecordingServer(t *testing.T, concurrentWrites bool) *recordingServer {
	t.Helper()
	s := &recordingServer{}
	handlers := sftp.InMemHandler()
	handlers.FilePut = &countingFileWriter{FileWriter: handlers.FilePut, server: s}

	clientConn, clientProxy := net.Pipe()
	serverProxy, serverConn := net.Pipe()
	secondWrite := make(chan struct{})
	firstWrite := make(chan struct{})
	go func() {
		defer serverProxy.Close()
		writes := 0
		for {
			pkt, err := readPacket(clientProxy)
			if err != nil {
				return
			}
			if pkt[4] == 4 {
				s.mu.Lock()
				s.closes++
				s.mu.Unlock()
			}
			if pkt[4] == 5 {
				s.mu.Lock()
				s.reads++
				offset := binary.BigEndian.Uint64(pkt[len(pkt)-12:])
				length := binary.BigEndian.Uint32(pkt[len(pkt)-4:])
				s.maxReadEnd = max(s.maxReadEnd, offset+uint64(length))
				s.mu.Unlock()
			}
			if pkt[4] == 6 {
				writes++
				if writes == 1 {
					close(firstWrite)
				}
				if writes == 2 {
					close(secondWrite)
				}
			}
			if _, err := serverProxy.Write(pkt); err != nil {
				return
			}
		}
	}()
	go func() {
		defer clientProxy.Close()
		gated := false
		for {
			pkt, err := readPacket(serverProxy)
			if err != nil {
				return
			}
			if concurrentWrites && !gated && pkt[4] == 101 {
				select {
				case <-firstWrite:
					gated = true
					select {
					case <-secondWrite:
						s.mu.Lock()
						s.pipelined = true
						s.mu.Unlock()
					case <-time.After(2 * time.Second):
					}
				default:
				}
			}
			if _, err := clientProxy.Write(pkt); err != nil {
				return
			}
		}
	}()
	server := sftp.NewRequestServer(serverConn, handlers)
	go func() { _ = server.Serve() }()

	client, err := sftp.NewClientPipe(clientConn, clientConn, sftp.UseConcurrentWrites(concurrentWrites))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	s.client = client
	return s
}

func (s *recordingServer) writeRequests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

func (s *recordingServer) readRequests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

// The LTX decoder reads a 6-byte header before each page. Each of those reads
// must not become its own read request, and buffering must not change the
// bytes a caller sees, for a whole file or for a range of one.
func TestReplicaClient_OpenLTXFile_BufferedReads(t *testing.T) {
	server := newRecordingServer(t, false)
	c := NewReplicaClient()
	c.Path = "/replica"
	c.conn = &replicaConn{sftp: server.client}

	const pageN = 256
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: 4096, Commit: pageN, MinTXID: 1, MaxTXID: 1}); err != nil {
		t.Fatal(err)
	}
	pages := make([][]byte, pageN)
	for i := range pages {
		pages[i] = make([]byte, 4096)
		if _, err := rand.Read(pages[i][:512]); err != nil {
			t.Fatal(err)
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: uint32(i + 1)}, pages[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	data := slices.Clone(buf.Bytes())
	if _, err := c.WriteLTXFile(context.Background(), 1, 1, 1, &buf); err != nil {
		t.Fatal(err)
	}

	reads := server.readRequests()
	rc, err := c.OpenLTXFile(context.Background(), 1, 1, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	dec := ltx.NewDecoder(rc)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	var hdr ltx.PageHeader
	page := make([]byte, 4096)
	for i := range pages {
		if err := dec.DecodePage(&hdr, page); err != nil {
			t.Fatal(err)
		} else if hdr.Pgno != uint32(i+1) || !bytes.Equal(page, pages[i]) {
			t.Fatalf("page %d differs from the page written", i+1)
		}
	}
	if err := dec.DecodePage(&hdr, page); err != io.EOF {
		t.Fatalf("expected end of pages, got %v", err)
	} else if err := dec.Close(); err != nil {
		t.Fatal(err)
	} else if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	if n, limit := server.readRequests()-reads, len(data)/sftpPacketSize+8; n > limit {
		t.Fatalf("decoding %d pages took %d read requests, want at most %d", pageN, n, limit)
	}

	for _, r := range []struct{ offset, size int64 }{{0, ltx.HeaderSize}, {1000, 3 * sftpPacketSize}, {int64(len(data)) - 500, 0}} {
		rc, err := c.OpenLTXFile(context.Background(), 1, 1, 1, r.offset, r.size)
		if err != nil {
			t.Fatal(err)
		}
		var got bytes.Buffer
		if _, err := io.CopyN(&got, rc, 1); err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(&got, rc)
		if closeErr := rc.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		end := int64(len(data))
		if r.size > 0 {
			end = r.offset + r.size
		}
		if err != nil {
			t.Fatal(err)
		} else if !bytes.Equal(got.Bytes(), data[r.offset:end]) {
			t.Fatalf("offset=%d size=%d: read %d bytes that differ from the file", r.offset, r.size, got.Len())
		}
	}
}

func TestReplicaClient_OpenLTXFile_WireRange(t *testing.T) {
	for _, tt := range []struct {
		name         string
		fileSize     int
		offset, size int64
	}{
		{"SmallFile", 2048, 0, 0},
		{"SmallTail", maxReadBufferSize + 500, maxReadBufferSize, 0},
		{"PartialPacket", 3 * sftpPacketSize, 1000, sftpPacketSize + 1},
		{"MultipleFills", 2 * maxReadBufferSize, 1000, maxReadBufferSize + 17},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := newRecordingServer(t, false)
			c := NewReplicaClient()
			c.Path = "/replica"
			c.conn = &replicaConn{sftp: server.client}
			data := testLTXFile(t, tt.fileSize)
			if _, err := c.WriteLTXFile(t.Context(), 1, 1, 1, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			rc, err := c.OpenLTXFile(t.Context(), 1, 1, 1, tt.offset, tt.size)
			if err != nil {
				t.Fatal(err)
			}
			defer rc.Close()
			end := tt.offset + tt.size
			if tt.size == 0 {
				end = int64(tt.fileSize)
			}
			got := make([]byte, end-tt.offset)
			if _, err := io.ReadFull(rc, got[:1]); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(rc, got[1:]); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data[tt.offset:end]) {
				t.Fatal("range contents differ")
			}
			server.mu.Lock()
			defer server.mu.Unlock()
			if server.maxReadEnd != uint64(end) {
				t.Fatalf("furthest wire read ended at %d, want %d", server.maxReadEnd, end)
			}
		})
	}
}

type countingFileWriter struct {
	sftp.FileWriter
	server *recordingServer
}

func (w *countingFileWriter) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	f, err := w.FileWriter.Filewrite(r)
	if err != nil {
		return nil, err
	}
	return &countingWriterAt{WriterAt: f, server: w.server}, nil
}

type countingWriterAt struct {
	io.WriterAt
	server *recordingServer
}

func (w *countingWriterAt) WriteAt(p []byte, off int64) (int, error) {
	w.server.mu.Lock()
	w.server.writes++
	w.server.mu.Unlock()
	if w.server.writeErr != nil {
		n, _ := w.WriterAt.WriteAt(p[:len(p)/2], off)
		return n, w.server.writeErr
	}
	return w.WriterAt.WriteAt(p, off)
}

func (w *countingWriterAt) Close() error {
	var err error
	if closer, ok := w.WriterAt.(io.Closer); ok {
		err = closer.Close()
	}
	return errors.Join(err, w.server.closeErr)
}

func TestReplicaClient_WriteLTXFile_Abort(t *testing.T) {
	for _, concurrent := range []bool{true, false} {
		for _, failure := range []string{"unexpected-eof", "reader", "short-write", "close"} {
			t.Run(fmt.Sprintf("ConcurrentWrites=%t/%s", concurrent, failure), func(t *testing.T) {
				server := newRecordingServer(t, concurrent)
				c := NewReplicaClient()
				c.Path = "/replica"
				c.ConcurrentWrites = concurrent
				c.conn = &replicaConn{sftp: server.client}
				data := testLTXFile(t, 3*sftpPacketSize+1234)
				pr, pw := io.Pipe()
				defer pr.Close()
				sourceErr := errors.New("source failed")
				switch failure {
				case "unexpected-eof":
					sourceErr = io.ErrUnexpectedEOF
				case "short-write":
					server.writeErr = io.ErrShortWrite
				case "close":
					server.closeErr = errors.New("close failed")
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					_, err := pw.Write(data)
					if failure == "reader" || failure == "unexpected-eof" {
						err = sourceErr
					}
					_ = pw.CloseWithError(err)
				}()
				info, err := c.WriteLTXFile(context.Background(), 1, 1, 1, pr)
				_ = pr.Close()
				<-done
				if err == nil || info != nil {
					t.Fatalf("published failed upload: info=%v err=%v", info, err)
				}
				if (failure == "reader" || failure == "unexpected-eof") && !errors.Is(err, sourceErr) {
					t.Fatalf("error=%v, want %v", err, sourceErr)
				}
				filename := litestream.LTXFilePath(c.Path, 1, 1, 1)
				if _, err := server.client.Stat(filename); !os.IsNotExist(err) {
					t.Fatalf("final file exists or stat failed: %v", err)
				}
				entries, err := server.client.ReadDir(path.Dir(filename))
				if err != nil {
					t.Fatal(err)
				}
				if len(entries) != 0 {
					t.Fatalf("temporary files remain: %v", entries)
				}
			})
		}
	}
}

func TestReplicaClient_OpenLTXFile_HealthyClosePreservesSibling(t *testing.T) {
	c := NewReplicaClient()
	c.Path = t.TempDir()
	startStallingServer(t, c, 0)

	data := testLTXFile(t, 1024)
	filename := litestream.LTXFilePath(c.Path, 0, 1, 1)
	if err := os.MkdirAll(path.Dir(filename), 0o755); err != nil {
		t.Fatal(err)
	} else if err := os.WriteFile(filename, data, 0o644); err != nil {
		t.Fatal(err)
	}

	reader := internal.NewResumableReader(t.Context(), c, 0, 1, 1, int64(len(data)), nil, slog.Default())
	defer reader.Close()
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatal(err)
	}
	sibling, err := c.OpenLTXFile(t.Context(), 0, 1, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sibling.Close()

	if err := reader.Close(); err != nil {
		t.Errorf("close healthy reader: %v", err)
	}
	got, err := io.ReadAll(sibling)
	if err != nil {
		t.Fatalf("read sibling: %v", err)
	} else if !bytes.Equal(got, data) {
		t.Fatal("sibling returned different LTX data")
	}
}

func TestReplicaClient_OpenLTXFile_CancelWithBufferedData(t *testing.T) {
	server := newRecordingServer(t, false)
	c := NewReplicaClient()
	c.Path = "/replica"
	c.conn = &replicaConn{sftp: server.client}
	data := testLTXFile(t, 1024)
	if _, err := c.WriteLTXFile(t.Context(), 1, 1, 1, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r, err := c.OpenLTXFile(ctx, 1, 1, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		t.Fatal(err)
	}
	cancel()
	if n, err := r.Read(b[:]); n != 0 || !errors.Is(err, context.Canceled) {
		t.Errorf("read after cancellation = %d, %v", n, err)
	}
	if n, err := io.Copy(io.Discard, r); n != 0 || !errors.Is(err, context.Canceled) {
		t.Errorf("copy after cancellation = %d, %v", n, err)
	}
	server.mu.Lock()
	closes := server.closes
	server.mu.Unlock()
	if err := r.Close(); err != nil {
		t.Fatalf("close after idle cancellation: %v", err)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if n := server.closes - closes; n != 1 {
		t.Errorf("file close sent %d close requests, want 1", n)
	}
}

func TestReplicaClient_OpenLTXFile_CancelStalledStat(t *testing.T) {
	c := NewReplicaClient()
	c.Path = t.TempDir()
	stalled := startStallingServer(t, c, 8)
	filename := litestream.LTXFilePath(c.Path, 0, 1, 1)
	if err := os.MkdirAll(path.Dir(filename), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, testLTXFile(t, 1024), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		r, err := c.OpenLTXFile(ctx, 0, 1, 1, 0, 0)
		if r != nil {
			_ = r.Close()
		}
		done <- err
	}()
	select {
	case <-stalled:
	case <-time.After(5 * time.Second):
		t.Fatal("file stat did not reach the server")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("open after cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("file stat remained blocked after cancellation")
	}
}

// A read from a server that stops answering must return once the context the
// file was opened with is done, so that a stalled restore can reconnect.
func TestReplicaClient_OpenLTXFile_ContextCanceledWhileStalled(t *testing.T) {
	c := NewReplicaClient()
	c.Path = t.TempDir()
	stalled := startStallingServer(t, c, 5) // SSH_FXP_READ

	filename := litestream.LTXFilePath(c.Path, 0, 1, 1)
	if err := os.MkdirAll(path.Dir(filename), 0o755); err != nil {
		t.Fatal(err)
	} else if err := os.WriteFile(filename, testLTXFile(t, 1024), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stalledReader, err := c.OpenLTXFile(ctx, 0, 1, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// The file is closed only once the read has returned: closing it sends a
	// request too, which a stalled server would never answer.
	errc := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(stalledReader)
		errc <- err
	}()
	select {
	case <-stalled:
	case <-time.After(5 * time.Second):
		t.Fatal("server never stalled")
	}

	cancel()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("expected error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read did not return after its context was canceled")
	}

	reopenedReader, err := c.OpenLTXFile(t.Context(), 0, 1, 1, 0, 0)
	if err != nil {
		t.Fatalf("first open after cancellation: %v", err)
	}
	_ = stalledReader.Close()
	if err := reopenedReader.Close(); err != nil {
		t.Fatalf("close replacement after closing stale file: %v", err)
	}
}

// startStallingServer points c at an SSH server that serves SFTP from the local
// filesystem until it receives a request of type stall, then stops answering
// without closing the connection. The returned channel is closed once the
// server has stalled.
func startStallingServer(t *testing.T, c *ReplicaClient, stall byte) <-chan struct{} {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	config := &ssh.ServerConfig{NoClientAuth: true}
	config.AddHostKey(signer)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.Host, c.User = ln.Addr().String(), "user"
	c.HostKey = string(ssh.MarshalAuthorizedKey(signer.PublicKey()))

	stalled, done := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { close(done); _ = ln.Close() })
	track := func(conn io.Closer) { go func() { <-done; _ = conn.Close() }() }

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			track(conn)
			go func() {
				_, chans, reqs, err := ssh.NewServerConn(conn, config)
				if err != nil {
					return
				}
				go ssh.DiscardRequests(reqs)
				for newCh := range chans {
					ch, chReqs, err := newCh.Accept()
					if err != nil {
						return
					}
					go func() {
						for req := range chReqs {
							_ = req.Reply(req.Type == "subsystem", nil)
						}
					}()

					serverConn, proxyConn := net.Pipe()
					track(serverConn)
					server, err := sftp.NewServer(serverConn)
					if err != nil {
						return
					}
					go func() { _ = server.Serve() }()
					go func() { _, _ = io.Copy(ch, proxyConn) }()
					go func() {
						for {
							pkt, err := readPacket(ch)
							if err != nil {
								return
							} else if pkt[4] == stall {
								once.Do(func() { close(stalled) })
								_, _ = io.Copy(io.Discard, ch)
								return
							} else if _, err := proxyConn.Write(pkt); err != nil {
								return
							}
						}
					}()
				}
			}()
		}
	}()
	return stalled
}
