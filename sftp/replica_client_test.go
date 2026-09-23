package sftp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/pkg/sftp"
	"github.com/superfly/ltx"
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
	c.sftpClient = sftpClient
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

// sftpPacketSize is the largest payload pkg/sftp puts in one write request by default.
const sftpPacketSize = 32 * 1024

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
			c.sftpClient = server.client

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

			limit := (len(data) + sftpPacketSize - 1) / sftpPacketSize
			if n := server.writeRequests(); n > limit {
				t.Fatalf("upload took %d write requests, want at most %d", n, limit)
			}
		})
	}
}

func TestFullChunkReader(t *testing.T) {
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

	r := fullChunkReader{pr}
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

// recordingServer is an in-memory SFTP server that counts the write requests it receives.
type recordingServer struct {
	client *sftp.Client

	mu     sync.Mutex
	writes int
}

func newRecordingServer(t *testing.T, concurrentWrites bool) *recordingServer {
	t.Helper()
	s := &recordingServer{}
	handlers := sftp.InMemHandler()
	handlers.FilePut = &countingFileWriter{FileWriter: handlers.FilePut, server: s}

	clientConn, serverConn := net.Pipe()
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
	return w.WriterAt.WriteAt(p, off)
}
