package sftp

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
)

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
