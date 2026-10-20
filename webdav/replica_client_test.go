package webdav_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/webdav"
)

func TestReplicaClient_Type(t *testing.T) {
	c := webdav.NewReplicaClient()
	if got, want := c.Type(), "webdav"; got != want {
		t.Fatalf("Type()=%v, want %v", got, want)
	}
}

func TestReplicaClient_Init_RequiresURL(t *testing.T) {
	c := webdav.NewReplicaClient()
	c.URL = ""

	if err := c.Init(context.TODO()); err == nil {
		t.Fatal("expected error when URL is empty")
	} else if got, want := err.Error(), "webdav url required"; got != want {
		t.Fatalf("error=%v, want %v", got, want)
	}
}

func TestReplicaClient_DeleteAll_NotFound(t *testing.T) {
	srv := newFakeWebDAVServer()
	srv.deleteReturn404["/missing"] = true

	ts := httptest.NewServer(srv)
	defer ts.Close()

	c := newTestReplicaClient(ts.URL)
	c.Path = "/missing"

	if err := c.DeleteAll(context.Background()); err != nil {
		t.Fatalf("DeleteAll returned error: %v", err)
	}
}

func TestReplicaClient_LTXFiles_PathNotFound(t *testing.T) {
	srv := newFakeWebDAVServer()
	srv.propfindReturn404["/missing/ltx/0"] = true

	ts := httptest.NewServer(srv)
	defer ts.Close()

	c := newTestReplicaClient(ts.URL)
	c.Path = "/missing"

	itr, err := c.LTXFiles(context.Background(), 0, 0, false)
	if err != nil {
		t.Fatalf("LTXFiles returned error: %v", err)
	}
	defer itr.Close()

	if itr.Next() {
		t.Fatal("expected no iterator items when directory is missing")
	}
}

func TestReplicaClient_OpenLTXFile_RangeFallback(t *testing.T) {
	srv := newFakeWebDAVServer()
	srv.ignoreRange = true // simulate server ignoring Range requests

	ts := httptest.NewServer(srv)
	defer ts.Close()

	c := newTestReplicaClient(ts.URL)
	c.Path = "/db"

	payload := []byte("ABCDEFGHIJ")
	ltxData := buildLTXPayload(1, 2, payload)

	if _, err := c.WriteLTXFile(context.Background(), 0, 1, 2, bytes.NewReader(ltxData)); err != nil {
		t.Fatalf("WriteLTXFile failed: %v", err)
	}

	rc, err := c.OpenLTXFile(context.Background(), 0, 1, 2, 0, int64(len(payload)))
	if err != nil {
		t.Fatalf("OpenLTXFile failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if want := ltxData[:len(payload)]; !bytes.Equal(got, want) {
		t.Fatalf("OpenLTXFile range fallback returned %q, want %q", got, want)
	}
}

func TestReplicaClient_OpenLTXFile_OffsetOnly(t *testing.T) {
	srv := newFakeWebDAVServer()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	c := newTestReplicaClient(ts.URL)
	c.Path = "/db"

	payload := []byte("ABCDEFGHIJ")
	ltxData := buildLTXPayload(10, 12, payload)

	if _, err := c.WriteLTXFile(context.Background(), 0, 10, 12, bytes.NewReader(ltxData)); err != nil {
		t.Fatalf("WriteLTXFile failed: %v", err)
	}

	offset := int64(len(ltxData) - len(payload)/2)
	rc, err := c.OpenLTXFile(context.Background(), 0, 10, 12, offset, 0)
	if err != nil {
		t.Fatalf("OpenLTXFile failed: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if want := ltxData[offset:]; !bytes.Equal(got, want) {
		t.Fatalf("OpenLTXFile offset reader returned %q, want %q", got, want)
	}
}

func TestReplicaClient_LTXFiles_UseMetadata(t *testing.T) {
	for _, tt := range []struct {
		name        string
		ignoreRange bool
	}{
		{"RangeSupported", false},
		{"RangeIgnored", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := newFakeWebDAVServer()
			srv.ignoreRange = tt.ignoreRange

			ts := httptest.NewServer(srv)
			defer ts.Close()

			c := newTestReplicaClient(ts.URL)
			c.Path = "/db"

			// The server stamps files with the upload time; the header has the real one.
			wantAt := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			for i := ltx.TXID(1); i <= 2; i++ {
				data := buildLTXPayloadAt(i, i, wantAt.Add(time.Duration(i)*time.Minute), []byte("payload"))
				if _, err := c.WriteLTXFile(context.Background(), 0, i, i, bytes.NewReader(data)); err != nil {
					t.Fatalf("WriteLTXFile failed: %v", err)
				}
			}

			infos, err := litestream.FindLTXFiles(context.Background(), c, 0, true, func(*ltx.FileInfo) (bool, error) { return true, nil })
			if err != nil {
				t.Fatal(err)
			} else if got, want := len(infos), 2; got != want {
				t.Fatalf("len=%d, want %d", got, want)
			}
			for _, info := range infos {
				want := wantAt.Add(time.Duration(info.MaxTXID) * time.Minute)
				if got := info.CreatedAt; !got.Equal(want) {
					t.Fatalf("TXID %d: CreatedAt=%v, want %v", info.MaxTXID, got, want)
				}
			}
		})
	}
}

func TestReplicaClient_MetadataFailure(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := newFakeWebDAVServer()
			filename := "/db/ltx/0/0000000000000002-0000000000000002.ltx"
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.Path == filename {
					w.WriteHeader(status)
					_, _ = w.Write(bytes.Repeat([]byte("x"), ltx.HeaderSize))
					return
				}
				srv.ServeHTTP(w, r)
			}))
			defer ts.Close()

			c := newTestReplicaClient(ts.URL)
			c.Path = "/db"
			base := time.Now().Add(-time.Hour)
			for i := ltx.TXID(1); i <= 2; i++ {
				data := buildLTXPayloadAt(i, i, base.Add(time.Duration(i)*time.Minute), []byte("payload"))
				if _, err := c.WriteLTXFile(context.Background(), 0, i, i, bytes.NewReader(data)); err != nil {
					t.Fatal(err)
				}
			}

			infos, err := litestream.FindLTXFiles(context.Background(), c, 0, false, func(*ltx.FileInfo) (bool, error) { return true, nil })
			if err != nil {
				t.Fatal(err)
			}
			if len(infos) != 2 {
				t.Fatalf("len=%d, want 2", len(infos))
			}

			if _, err := litestream.FindLTXFiles(context.Background(), c, 0, true, func(*ltx.FileInfo) (bool, error) { return true, nil }); err == nil {
				t.Fatal("expected metadata error")
			}
			infos, err = litestream.CalcRestorePlan(context.Background(), c, 0, base.Add(3*time.Minute), slog.Default())
			if err == nil {
				t.Fatalf("expected metadata error, got restore plan: %v", infos)
			}
			if infos != nil {
				t.Fatalf("expected no restore plan, got %v", infos)
			}
		})
	}
}

func TestReplicaClient_LTXFiles_UseMetadata_ContextCanceled(t *testing.T) {
	srv := newFakeWebDAVServer()

	ts := httptest.NewServer(srv)
	defer ts.Close()

	c := newTestReplicaClient(ts.URL)
	c.Path = "/db"

	data := buildLTXPayload(1, 1, []byte("payload"))
	if _, err := c.WriteLTXFile(context.Background(), 0, 1, 1, bytes.NewReader(data)); err != nil {
		t.Fatalf("WriteLTXFile failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := c.LTXFiles(ctx, 0, 0, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want %v", err, context.Canceled)
	}
}

// newTestReplicaClient returns a replica client pointed at a test server.
func newTestReplicaClient(baseURL string) *webdav.ReplicaClient {
	c := webdav.NewReplicaClient()
	c.URL = baseURL
	c.Timeout = time.Second
	return c
}

// buildLTXPayload constructs a minimal valid LTX file with payload data.
func buildLTXPayload(minTXID, maxTXID ltx.TXID, payload []byte) []byte {
	return buildLTXPayloadAt(minTXID, maxTXID, time.Now(), payload)
}

// buildLTXPayloadAt is buildLTXPayload with a given header timestamp.
func buildLTXPayloadAt(minTXID, maxTXID ltx.TXID, timestamp time.Time, payload []byte) []byte {
	hdr := ltx.Header{
		Version:   1,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: timestamp.UnixMilli(),
	}
	headerBytes, _ := hdr.MarshalBinary()
	return append(headerBytes, payload...)
}

// fakeWebDAVServer provides a minimal in-memory WebDAV implementation for tests.
type fakeWebDAVServer struct {
	mu                sync.Mutex
	files             map[string][]byte
	ignoreRange       bool
	deleteReturn404   map[string]bool
	propfindReturn404 map[string]bool
}

func newFakeWebDAVServer() *fakeWebDAVServer {
	return &fakeWebDAVServer{
		files:             make(map[string][]byte),
		deleteReturn404:   make(map[string]bool),
		propfindReturn404: make(map[string]bool),
	}
}

func (s *fakeWebDAVServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch r.Method {
	case http.MethodOptions:
		w.WriteHeader(http.StatusOK)
	case "MKCOL":
		w.WriteHeader(http.StatusCreated)
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		s.files[r.URL.Path] = body
		w.WriteHeader(http.StatusCreated)
	case http.MethodGet:
		s.handleGet(w, r)
	case http.MethodDelete:
		s.handleDelete(w, r)
	case "PROPFIND":
		s.handlePropfind(w, r)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func (s *fakeWebDAVServer) handleGet(w http.ResponseWriter, r *http.Request) {
	data, ok := s.files[r.URL.Path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" && !s.ignoreRange {
		start, end, err := parseRange(rangeHeader, len(data))
		if err != nil {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}

		w.Header().Set("Content-Length", strconv.Itoa(end-start))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start:end])
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *fakeWebDAVServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimSuffix(r.URL.Path, "/")
	if s.deleteReturn404[trimmed] {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if _, ok := s.files[r.URL.Path]; ok {
		delete(s.files, r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

func (s *fakeWebDAVServer) handlePropfind(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimSuffix(r.URL.Path, "/")
	if s.propfindReturn404[trimmed] {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// List the files directly under the path, stamped with the current time
	// like a server that records when it received them.
	now := time.Now().UTC().Format(http.TimeFormat)
	var children strings.Builder
	for name, data := range s.files {
		if path.Dir(name) == trimmed {
			fmt.Fprintf(&children, `<d:response><d:href>%s</d:href><d:propstat><d:prop><d:getcontentlength>%d</d:getcontentlength><d:getlastmodified>%s</d:getlastmodified></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>`, name, len(data), now)
		}
	}

	response := fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<d:multistatus xmlns:d="DAV:">
  <d:response>
    <d:href>%s/</d:href>
    <d:propstat>
      <d:prop>
        <d:resourcetype><d:collection/></d:resourcetype>
        <d:getcontentlength>0</d:getcontentlength>
        <d:getlastmodified>%s</d:getlastmodified>
      </d:prop>
      <d:status>HTTP/1.1 200 OK</d:status>
    </d:propstat>
  </d:response>%s
</d:multistatus>`, trimmed, now, children.String())

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.WriteHeader(207)
	_, _ = w.Write([]byte(response))
}

func parseRange(header string, size int) (start, end int, err error) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, fmt.Errorf("unsupported range header: %s", header)
	}
	parts := strings.Split(strings.TrimPrefix(header, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range header: %s", header)
	}
	start, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	if parts[1] == "" {
		end = size
	} else {
		end, err = strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, err
		}
		end++ // range end is inclusive
	}

	if start < 0 || end > size || start >= end {
		return 0, 0, fmt.Errorf("invalid range [%d, %d) for size %d", start, end, size)
	}
	return start, end, nil
}
