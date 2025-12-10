package webdav_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

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

// newTestReplicaClient returns a replica client pointed at a test server.
func newTestReplicaClient(baseURL string) *webdav.ReplicaClient {
	c := webdav.NewReplicaClient()
	c.URL = baseURL
	c.Timeout = time.Second
	return c
}

// buildLTXPayload constructs a minimal valid LTX file with payload data.
func buildLTXPayload(minTXID, maxTXID ltx.TXID, payload []byte) []byte {
	hdr := ltx.Header{
		Version:   1,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: time.Now().UnixMilli(),
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
  </d:response>
</d:multistatus>`, trimmed, time.Now().UTC().Format(http.TimeFormat))

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
