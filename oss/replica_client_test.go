package oss

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/superfly/ltx"
)

func TestReplicaClient_Type(t *testing.T) {
	c := NewReplicaClient()
	if got := c.Type(); got != ReplicaClientType {
		t.Errorf("Type() = %q, want %q", got, ReplicaClientType)
	}
	if got := c.Type(); got != "oss" {
		t.Errorf("Type() = %q, want %q", got, "oss")
	}
}

func TestReplicaClient_Init_BucketValidation(t *testing.T) {
	t.Run("EmptyBucket", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "" // Empty bucket name
		c.Region = "cn-hangzhou"

		err := c.Init(t.Context())
		if err == nil {
			t.Fatal("expected error for empty bucket name")
		}
		if got := err.Error(); got != "oss: bucket name is required" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("ValidBucketWithRegion", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "test-bucket"
		c.Region = "cn-hangzhou"
		c.AccessKeyID = "test-key"
		c.AccessKeySecret = "test-secret"

		// Init should succeed (client will be created even without real credentials)
		err := c.Init(t.Context())
		if err != nil {
			t.Errorf("Init() should succeed with valid bucket: %v", err)
		}
	})

	t.Run("ValidBucketDefaultRegion", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "test-bucket"
		// Region is empty, should use DefaultRegion
		c.AccessKeyID = "test-key"
		c.AccessKeySecret = "test-secret"

		err := c.Init(t.Context())
		if err != nil {
			t.Errorf("Init() should succeed with default region: %v", err)
		}
	})
}

func TestReplicaClient_Init_Idempotent(t *testing.T) {
	c := NewReplicaClient()
	c.Bucket = "test-bucket"
	c.AccessKeyID = "test-key"
	c.AccessKeySecret = "test-secret"

	// First init
	if err := c.Init(t.Context()); err != nil {
		t.Fatalf("first Init() failed: %v", err)
	}

	// Second init should be a no-op
	if err := c.Init(t.Context()); err != nil {
		t.Fatalf("second Init() failed: %v", err)
	}
}

func TestParseURL(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		wantBucket string
		wantRegion string
		wantKey    string
		wantErr    bool
	}{
		{
			name:       "SimpleOSSURL",
			url:        "oss://my-bucket/path/to/file",
			wantBucket: "my-bucket",
			wantRegion: "",
			wantKey:    "path/to/file",
			wantErr:    false,
		},
		{
			name:       "OSSURLWithRegion",
			url:        "oss://my-bucket.oss-cn-hangzhou.aliyuncs.com/backup",
			wantBucket: "my-bucket",
			wantRegion: "cn-hangzhou",
			wantKey:    "backup",
			wantErr:    false,
		},
		{
			name:       "OSSURLNoPath",
			url:        "oss://my-bucket",
			wantBucket: "my-bucket",
			wantRegion: "",
			wantKey:    "",
			wantErr:    false,
		},
		{
			name:       "InvalidScheme",
			url:        "s3://my-bucket/path",
			wantBucket: "",
			wantRegion: "",
			wantKey:    "",
			wantErr:    true,
		},
		{
			name:       "HTTPScheme",
			url:        "http://my-bucket/path",
			wantBucket: "",
			wantRegion: "",
			wantKey:    "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, region, key, err := ParseURL(tt.url)

			if (err != nil) != tt.wantErr {
				t.Errorf("ParseURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				return
			}

			if bucket != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", bucket, tt.wantBucket)
			}
			if region != tt.wantRegion {
				t.Errorf("region = %q, want %q", region, tt.wantRegion)
			}
			if key != tt.wantKey {
				t.Errorf("key = %q, want %q", key, tt.wantKey)
			}
		})
	}
}

func TestParseHost(t *testing.T) {
	tests := []struct {
		name       string
		host       string
		wantBucket string
		wantRegion string
	}{
		{
			name:       "StandardOSSURL",
			host:       "my-bucket.oss-cn-hangzhou.aliyuncs.com",
			wantBucket: "my-bucket",
			wantRegion: "cn-hangzhou",
		},
		{
			name:       "OSSURLBeijingRegion",
			host:       "test-bucket.oss-cn-beijing.aliyuncs.com",
			wantBucket: "test-bucket",
			wantRegion: "cn-beijing",
		},
		{
			name:       "OSSURLShanghaiRegion",
			host:       "data-bucket.oss-cn-shanghai.aliyuncs.com",
			wantBucket: "data-bucket",
			wantRegion: "cn-shanghai",
		},
		{
			name:       "InternalOSSURL",
			host:       "my-bucket.oss-cn-hangzhou-internal.aliyuncs.com",
			wantBucket: "my-bucket",
			wantRegion: "cn-hangzhou",
		},
		{
			name:       "InternalOSSURLBeijing",
			host:       "test-bucket.oss-cn-beijing-internal.aliyuncs.com",
			wantBucket: "test-bucket",
			wantRegion: "cn-beijing",
		},
		{
			name:       "SimpleBucketName",
			host:       "my-bucket",
			wantBucket: "my-bucket",
			wantRegion: "",
		},
		{
			name:       "BucketWithHyphens",
			host:       "my-test-bucket-2024.oss-cn-shenzhen.aliyuncs.com",
			wantBucket: "my-test-bucket-2024",
			wantRegion: "cn-shenzhen",
		},
		{
			name:       "OSSURLWithNumbers",
			host:       "bucket123.oss-cn-hangzhou.aliyuncs.com",
			wantBucket: "bucket123",
			wantRegion: "cn-hangzhou",
		},
		{
			name:       "OSSURLHongKong",
			host:       "hk-bucket.oss-cn-hongkong.aliyuncs.com",
			wantBucket: "hk-bucket",
			wantRegion: "cn-hongkong",
		},
		{
			name:       "OSSURLSingapore",
			host:       "sg-bucket.oss-ap-southeast-1.aliyuncs.com",
			wantBucket: "sg-bucket",
			wantRegion: "ap-southeast-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, region, _ := ParseHost(tt.host)

			if bucket != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", bucket, tt.wantBucket)
			}
			if region != tt.wantRegion {
				t.Errorf("region = %q, want %q", region, tt.wantRegion)
			}
		})
	}
}

func TestIsNotExists(t *testing.T) {
	t.Run("NilError", func(t *testing.T) {
		if isNotExists(nil) {
			t.Error("isNotExists should return false for nil error")
		}
	})

	t.Run("RegularError", func(t *testing.T) {
		regularErr := errors.New("regular error")
		if isNotExists(regularErr) {
			t.Error("isNotExists should return false for regular error")
		}
	})

	t.Run("WrappedError", func(t *testing.T) {
		wrappedErr := errors.New("wrapped: something went wrong")
		if isNotExists(wrappedErr) {
			t.Error("isNotExists should return false for wrapped non-ServiceError")
		}
	})
}

func TestLtxPath(t *testing.T) {
	c := NewReplicaClient()
	c.Path = "backups"

	tests := []struct {
		level    int
		filename string
		want     string
	}{
		{0, "00000001-00000001.ltx", "backups/0000/00000001-00000001.ltx"},
		{1, "00000001-00000010.ltx", "backups/0001/00000001-00000010.ltx"},
		{15, "00000001-000000ff.ltx", "backups/000f/00000001-000000ff.ltx"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := c.ltxPath(tt.level, tt.filename)
			if got != tt.want {
				t.Errorf("ltxPath(%d, %q) = %q, want %q", tt.level, tt.filename, got, tt.want)
			}
		})
	}
}

func TestDeleteResultError(t *testing.T) {
	ptr := func(s string) *string { return &s }

	t.Run("NilResult", func(t *testing.T) {
		requested := []oss.DeleteObject{{Key: ptr("key1")}}
		if err := deleteResultError(requested, nil); err != nil {
			t.Errorf("expected nil error for nil result, got %v", err)
		}
	})

	t.Run("AllDeleted", func(t *testing.T) {
		requested := []oss.DeleteObject{
			{Key: ptr("key1")},
			{Key: ptr("key2")},
		}
		result := &oss.DeleteMultipleObjectsResult{
			DeletedObjects: []oss.DeletedInfo{
				{Key: ptr("key1")},
				{Key: ptr("key2")},
			},
		}
		if err := deleteResultError(requested, result); err != nil {
			t.Errorf("expected nil error when all deleted, got %v", err)
		}
	})

	t.Run("SomeNotDeleted", func(t *testing.T) {
		requested := []oss.DeleteObject{
			{Key: ptr("key1")},
			{Key: ptr("key2")},
			{Key: ptr("key3")},
		}
		result := &oss.DeleteMultipleObjectsResult{
			DeletedObjects: []oss.DeletedInfo{
				{Key: ptr("key1")},
				// key2 and key3 not deleted
			},
		}
		err := deleteResultError(requested, result)
		if err == nil {
			t.Fatal("expected error when some keys not deleted")
		}
		errStr := err.Error()
		if !contains(errStr, "key2") {
			t.Errorf("error should mention key2: %s", errStr)
		}
		if !contains(errStr, "key3") {
			t.Errorf("error should mention key3: %s", errStr)
		}
	})

	t.Run("EmptyRequested", func(t *testing.T) {
		requested := []oss.DeleteObject{}
		result := &oss.DeleteMultipleObjectsResult{}
		if err := deleteResultError(requested, result); err != nil {
			t.Errorf("expected nil error for empty requested, got %v", err)
		}
	})

	t.Run("NilKeyInRequested", func(t *testing.T) {
		requested := []oss.DeleteObject{
			{Key: nil}, // nil key should be skipped
			{Key: ptr("key1")},
		}
		result := &oss.DeleteMultipleObjectsResult{
			DeletedObjects: []oss.DeletedInfo{
				{Key: ptr("key1")},
			},
		}
		if err := deleteResultError(requested, result); err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	})
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func mustOSSLTX(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   1,
		MaxTXID:   1,
		Timestamp: 1700000000000,
	}); err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	enc.SetPostApplyChecksum(ltx.ChecksumFlag | 1)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestReplicaClient_BareBucketRootPrefix(t *testing.T) {
	ltxData := mustOSSLTX(t)

	newMockOSS := func(t *testing.T, initial map[string][]byte) (*httptest.Server, *sync.Mutex, map[string][]byte) {
		t.Helper()
		var mu sync.Mutex
		objects := make(map[string][]byte, len(initial))
		for k, v := range initial {
			objects[k] = append([]byte(nil), v...)
		}

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()

			const bucketPrefix = "/test-bucket"
			switch {
			case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
				prefix := r.URL.Query().Get("prefix")
				maxKeys, _ := strconv.Atoi(r.URL.Query().Get("max-keys"))

				mu.Lock()
				var keys []string
				for k := range objects {
					if strings.HasPrefix(k, prefix) {
						keys = append(keys, k)
					}
				}
				mu.Unlock()

				for i := 0; i < len(keys); i++ {
					for j := i + 1; j < len(keys); j++ {
						if keys[j] < keys[i] {
							keys[i], keys[j] = keys[j], keys[i]
						}
					}
				}
				if maxKeys > 0 && len(keys) > maxKeys {
					keys = keys[:maxKeys]
				}

				var b strings.Builder
				b.WriteString(`<ListBucketResult><IsTruncated>false</IsTruncated>`)
				for _, k := range keys {
					mu.Lock()
					sz := len(objects[k])
					mu.Unlock()
					fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>%d</Size><LastModified>2026-10-30T00:00:00Z</LastModified></Contents>`, k, sz)
				}
				b.WriteString(`</ListBucketResult>`)
				w.Header().Set("Content-Type", "application/xml")
				_, _ = io.WriteString(w, b.String())

			case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
				reqPath := r.URL.EscapedPath()
				key := strings.TrimPrefix(reqPath, bucketPrefix+"/")
				if unesc, err := url.PathUnescape(key); err == nil {
					key = unesc
				}
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprintf(w, `<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>%s</Key><UploadId>mock-upload</UploadId></InitiateMultipartUploadResult>`, key)

			case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "":
				reqPath := r.URL.EscapedPath()
				key := strings.TrimPrefix(reqPath, bucketPrefix+"/")
				if unesc, err := url.PathUnescape(key); err == nil {
					key = unesc
				}
				w.Header().Set("Content-Type", "application/xml")
				fmt.Fprintf(w, `<CompleteMultipartUploadResult><Bucket>test-bucket</Bucket><Key>%s</Key><ETag>"mock-etag"</ETag></CompleteMultipartUploadResult>`, key)

			case r.Method == http.MethodPut:
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				reqPath := r.URL.EscapedPath()
				key := strings.TrimPrefix(reqPath, bucketPrefix+"/")
				if unesc, err := url.PathUnescape(key); err == nil {
					key = unesc
				}
				mu.Lock()
				objects[key] = body
				mu.Unlock()
				w.Header().Set("ETag", `"mock-etag"`)
				w.WriteHeader(http.StatusOK)

			case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
				body, _ := io.ReadAll(r.Body)
				s := string(body)
				var deleted []string
				for {
					start := strings.Index(s, "<Key>")
					if start == -1 {
						break
					}
					s = s[start+len("<Key>"):]
					end := strings.Index(s, "</Key>")
					if end == -1 {
						break
					}
					k := s[:end]
					s = s[end+len("</Key>"):]
					mu.Lock()
					delete(objects, k)
					mu.Unlock()
					deleted = append(deleted, k)
				}
				var b strings.Builder
				b.WriteString(`<DeleteResult>`)
				for _, k := range deleted {
					fmt.Fprintf(&b, `<Deleted><Key>%s</Key></Deleted>`, k)
				}
				b.WriteString(`</DeleteResult>`)
				w.Header().Set("Content-Type", "application/xml")
				_, _ = io.WriteString(w, b.String())

			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		t.Cleanup(srv.Close)
		return srv, &mu, objects
	}

	newClient := func(endpoint, p string, logger *slog.Logger) *ReplicaClient {
		c := NewReplicaClient()
		c.Bucket = "test-bucket"
		c.Path = p
		c.Region = "cn-hangzhou"
		c.Endpoint = endpoint
		c.AccessKeyID = "test-key"
		c.AccessKeySecret = "test-secret"
		if logger != nil {
			c.SetLogger(logger)
		}
		return c
	}

	t.Run("FreshEmptyBucketUsesCleanRootKeys", func(t *testing.T) {
		srv, mu, objects := newMockOSS(t, nil)
		c := newClient(srv.URL, "", nil)

		if _, err := c.WriteLTXFile(context.Background(), 0, 1, 1, bytes.NewReader(ltxData)); err != nil {
			t.Fatalf("WriteLTXFile() error = %v", err)
		}

		wantKey := "0000/0000000000000001-0000000000000001.ltx"
		mu.Lock()
		_, exists := objects[wantKey]
		mu.Unlock()
		if !exists {
			t.Fatalf("expected object %q, got keys %v", wantKey, objects)
		}
	})

	t.Run("ExplicitDotPathBypassesDiscovery", func(t *testing.T) {
		srv, mu, objects := newMockOSS(t, map[string][]byte{
			"./notes.txt": []byte("keep"),
		})
		c := newClient(srv.URL, ".", nil)

		ctx := context.Background()
		if _, err := c.WriteLTXFile(ctx, 0, 1, 1, bytes.NewReader(ltxData)); err != nil {
			t.Fatalf("WriteLTXFile() error = %v", err)
		}

		wantKey := "./0000/0000000000000001-0000000000000001.ltx"
		mu.Lock()
		_, exists := objects[wantKey]
		mu.Unlock()
		if !exists {
			t.Fatalf("expected explicit dot key %q, got keys %v", wantKey, objects)
		}

		if err := c.DeleteAll(ctx); err != nil {
			t.Fatalf("DeleteAll() error = %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if _, ok := objects[wantKey]; ok {
			t.Fatalf("expected %q to be deleted", wantKey)
		}
		if _, ok := objects["./notes.txt"]; !ok {
			t.Fatal("expected ./notes.txt to be preserved during DeleteAll")
		}
	})

	t.Run("LegacyDotLayoutPreservedOnUpgrade", func(t *testing.T) {
		srv, mu, objects := newMockOSS(t, map[string][]byte{
			"./0000/0000000000000001-0000000000000001.ltx": ltxData,
		})
		var logBuf bytes.Buffer
		c := newClient(srv.URL, "", slog.New(slog.NewTextHandler(&logBuf, nil)))

		if _, err := c.WriteLTXFile(context.Background(), 0, 2, 2, bytes.NewReader(ltxData)); err != nil {
			t.Fatalf("WriteLTXFile() error = %v", err)
		}

		wantKey := "./0000/0000000000000002-0000000000000002.ltx"
		mu.Lock()
		_, exists := objects[wantKey]
		mu.Unlock()
		if !exists {
			t.Fatalf("expected object %q, got keys %v", wantKey, objects)
		}
		if !strings.Contains(logBuf.String(), "legacy root replica prefix detected") {
			t.Fatalf("expected warning log, got %q", logBuf.String())
		}
	})

	t.Run("LegacySlashLayoutPreservedOnUpgrade", func(t *testing.T) {
		srv, mu, objects := newMockOSS(t, map[string][]byte{
			"/0000/0000000000000001-0000000000000001.ltx": ltxData,
		})
		var logBuf bytes.Buffer
		c := newClient(srv.URL, "", slog.New(slog.NewTextHandler(&logBuf, nil)))

		if _, err := c.WriteLTXFile(context.Background(), 0, 2, 2, bytes.NewReader(ltxData)); err != nil {
			t.Fatalf("WriteLTXFile() error = %v", err)
		}

		wantKey := "/0000/0000000000000002-0000000000000002.ltx"
		mu.Lock()
		_, exists := objects[wantKey]
		mu.Unlock()
		if !exists {
			t.Fatalf("expected object %q, got keys %v", wantKey, objects)
		}
		if !strings.Contains(logBuf.String(), "legacy root replica prefix detected") {
			t.Fatalf("expected warning log, got %q", logBuf.String())
		}
	})

	t.Run("AmbiguousLayoutsReturnError", func(t *testing.T) {
		srv, _, _ := newMockOSS(t, map[string][]byte{
			"0000/0000000000000001-0000000000000001.ltx":   ltxData,
			"./0000/0000000000000001-0000000000000001.ltx": ltxData,
		})
		c := newClient(srv.URL, "", nil)

		if _, err := c.LTXFiles(context.Background(), 0, 0, false); err == nil {
			t.Fatal("expected error when multiple root layouts exist")
		} else if !strings.Contains(err.Error(), "multiple root replica prefixes") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("DeleteAllAtCleanRootPreservesUnrelatedObjects", func(t *testing.T) {
		srv, mu, objects := newMockOSS(t, map[string][]byte{
			"0000/0000000000000001-0000000000000001.ltx": ltxData,
			"README.txt": []byte("keep"),
		})
		c := newClient(srv.URL, "", nil)

		if err := c.DeleteAll(context.Background()); err != nil {
			t.Fatalf("DeleteAll() error = %v", err)
		}

		mu.Lock()
		defer mu.Unlock()
		if _, ok := objects["0000/0000000000000001-0000000000000001.ltx"]; ok {
			t.Fatal("expected L0 LTX file to be deleted")
		}
		if _, ok := objects["README.txt"]; !ok {
			t.Fatal("expected README.txt to be preserved")
		}
	})

	t.Run("DeleteAllAtLegacyRootPreservesUnrelatedObjectsAndResetsCache", func(t *testing.T) {
		srv, mu, objects := newMockOSS(t, map[string][]byte{
			"/0009/0000000000000001-0000000000000001.ltx": ltxData,
			"/notes.txt": []byte("keep"),
			"/other-db/0000/0000000000000001-0000000000000001.ltx": ltxData,
		})
		c := newClient(srv.URL, "", nil)

		ctx := context.Background()
		if err := c.DeleteAll(ctx); err != nil {
			t.Fatalf("DeleteAll() error = %v", err)
		}

		mu.Lock()
		if _, ok := objects["/0009/0000000000000001-0000000000000001.ltx"]; ok {
			mu.Unlock()
			t.Fatal("expected legacy L9 LTX file to be deleted")
		}
		for _, keep := range []string{"/notes.txt", "/other-db/0000/0000000000000001-0000000000000001.ltx"} {
			if _, ok := objects[keep]; !ok {
				mu.Unlock()
				t.Fatalf("expected unrelated object %q to be preserved", keep)
			}
		}
		mu.Unlock()

		if _, err := c.WriteLTXFile(ctx, 0, 1, 1, bytes.NewReader(ltxData)); err != nil {
			t.Fatalf("WriteLTXFile() after DeleteAll error = %v", err)
		}
		wantKey := "0000/0000000000000001-0000000000000001.ltx"
		mu.Lock()
		_, exists := objects[wantKey]
		mu.Unlock()
		if !exists {
			t.Fatalf("expected clean-root object %q after DeleteAll, got keys %v", wantKey, objects)
		}
	})

	t.Run("LTXFilesIgnoresNestedSubdirectoryLTX", func(t *testing.T) {
		srv, _, _ := newMockOSS(t, map[string][]byte{
			"0000/0000000000000001-0000000000000001.ltx":              ltxData,
			"0000/archive/0000/0000000000000002-0000000000000002.ltx": ltxData,
		})
		c := newClient(srv.URL, "", nil)

		itr, err := c.LTXFiles(context.Background(), 0, 0, false)
		if err != nil {
			t.Fatalf("LTXFiles() error = %v", err)
		}
		defer itr.Close()

		var got []ltx.TXID
		for itr.Next() {
			got = append(got, itr.Item().MinTXID)
		}
		if err := itr.Err(); err != nil {
			t.Fatalf("iterator error = %v", err)
		}
		if len(got) != 1 || got[0] != 1 {
			t.Fatalf("got TXIDs %v, want [1]", got)
		}
	})
}
