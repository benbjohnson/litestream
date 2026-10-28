package gs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
)

func ltxTestData(tb testing.TB, minTXID, maxTXID ltx.TXID, payload []byte) []byte {
	tb.Helper()

	hdr := ltx.Header{
		Version:   1,
		PageSize:  4096,
		Commit:    1,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Timestamp: time.Now().UnixMilli(),
	}

	buf, err := hdr.MarshalBinary()
	if err != nil {
		tb.Fatalf("marshal header: %v", err)
	}

	return append(buf, payload...)
}

func setupTestClient(tb testing.TB) (*ReplicaClient, *fakestorage.Server) {
	tb.Helper()

	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true})
	if err != nil {
		tb.Fatalf("new server: %v", err)
	}

	bucket := "litestream-test"
	server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucket})

	client := server.Client()

	rc := NewReplicaClient()
	rc.client = client
	rc.bkt = client.Bucket(bucket)
	rc.Bucket = bucket
	rc.Path = "integration"

	return rc, server
}

func TestReplicaClient_OpenLTXFileReadsFullObject(t *testing.T) {
	rc, server := setupTestClient(t)
	defer server.Stop()

	ctx := context.Background()
	data := ltxTestData(t, ltx.TXID(1), ltx.TXID(1), []byte("hello"))

	if _, err := rc.WriteLTXFile(ctx, 0, ltx.TXID(1), ltx.TXID(1), bytes.NewReader(data)); err != nil {
		t.Fatalf("WriteLTXFile: %v", err)
	}

	r, err := rc.OpenLTXFile(ctx, 0, ltx.TXID(1), ltx.TXID(1), 0, 0)
	if err != nil {
		t.Fatalf("OpenLTXFile: %v", err)
	}
	defer r.Close()

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if !bytes.Equal(out, data) {
		t.Fatalf("unexpected replica content: got %q, want %q", out, data)
	}
}

func TestReplicaClient_LTXFilesSeek(t *testing.T) {
	rc, server := setupTestClient(t)
	defer server.Stop()

	ctx := context.Background()
	for _, txID := range []ltx.TXID{1, 3, 5} {
		data := ltxTestData(t, txID, txID, nil)
		if _, err := rc.WriteLTXFile(ctx, 0, txID, txID, bytes.NewReader(data)); err != nil {
			t.Fatalf("WriteLTXFile(%s): %v", txID, err)
		}
	}

	for _, tt := range []struct {
		name string
		seek ltx.TXID
		want []ltx.TXID
	}{
		{name: "exact", seek: 3, want: []ltx.TXID{3, 5}},
		{name: "next available", seek: 2, want: []ltx.TXID{3, 5}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			itr, err := rc.LTXFiles(ctx, 0, tt.seek, false)
			if err != nil {
				t.Fatalf("LTXFiles: %v", err)
			}

			var got []ltx.TXID
			for itr.Next() {
				got = append(got, itr.Item().MinTXID)
			}
			if err := itr.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("MinTXIDs=%v, want %v", got, tt.want)
			}
		})
	}
}

var errTestSource = errors.New("injected source failure")

type failAfterReader struct {
	r         *bytes.Reader
	remaining int
}

func (r *failAfterReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, errTestSource
	}

	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.r.Read(p)
	r.remaining -= n
	if err != nil {
		return n, errTestSource
	}
	return n, nil
}

func TestReplicaClient_WriteLTXFileAbortsOnSourceError(t *testing.T) {
	rc, server := setupTestClient(t)
	defer server.Stop()

	ctx := context.Background()
	minTXID, maxTXID := ltx.TXID(1), ltx.TXID(1)
	data := ltxTestData(t, minTXID, maxTXID, bytes.Repeat([]byte("payload"), 1024))
	rd := &failAfterReader{r: bytes.NewReader(data), remaining: len(data) - 1}

	if _, err := rc.WriteLTXFile(ctx, 0, minTXID, maxTXID, rd); !errors.Is(err, errTestSource) {
		t.Fatalf("WriteLTXFile error = %v, want %v", err, errTestSource)
	}

	key := litestream.LTXFilePath(rc.Path, 0, minTXID, maxTXID)
	if _, err := rc.bkt.Object(key).Attrs(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("object attrs error = %v, want %v", err, storage.ErrObjectNotExist)
	}
}

func TestReplicaClient_InitContextCanceled(t *testing.T) {
	for _, disableNewAuth := range []string{"true", "false"} {
		t.Run("disable_new_auth="+disableNewAuth, func(t *testing.T) {
			var exchanges, requests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/token" {
					if err := r.ParseForm(); err != nil {
						t.Errorf("parse token request: %v", err)
						http.Error(w, "invalid form", http.StatusBadRequest)
						return
					}
					if got := r.Form.Get("subject_token"); got != "test-subject-token" {
						t.Errorf("subject token = %q", got)
					}
					exchanges.Add(1)
					if _, err := io.WriteString(w, `{"access_token":"test-access-token","token_type":"Bearer","expires_in":3600}`); err != nil {
						t.Errorf("write token response: %v", err)
					}
					return
				}
				if r.URL.Path == "/storage/v1/b/test-bucket/o/test-object" && r.Method == http.MethodDelete {
					if got := r.Header.Get("Authorization"); got != "Bearer test-access-token" {
						t.Errorf("delete authorization = %q", got)
					}
					requests.Add(1)
					w.WriteHeader(http.StatusNoContent)
					return
				}
				if r.URL.Path != "/storage/v1/b/test-bucket/o" || r.Method != http.MethodGet {
					t.Errorf("unexpected storage request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
					return
				}
				if got := r.Header.Get("Authorization"); got != "Bearer test-access-token" {
					t.Errorf("authorization = %q", got)
				}
				requests.Add(1)
				if _, err := io.WriteString(w, `{"items":[{"name":"test-object"}]}`); err != nil {
					t.Errorf("write storage response: %v", err)
				}
			}))
			t.Cleanup(server.Close)

			transport := server.Client().Transport.(*http.Transport).Clone()
			transport.TLSClientConfig.ServerName = "example.com"
			transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}
			originalTransport := http.DefaultTransport
			http.DefaultTransport = transport
			t.Cleanup(func() {
				http.DefaultTransport = originalTransport
				transport.CloseIdleConnections()
			})

			tokenPath := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenPath, []byte("test-subject-token"), 0600); err != nil {
				t.Fatal(err)
			}
			credentials, err := json.Marshal(map[string]any{
				"type":               "external_account",
				"audience":           "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test/providers/test",
				"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
				"token_url":          server.URL + "/token",
				"credential_source":  map[string]string{"file": tokenPath},
			})
			if err != nil {
				t.Fatal(err)
			}
			credentialsPath := filepath.Join(t.TempDir(), "credentials.json")
			if err := os.WriteFile(credentialsPath, credentials, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentialsPath)
			t.Setenv("STORAGE_EMULATOR_HOST", "")
			t.Setenv("GOOGLE_API_GO_EXPERIMENTAL_DISABLE_NEW_AUTH_LIB", disableNewAuth)

			rc := NewReplicaClient()
			rc.Bucket = "test-bucket"
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := rc.Init(ctx); err != nil {
				t.Fatalf("Init: %v", err)
			}
			t.Cleanup(func() {
				if err := rc.client.Close(); err != nil {
					t.Error(err)
				}
			})
			cancel()

			operationCtx, operationCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer operationCancel()
			if err := rc.DeleteAll(operationCtx); err != nil {
				t.Fatalf("DeleteAll after Init context cancellation: %v", err)
			}
			if err := rc.DeleteAll(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("DeleteAll with canceled operation context: %v", err)
			}
			if exchanges.Load() != 1 || requests.Load() != 2 {
				t.Fatalf("token exchanges = %d, storage requests = %d", exchanges.Load(), requests.Load())
			}
		})
	}
}
