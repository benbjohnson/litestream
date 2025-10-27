package gs

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/superfly/ltx"
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
