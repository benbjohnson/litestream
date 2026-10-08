package gs

import (
	"bytes"
	"context"
	"io"
	"slices"
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
