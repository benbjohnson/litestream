//go:build vfs

package s3

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	// Link SQLite symbols for sqlite3vfs, as in ../vfs_link_test.go.
	_ "github.com/mattn/go-sqlite3"
	"github.com/superfly/ltx"

	litestream "github.com/benbjohnson/litestream"
)

func TestReplicaClient_VFSPoll(t *testing.T) {
	for _, shuffled := range []bool{false, true} {
		t.Run(fmt.Sprintf("Shuffled=%v", shuffled), func(t *testing.T) {
			objects := make(map[ltx.TXID]listingTestObject)
			data := make(map[string][]byte)
			for txid := ltx.TXID(1); txid <= 3; txid++ {
				var buf bytes.Buffer
				enc, err := ltx.NewEncoder(&buf)
				if err != nil {
					t.Fatal(err)
				}
				if err := enc.EncodeHeader(ltx.Header{
					Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: 4096,
					Commit: 1, MinTXID: txid, MaxTXID: txid,
				}); err != nil {
					t.Fatal(err)
				}
				if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, bytes.Repeat([]byte{byte(txid)}, 4096)); err != nil {
					t.Fatal(err)
				}
				if err := enc.Close(); err != nil {
					t.Fatal(err)
				}
				obj := newListingTestObject(0, txid, txid)
				obj.Size = int64(buf.Len())
				objects[txid], data[obj.Key] = obj, buf.Bytes()
			}
			var updated atomic.Bool
			c := newListingTestClient(t, "test-bucket", "https://objects.example.test", func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("list-type") == "2" {
					if r.URL.Query().Get("prefix") != "replica/0000/" {
						writeListingTestResponse(t, w, nil, "")
					} else if updated.Load() {
						updates := []listingTestObject{objects[1], objects[2], objects[3]}
						if shuffled {
							updates[1], updates[2] = updates[2], updates[1]
						}
						writeListingTestResponse(t, w, updates, "")
					} else {
						writeListingTestResponse(t, w, []listingTestObject{objects[1]}, "")
					}
					return
				}
				key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
				body, ok := data[key]
				if !ok {
					t.Errorf("unexpected object request: %s", r.URL)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				http.ServeContent(w, r, key, time.Time{}, bytes.NewReader(body))
			})
			f := litestream.NewVFSFile(c, "listing-test.db", c.logger)
			f.PollInterval = 10 * time.Millisecond
			t.Cleanup(func() {
				if err := f.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := f.Open(); err != nil {
				t.Fatal(err)
			}
			if got := f.Pos().TXID; got != 1 {
				t.Fatalf("startup TXID = %s, want 1", got)
			}
			// Warm the page cache so polling must also invalidate the old page.
			page := make([]byte, 4096)
			if _, err := f.ReadAt(page, 0); err != nil || page[100] != 1 {
				t.Fatalf("initial page: marker=%d err=%v", page[100], err)
			}
			updated.Store(true)
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			for f.Pos().TXID != 3 {
				select {
				case <-ticker.C:
				case <-timer.C:
					t.Fatalf("polling did not reach TXID 3, have %s", f.Pos().TXID)
				}
			}
			if _, err := f.ReadAt(page, 0); err != nil || page[100] != 3 {
				t.Fatalf("updated page: marker=%d err=%v", page[100], err)
			}
		})
	}
}
