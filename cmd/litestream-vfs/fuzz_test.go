//go:build vfs
// +build vfs

package main_test

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// TestVFS_FuzzSeedCorpus runs a handful of fixed corpora so `go test`
// exercises the same logic as the fuzz harness without requiring
// `-fuzz=...`.
func TestVFS_FuzzSeedCorpus(t *testing.T) {
	seeds := [][]byte{
		[]byte{0x00, 0x01, 0x02},
		[]byte("litestream vfs fuzz"),
		[]byte{0xFF, 0x10, 0x42, 0x7F},
	}
	for _, seed := range seeds {
		runVFSFuzzWorkload(t, seed)
	}
}

// FuzzVFSReplicaReadPatterns exercises random combinations of reads,
// aggregates, and ordering queries against the VFS replica. Enable with:
//
//	go test ./cmd/litestream-vfs -tags vfs -fuzz=FuzzVFSReplicaReadPatterns
func FuzzVFSReplicaReadPatterns(f *testing.F) {
	f.Add([]byte("seed"))
	f.Add([]byte{0x1, 0x2, 0x3, 0x4})
	f.Add([]byte{0xAA, 0xBB, 0xCC})

	f.Fuzz(func(t *testing.T, data []byte) {
		runVFSFuzzWorkload(t, data)
	})
}

func runVFSFuzzWorkload(tb testing.TB, corpus []byte) {
	tb.Helper()
	if len(corpus) == 0 {
		corpus = []byte{0}
	}
	if len(corpus) > 256 {
		corpus = corpus[:256]
	}

	client := file.NewReplicaClient(tb.TempDir())
	if err := os.MkdirAll(client.LTXLevelDir(0), 0o755); err != nil {
		tb.Fatalf("init replica dir: %v", err)
	}
	_, primary := openReplicatedPrimary(tb, client, 15*time.Millisecond, 15*time.Millisecond)
	defer testingutil.MustCloseSQLDB(tb, primary)

	if _, err := primary.Exec(`CREATE TABLE fuzz (
        id INTEGER PRIMARY KEY,
        value TEXT,
        grp INTEGER
    )`); err != nil {
		tb.Fatalf("create table: %v", err)
	}

	// Deterministic seed data so we have plenty of rows/pages to hydrate.
	for i := 0; i < 128; i++ {
		payload := fmt.Sprintf("row-%03d-%s", i, strings.Repeat("x", (i%17)+8))
		if _, err := primary.Exec("INSERT INTO fuzz (value, grp) VALUES (?, ?)", payload, i%11); err != nil {
			tb.Fatalf("seed insert: %v", err)
		}
	}

	vfs := newVFS(tb, client)
	vfs.PollInterval = 15 * time.Millisecond
	vfsName := registerTestVFS(tb, vfs)
	replica := openVFSReplicaDB(tb, vfsName)
	defer replica.Close()

	require.Eventually(tb, func() bool {
		var primaryCount, replicaCount int
		if err := primary.QueryRow("SELECT COUNT(*) FROM fuzz").Scan(&primaryCount); err != nil {
			return false
		}
		if err := replica.QueryRow("SELECT COUNT(*) FROM fuzz").Scan(&replicaCount); err != nil {
			return false
		}
		return primaryCount == replicaCount
	}, 5*time.Second, 20*time.Millisecond, "replica should catch up to primary")

	const maxOps = 128
	for i := 0; i < len(corpus) && i < maxOps; i++ {
		op := corpus[i] % 6
		switch op {
		case 0:
			id := int(corpus[i])%128 + 1
			var value string
			err := replica.QueryRow("SELECT value FROM fuzz WHERE id = ?", id).Scan(&value)
			if err != nil && err != sql.ErrNoRows {
				tb.Fatalf("select by id: %v", err)
			}
		case 1:
			var count int
			if err := replica.QueryRow("SELECT COUNT(*) FROM fuzz WHERE grp = ?", int(corpus[i])%11).Scan(&count); err != nil {
				tb.Fatalf("count grp: %v", err)
			}
		case 2:
			rows, err := replica.Query("SELECT value FROM fuzz ORDER BY value DESC LIMIT 5 OFFSET ?", int(corpus[i])%10)
			if err != nil {
				tb.Fatalf("ordered scan: %v", err)
			}
			for rows.Next() {
				var v string
				if err := rows.Scan(&v); err != nil {
					tb.Fatalf("scan ordered: %v", err)
				}
			}
			if err := rows.Err(); err != nil {
				tb.Fatalf("ordered rows err: %v", err)
			}
			rows.Close()
		case 3:
			var sum int
			if err := replica.QueryRow("SELECT SUM(LENGTH(value)) FROM fuzz WHERE id BETWEEN ? AND ?",
				int(corpus[i])%64+1, int(corpus[i])%64+64).Scan(&sum); err != nil {
				tb.Fatalf("sum lengths: %v", err)
			}
		case 4:
			// Cross-check counts between primary & replica for a random grp.
			grp := int(corpus[i]) % 11
			var pc, rc int
			if err := primary.QueryRow("SELECT COUNT(*) FROM fuzz WHERE grp = ?", grp).Scan(&pc); err != nil {
				tb.Fatalf("primary grp count: %v", err)
			}
			if err := replica.QueryRow("SELECT COUNT(*) FROM fuzz WHERE grp = ?", grp).Scan(&rc); err != nil {
				tb.Fatalf("replica grp count: %v", err)
			}
			if pc != rc {
				tb.Fatalf("count mismatch grp=%d primary=%d replica=%d", grp, pc, rc)
			}
		case 5:
			// Random LIKE query to exercise page cache churn.
			pattern := fmt.Sprintf("row-%%%02x%%", corpus[i])
			rows, err := replica.Query("SELECT id FROM fuzz WHERE value LIKE ? LIMIT 3", pattern)
			if err != nil {
				tb.Fatalf("like query: %v", err)
			}
			rows.Close()
		}
	}
}
