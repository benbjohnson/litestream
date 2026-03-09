//go:build profile

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/felixge/fgprof"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"

	_ "modernc.org/sqlite"
)

// TestIdleCPUProfile starts N databases with file-based replicas and no writes,
// then exposes pprof and fgprof endpoints for interactive profiling.
//
// This test is designed for manual CPU profiling to understand idle overhead
// when running many Litestream instances on a single machine.
//
// Usage:
//
//	# Start with 100 idle databases on default port:
//	PROFILE_DB_COUNT=100 go test -tags=profile -run TestIdleCPUProfile -timeout=0 -v ./tests/integration/
//
//	# Custom listen address:
//	PROFILE_ADDR=:9090 PROFILE_DB_COUNT=50 go test -tags=profile -run TestIdleCPUProfile -timeout=0 -v ./tests/integration/
//
//	# Then in another terminal:
//	go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
//	go tool pprof http://localhost:6060/debug/fgprof?seconds=30
//	go tool pprof http://localhost:6060/debug/pprof/goroutine
//	go tool pprof http://localhost:6060/debug/pprof/heap
func TestIdleCPUProfile(t *testing.T) {
	dbCount := 10
	if s := os.Getenv("PROFILE_DB_COUNT"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("invalid PROFILE_DB_COUNT: %v", err)
		}
		dbCount = n
	}

	addr := ":6060"
	if s := os.Getenv("PROFILE_ADDR"); s != "" {
		addr = s
	}

	// Register fgprof handler alongside net/http/pprof (registered via blank import).
	http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())

	// Start HTTP server for profiling.
	go func() {
		log.Printf("pprof server listening on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("pprof server error: %v", err)
		}
	}()

	// Create temporary root directory for all databases.
	rootDir := t.TempDir()

	// Start N databases with monitoring enabled (the idle hot path).
	type instance struct {
		db    *litestream.DB
		sqldb *sql.DB
	}
	instances := make([]instance, 0, dbCount)

	for i := range dbCount {
		dbPath := filepath.Join(rootDir, fmt.Sprintf("db%d", i), "db")
		replicaDir := filepath.Join(rootDir, fmt.Sprintf("db%d", i), "replica")

		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		// Create database with WAL mode and seed data.
		sqldb, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatalf("open sql db %d: %v", i, err)
		}
		if _, err := sqldb.Exec(`PRAGMA journal_mode = wal`); err != nil {
			t.Fatalf("set wal mode db %d: %v", i, err)
		}
		if _, err := sqldb.Exec(`CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
			t.Fatalf("create table db %d: %v", i, err)
		}
		if _, err := sqldb.Exec(`INSERT INTO data (value) VALUES ('seed')`); err != nil {
			t.Fatalf("insert seed db %d: %v", i, err)
		}

		// Configure Litestream DB with monitoring enabled.
		db := litestream.NewDB(dbPath)
		db.Replica = litestream.NewReplica(db)
		db.Replica.Client = file.NewReplicaClient(replicaDir)

		if err := db.Open(); err != nil {
			t.Fatalf("open litestream db %d: %v", i, err)
		}

		// Do an initial sync so there's a valid LTX baseline.
		if err := db.Sync(context.Background()); err != nil {
			t.Fatalf("initial sync db %d: %v", i, err)
		}

		instances = append(instances, instance{db: db, sqldb: sqldb})
	}

	t.Logf("started %d idle databases with monitoring (interval=%s)", dbCount, litestream.DefaultMonitorInterval)
	t.Logf("")
	t.Logf("profiling endpoints:")
	t.Logf("  CPU (on-cpu):     go tool pprof http://localhost%s/debug/pprof/profile?seconds=30", addr)
	t.Logf("  CPU (on+off-cpu): go tool pprof http://localhost%s/debug/fgprof?seconds=30", addr)
	t.Logf("  goroutines:       go tool pprof http://localhost%s/debug/pprof/goroutine", addr)
	t.Logf("  heap:             go tool pprof http://localhost%s/debug/pprof/heap", addr)
	t.Logf("")
	t.Logf("press Ctrl+C to stop")

	// Block until interrupted.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()

	t.Logf("shutting down %d databases...", dbCount)
	for i, inst := range instances {
		if err := inst.db.Close(context.Background()); err != nil {
			t.Logf("close litestream db %d: %v", i, err)
		}
		if err := inst.sqldb.Close(); err != nil {
			t.Logf("close sql db %d: %v", i, err)
		}
	}
}
