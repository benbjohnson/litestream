package litestream_test

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"modernc.org/sqlite"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

// TestDB_WriterKilledBeforeWALIndexUpdate simulates a writer killed with
// SIGKILL after it wrote and synced a commit's WAL frames but before it
// updated the wal-index header in the -shm file. SQLite treats those frames
// as uncommitted: Litestream's open connection keeps the wal-index valid, so
// no WAL recovery runs and the next writer reuses those frame slots.
// Putting back the wal-index header from before the commit leaves the same
// state. Litestream must not replicate the killed writer's transaction.
func TestDB_WriterKilledBeforeWALIndexUpdate(t *testing.T) {
	if args := os.Args; len(args) >= 5 && args[len(args)-4] == "--" {
		action, shmPath, headerPath := args[len(args)-3], args[len(args)-2], args[len(args)-1]
		shm, err := os.OpenFile(shmPath, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := shm.Close(); err != nil {
				t.Error(err)
			}
		}()
		switch action {
		case "save":
			hdr := make([]byte, 96)
			if _, err := shm.ReadAt(hdr, 0); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(headerPath, hdr, 0600); err != nil {
				t.Fatal(err)
			}
		case "restore":
			hdr, err := os.ReadFile(headerPath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := shm.WriteAt(hdr, 0); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unknown SHM action %q", action)
		}
		return
	}
	testDBWriterKilledBeforeWALIndexUpdate(t, "")
}

func TestDB_WriterKilledBeforeWALIndexUpdate_UnusableHeader(t *testing.T) {
	for _, name := range []string{"copies differ", "uninitialized", "invalid checksum"} {
		t.Run(name, func(t *testing.T) {
			testDBWriterKilledBeforeWALIndexUpdate(t, name)
		})
	}
}

func testDBWriterKilledBeforeWALIndexUpdate(t *testing.T, unusableHeader string) {
	t.Helper()
	ctx := t.Context()
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatal(err)
	} else if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (v) VALUES ('committed')`); err != nil {
		t.Fatal(err)
	} else if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	headerPath := filepath.Join(t.TempDir(), "header")
	for _, action := range []string{"save", "restore"} {
		if action == "restore" {
			if _, err := sqldb.ExecContext(ctx, `INSERT INTO t (v) VALUES ('killed')`); err != nil {
				t.Fatal(err)
			}
		}
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDB_WriterKilledBeforeWALIndexUpdate$", "--", action, db.Path()+"-shm", headerPath)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("SHM %s: %v\n%s", action, err, output)
		}
	}

	var n int
	if err := sqldb.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("database has %d rows after the simulated kill, want 1", n)
	}

	want := 1
	if unusableHeader != "" {
		hdr, err := os.ReadFile(headerPath)
		if err != nil {
			t.Fatal(err)
		}
		switch unusableHeader {
		case "invalid checksum":
			binary.NativeEndian.PutUint32(hdr[16:], binary.NativeEndian.Uint32(hdr[16:])+1)
			copy(hdr[48:], hdr[:48])
		case "copies differ":
			hdr[56] ^= 1
		case "uninitialized":
			hdr[12], hdr[60] = 0, 0
		}
		if err := os.WriteFile(headerPath, hdr, 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDB_WriterKilledBeforeWALIndexUpdate$", "--", "restore", db.Path()+"-shm", headerPath)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("SHM restore: %v\n%s", err, output)
		}
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		want = 2
		if err := sqldb.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
			t.Fatal(err)
		} else if n != want {
			t.Fatalf("SQLite recovered %d rows, want %d", n, want)
		}
	}

	// Litestream's next checkpoint copies the WAL, checkpoints and restarts it.
	if err := db.Checkpoint(ctx, litestream.CheckpointModePassive); err != nil {
		t.Fatal(err)
	} else if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	restorePath := filepath.Join(t.TempDir(), "restored.db")
	if err := db.Replica.Restore(ctx, litestream.RestoreOptions{OutputPath: restorePath}); err != nil {
		t.Fatal(err)
	}
	restored := testingutil.MustOpenSQLDB(t, restorePath)
	defer testingutil.MustCloseSQLDB(t, restored)
	if err := restored.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	} else if n != want {
		t.Fatalf("restored %d rows, want %d to match SQLite", n, want)
	}
}

func TestDB_ClosePreservesWALLocks(t *testing.T) {
	if args := os.Args; len(args) >= 3 && args[len(args)-2] == "--" {
		sqldb, err := sql.Open("sqlite", args[len(args)-1]+"?_pragma=busy_timeout(25)")
		if err != nil {
			t.Fatal(err)
		}
		defer testingutil.MustCloseSQLDB(t, sqldb)
		if _, err := sqldb.Exec(`INSERT INTO t (id) VALUES (200)`); err != nil {
			var sqliteErr *sqlite.Error
			if errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == 5 {
				t.Log("writer-busy")
				return
			}
			t.Fatal(err)
		}
		t.Log("writer-committed")
		return
	}

	for _, canceled := range []bool{false, true} {
		name := "normal"
		if canceled {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			db, sqldb := testingutil.MustOpenDBs(t)
			defer testingutil.MustCloseDBs(t, db, sqldb)
			if _, err := sqldb.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY)`); err != nil {
				t.Fatal(err)
			}
			if err := db.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			tx, err := sqldb.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
					t.Error(err)
				}
			}()
			if _, err := tx.ExecContext(ctx, `INSERT INTO t (id) VALUES (100)`); err != nil {
				t.Fatal(err)
			}

			var committedBeforeRelease bool
			for attempt := range 3 {
				if attempt == 1 {
					closeCtx, cancel := context.WithCancel(ctx)
					if canceled {
						cancel()
					}
					err := db.Close(closeCtx)
					cancel()
					if err != nil && !(canceled && errors.Is(err, context.Canceled)) {
						t.Fatal(err)
					}
				} else if attempt == 2 {
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
					if committedBeforeRelease {
						break
					}
				}

				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDB_ClosePreservesWALLocks$", "-test.v", "--", db.Path())
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("writer: %v\n%s", err, output)
				}
				committed := strings.Contains(string(output), "writer-committed")
				busy := strings.Contains(string(output), "writer-busy")
				if committed == busy {
					t.Fatalf("unexpected writer result: %s", output)
				}
				switch attempt {
				case 0:
					if !busy {
						t.Fatal("external writer acquired the lock before Close")
					}
				case 1:
					committedBeforeRelease = committed
				case 2:
					if !committed {
						t.Fatal("external writer could not commit after the first writer committed")
					}
				}
			}
			var n int
			if err := sqldb.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
				t.Fatal(err)
			} else if n != 2 {
				t.Fatalf("both writers acknowledged commits but only %d rows remain", n)
			}
			if committedBeforeRelease {
				t.Fatal("external writer committed while the first writer still held its transaction")
			}
		})
	}
}
