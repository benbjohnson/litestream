//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// DirWatchTestDB extends TestDB with directory-specific functionality
type DirWatchTestDB struct {
	*TestDB
	DirPath     string
	Pattern     string
	Recursive   bool
	Watch       bool
	ReplicaPath string
}

// SetupDirectoryWatchTest creates a test environment for directory watching
func SetupDirectoryWatchTest(t *testing.T, name string, pattern string, recursive bool) *DirWatchTestDB {
	t.Helper()

	baseDB := SetupTestDB(t, name)
	dirPath := filepath.Join(baseDB.TempDir, "databases")
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		t.Fatalf("create databases directory: %v", err)
	}

	replicaPath := filepath.Join(baseDB.TempDir, "replica")

	return &DirWatchTestDB{
		TestDB:      baseDB,
		DirPath:     dirPath,
		Pattern:     pattern,
		Recursive:   recursive,
		Watch:       true,
		ReplicaPath: replicaPath,
	}
}

// CreateDirectoryWatchConfig generates YAML config for directory watching
func (db *DirWatchTestDB) CreateDirectoryWatchConfig() (string, error) {
	configPath := filepath.Join(db.TempDir, "litestream.yml")
	config := fmt.Sprintf(`dbs:
  - dir: %s
    pattern: %q
    recursive: %t
    watch: %t
    replica:
      type: file
      path: %s
`,
		filepath.ToSlash(db.DirPath),
		db.Pattern,
		db.Recursive,
		db.Watch,
		filepath.ToSlash(db.ReplicaPath),
	)

	if err := os.WriteFile(configPath, []byte(config), 0644); err != nil {
		return "", fmt.Errorf("write config: %w", err)
	}

	db.ConfigPath = configPath
	return configPath, nil
}

// CreateDatabaseInDir creates a SQLite database with optional subdirectory
func CreateDatabaseInDir(t *testing.T, dirPath, subDir, name string) string {
	t.Helper()

	dbDir := dirPath
	if subDir != "" {
		dbDir = filepath.Join(dirPath, subDir)
		if err := os.MkdirAll(dbDir, 0755); err != nil {
			t.Fatalf("create subdirectory %s: %v", subDir, err)
		}
		// Give directory monitor time to register watch on new subdirectory
		// to avoid race where database is created before watch is active
		time.Sleep(500 * time.Millisecond)
	}

	dbPath := filepath.Join(dbDir, name)
	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open database %s: %v", dbPath, err)
	}
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatalf("set WAL mode for %s: %v", dbPath, err)
	}

	// Create a simple table to make it a real database
	if _, err := sqlDB.Exec("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, data TEXT)"); err != nil {
		t.Fatalf("create table in %s: %v", dbPath, err)
	}

	return dbPath
}

// CreateDatabaseWithData creates a database with specified number of rows
func CreateDatabaseWithData(t *testing.T, dbPath string, rowCount int) error {
	t.Helper()

	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer sqlDB.Close()

	if _, err := sqlDB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("set WAL mode: %w", err)
	}

	if _, err := sqlDB.Exec("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, data TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)"); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// Insert data in batches
	tx, err := sqlDB.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	stmt, err := tx.Prepare("INSERT INTO test (data) VALUES (?)")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare statement: %w", err)
	}

	for i := 0; i < rowCount; i++ {
		if _, err := stmt.Exec(fmt.Sprintf("test data %d", i)); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert row %d: %w", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// CreateFakeDatabase creates a file that looks like a database but isn't
func CreateFakeDatabase(t *testing.T, dirPath, name string, content []byte) string {
	t.Helper()

	dbPath := filepath.Join(dirPath, name)
	if err := os.WriteFile(dbPath, content, 0644); err != nil {
		t.Fatalf("write fake database %s: %v", dbPath, err)
	}

	return dbPath
}

// WaitForDatabaseInReplica polls until database appears in replica
// For directory watching, dbPath can be the full path or just the database name
func WaitForDatabaseInReplica(t *testing.T, replicaPath, dbPath string, timeout time.Duration) error {
	t.Helper()

	// Replica structure: <replica_path>/<db_name>/ltx/0/*.ltx
	dbName := filepath.Base(dbPath)

	// Try to find the database in the replica directory
	// It could be at the root level or nested in subdirectories
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// Walk the replica directory to find the database
		found := false
		filepath.Walk(replicaPath, func(path string, info os.FileInfo, err error) error {
			if err != nil || found {
				return nil
			}

			// Check if this directory matches the database name and has LTX files
			if info.IsDir() && filepath.Base(path) == dbName {
				ltxDir := filepath.Join(path, "ltx", "0")
				if _, err := os.Stat(ltxDir); err == nil {
					entries, err := os.ReadDir(ltxDir)
					if err == nil {
						for _, entry := range entries {
							if strings.HasSuffix(entry.Name(), ".ltx") {
								relPath, _ := filepath.Rel(replicaPath, path)
								t.Logf("Database %s detected in replica at %s (found %s)", dbName, relPath, entry.Name())
								found = true
								return nil
							}
						}
					}
				}
			}
			return nil
		})

		if found {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("database %s not found in replica after %v", dbName, timeout)
}

// VerifyDatabaseRemoved checks database no longer in replica (no new writes)
func VerifyDatabaseRemoved(t *testing.T, replicaPath, dbPath string, timeout time.Duration) error {
	t.Helper()

	// Replica structure: <replica_path>/<db_name>/ltx/0/*.ltx
	dbName := filepath.Base(dbPath)
	ltxDir := filepath.Join(replicaPath, dbName, "ltx", "0")

	// Count initial LTX files (using existing countLTXFiles helper)
	initialCount := countLTXFiles(ltxDir)

	t.Logf("Initial LTX count for %s: %d", dbName, initialCount)

	// Wait and verify no new files are created
	time.Sleep(timeout)

	finalCount := countLTXFiles(ltxDir)

	if finalCount > initialCount {
		return fmt.Errorf("database %s still being replicated (%d -> %d LTX files)", dbName, initialCount, finalCount)
	}

	t.Logf("Database %s stopped replicating", dbName)
	return nil
}

// CountDatabasesInReplica counts distinct databases being replicated
func CountDatabasesInReplica(replicaPath string) (int, error) {
	if _, err := os.Stat(replicaPath); os.IsNotExist(err) {
		return 0, nil
	}

	entries, err := os.ReadDir(replicaPath)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// Check if this database directory has LTX files
		ltxDir := filepath.Join(replicaPath, entry.Name(), "ltx", "0")
		if countLTXFiles(ltxDir) > 0 {
			count++
		}
	}

	return count, nil
}

// StartContinuousWrites launches goroutine writing to database at specified rate
func StartContinuousWrites(ctx context.Context, t *testing.T, dbPath string, writesPerSec int) (*sync.WaitGroup, context.CancelFunc, error) {
	t.Helper()

	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open database: %w", err)
	}

	if _, err := sqlDB.Exec("CREATE TABLE IF NOT EXISTS load_test (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP)"); err != nil {
		sqlDB.Close()
		return nil, nil, fmt.Errorf("create table: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer sqlDB.Close()

		ticker := time.NewTicker(time.Second / time.Duration(writesPerSec))
		defer ticker.Stop()

		counter := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				counter++
				if _, err := sqlDB.Exec("INSERT INTO load_test (data) VALUES (?)", fmt.Sprintf("data-%d", counter)); err != nil {
					if !strings.Contains(err.Error(), "database is locked") {
						t.Logf("Write error in %s: %v", filepath.Base(dbPath), err)
					}
				}
			}
		}
	}()

	return wg, cancel, nil
}

// CreateMultipleDatabasesConcurrently creates databases using goroutines
func CreateMultipleDatabasesConcurrently(t *testing.T, dirPath string, count int, pattern string) []string {
	t.Helper()

	var wg sync.WaitGroup
	var mu sync.Mutex
	paths := make([]string, 0, count)

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			name := fmt.Sprintf("db-%03d%s", idx, filepath.Ext(pattern))
			dbPath := CreateDatabaseInDir(t, dirPath, "", name)

			mu.Lock()
			paths = append(paths, dbPath)
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	return paths
}

// GetRowCount returns the number of rows in a test table
func GetRowCount(dbPath, tableName string) (int, error) {
	sqlDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return 0, fmt.Errorf("open database: %w", err)
	}
	defer sqlDB.Close()

	var count int
	err = sqlDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query count: %w", err)
	}

	return count, nil
}

// Helper functions

func getRelativeDBPath(dbPath, replicaBase string) (string, error) {
	// Extract just the database name (not the full path)
	// The replica structure mirrors the source directory structure
	return filepath.Base(dbPath), nil
}

func hasLTXFiles(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".ltx") {
			return true, nil
		}
	}
	return false, nil
}

// CheckForCriticalErrors returns errors from the log, filtering out known benign errors
func CheckForCriticalErrors(t *testing.T, db *TestDB) ([]string, error) {
	t.Helper()

	allErrors, err := db.CheckForErrors()
	if err != nil {
		return nil, err
	}

	// Filter out known benign errors
	var criticalErrors []string
	for _, errLine := range allErrors {
		// Skip benign compaction errors that can occur during concurrent writes
		if strings.Contains(errLine, "page size not initialized yet") {
			continue
		}
		// Skip benign database removal errors that occur when closing databases
		if strings.Contains(errLine, "remove database from store") &&
			(strings.Contains(errLine, "transaction has already been committed or rolled back") ||
				strings.Contains(errLine, "no such file or directory") ||
				strings.Contains(errLine, "disk I/O error")) {
			continue
		}
		criticalErrors = append(criticalErrors, errLine)
	}

	return criticalErrors, nil
}
