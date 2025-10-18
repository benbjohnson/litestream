//go:build integration

package integration

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type LoadPattern string

const (
	LoadPatternConstant LoadPattern = "constant"
	LoadPatternBurst    LoadPattern = "burst"
	LoadPatternRandom   LoadPattern = "random"
	LoadPatternWave     LoadPattern = "wave"
)

type LoadConfig struct {
	WriteRate   int
	Duration    time.Duration
	Pattern     LoadPattern
	PayloadSize int
	ReadRatio   float64
	Workers     int
}

func DefaultLoadConfig() *LoadConfig {
	return &LoadConfig{
		WriteRate:   100,
		Duration:    1 * time.Minute,
		Pattern:     LoadPatternConstant,
		PayloadSize: 1024,
		ReadRatio:   0.2,
		Workers:     1,
	}
}

type PopulateConfig struct {
	TargetSize string
	RowSize    int
	BatchSize  int
	TableCount int
	IndexRatio float64
	PageSize   int
}

func DefaultPopulateConfig() *PopulateConfig {
	return &PopulateConfig{
		TargetSize: "100MB",
		RowSize:    1024,
		BatchSize:  1000,
		TableCount: 1,
		IndexRatio: 0.2,
		PageSize:   4096,
	}
}

func CreateComplexTestSchema(db *sql.DB) error {
	schemas := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			username TEXT NOT NULL UNIQUE,
			email TEXT NOT NULL,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS posts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			title TEXT NOT NULL,
			content TEXT,
			created_at INTEGER NOT NULL,
			FOREIGN KEY (user_id) REFERENCES users(id)
		)`,
		`CREATE TABLE IF NOT EXISTS comments (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			post_id INTEGER NOT NULL,
			user_id INTEGER NOT NULL,
			content TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			FOREIGN KEY (post_id) REFERENCES posts(id),
			FOREIGN KEY (user_id) REFERENCES users(id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id)`,
		`CREATE INDEX IF NOT EXISTS idx_comments_created_at ON comments(created_at)`,
	}

	for _, schema := range schemas {
		if _, err := db.Exec(schema); err != nil {
			return fmt.Errorf("execute schema: %w", err)
		}
	}

	return nil
}

func PopulateComplexTestData(db *sql.DB, userCount, postsPerUser, commentsPerPost int) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	userStmt, err := tx.Prepare("INSERT INTO users (username, email, created_at) VALUES (?, ?, ?)")
	if err != nil {
		return fmt.Errorf("prepare user statement: %w", err)
	}
	defer userStmt.Close()

	postStmt, err := tx.Prepare("INSERT INTO posts (user_id, title, content, created_at) VALUES (?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("prepare post statement: %w", err)
	}
	defer postStmt.Close()

	commentStmt, err := tx.Prepare("INSERT INTO comments (post_id, user_id, content, created_at) VALUES (?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("prepare comment statement: %w", err)
	}
	defer commentStmt.Close()

	now := time.Now().Unix()

	for u := 1; u <= userCount; u++ {
		userResult, err := userStmt.Exec(
			fmt.Sprintf("user%d", u),
			fmt.Sprintf("user%d@test.com", u),
			now,
		)
		if err != nil {
			return fmt.Errorf("insert user: %w", err)
		}

		userID, err := userResult.LastInsertId()
		if err != nil {
			return fmt.Errorf("get user id: %w", err)
		}

		for p := 1; p <= postsPerUser; p++ {
			postResult, err := postStmt.Exec(
				userID,
				fmt.Sprintf("Post %d from user %d", p, u),
				generateRandomContent(100),
				now,
			)
			if err != nil {
				return fmt.Errorf("insert post: %w", err)
			}

			postID, err := postResult.LastInsertId()
			if err != nil {
				return fmt.Errorf("get post id: %w", err)
			}

			for c := 1; c <= commentsPerPost; c++ {
				commentUserID := (u + c) % userCount
				if commentUserID == 0 {
					commentUserID = userCount
				}

				_, err := commentStmt.Exec(
					postID,
					commentUserID,
					generateRandomContent(50),
					now,
				)
				if err != nil {
					return fmt.Errorf("insert comment: %w", err)
				}
			}
		}
	}

	return tx.Commit()
}

func generateRandomContent(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	b := make([]byte, length)
	rand.Read(b)

	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}

	return string(b)
}

type TestScenario struct {
	Name        string
	Description string
	Setup       func(*sql.DB) error
	Validate    func(*sql.DB, *sql.DB) error
}

func LargeWALScenario() *TestScenario {
	return &TestScenario{
		Name:        "Large WAL",
		Description: "Generate large WAL file to test handling",
		Setup: func(db *sql.DB) error {
			if _, err := db.Exec(`
				CREATE TABLE test_wal (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					data BLOB
				)
			`); err != nil {
				return err
			}

			data := make([]byte, 10*1024)
			rand.Read(data)

			for i := 0; i < 10000; i++ {
				if _, err := db.Exec("INSERT INTO test_wal (data) VALUES (?)", data); err != nil {
					return err
				}
			}

			return nil
		},
		Validate: func(source, restored *sql.DB) error {
			var sourceCount, restoredCount int

			if err := source.QueryRow("SELECT COUNT(*) FROM test_wal").Scan(&sourceCount); err != nil {
				return fmt.Errorf("query source: %w", err)
			}

			if err := restored.QueryRow("SELECT COUNT(*) FROM test_wal").Scan(&restoredCount); err != nil {
				return fmt.Errorf("query restored: %w", err)
			}

			if sourceCount != restoredCount {
				return fmt.Errorf("count mismatch: source=%d, restored=%d", sourceCount, restoredCount)
			}

			return nil
		},
	}
}

func RapidCheckpointsScenario() *TestScenario {
	return &TestScenario{
		Name:        "Rapid Checkpoints",
		Description: "Test rapid checkpoint operations",
		Setup: func(db *sql.DB) error {
			if _, err := db.Exec(`
				CREATE TABLE test_checkpoints (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					data TEXT,
					timestamp INTEGER
				)
			`); err != nil {
				return err
			}

			for i := 0; i < 1000; i++ {
				if _, err := db.Exec(
					"INSERT INTO test_checkpoints (data, timestamp) VALUES (?, ?)",
					fmt.Sprintf("data %d", i),
					time.Now().Unix(),
				); err != nil {
					return err
				}

				if i%100 == 0 {
					if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
						return err
					}
				}
			}

			return nil
		},
		Validate: func(source, restored *sql.DB) error {
			var sourceCount, restoredCount int

			if err := source.QueryRow("SELECT COUNT(*) FROM test_checkpoints").Scan(&sourceCount); err != nil {
				return fmt.Errorf("query source: %w", err)
			}

			if err := restored.QueryRow("SELECT COUNT(*) FROM test_checkpoints").Scan(&restoredCount); err != nil {
				return fmt.Errorf("query restored: %w", err)
			}

			if sourceCount != restoredCount {
				return fmt.Errorf("count mismatch: source=%d, restored=%d", sourceCount, restoredCount)
			}

			return nil
		},
	}
}
