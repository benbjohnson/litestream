package testingutil

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gs"
	"github.com/benbjohnson/litestream/internal"
	"github.com/benbjohnson/litestream/nats"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
)

var (
	// Enables integration tests.
	integration = flag.Bool("integration", false, "")
	// Enables specific types of replicas to be tested.
	replicaClientTypes = flag.String("replica-clients", "file", "")
	// Sets the log level for the tests.
	logLevel = flag.String("log.level", "debug", "")
)

// S3 settings
var (
	// Replica client settings
	s3AccessKeyID     = flag.String("s3-access-key-id", os.Getenv("LITESTREAM_S3_ACCESS_KEY_ID"), "")
	s3SecretAccessKey = flag.String("s3-secret-access-key", os.Getenv("LITESTREAM_S3_SECRET_ACCESS_KEY"), "")
	s3Region          = flag.String("s3-region", os.Getenv("LITESTREAM_S3_REGION"), "")
	s3Bucket          = flag.String("s3-bucket", os.Getenv("LITESTREAM_S3_BUCKET"), "")
	s3Path            = flag.String("s3-path", os.Getenv("LITESTREAM_S3_PATH"), "")
	s3Endpoint        = flag.String("s3-endpoint", os.Getenv("LITESTREAM_S3_ENDPOINT"), "")
	s3ForcePathStyle  = flag.Bool("s3-force-path-style", os.Getenv("LITESTREAM_S3_FORCE_PATH_STYLE") == "true", "")
	s3SkipVerify      = flag.Bool("s3-skip-verify", os.Getenv("LITESTREAM_S3_SKIP_VERIFY") == "true", "")
)

// Google cloud storage settings
var (
	gsBucket = flag.String("gs-bucket", os.Getenv("LITESTREAM_GS_BUCKET"), "")
	gsPath   = flag.String("gs-path", os.Getenv("LITESTREAM_GS_PATH"), "")
)

// Azure blob storage settings
var (
	absAccountName = flag.String("abs-account-name", os.Getenv("LITESTREAM_ABS_ACCOUNT_NAME"), "")
	absAccountKey  = flag.String("abs-account-key", os.Getenv("LITESTREAM_ABS_ACCOUNT_KEY"), "")
	absBucket      = flag.String("abs-bucket", os.Getenv("LITESTREAM_ABS_BUCKET"), "")
	absPath        = flag.String("abs-path", os.Getenv("LITESTREAM_ABS_PATH"), "")
)

// SFTP settings
var (
	sftpHost     = flag.String("sftp-host", os.Getenv("LITESTREAM_SFTP_HOST"), "")
	sftpUser     = flag.String("sftp-user", os.Getenv("LITESTREAM_SFTP_USER"), "")
	sftpPassword = flag.String("sftp-password", os.Getenv("LITESTREAM_SFTP_PASSWORD"), "")
	sftpKeyPath  = flag.String("sftp-key-path", os.Getenv("LITESTREAM_SFTP_KEY_PATH"), "")
	sftpPath     = flag.String("sftp-path", os.Getenv("LITESTREAM_SFTP_PATH"), "")
)

// NATS settings
var (
	natsURL      = flag.String("nats-url", os.Getenv("LITESTREAM_NATS_URL"), "")
	natsBucket   = flag.String("nats-bucket", os.Getenv("LITESTREAM_NATS_BUCKET"), "")
	natsCreds    = flag.String("nats-creds", os.Getenv("LITESTREAM_NATS_CREDS"), "")
	natsUsername = flag.String("nats-username", os.Getenv("LITESTREAM_NATS_USERNAME"), "")
	natsPassword = flag.String("nats-password", os.Getenv("LITESTREAM_NATS_PASSWORD"), "")
)

func Integration() bool {
	return *integration
}

func ReplicaClientTypes() []string {
	return strings.Split(*replicaClientTypes, ",")
}

func NewDB(tb testing.TB, path string) *litestream.DB {
	tb.Helper()
	tb.Logf("db=%s", path)

	level := slog.LevelDebug
	if strings.EqualFold(*logLevel, "trace") {
		level = internal.LevelTrace
	}

	db := litestream.NewDB(path)
	db.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: internal.ReplaceAttr,
	}))
	return db
}

// MustOpenDBs returns a new instance of a DB & associated SQL DB.
func MustOpenDBs(tb testing.TB) (*litestream.DB, *sql.DB) {
	tb.Helper()
	db := MustOpenDB(tb)
	return db, MustOpenSQLDB(tb, db.Path())
}

// MustCloseDBs closes db & sqldb and removes the parent directory.
func MustCloseDBs(tb testing.TB, db *litestream.DB, sqldb *sql.DB) {
	tb.Helper()
	MustCloseDB(tb, db)
	MustCloseSQLDB(tb, sqldb)
}

// MustOpenDB returns a new instance of a DB.
func MustOpenDB(tb testing.TB) *litestream.DB {
	tb.Helper()
	dir := tb.TempDir()
	return MustOpenDBAt(tb, filepath.Join(dir, "db"))
}

// MustOpenDBAt returns a new instance of a DB for a given path.
func MustOpenDBAt(tb testing.TB, path string) *litestream.DB {
	tb.Helper()
	db := NewDB(tb, path)
	db.MonitorInterval = 0 // disable background goroutine
	db.Replica = litestream.NewReplica(db)
	db.Replica.Client = NewFileReplicaClient(tb)
	db.Replica.MonitorEnabled = false // disable background goroutine
	if err := db.Open(); err != nil {
		tb.Fatal(err)
	}
	return db
}

// MustCloseDB closes db and removes its parent directory.
func MustCloseDB(tb testing.TB, db *litestream.DB) {
	tb.Helper()
	if err := db.Close(context.Background()); err != nil && !strings.Contains(err.Error(), `database is closed`) && !strings.Contains(err.Error(), `file already closed`) {
		tb.Fatal(err)
	} else if err := os.RemoveAll(filepath.Dir(db.Path())); err != nil {
		tb.Fatal(err)
	}
}

// MustOpenSQLDB returns a database/sql DB.
func MustOpenSQLDB(tb testing.TB, path string) *sql.DB {
	tb.Helper()
	d, err := sql.Open("sqlite", path)
	if err != nil {
		tb.Fatal(err)
	} else if _, err := d.ExecContext(context.Background(), `PRAGMA journal_mode = wal;`); err != nil {
		tb.Fatal(err)
	}
	return d
}

// MustCloseSQLDB closes a database/sql DB.
func MustCloseSQLDB(tb testing.TB, d *sql.DB) {
	tb.Helper()
	if err := d.Close(); err != nil {
		tb.Fatal(err)
	}
}

// NewReplicaClient returns a new client for integration testing by type name.
func NewReplicaClient(tb testing.TB, typ string) litestream.ReplicaClient {
	tb.Helper()

	switch typ {
	case file.ReplicaClientType:
		return NewFileReplicaClient(tb)
	case s3.ReplicaClientType:
		return NewS3ReplicaClient(tb)
	case gs.ReplicaClientType:
		return NewGSReplicaClient(tb)
	case abs.ReplicaClientType:
		return NewABSReplicaClient(tb)
	case sftp.ReplicaClientType:
		return NewSFTPReplicaClient(tb)
	case nats.ReplicaClientType:
		return NewNATSReplicaClient(tb)
	default:
		tb.Fatalf("invalid replica client type: %q", typ)
		return nil
	}
}

// NewFileReplicaClient returns a new client for integration testing.
func NewFileReplicaClient(tb testing.TB) *file.ReplicaClient {
	tb.Helper()
	return file.NewReplicaClient(tb.TempDir())
}

// NewS3ReplicaClient returns a new client for integration testing.
func NewS3ReplicaClient(tb testing.TB) *s3.ReplicaClient {
	tb.Helper()

	c := s3.NewReplicaClient()
	c.AccessKeyID = *s3AccessKeyID
	c.SecretAccessKey = *s3SecretAccessKey
	c.Region = *s3Region
	c.Bucket = *s3Bucket
	c.Path = path.Join(*s3Path, fmt.Sprintf("%016x", rand.Uint64()))
	c.Endpoint = *s3Endpoint
	c.ForcePathStyle = *s3ForcePathStyle
	c.SkipVerify = *s3SkipVerify
	return c
}

// NewGSReplicaClient returns a new client for integration testing.
func NewGSReplicaClient(tb testing.TB) *gs.ReplicaClient {
	tb.Helper()

	// Log basic diagnostic information for integration test troubleshooting
	tb.Logf("GCS Integration Test Setup:")
	credsSet := "not set"
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		credsSet = "set"
	}
	tb.Logf("  GOOGLE_APPLICATION_CREDENTIALS: %s", credsSet)
	tb.Logf("  LITESTREAM_GS_BUCKET: %s", *gsBucket)
	tb.Logf("  LITESTREAM_GS_PATH: %s", *gsPath)

	c := gs.NewReplicaClient()
	c.Bucket = *gsBucket
	c.Path = path.Join(*gsPath, fmt.Sprintf("%016x", rand.Uint64()))

	// Test basic connectivity
	ctx := context.Background()
	if err := c.Init(ctx); err != nil {
		tb.Logf("GCS client initialization failed: %v", err)
		tb.Logf("This may indicate credential or project issues")
		return c // Return anyway to let the actual test show the detailed error
	}
	tb.Logf("GCS client initialized successfully")

	return c
}

// NewABSReplicaClient returns a new client for integration testing.
func NewABSReplicaClient(tb testing.TB) *abs.ReplicaClient {
	tb.Helper()

	c := abs.NewReplicaClient()
	c.AccountName = *absAccountName
	c.AccountKey = *absAccountKey
	c.Bucket = *absBucket
	c.Path = path.Join(*absPath, fmt.Sprintf("%016x", rand.Uint64()))
	return c
}

// NewSFTPReplicaClient returns a new client for integration testing.
func NewSFTPReplicaClient(tb testing.TB) *sftp.ReplicaClient {
	tb.Helper()

	c := sftp.NewReplicaClient()
	c.Host = *sftpHost
	c.User = *sftpUser
	c.Password = *sftpPassword
	c.KeyPath = *sftpKeyPath
	c.Path = path.Join(*sftpPath, fmt.Sprintf("%016x", rand.Uint64()))
	return c
}

// NewNATSReplicaClient returns a new client for integration testing.
func NewNATSReplicaClient(tb testing.TB) *nats.ReplicaClient {
	tb.Helper()

	c := nats.NewReplicaClient()
	c.URL = *natsURL
	c.BucketName = *natsBucket
	c.Creds = *natsCreds
	c.Username = *natsUsername
	c.Password = *natsPassword
	return c
}

// MustDeleteAll deletes all objects under the client's path.
func MustDeleteAll(tb testing.TB, c litestream.ReplicaClient) {
	tb.Helper()

	if err := c.DeleteAll(context.Background()); err != nil {
		tb.Fatalf("cannot delete all: %s", err)
	}

	switch c := c.(type) {
	case *sftp.ReplicaClient:
		if err := c.Cleanup(context.Background()); err != nil {
			tb.Fatalf("cannot cleanup sftp: %s", err)
		}
	}
}
