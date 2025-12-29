package testingutil

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	sftpserver "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gs"
	"github.com/benbjohnson/litestream/internal"
	"github.com/benbjohnson/litestream/nats"
	"github.com/benbjohnson/litestream/oss"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
	"github.com/benbjohnson/litestream/webdav"
)

const (
	defaultTigrisEndpoint = "https://fly.storage.tigris.dev"
	defaultTigrisRegion   = "auto"
	defaultTigrisBucket   = "litestream-dev"
	defaultTigrisPathRoot = "integration-tests"
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

// Tigris settings (S3-compatible)
var (
	tigrisAccessKeyID     = flag.String("tigris-access-key-id", os.Getenv("LITESTREAM_TIGRIS_ACCESS_KEY_ID"), "")
	tigrisSecretAccessKey = flag.String("tigris-secret-access-key", os.Getenv("LITESTREAM_TIGRIS_SECRET_ACCESS_KEY"), "")
)

// Cloudflare R2 settings (S3-compatible)
var (
	r2AccessKeyID     = flag.String("r2-access-key-id", os.Getenv("LITESTREAM_R2_ACCESS_KEY_ID"), "")
	r2SecretAccessKey = flag.String("r2-secret-access-key", os.Getenv("LITESTREAM_R2_SECRET_ACCESS_KEY"), "")
	r2Endpoint        = flag.String("r2-endpoint", os.Getenv("LITESTREAM_R2_ENDPOINT"), "")
	r2Bucket          = flag.String("r2-bucket", os.Getenv("LITESTREAM_R2_BUCKET"), "")
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

// WebDAV settings
var (
	webdavURL      = flag.String("webdav-url", os.Getenv("LITESTREAM_WEBDAV_URL"), "")
	webdavUsername = flag.String("webdav-username", os.Getenv("LITESTREAM_WEBDAV_USERNAME"), "")
	webdavPassword = flag.String("webdav-password", os.Getenv("LITESTREAM_WEBDAV_PASSWORD"), "")
	webdavPath     = flag.String("webdav-path", os.Getenv("LITESTREAM_WEBDAV_PATH"), "")
)

// NATS settings
var (
	natsURL      = flag.String("nats-url", os.Getenv("LITESTREAM_NATS_URL"), "")
	natsBucket   = flag.String("nats-bucket", os.Getenv("LITESTREAM_NATS_BUCKET"), "")
	natsCreds    = flag.String("nats-creds", os.Getenv("LITESTREAM_NATS_CREDS"), "")
	natsUsername = flag.String("nats-username", os.Getenv("LITESTREAM_NATS_USERNAME"), "")
	natsPassword = flag.String("nats-password", os.Getenv("LITESTREAM_NATS_PASSWORD"), "")
)

// Alibaba Cloud OSS settings
var (
	ossAccessKeyID     = flag.String("oss-access-key-id", os.Getenv("LITESTREAM_OSS_ACCESS_KEY_ID"), "")
	ossAccessKeySecret = flag.String("oss-access-key-secret", os.Getenv("LITESTREAM_OSS_ACCESS_KEY_SECRET"), "")
	ossRegion          = flag.String("oss-region", os.Getenv("LITESTREAM_OSS_REGION"), "")
	ossBucket          = flag.String("oss-bucket", os.Getenv("LITESTREAM_OSS_BUCKET"), "")
	ossPath            = flag.String("oss-path", os.Getenv("LITESTREAM_OSS_PATH"), "")
	ossEndpoint        = flag.String("oss-endpoint", os.Getenv("LITESTREAM_OSS_ENDPOINT"), "")
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
	db.MonitorInterval = 0     // disable background goroutine
	db.ShutdownSyncTimeout = 0 // disable shutdown sync retry for faster tests
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
	} else if _, err := d.ExecContext(context.Background(), `PRAGMA busy_timeout = 5000;`); err != nil {
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
	case webdav.ReplicaClientType:
		return NewWebDAVReplicaClient(tb)
	case nats.ReplicaClientType:
		return NewNATSReplicaClient(tb)
	case oss.ReplicaClientType:
		return NewOSSReplicaClient(tb)
	case "tigris":
		return NewTigrisReplicaClient(tb)
	case "r2":
		return NewR2ReplicaClient(tb)
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

// NewTigrisReplicaClient returns an S3 client configured for Fly.io Tigris.
func NewTigrisReplicaClient(tb testing.TB) *s3.ReplicaClient {
	tb.Helper()

	if *tigrisAccessKeyID == "" || *tigrisSecretAccessKey == "" {
		tb.Fatalf("tigris credentials not configured (set LITESTREAM_TIGRIS_ACCESS_KEY_ID/SECRET_ACCESS_KEY)")
	}

	c := s3.NewReplicaClient()
	c.AccessKeyID = *tigrisAccessKeyID
	c.SecretAccessKey = *tigrisSecretAccessKey
	c.Region = defaultTigrisRegion
	c.Bucket = defaultTigrisBucket
	c.Path = path.Join(defaultTigrisPathRoot, fmt.Sprintf("%016x", rand.Uint64()))
	c.Endpoint = defaultTigrisEndpoint
	c.ForcePathStyle = true
	c.RequireContentMD5 = false
	return c
}

// NewR2ReplicaClient returns an S3 client configured for Cloudflare R2.
// Skips the test if R2 credentials are not configured.
func NewR2ReplicaClient(tb testing.TB) *s3.ReplicaClient {
	tb.Helper()

	if *r2AccessKeyID == "" || *r2SecretAccessKey == "" {
		tb.Skip("r2 credentials not configured (set LITESTREAM_R2_ACCESS_KEY_ID/SECRET_ACCESS_KEY)")
	}
	if *r2Endpoint == "" {
		tb.Skip("r2 endpoint not configured (set LITESTREAM_R2_ENDPOINT)")
	}
	if *r2Bucket == "" {
		tb.Skip("r2 bucket not configured (set LITESTREAM_R2_BUCKET)")
	}

	c := s3.NewReplicaClient()
	c.AccessKeyID = *r2AccessKeyID
	c.SecretAccessKey = *r2SecretAccessKey
	c.Region = "auto"
	c.Bucket = *r2Bucket
	c.Path = path.Join("integration-tests", fmt.Sprintf("%016x", rand.Uint64()))
	c.Endpoint = *r2Endpoint
	c.ForcePathStyle = true
	c.SignPayload = true
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

// NewWebDAVReplicaClient returns a new client for integration testing.
func NewWebDAVReplicaClient(tb testing.TB) *webdav.ReplicaClient {
	tb.Helper()

	c := webdav.NewReplicaClient()
	c.URL = *webdavURL
	c.Username = *webdavUsername
	c.Password = *webdavPassword
	c.Path = path.Join(*webdavPath, fmt.Sprintf("%016x", rand.Uint64()))
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

// NewOSSReplicaClient returns a new client for integration testing.
func NewOSSReplicaClient(tb testing.TB) *oss.ReplicaClient {
	tb.Helper()

	c := oss.NewReplicaClient()
	c.AccessKeyID = *ossAccessKeyID
	c.AccessKeySecret = *ossAccessKeySecret
	c.Region = *ossRegion
	c.Bucket = *ossBucket
	c.Path = path.Join(*ossPath, fmt.Sprintf("%016x", rand.Uint64()))
	c.Endpoint = *ossEndpoint
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

func MockSFTPServer(t *testing.T, hostKey ssh.Signer) string {
	config := &ssh.ServerConfig{NoClientAuth: true}
	config.AddHostKey(hostKey)

	listener, err := net.Listen("tcp", "127.0.0.1:0") // random available port
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go func() {
				_, chans, reqs, err := ssh.NewServerConn(conn, config)
				if err != nil {
					return
				}
				go ssh.DiscardRequests(reqs)

				for ch := range chans {
					if ch.ChannelType() != "session" {
						ch.Reject(ssh.UnknownChannelType, "unsupported")
						continue
					}
					channel, requests, err := ch.Accept()
					if err != nil {
						return
					}

					go func(in <-chan *ssh.Request) {
						for req := range in {
							if req.Type == "subsystem" && string(req.Payload[4:]) == "sftp" {
								req.Reply(true, nil)

								server, err := sftpserver.NewServer(channel)
								if err != nil {
									return
								}
								if err := server.Serve(); err != nil && err != io.EOF {
									t.Logf("SFTP server error: %v", err)
								}
								return
							}
							req.Reply(false, nil)
						}
					}(requests)
				}
			}()
		}
	}()

	return listener.Addr().String()
}
