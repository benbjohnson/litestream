package litestream_test

import (
	"flag"
	"os"
)

// Enables integration tests.
var integration = flag.String("integration", "file", "")

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
	gcsBucket = flag.String("gcs-bucket", os.Getenv("LITESTREAM_GCS_BUCKET"), "")
	gcsPath   = flag.String("gcs-path", os.Getenv("LITESTREAM_GCS_PATH"), "")
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
