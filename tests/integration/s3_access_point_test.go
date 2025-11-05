//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestS3AccessPointLocalStack verifies replication to an S3 access point via LocalStack.
func TestS3AccessPointLocalStack(t *testing.T) {
	RequireBinaries(t)
	RequireDocker(t)

	containerName, endpoint := StartMinioTestContainer(t)
	t.Cleanup(func() {
		StopMinioTestContainer(t, containerName)
	})

	ctx := context.Background()
	configEndpoint := strings.Replace(endpoint, "localhost", "s3-accesspoint.127.0.0.1.nip.io", 1)
	s3Client := newMinioS3Client(t, configEndpoint, false)

	accountID := "000000000000"
	accessPointName := fmt.Sprintf("litestream-ap-%d", time.Now().UnixNano())
	bucket := fmt.Sprintf("%s-%s", accessPointName, accountID)
	accessPointARN := fmt.Sprintf("arn:aws:s3:us-east-1:%s:accesspoint/%s", accountID, accessPointName)

	createBucket(t, ctx, s3Client, bucket)
	t.Cleanup(func() {
		if os.Getenv("SOAK_KEEP_TEMP") != "" {
			t.Logf("SOAK_KEEP_TEMP set, preserving bucket %s", bucket)
			return
		}
		if err := clearBucket(ctx, s3Client, bucket); err != nil {
			t.Logf("warn: clear bucket: %v", err)
		}
	})

	t.Logf("simulated access point ARN: %s", accessPointARN)

	db := SetupTestDB(t, "localstack-accesspoint")
	if err := db.Create(); err != nil {
		t.Fatalf("create db: %v", err)
	}
	if err := db.Populate("5MB"); err != nil {
		t.Fatalf("populate db: %v", err)
	}

	replicaURL := fmt.Sprintf("s3://%s/test-prefix", accessPointARN)
	db.ReplicaURL = replicaURL

	configPath := WriteS3AccessPointConfig(t, db.Path, replicaURL, configEndpoint, false, "minioadmin", "minioadmin")
	db.ConfigPath = configPath

	if err := db.StartLitestreamWithConfig(configPath); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	t.Cleanup(func() {
		_ = db.StopLitestream()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.GenerateLoad(ctx, 200, 5*time.Second, "steady"); err != nil {
		t.Fatalf("generate load: %v", err)
	}

	// Wait for uploaded LTX files to appear in the underlying bucket.
	waitForObjects(t, s3Client, bucket, "test-prefix", 30*time.Second)

	if err := db.StopLitestream(); err != nil {
		t.Fatalf("stop litestream: %v", err)
	}
	db.LitestreamCmd = nil

	restoredPath := filepath.Join(db.TempDir, "restored-access-point.db")
	if err := db.Restore(restoredPath); err != nil {
		t.Fatalf("restore: %v", err)
	}

	if err := compareRowCounts(db.Path, restoredPath); err != nil {
		t.Fatalf("row compare: %v", err)
	}
}

func newMinioS3Client(t *testing.T, endpoint string, forcePathStyle bool) *awss3.Client {
	t.Helper()

	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               endpoint,
			SigningRegion:     "us-east-1",
			HostnameImmutable: true,
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
		config.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.UsePathStyle = forcePathStyle
	})
}

func createBucket(t *testing.T, ctx context.Context, client *awss3.Client, bucket string) {
	t.Helper()

	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
}

func clearBucket(ctx context.Context, client *awss3.Client, bucket string) error {
	paginator := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}
		if len(page.Contents) == 0 {
			break
		}
		objs := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
		for _, item := range page.Contents {
			objs = append(objs, s3types.ObjectIdentifier{Key: item.Key})
		}
		if _, err := client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &s3types.Delete{Objects: objs, Quiet: aws.Bool(true)},
		}); err != nil {
			return fmt.Errorf("delete objects: %w", err)
		}
	}
	return nil
}

func waitForObjects(t *testing.T, client *awss3.Client, bucket, prefix string, timeout time.Duration) {
	t.Helper()

	trimmedPrefix := strings.Trim(prefix, "/")
	if trimmedPrefix != "" {
		trimmedPrefix += "/"
	}

	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		out, err := client.ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(trimmedPrefix),
		})
		if err == nil && len(out.Contents) > 0 {
			return
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("no objects yet")
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for objects in bucket %s with prefix %s: last err %v", bucket, trimmedPrefix, lastErr)
		}
		time.Sleep(2 * time.Second)
	}
}

func compareRowCounts(srcPath, restoredPath string) error {
	srcDB, err := sql.Open("sqlite3", srcPath)
	if err != nil {
		return fmt.Errorf("open source db: %w", err)
	}
	defer srcDB.Close()

	restoredDB, err := sql.Open("sqlite3", restoredPath)
	if err != nil {
		return fmt.Errorf("open restored db: %w", err)
	}
	defer restoredDB.Close()

	tableName, err := findUserTable(srcDB)
	if err != nil {
		return err
	}

	var srcCount, restoredCount int
	if err := srcDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&srcCount); err != nil {
		return fmt.Errorf("count source: %w", err)
	}
	if err := restoredDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&restoredCount); err != nil {
		return fmt.Errorf("count restored: %w", err)
	}
	if srcCount != restoredCount {
		return fmt.Errorf("row mismatch: source=%d restored=%d", srcCount, restoredCount)
	}
	return nil
}

func findUserTable(db *sql.DB) (string, error) {
	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1`).Scan(&name)
	if err != nil {
		return "", fmt.Errorf("find table: %w", err)
	}
	return name, nil
}
