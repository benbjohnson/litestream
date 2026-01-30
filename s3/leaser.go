package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/benbjohnson/litestream"
)

const (
	DefaultLeaseTTL  = 30 * time.Second
	DefaultLeasePath = "lock.json"
	LeaserType       = "s3"
)

var _ litestream.Leaser = (*Leaser)(nil)

type Leaser struct {
	client *ReplicaClient
	logger *slog.Logger

	TTL   time.Duration
	Owner string
}

func NewLeaser(client *ReplicaClient) *Leaser {
	owner, _ := os.Hostname()
	if owner == "" {
		owner = fmt.Sprintf("pid-%d", os.Getpid())
	} else {
		owner = fmt.Sprintf("%s:%d", owner, os.Getpid())
	}

	return &Leaser{
		client: client,
		logger: slog.Default().WithGroup("s3-leaser"),
		TTL:    DefaultLeaseTTL,
		Owner:  owner,
	}
}

func (l *Leaser) Type() string {
	return LeaserType
}

func (l *Leaser) lockKey() string {
	if l.client.Path == "" {
		return DefaultLeasePath
	}
	return l.client.Path + "/" + DefaultLeasePath
}

func (l *Leaser) AcquireLease(ctx context.Context) (*litestream.Lease, error) {
	if err := l.client.Init(ctx); err != nil {
		return nil, fmt.Errorf("init s3 client: %w", err)
	}

	existing, etag, err := l.readLease(ctx)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read existing lease: %w", err)
	}

	if existing != nil && !existing.IsExpired() {
		return nil, &litestream.LeaseExistsError{
			Owner:     existing.Owner,
			ExpiresAt: existing.ExpiresAt,
		}
	}

	var token int64 = 1
	if existing != nil {
		token = existing.Token + 1
	}

	newLease := &litestream.Lease{
		Token:     token,
		ExpiresAt: time.Now().Add(l.TTL),
		Owner:     l.Owner,
	}

	newETag, err := l.writeLease(ctx, newLease, etag)
	if err != nil {
		return nil, err
	}

	newLease.ETag = newETag
	l.logger.Debug("lease acquired",
		"token", newLease.Token,
		"owner", newLease.Owner,
		"expires_at", newLease.ExpiresAt,
		"etag", newLease.ETag)

	return newLease, nil
}

func (l *Leaser) RenewLease(ctx context.Context, lease *litestream.Lease) (*litestream.Lease, error) {
	if err := l.client.Init(ctx); err != nil {
		return nil, fmt.Errorf("init s3 client: %w", err)
	}

	if lease == nil || lease.ETag == "" {
		return nil, litestream.ErrLeaseNotHeld
	}

	newLease := &litestream.Lease{
		Token:     lease.Token,
		ExpiresAt: time.Now().Add(l.TTL),
		Owner:     l.Owner,
	}

	newETag, err := l.writeLease(ctx, newLease, lease.ETag)
	if err != nil {
		var leaseErr *litestream.LeaseExistsError
		if errors.As(err, &leaseErr) {
			return nil, litestream.ErrLeaseNotHeld
		}
		return nil, err
	}

	newLease.ETag = newETag
	l.logger.Debug("lease renewed",
		"token", newLease.Token,
		"owner", newLease.Owner,
		"expires_at", newLease.ExpiresAt,
		"etag", newLease.ETag)

	return newLease, nil
}

func (l *Leaser) ReleaseLease(ctx context.Context, lease *litestream.Lease) error {
	if err := l.client.Init(ctx); err != nil {
		return fmt.Errorf("init s3 client: %w", err)
	}

	if lease == nil || lease.ETag == "" {
		return litestream.ErrLeaseNotHeld
	}

	key := l.lockKey()
	_, err := l.client.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(l.client.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotExists(err) || isNotFoundError(err) {
			return nil
		}
		return fmt.Errorf("delete lease: %w", err)
	}

	l.logger.Debug("lease released",
		"token", lease.Token,
		"owner", lease.Owner)

	return nil
}

type lockFile struct {
	Token     int64   `json:"token"`
	ExpiresAt float64 `json:"expires_at"`
	Owner     string  `json:"owner,omitempty"`
}

func (l *Leaser) readLease(ctx context.Context) (*litestream.Lease, string, error) {
	key := l.lockKey()

	out, err := l.client.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.client.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotExists(err) || isNotFoundError(err) {
			return nil, "", os.ErrNotExist
		}
		return nil, "", fmt.Errorf("get lock file: %w", err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read lock file: %w", err)
	}

	var lf lockFile
	if err := json.Unmarshal(data, &lf); err != nil {
		return nil, "", fmt.Errorf("unmarshal lock file: %w", err)
	}

	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}

	lease := &litestream.Lease{
		Token:     lf.Token,
		ExpiresAt: unixToTime(lf.ExpiresAt),
		Owner:     lf.Owner,
		ETag:      etag,
	}

	return lease, etag, nil
}

func (l *Leaser) writeLease(ctx context.Context, lease *litestream.Lease, etag string) (string, error) {
	key := l.lockKey()

	lf := lockFile{
		Token:     lease.Token,
		ExpiresAt: timeToUnix(lease.ExpiresAt),
		Owner:     lease.Owner,
	}

	data, err := json.Marshal(lf)
	if err != nil {
		return "", fmt.Errorf("marshal lock file: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(l.client.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}

	if etag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(etag)
	}

	out, err := l.client.s3.PutObject(ctx, input)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", &litestream.LeaseExistsError{
				Owner:     lease.Owner,
				ExpiresAt: lease.ExpiresAt,
			}
		}
		return "", fmt.Errorf("put lock file: %w", err)
	}

	newETag := ""
	if out.ETag != nil {
		newETag = *out.ETag
	}

	return newETag, nil
}

func timeToUnix(t time.Time) float64 {
	return float64(t.UnixNano()) / float64(time.Second)
}

func unixToTime(f float64) time.Time {
	sec := int64(f)
	nsec := int64((f - float64(sec)) * float64(time.Second))
	return time.Unix(sec, nsec).UTC()
}

func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "PreconditionFailed" || code == "412"
	}

	var respErr *smithy.OperationError
	if errors.As(err, &respErr) {
		if httpErr, ok := respErr.Err.(interface{ HTTPStatusCode() int }); ok {
			return httpErr.HTTPStatusCode() == 412
		}
	}

	return false
}

func isNotFoundError(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NoSuchKey" || code == "NotFound" || code == "404"
	}

	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}

	var respErr *smithy.OperationError
	if errors.As(err, &respErr) {
		if httpErr, ok := respErr.Err.(interface{ HTTPStatusCode() int }); ok {
			return httpErr.HTTPStatusCode() == 404
		}
	}

	return false
}
