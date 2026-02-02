package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

var (
	_ litestream.Leaser = (*Leaser)(nil)

	ErrLeaseRequired     = errors.New("lease required")
	ErrLeaseETagRequired = errors.New("lease etag required")
)

// S3API is the interface for S3 operations needed by Leaser.
type S3API interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type Leaser struct {
	s3     S3API
	logger *slog.Logger

	Bucket string
	Path   string
	TTL    time.Duration
	Owner  string
}

func NewLeaser() *Leaser {
	owner, _ := os.Hostname()
	if owner == "" {
		owner = fmt.Sprintf("pid-%d", os.Getpid())
	} else {
		owner = fmt.Sprintf("%s:%d", owner, os.Getpid())
	}

	return &Leaser{
		logger: slog.Default().WithGroup("s3-leaser"),
		TTL:    DefaultLeaseTTL,
		Owner:  owner,
	}
}

func (l *Leaser) Client() S3API {
	return l.s3
}

func (l *Leaser) SetClient(client S3API) {
	l.s3 = client
}

func (l *Leaser) Type() string {
	return LeaserType
}

func (l *Leaser) lockKey() string {
	if l.Path == "" {
		return DefaultLeasePath
	}
	return l.Path + "/" + DefaultLeasePath
}

func (l *Leaser) AcquireLease(ctx context.Context) (*litestream.Lease, error) {
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

	var generation int64 = 1
	if existing != nil {
		generation = existing.Generation + 1
	}

	newLease := &litestream.Lease{
		Generation: generation,
		ExpiresAt:  time.Now().Add(l.TTL),
		Owner:      l.Owner,
	}

	newETag, err := l.writeLease(ctx, newLease, etag)
	if err != nil {
		var leaseErr *litestream.LeaseExistsError
		if errors.As(err, &leaseErr) {
			if current, _, readErr := l.readLease(ctx); readErr == nil && current != nil {
				return nil, &litestream.LeaseExistsError{
					Owner:     current.Owner,
					ExpiresAt: current.ExpiresAt,
				}
			}
		}
		return nil, err
	}

	newLease.ETag = newETag
	l.logger.Debug("lease acquired",
		"generation", newLease.Generation,
		"owner", newLease.Owner,
		"expires_at", newLease.ExpiresAt,
		"etag", newLease.ETag)

	return newLease, nil
}

func (l *Leaser) RenewLease(ctx context.Context, lease *litestream.Lease) (*litestream.Lease, error) {
	if lease == nil {
		return nil, ErrLeaseRequired
	}
	if lease.ETag == "" {
		return nil, ErrLeaseETagRequired
	}

	newLease := &litestream.Lease{
		Generation: lease.Generation,
		ExpiresAt:  time.Now().Add(l.TTL),
		Owner:      l.Owner,
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
		"generation", newLease.Generation,
		"owner", newLease.Owner,
		"expires_at", newLease.ExpiresAt,
		"etag", newLease.ETag)

	return newLease, nil
}

func (l *Leaser) ReleaseLease(ctx context.Context, lease *litestream.Lease) error {
	if lease == nil {
		return ErrLeaseRequired
	}
	if lease.ETag == "" {
		return ErrLeaseETagRequired
	}

	key := l.lockKey()
	_, err := l.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:  aws.String(l.Bucket),
		Key:     aws.String(key),
		IfMatch: aws.String(lease.ETag),
	})
	if err != nil {
		if isNotExists(err) || isNotFoundError(err) {
			l.logger.Warn("lease already deleted unexpectedly",
				"generation", lease.Generation,
				"owner", lease.Owner)
			return nil
		}
		if isPreconditionFailed(err) {
			return litestream.ErrLeaseNotHeld
		}
		return fmt.Errorf("delete lease: %w", err)
	}

	l.logger.Debug("lease released",
		"generation", lease.Generation,
		"owner", lease.Owner)

	return nil
}

func (l *Leaser) readLease(ctx context.Context) (*litestream.Lease, string, error) {
	key := l.lockKey()

	out, err := l.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotExists(err) || isNotFoundError(err) {
			return nil, "", os.ErrNotExist
		}
		return nil, "", fmt.Errorf("get lock file: %w", err)
	}
	defer out.Body.Close()

	var lease litestream.Lease
	if err := json.NewDecoder(out.Body).Decode(&lease); err != nil {
		return nil, "", fmt.Errorf("decode lock file: %w", err)
	}

	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	lease.ETag = etag

	return &lease, etag, nil
}

func (l *Leaser) writeLease(ctx context.Context, lease *litestream.Lease, etag string) (string, error) {
	key := l.lockKey()

	data, err := json.Marshal(lease)
	if err != nil {
		return "", fmt.Errorf("marshal lock file: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(l.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}

	if etag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(etag)
	}

	out, err := l.s3.PutObject(ctx, input)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", &litestream.LeaseExistsError{}
		}
		return "", fmt.Errorf("put lock file: %w", err)
	}

	newETag := ""
	if out.ETag != nil {
		newETag = *out.ETag
	}

	return newETag, nil
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
