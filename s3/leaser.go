package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/benbjohnson/litestream"
)

// LockFileExt is the file extension used for epoch lock files.
const LockFileExt = ".lock"

var _ litestream.Leaser = (*Leaser)(nil)

// Leaser is an implementation of a distributed lease using S3 conditional writes.
type Leaser struct {
	s3 *s3.S3 // s3 service

	// Required. The amount of time that a lease is held for before expiring.
	// This can be extended by renewing the lease before expiration.
	LeaseTimeout time.Duration

	// Optional. The name of the owner when a lease is acquired (e.g. hostname).
	// This is used to provide human-readable information to see current leadership.
	Owner string

	// AWS authentication keys.
	AccessKeyID     string
	SecretAccessKey string

	// S3 bucket information
	Region         string
	Bucket         string
	Path           string
	Endpoint       string
	ForcePathStyle bool
	SkipVerify     bool
}

// NewLeaser returns a new instance of ReplicaClient.
func NewLeaser() *Leaser {
	return &Leaser{
		LeaseTimeout: litestream.DefaultLeaseTimeout,
	}
}

// Type returns "s3" as the leaser type.
func (l *Leaser) Type() string { return "s3" }

func (l *Leaser) Open() error {
	sess, err := newSession(context.Background(), l.AccessKeyID, l.SecretAccessKey, l.Region, l.Bucket, l.Path, l.Endpoint, l.ForcePathStyle, l.SkipVerify)
	if err != nil {
		return err
	}
	l.s3 = s3.New(sess)
	return nil
}

// Epochs returns a list of epoch numbers from lease files in S3.
//
// Typically there should only be one or two as they will be cleaned up after
// expiration. The returned epoches are not guaranteed to be live.
func (l *Leaser) Epochs(ctx context.Context) ([]int64, error) {
	var a []int64
	err := l.s3.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(l.Bucket),
		Prefix: aws.String(l.Path + "/"),
	}, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		for _, obj := range page.Contents {
			// Skip any files that don't match the lock file format.
			name := path.Base(aws.StringValue(obj.Key))
			if !lockFileRegex.MatchString(name) {
				continue
			}

			// Parse epoch from base filename.
			epochStr := strings.TrimSuffix(name, LockFileExt)
			epoch, _ := strconv.ParseInt(epochStr, 16, 64)

			a = append(a, epoch)
		}
		return true
	})

	// Epochs should be sorted anyway but perform an explicit sort to be safe.
	slices.Sort(a)

	return a, err
}

// AcquireLease attempts to acquire a new lease.
func (l *Leaser) AcquireLease(ctx context.Context) (*litestream.Lease, error) {
	return l.acquireLease(ctx, 0)
}

// RenewLease renews an existing lease with the same timeout.
func (l *Leaser) RenewLease(ctx context.Context, lease *litestream.Lease) (*litestream.Lease, error) {
	return l.acquireLease(ctx, lease.Epoch)
}

func (l *Leaser) acquireLease(ctx context.Context, prevEpoch int64) (*litestream.Lease, error) {
	// List all epochs for all available lock files.
	epochs, err := l.Epochs(ctx)
	if err != nil {
		return nil, fmt.Errorf("epochs: %w", err)
	}

	// Check if current epoch is valid and has not expired yet.
	var epoch int64
	if len(epochs) > 0 {
		epoch = epochs[len(epochs)-1]

		// Only check current lease if we don't have a previous lease that we
		// are renewing or if the lease epoch doesn't match that previous lease.
		if prevEpoch == 0 || epoch != prevEpoch {
			if lease, err := l.lease(ctx, epoch); os.IsNotExist(err) {
				// No lease, skip error checking
			} else if err != nil {
				return nil, fmt.Errorf("fetch lease (%d): %w", epoch, err)
			} else if !lease.Expired() {
				return nil, litestream.NewLeaseExistsError(lease)
			}
		}
	}

	// At this point we assume there is no lease owner so try to acquire the
	// lock using the next epoch.
	epoch++
	lease, err := l.createLease(ctx, epoch)
	if err != nil {
		return nil, fmt.Errorf("create lease: %w", err)
	}

	// Reap old lease files in the background. This does not affect correctness
	// since we have a new lease file in existence so we will just log errors.
	for _, epoch := range epochs {
		epoch := epoch
		go func() {
			if err := l.DeleteLease(ctx, epoch); err != nil {
				slog.Warn("cannot reap lease",
					slog.Int64("epoch", epoch),
					slog.Any("error", err))
			}
		}()
	}

	return lease, nil
}

// ReleaseLease releases an previously acquired lease via expiration.
func (l *Leaser) ReleaseLease(ctx context.Context, epoch int64) error {
	// Check if lease exists. Ignore if it has already been reaped.
	lease, err := l.lease(ctx, epoch)
	if os.IsNotExist(err) {
		return nil // no lease file, exit
	} else if err != nil {
		return fmt.Errorf("fetch lease: %w", err)
	} else if lease.Timeout <= 0 {
		return nil // lease file already released
	}

	// Invalidate timeout so it expires immediately.
	lease.Timeout = 0

	// Overwrite previous object with expired lease.
	body, err := json.MarshalIndent(lease, "", "  ")
	if err != nil {
		return err
	}

	// Construct PUT with a conditional write and send to S3.
	_, err = l.s3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(l.leaseKey(lease.Epoch)),
		Body:   bytes.NewReader(body),
	})
	return err
}

// DeleteLease removes the lease by deleting its underlying file.
// This is typically used for reaping expired leases or for test cleanup.
func (l *Leaser) DeleteLease(ctx context.Context, epoch int64) error {
	_, err := l.s3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(l.leaseKey(epoch)),
	})
	if err != nil && !isNotExists(err) {
		return err
	}
	return nil
}

func (l *Leaser) createLease(ctx context.Context, epoch int64) (*litestream.Lease, error) {
	// Marshal lease object into formatted JSON. We set the last modified date
	// to the current time to be conservative. We could also refetch the lease
	// object so we'd get an accurate LastModified date but this should be ok.
	lease := &litestream.Lease{
		Epoch:   epoch,
		ModTime: time.Now(),
		Timeout: l.LeaseTimeout,
		Owner:   l.Owner,
	}
	body, err := json.MarshalIndent(lease, "", "  ")
	if err != nil {
		return nil, err
	}

	// Construct PUT with a conditional write and send to S3.
	req, _ := l.s3.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(l.leaseKey(epoch)),
		Body:   bytes.NewReader(body),
	})
	req.HTTPRequest.Header.Add("If-None-Match", "*")

	var awsErr awserr.Error
	if err := req.Send(); errors.As(err, &awsErr) && awsErr.Code() == "PreconditionFailed" {
		// If precondition failed then another instance raced and got the epoch
		// first so we should return an error with that lease.
		currentLease, err := l.lease(ctx, epoch)
		if err != nil {
			return nil, fmt.Errorf("fetch conflicting lease (%d): %w", epoch, err)
		}
		return nil, litestream.NewLeaseExistsError(currentLease)
	} else if err != nil {
		return nil, err
	} else if req.Error != nil {
		return nil, req.Error
	}
	return lease, nil
}

// lease fetches & decodes a lease file by epoch.
func (l *Leaser) lease(ctx context.Context, epoch int64) (*litestream.Lease, error) {
	output, err := l.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(l.leaseKey(epoch)),
	})
	if isNotExists(err) {
		return nil, os.ErrNotExist
	} else if err != nil {
		return nil, err
	}

	var lease litestream.Lease
	if err := json.NewDecoder(output.Body).Decode(&lease); err != nil {
		return nil, err
	}
	lease.ModTime = aws.TimeValue(output.LastModified)
	return &lease, nil
}

func (l *Leaser) leaseKey(epoch int64) string {
	return fmt.Sprintf("%s/%016x%s", l.Path, epoch, LockFileExt)
}

var lockFileRegex = regexp.MustCompile(`^[0-9a-f]{16}\.lock$`)
