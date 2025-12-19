package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/benbjohnson/litestream"
)

// LockFileExt is the file extension used for epoch lock files.
const LockFileExt = ".lock"

var _ litestream.Leaser = (*Leaser)(nil)

// Leaser implements distributed leasing using S3 conditional writes.
// It uses the If-None-Match header to ensure atomic lease creation.
type Leaser struct {
	mu     sync.Mutex
	s3     *s3.Client
	logger *slog.Logger

	// LeaseTimeout is the duration a lease is held before expiring.
	// Defaults to litestream.DefaultLeaseTimeout (30s).
	LeaseTimeout time.Duration

	// Owner identifies this process when a lease is acquired (e.g. hostname).
	// Used for debugging and observability.
	Owner string

	// AWS authentication keys.
	AccessKeyID     string
	SecretAccessKey string

	// S3 bucket information
	Region string
	Bucket string
	// Path is the lease prefix in the bucket (global across DBs).
	Path           string
	Endpoint       string
	ForcePathStyle bool
	SkipVerify     bool
}

// NewLeaser returns a new instance of Leaser with default settings.
func NewLeaser() *Leaser {
	return &Leaser{
		LeaseTimeout: litestream.DefaultLeaseTimeout,
		Owner:        defaultOwner(),
		logger:       slog.Default().WithGroup("s3-leaser"),
	}
}

// Type returns "s3" as the leaser type.
func (l *Leaser) Type() string { return ReplicaClientType }

// Init initializes the connection to S3. No-op if already initialized.
func (l *Leaser) Init(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.s3 != nil {
		return nil
	}

	if l.Bucket == "" {
		return fmt.Errorf("s3 leaser: bucket name is required")
	}
	if l.Owner == "" {
		l.Owner = defaultOwner()
	}
	if l.Path == "" {
		l.Path = "leases"
	} else {
		l.Path = strings.Trim(l.Path, "/")
		if l.Path == "" {
			l.Path = "leases"
		}
	}

	region := l.Region
	if region == "" {
		if l.Endpoint == "" {
			var err error
			if region, err = l.findBucketRegion(ctx, l.Bucket); err != nil {
				return fmt.Errorf("s3 leaser: cannot lookup bucket region: %w", err)
			}
		} else {
			region = DefaultRegion
		}
	}

	httpClient := l.newHTTPClient()

	configOpts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
		config.WithRetryMode(aws.RetryModeAdaptive),
		config.WithRetryMaxAttempts(5),
		config.WithHTTPClient(httpClient),
	}

	if l.AccessKeyID != "" && l.SecretAccessKey != "" {
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(l.AccessKeyID, l.SecretAccessKey, ""),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return fmt.Errorf("s3 leaser: cannot load aws config: %w", err)
	}

	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = l.ForcePathStyle
		},
	}

	if endpoint, disableHTTPS := l.endpointWithScheme(); endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			if disableHTTPS {
				o.EndpointOptions.DisableHTTPS = true
			}
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		})
	}

	l.s3 = s3.NewFromConfig(cfg, s3Opts...)
	return nil
}

// Epochs returns a sorted list of epoch numbers from lease files in S3.
// Typically there should only be one or two as old leases are reaped.
func (l *Leaser) Epochs(ctx context.Context) ([]int64, error) {
	if err := l.Init(ctx); err != nil {
		return nil, err
	}

	var epochs []int64
	paginator := s3.NewListObjectsV2Paginator(l.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(l.Bucket),
		Prefix: aws.String(l.Path + "/"),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list epochs: %w", err)
		}

		for _, obj := range page.Contents {
			name := path.Base(aws.ToString(obj.Key))
			if !lockFileRegex.MatchString(name) {
				continue
			}

			epochStr := strings.TrimSuffix(name, LockFileExt)
			epoch, _ := strconv.ParseInt(epochStr, 16, 64)
			epochs = append(epochs, epoch)
		}
	}

	slices.Sort(epochs)
	return epochs, nil
}

// AcquireLease attempts to acquire a new lease.
func (l *Leaser) AcquireLease(ctx context.Context) (*litestream.Lease, error) {
	return l.acquireLease(ctx, 0)
}

// RenewLease renews an existing lease with an incremented epoch.
func (l *Leaser) RenewLease(ctx context.Context, lease *litestream.Lease) (*litestream.Lease, error) {
	return l.acquireLease(ctx, lease.Epoch)
}

func (l *Leaser) acquireLease(ctx context.Context, prevEpoch int64) (*litestream.Lease, error) {
	if err := l.Init(ctx); err != nil {
		return nil, err
	}

	epochs, err := l.Epochs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list epochs: %w", err)
	}

	var epoch int64
	if len(epochs) > 0 {
		epoch = epochs[len(epochs)-1]

		if prevEpoch != 0 && epoch == prevEpoch {
			lease, err := l.fetchLease(ctx, epoch)
			if os.IsNotExist(err) {
				// Lease was reaped, continue to acquire
			} else if err != nil {
				return nil, fmt.Errorf("fetch lease (epoch %d): %w", epoch, err)
			} else if !lease.Expired() && lease.Owner != "" && l.Owner != "" && lease.Owner != l.Owner {
				return nil, fmt.Errorf("cannot renew lease: owner mismatch (have %q, lease owner %q)", l.Owner, lease.Owner)
			}
		} else {
			if lease, err := l.fetchLease(ctx, epoch); os.IsNotExist(err) {
				// Lease was reaped, continue to acquire
			} else if err != nil {
				return nil, fmt.Errorf("fetch lease (epoch %d): %w", epoch, err)
			} else if !lease.Expired() {
				return nil, litestream.NewLeaseExistsError(lease)
			}
		}
	}

	// Create new lease with next epoch
	epoch++
	lease, err := l.createLease(ctx, epoch)
	if err != nil {
		return nil, fmt.Errorf("create lease: %w", err)
	}

	// Reap old lease files in the background
	for _, oldEpoch := range epochs {
		go func(e int64) {
			if err := l.DeleteLease(context.Background(), e); err != nil {
				l.logger.Warn("cannot reap lease",
					slog.Int64("epoch", e),
					slog.Any("error", err))
			}
		}(oldEpoch)
	}

	return lease, nil
}

// ReleaseLease releases a previously acquired lease by setting its timeout to zero.
func (l *Leaser) ReleaseLease(ctx context.Context, epoch int64) error {
	if err := l.Init(ctx); err != nil {
		return err
	}

	lease, err := l.fetchLease(ctx, epoch)
	if os.IsNotExist(err) {
		return nil // already reaped
	} else if err != nil {
		return fmt.Errorf("fetch lease: %w", err)
	} else if lease.Timeout <= 0 {
		return nil // already released
	}
	if lease.Owner != "" && l.Owner != "" && lease.Owner != l.Owner {
		return fmt.Errorf("release lease: owner mismatch (have %q, lease owner %q)", l.Owner, lease.Owner)
	}

	lease.Timeout = 0

	body, err := json.MarshalIndent(lease, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal lease: %w", err)
	}

	_, err = l.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(l.leaseKey(epoch)),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	return nil
}

// DeleteLease removes the lease file from S3.
func (l *Leaser) DeleteLease(ctx context.Context, epoch int64) error {
	if err := l.Init(ctx); err != nil {
		return err
	}

	_, err := l.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(l.leaseKey(epoch)),
	})
	if err != nil && !isNotExists(err) {
		return fmt.Errorf("delete lease: %w", err)
	}
	return nil
}

func (l *Leaser) createLease(ctx context.Context, epoch int64) (*litestream.Lease, error) {
	lease := &litestream.Lease{
		Epoch:   epoch,
		ModTime: time.Now(),
		Timeout: l.LeaseTimeout,
		Owner:   l.Owner,
	}

	body, err := json.MarshalIndent(lease, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal lease: %w", err)
	}

	_, err = l.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(l.Bucket),
		Key:         aws.String(l.leaseKey(epoch)),
		Body:        bytes.NewReader(body),
		IfNoneMatch: aws.String("*"),
	})

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && isPreconditionFailed(apiErr) {
		currentLease, fetchErr := l.fetchLease(ctx, epoch)
		if fetchErr != nil {
			return nil, fmt.Errorf("fetch conflicting lease (epoch %d): %w", epoch, fetchErr)
		}
		return nil, litestream.NewLeaseExistsError(currentLease)
	} else if err != nil {
		return nil, fmt.Errorf("put lease: %w", err)
	}

	modTime, err := l.leaseModTime(ctx, epoch)
	if err != nil {
		_ = l.DeleteLease(ctx, epoch)
		return nil, fmt.Errorf("get lease modtime: %w", err)
	}
	lease.ModTime = modTime

	return lease, nil
}

func (l *Leaser) fetchLease(ctx context.Context, epoch int64) (*litestream.Lease, error) {
	output, err := l.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(l.leaseKey(epoch)),
	})
	if isNotExists(err) {
		return nil, os.ErrNotExist
	} else if err != nil {
		return nil, fmt.Errorf("get lease: %w", err)
	}
	defer output.Body.Close()

	var lease litestream.Lease
	if err := json.NewDecoder(output.Body).Decode(&lease); err != nil {
		return nil, fmt.Errorf("decode lease: %w", err)
	}
	lease.ModTime = aws.ToTime(output.LastModified)
	return &lease, nil
}

func (l *Leaser) leaseKey(epoch int64) string {
	return fmt.Sprintf("%s/%016x%s", l.Path, epoch, LockFileExt)
}

func (l *Leaser) leaseModTime(ctx context.Context, epoch int64) (time.Time, error) {
	output, err := l.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(l.Bucket),
		Key:    aws.String(l.leaseKey(epoch)),
	})
	if isNotExists(err) {
		return time.Time{}, os.ErrNotExist
	} else if err != nil {
		return time.Time{}, fmt.Errorf("head lease: %w", err)
	}
	return aws.ToTime(output.LastModified), nil
}

// isPreconditionFailed checks if the error is a 412 Precondition Failed response.
func isPreconditionFailed(err smithy.APIError) bool {
	code := err.ErrorCode()
	return code == "PreconditionFailed" || code == "412"
}

var lockFileRegex = regexp.MustCompile(`^[0-9a-f]{16}\.lock$`)

func (l *Leaser) findBucketRegion(ctx context.Context, bucket string) (string, error) {
	configOpts := []func(*config.LoadOptions) error{
		config.WithHTTPClient(l.newHTTPClient()),
	}

	if l.AccessKeyID != "" && l.SecretAccessKey != "" {
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(l.AccessKeyID, l.SecretAccessKey, ""),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return "", fmt.Errorf("s3 leaser: cannot load aws config for region lookup: %w", err)
	}
	cfg.Region = DefaultRegion

	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = l.ForcePathStyle
		},
	}
	if endpoint, disableHTTPS := l.endpointWithScheme(); endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			if disableHTTPS {
				o.EndpointOptions.DisableHTTPS = true
			}
		})
	}

	client := s3.NewFromConfig(cfg, s3Opts...)

	out, err := client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", err
	}

	if out.LocationConstraint == "" {
		return DefaultRegion, nil
	}
	return string(out.LocationConstraint), nil
}

func (l *Leaser) endpointWithScheme() (string, bool) {
	if l.Endpoint == "" {
		return "", false
	}
	endpoint := l.Endpoint
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}
	return endpoint, strings.HasPrefix(endpoint, "http://")
}

func (l *Leaser) newHTTPClient() *http.Client {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	if l.SkipVerify {
		httpClient.Transport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}

	return httpClient
}

func defaultOwner() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s:%d", host, os.Getpid())
}
