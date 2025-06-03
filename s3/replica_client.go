package s3

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"regexp"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "s3"

// MaxKeys is the number of keys S3 can operate on per batch.
const MaxKeys = 1000

// DefaultRegion is the region used if one is not specified.
const DefaultRegion = "us-east-1"

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files to S3.
type ReplicaClient struct {
	mu       sync.Mutex
	s3       *s3.S3 // s3 service
	uploader *s3manager.Uploader

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

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{}
}

// Type returns "s3" as the client type.
func (c *ReplicaClient) Type() string {
	return ReplicaClientType
}

// Init initializes the connection to S3. No-op if already initialized.
func (c *ReplicaClient) Init(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.s3 != nil {
		return nil
	}

	// Look up region if not specified and no endpoint is used.
	// Endpoints are typically used for non-S3 object stores and do not
	// necessarily require a region.
	region := c.Region
	if region == "" {
		if c.Endpoint == "" {
			if region, err = c.findBucketRegion(ctx, c.Bucket); err != nil {
				return fmt.Errorf("cannot lookup bucket region: %w", err)
			}
		} else {
			region = DefaultRegion // default for non-S3 object stores
		}
	}

	// Create new AWS session.
	config := c.config()
	if region != "" {
		config.Region = aws.String(region)
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return fmt.Errorf("cannot create aws session: %w", err)
	}
	c.s3 = s3.New(sess)
	c.uploader = s3manager.NewUploader(sess)
	return nil
}

// config returns the AWS configuration. Uses the default credential chain
// unless a key/secret are explicitly set.
func (c *ReplicaClient) config() *aws.Config {
	config := &aws.Config{}

	if c.AccessKeyID != "" || c.SecretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(c.AccessKeyID, c.SecretAccessKey, "")
	}
	if c.Endpoint != "" {
		config.Endpoint = aws.String(c.Endpoint)
	}
	if c.ForcePathStyle {
		config.S3ForcePathStyle = aws.Bool(c.ForcePathStyle)
	}
	if c.SkipVerify {
		config.HTTPClient = &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}}
	}

	return config
}

func (c *ReplicaClient) findBucketRegion(ctx context.Context, bucket string) (string, error) {
	// Connect to US standard region to fetch info.
	config := c.config()
	config.Region = aws.String(DefaultRegion)
	sess, err := session.NewSession(config)
	if err != nil {
		return "", err
	}

	// Fetch bucket location, if possible. Must be bucket owner.
	// This call can return a nil location which means it's in us-east-1.
	if out, err := s3.New(sess).GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		return "", err
	} else if out.LocationConstraint != nil {
		return *out.LocationConstraint, nil
	}
	return DefaultRegion, nil
}

// DeleteAll deletes all LTX files.
func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	// Collect all files.
	var objIDs []*s3.ObjectIdentifier
	if err := c.s3.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(c.Bucket),
		Prefix: aws.String(c.Path),
	}, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		for _, obj := range page.Contents {
			objIDs = append(objIDs, &s3.ObjectIdentifier{Key: obj.Key})
		}
		return true
	}); err != nil {
		return err
	}

	// Delete all files in batches.
	for len(objIDs) > 0 {
		n := MaxKeys
		if len(objIDs) < n {
			n = len(objIDs)
		}

		out, err := c.s3.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.Bucket),
			Delete: &s3.Delete{Objects: objIDs[:n], Quiet: aws.Bool(true)},
		})
		if err != nil {
			return err
		}
		if err := deleteOutputError(out); err != nil {
			return err
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

		objIDs = objIDs[n:]
	}

	// log.Printf("%s(%s): retainer: deleting: %s", r.db.Path(), r.Name())

	return nil
}

// LTXFiles returns an iterator over all available LTX files.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int) (ltx.FileIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	return newLTXFileIterator(ctx, c, level), nil
}

// WriteLTXFile writes an LTX file from rd into a remote location.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, rd io.Reader) (info *ltx.FileInfo, err error) {
	if err := c.Init(ctx); err != nil {
		return info, err
	}

	key := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)
	startTime := time.Now()

	rc := internal.NewReadCounter(rd)
	if _, err := c.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
		Body:   rc,
	}); err != nil {
		return info, err
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(rc.N()))

	return &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      rc.N(),
		CreatedAt: startTime.UTC(),
	}, nil
}

// OpenLTXFile returns a reader for an LTX file
// Returns os.ErrNotExist if no matching index/offset is found.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	key := litestream.LTXFilePath(c.Path, level, minTXID, maxTXID)
	out, err := c.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
	})
	if isNotExists(err) {
		return nil, os.ErrNotExist
	} else if err != nil {
		return nil, err
	}
	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "GET").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "GET").Add(float64(aws.Int64Value(out.ContentLength)))

	return out.Body, nil
}

// DeleteLTXFiles deletes a set of LTX files.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	objIDs := make([]*s3.ObjectIdentifier, MaxKeys)
	for len(a) > 0 {
		n := MaxKeys
		if len(a) < n {
			n = len(a)
		}

		// Generate a batch of object IDs for deleting the LTX files.
		for i, info := range a[:n] {
			key := litestream.LTXFilePath(c.Path, info.Level, info.MinTXID, info.MaxTXID)
			objIDs[i] = &s3.ObjectIdentifier{Key: &key}
		}

		// Delete S3 objects in bulk.
		out, err := c.s3.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.Bucket),
			Delete: &s3.Delete{Objects: objIDs[:n], Quiet: aws.Bool(true)},
		})
		if err != nil {
			return err
		}
		if err := deleteOutputError(out); err != nil {
			return err
		}
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "DELETE").Inc()

		a = a[n:]
	}

	return nil
}

type ltxFileIterator struct {
	client *ReplicaClient
	level  int

	ch     chan *ltx.FileInfo
	g      errgroup.Group
	ctx    context.Context
	cancel func()

	info *ltx.FileInfo
	err  error
}

func newLTXFileIterator(ctx context.Context, client *ReplicaClient, level int) *ltxFileIterator {
	itr := &ltxFileIterator{
		client: client,
		level:  level,
		ch:     make(chan *ltx.FileInfo),
	}

	itr.ctx, itr.cancel = context.WithCancel(ctx)
	itr.g.Go(itr.fetch)

	return itr
}

// fetch runs in a separate goroutine to fetch pages of objects and stream them to a channel.
func (itr *ltxFileIterator) fetch() error {
	defer close(itr.ch)

	dir := litestream.LTXLevelDir(itr.client.Path, itr.level)

	return itr.client.s3.ListObjectsPagesWithContext(itr.ctx, &s3.ListObjectsInput{
		Bucket:    aws.String(itr.client.Bucket),
		Prefix:    aws.String(dir + "/"),
		Delimiter: aws.String("/"),
	}, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "LIST").Inc()

		for _, obj := range page.Contents {
			key := path.Base(aws.StringValue(obj.Key))
			minTXID, maxTXID, err := ltx.ParseFilename(key)
			if err != nil {
				continue
			}

			info := &ltx.FileInfo{
				Level:     itr.level,
				MinTXID:   minTXID,
				MaxTXID:   maxTXID,
				Size:      aws.Int64Value(obj.Size),
				CreatedAt: obj.LastModified.UTC(),
			}

			select {
			case <-itr.ctx.Done():
				return false
			case itr.ch <- info:
			}
		}
		return true
	})
}

func (itr *ltxFileIterator) Close() (err error) {
	err = itr.err

	// Cancel context and wait for error group to finish.
	itr.cancel()
	if e := itr.g.Wait(); e != nil && err == nil {
		err = e
	}

	return err
}

func (itr *ltxFileIterator) Next() bool {
	// Exit if an error has already occurred.
	if itr.err != nil {
		return false
	}

	// Return false if context was canceled or if there are no more segments.
	// Otherwise fetch the next segment and store it on the iterator.
	select {
	case <-itr.ctx.Done():
		return false
	case info, ok := <-itr.ch:
		if !ok {
			return false
		}
		itr.info = info
		return true
	}
}

func (itr *ltxFileIterator) Err() error { return itr.err }

func (itr *ltxFileIterator) Item() *ltx.FileInfo {
	return itr.info
}

// ParseHost extracts data from a hostname depending on the service provider.
func ParseHost(s string) (bucket, region, endpoint string, forcePathStyle bool) {
	// Extract port if one is specified.
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		host = s
	}

	// Default to path-based URLs, except for with AWS S3 itself.
	forcePathStyle = true

	// Extract fields from provider-specific host formats.
	scheme := "https"
	if a := localhostRegex.FindStringSubmatch(host); a != nil {
		bucket, region = a[1], "us-east-1"
		scheme, endpoint = "http", "localhost"
	} else if a := backblazeRegex.FindStringSubmatch(host); a != nil {
		bucket, region = a[1], a[2]
		endpoint = fmt.Sprintf("s3.%s.backblazeb2.com", region)
	} else if a := filebaseRegex.FindStringSubmatch(host); a != nil {
		bucket, endpoint = a[1], "s3.filebase.com"
	} else if a := digitalOceanRegex.FindStringSubmatch(host); a != nil {
		bucket, region = a[1], a[2]
		endpoint = fmt.Sprintf("%s.digitaloceanspaces.com", region)
	} else if a := scalewayRegex.FindStringSubmatch(host); a != nil {
		bucket, region = a[1], a[2]
		endpoint = fmt.Sprintf("s3.%s.scw.cloud", region)
	} else if a := linodeRegex.FindStringSubmatch(host); a != nil {
		bucket, region = a[1], a[2]
		endpoint = fmt.Sprintf("%s.linodeobjects.com", region)
	} else {
		bucket = host
		forcePathStyle = false
	}

	// Add port back to endpoint, if available.
	if endpoint != "" && port != "" {
		endpoint = net.JoinHostPort(endpoint, port)
	}

	// Prepend scheme to endpoint.
	if endpoint != "" {
		endpoint = scheme + "://" + endpoint
	}

	return bucket, region, endpoint, forcePathStyle
}

var (
	localhostRegex    = regexp.MustCompile(`^(?:(.+)\.)?localhost$`)
	backblazeRegex    = regexp.MustCompile(`^(?:(.+)\.)?s3.([^.]+)\.backblazeb2.com$`)
	filebaseRegex     = regexp.MustCompile(`^(?:(.+)\.)?s3.filebase.com$`)
	digitalOceanRegex = regexp.MustCompile(`^(?:(.+)\.)?([^.]+)\.digitaloceanspaces.com$`)
	scalewayRegex     = regexp.MustCompile(`^(?:(.+)\.)?s3.([^.]+)\.scw\.cloud$`)
	linodeRegex       = regexp.MustCompile(`^(?:(.+)\.)?([^.]+)\.linodeobjects.com$`)
)

func isNotExists(err error) bool {
	switch err := err.(type) {
	case awserr.Error:
		return err.Code() == `NoSuchKey`
	default:
		return false
	}
}

func deleteOutputError(out *s3.DeleteObjectsOutput) error {
	switch len(out.Errors) {
	case 0:
		return nil
	case 1:
		return fmt.Errorf("deleting object %s: %s - %s", aws.StringValue(out.Errors[0].Key), aws.StringValue(out.Errors[0].Code), aws.StringValue(out.Errors[0].Message))
	default:
		return fmt.Errorf("%d errors occurred deleting objects, %s: %s - (%s (and %d others)",
			len(out.Errors), aws.StringValue(out.Errors[0].Key), aws.StringValue(out.Errors[0].Code), aws.StringValue(out.Errors[0].Message), len(out.Errors)-1)
	}
}
