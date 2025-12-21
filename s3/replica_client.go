package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyxml "github.com/aws/smithy-go/encoding/xml"
	"github.com/aws/smithy-go/middleware"
	smithytime "github.com/aws/smithy-go/time"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

func init() {
	litestream.RegisterReplicaClientFactory("s3", NewReplicaClientFromURL)
}

// ReplicaClientType is the client type for this package.
const ReplicaClientType = "s3"

// MetadataKeyTimestamp is the metadata key for storing LTX file timestamps in S3.
const MetadataKeyTimestamp = "litestream-timestamp"

// MaxKeys is the number of keys S3 can operate on per batch.
const MaxKeys = 1000

// DefaultRegion is the region used if one is not specified.
const DefaultRegion = "us-east-1"

// contentMD5StackKey is used to pass the precomputed Content-MD5 checksum
// through the middleware stack from Serialize to Finalize phase.
type contentMD5StackKey struct{}

var _ litestream.ReplicaClient = (*ReplicaClient)(nil)

// ReplicaClient is a client for writing LTX files to S3.
type ReplicaClient struct {
	mu       sync.Mutex
	s3       *s3.Client // s3 service
	uploader *manager.Uploader
	logger   *slog.Logger

	// AWS authentication keys.
	AccessKeyID     string
	SecretAccessKey string

	// S3 bucket information
	Region            string
	Bucket            string
	Path              string
	Endpoint          string
	ForcePathStyle    bool
	SkipVerify        bool
	SignPayload       bool
	RequireContentMD5 bool

	// Upload configuration
	PartSize    int64 // Part size for multipart uploads (default: 5MB)
	Concurrency int   // Number of concurrent parts to upload (default: 5)
}

// NewReplicaClient returns a new instance of ReplicaClient.
func NewReplicaClient() *ReplicaClient {
	return &ReplicaClient{
		logger:            slog.Default().WithGroup(ReplicaClientType),
		RequireContentMD5: true,
		SignPayload:       true,
	}
}

// NewReplicaClientFromURL creates a new ReplicaClient from URL components.
// This is used by the replica client factory registration.
func NewReplicaClientFromURL(scheme, host, urlPath string, query url.Values, userinfo *url.Userinfo) (litestream.ReplicaClient, error) {
	client := NewReplicaClient()

	var (
		bucket         string
		region         string
		endpoint       string
		forcePathStyle bool
		skipVerify     bool
		signPayload    bool
		signPayloadSet bool
		requireMD5     bool
		requireMD5Set  bool
	)

	// Parse host for bucket and region
	if strings.HasPrefix(host, "arn:") {
		bucket = host
		region = litestream.RegionFromS3ARN(host)
	} else {
		bucket, region, endpoint, forcePathStyle = ParseHost(host)
	}

	// Override with query parameters if provided
	if qEndpoint := query.Get("endpoint"); qEndpoint != "" {
		// Ensure endpoint has a scheme
		if !strings.HasPrefix(qEndpoint, "http://") && !strings.HasPrefix(qEndpoint, "https://") {
			qEndpoint = "http://" + qEndpoint
		}
		endpoint = qEndpoint
		// Default to path style for custom endpoints unless explicitly set to false
		if query.Get("forcePathStyle") != "false" {
			forcePathStyle = true
		}
	}
	if qRegion := query.Get("region"); qRegion != "" {
		region = qRegion
	}
	if qForcePathStyle := query.Get("forcePathStyle"); qForcePathStyle != "" {
		forcePathStyle = qForcePathStyle == "true"
	}
	if qSkipVerify := query.Get("skipVerify"); qSkipVerify != "" {
		skipVerify = qSkipVerify == "true"
	}
	if v, ok := litestream.BoolQueryValue(query, "signPayload", "sign-payload"); ok {
		signPayload = v
		signPayloadSet = true
	}
	if v, ok := litestream.BoolQueryValue(query, "requireContentMD5", "require-content-md5"); ok {
		requireMD5 = v
		requireMD5Set = true
	}

	// Ensure bucket is set
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for s3 replica URL")
	}

	// Detect S3-compatible provider endpoints for applying appropriate defaults.
	isTigris := litestream.IsTigrisEndpoint(endpoint)
	isDigitalOcean := litestream.IsDigitalOceanEndpoint(endpoint)
	isBackblaze := litestream.IsBackblazeEndpoint(endpoint)
	isFilebase := litestream.IsFilebaseEndpoint(endpoint)
	isScaleway := litestream.IsScalewayEndpoint(endpoint)
	isCloudflareR2 := litestream.IsCloudflareR2Endpoint(endpoint)
	isMinIO := litestream.IsMinIOEndpoint(endpoint)

	// Track if forcePathStyle was explicitly set via query parameter.
	forcePathStyleSet := query.Get("forcePathStyle") != ""

	// Read authentication from environment variables
	if v := os.Getenv("AWS_ACCESS_KEY_ID"); v != "" {
		client.AccessKeyID = v
	} else if v := os.Getenv("LITESTREAM_ACCESS_KEY_ID"); v != "" {
		client.AccessKeyID = v
	}
	if v := os.Getenv("AWS_SECRET_ACCESS_KEY"); v != "" {
		client.SecretAccessKey = v
	} else if v := os.Getenv("LITESTREAM_SECRET_ACCESS_KEY"); v != "" {
		client.SecretAccessKey = v
	}

	// Apply provider-specific defaults for S3-compatible providers.
	if isTigris {
		// Tigris: requires signed payloads, no MD5
		if !signPayloadSet {
			signPayload, signPayloadSet = true, true
		}
		if !requireMD5Set {
			requireMD5, requireMD5Set = false, true
		}
	}
	if isDigitalOcean || isBackblaze || isFilebase || isScaleway || isCloudflareR2 || isMinIO {
		// All these providers require signed payloads (don't support UNSIGNED-PAYLOAD)
		if !signPayloadSet {
			signPayload, signPayloadSet = true, true
		}
	}
	if !forcePathStyleSet {
		// Filebase, Backblaze B2, and MinIO require path-style URLs
		if isFilebase || isBackblaze || isMinIO {
			forcePathStyle = true
		}
	}

	// Configure client
	client.Bucket = bucket
	client.Path = urlPath
	client.Region = region
	client.Endpoint = endpoint
	client.ForcePathStyle = forcePathStyle
	client.SkipVerify = skipVerify

	if signPayloadSet {
		client.SignPayload = signPayload
	}
	if requireMD5Set {
		client.RequireContentMD5 = requireMD5
	}

	return client, nil
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

	// Validate required configuration
	if c.Bucket == "" {
		return fmt.Errorf("s3: bucket name is required")
	}

	// Look up region if not specified and no endpoint is used.
	// Endpoints are typically used for non-S3 object stores and do not
	// necessarily require a region.
	region := c.Region
	if region == "" {
		if c.Endpoint == "" {
			if region, err = c.findBucketRegion(ctx, c.Bucket); err != nil {
				return fmt.Errorf("s3: cannot lookup bucket region: %w", err)
			}
		} else {
			region = DefaultRegion // default for non-S3 object stores
		}
	}

	// Create HTTP client with 24 hour timeout for long-running operations
	httpClient := &http.Client{
		Timeout: 24 * time.Hour,
	}

	// Configure transport for insecure connections if needed
	if c.SkipVerify {
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

	// Build configuration options
	configOpts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
		// Use adaptive retry mode for better resilience with 24 hour timeout
		// This matches Azure's approach for long-running operations
		config.WithRetryMode(aws.RetryModeAdaptive),
		config.WithRetryMaxAttempts(10), // Increase retry attempts for resilience
	}

	// Add HTTP client with proper timeout
	configOpts = append(configOpts, config.WithHTTPClient(httpClient))

	// Add static credentials if provided, otherwise use default credential chain
	// Default credential chain includes:
	// - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
	// - Shared credentials file (~/.aws/credentials)
	// - EC2 Instance Profile credentials
	// - ECS Task Role credentials
	// - Web Identity Token credentials (for EKS)
	if c.AccessKeyID != "" && c.SecretAccessKey != "" {
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, ""),
		))
	}

	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return fmt.Errorf("s3: cannot load aws config: %w", err)
	}

	// Create S3 client options
	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = c.ForcePathStyle
			o.UseARNRegion = true
			// Add User-Agent and optional middleware.
			o.APIOptions = append(o.APIOptions, c.middlewareOption())
		},
	}

	// S3-compatible providers (Tigris, Backblaze B2, MinIO, Filebase, etc.) don't
	// support aws-chunked content encoding used by default checksum calculation
	// in AWS SDK Go v2 v1.73.0+. Disable automatic checksum calculation for all
	// custom endpoints.
	// See: https://github.com/benbjohnson/litestream/issues/918
	if c.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		})
	}

	// Add custom endpoint if specified
	c.configureEndpoint(&s3Opts)

	// Create S3 client
	c.s3 = s3.NewFromConfig(cfg, s3Opts...)

	// Configure uploader with custom options if specified
	uploaderOpts := []func(*manager.Uploader){}
	if c.PartSize > 0 {
		uploaderOpts = append(uploaderOpts, func(u *manager.Uploader) {
			u.PartSize = c.PartSize
		})
	}
	if c.Concurrency > 0 {
		uploaderOpts = append(uploaderOpts, func(u *manager.Uploader) {
			u.Concurrency = c.Concurrency
		})
	}
	c.uploader = manager.NewUploader(c.s3, uploaderOpts...)

	return nil
}

// configureEndpoint adds custom endpoint configuration to S3 client options if needed.
func (c *ReplicaClient) configureEndpoint(opts *[]func(*s3.Options)) {
	if c.Endpoint != "" {
		*opts = append(*opts, func(o *s3.Options) {
			o.UsePathStyle = c.ForcePathStyle

			endpoint := c.Endpoint
			// Add scheme if not present
			if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
				endpoint = "https://" + endpoint
			}

			o.BaseEndpoint = aws.String(endpoint)
			// For MinIO and other S3-compatible services
			if strings.HasPrefix(endpoint, "http://") {
				o.EndpointOptions.DisableHTTPS = true
			}
		})
	}
}

// findBucketRegion looks up the AWS region for a bucket. Returns blank if non-S3.
func (c *ReplicaClient) findBucketRegion(ctx context.Context, bucket string) (string, error) {
	// Build a config with credentials but no region
	configOpts := []func(*config.LoadOptions) error{}

	// Add static credentials if provided
	if c.AccessKeyID != "" && c.SecretAccessKey != "" {
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, ""),
		))
	}

	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return "", fmt.Errorf("s3: cannot load aws config for region lookup: %w", err)
	}

	// Use default region for initial region lookup
	cfg.Region = DefaultRegion

	// Create S3 client options
	s3Opts := []func(*s3.Options){}

	// Configure custom endpoint for region lookup
	c.configureEndpoint(&s3Opts)

	client := s3.NewFromConfig(cfg, s3Opts...)

	// Get bucket location
	out, err := client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", err
	}

	// Convert location constraint to region
	if out.LocationConstraint == "" {
		return DefaultRegion, nil
	}
	return string(out.LocationConstraint), nil
}

// LTXFiles returns an iterator over all LTX files on the replica for the given level.
// When useMetadata is true, fetches accurate timestamps from S3 metadata via HeadObject.
// When false, uses fast LastModified timestamps from LIST operation.
func (c *ReplicaClient) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}
	return newFileIterator(ctx, c, level, seek, useMetadata), nil
}

// OpenLTXFile returns a reader for an LTX file
// Returns os.ErrNotExist if no matching index/offset is found.
func (c *ReplicaClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	var rangeStr string
	if size > 0 {
		rangeStr = fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
	} else {
		rangeStr = fmt.Sprintf("bytes=%d-", offset)
	}

	// Build the key from the file info
	filename := ltx.FormatFilename(minTXID, maxTXID)
	key := c.Path + "/" + fmt.Sprintf("%04x/%s", level, filename)
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.Bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		if isNotExists(err) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("s3: get object %s: %w", key, err)
	}
	return out.Body, nil
}

// WriteLTXFile writes an LTX file to the replica.
// Extracts timestamp from LTX header and stores it in S3 metadata to preserve original creation time.
func (c *ReplicaClient) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if err := c.Init(ctx); err != nil {
		return nil, err
	}

	// Use TeeReader to peek at LTX header while preserving data for upload
	var buf bytes.Buffer
	teeReader := io.TeeReader(r, &buf)

	// Extract timestamp from LTX header
	hdr, _, err := ltx.PeekHeader(teeReader)
	if err != nil {
		return nil, fmt.Errorf("extract timestamp from LTX header: %w", err)
	}
	timestamp := time.UnixMilli(hdr.Timestamp).UTC()

	// Combine buffered data with rest of reader
	rc := internal.NewReadCounter(io.MultiReader(&buf, r))

	filename := ltx.FormatFilename(minTXID, maxTXID)
	key := c.Path + "/" + fmt.Sprintf("%04x/%s", level, filename)

	// Store timestamp in S3 metadata for accurate timestamp retrieval
	metadata := map[string]string{
		MetadataKeyTimestamp: timestamp.Format(time.RFC3339Nano),
	}

	out, err := c.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(c.Bucket),
		Key:      aws.String(key),
		Body:     rc,
		Metadata: metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("s3: upload to %s: %w", key, err)
	}

	// Build file info from the uploaded file
	info := &ltx.FileInfo{
		Level:     level,
		MinTXID:   minTXID,
		MaxTXID:   maxTXID,
		Size:      rc.N(),
		CreatedAt: timestamp,
	}

	internal.OperationTotalCounterVec.WithLabelValues(ReplicaClientType, "PUT").Inc()
	internal.OperationBytesCounterVec.WithLabelValues(ReplicaClientType, "PUT").Add(float64(rc.N()))

	// ETag indicates successful upload
	if out.ETag == nil {
		return nil, fmt.Errorf("s3: upload failed: no ETag returned")
	}

	return info, nil
}

func (c *ReplicaClient) middlewareOption() func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		if c.RequireContentMD5 {
			if err := stack.Serialize.Add(
				middleware.SerializeMiddlewareFunc(
					"LitestreamComputeDeleteContentMD5",
					func(ctx context.Context, in middleware.SerializeInput, next middleware.SerializeHandler) (
						out middleware.SerializeOutput, metadata middleware.Metadata, err error,
					) {
						if middleware.GetOperationName(ctx) != "DeleteObjects" {
							return next.HandleSerialize(ctx, in)
						}

						input, ok := in.Parameters.(*s3.DeleteObjectsInput)
						if !ok || input == nil || input.Delete == nil || len(input.Delete.Objects) == 0 {
							return next.HandleSerialize(ctx, in)
						}

						checksum, err := computeDeleteObjectsContentMD5(input.Delete)
						if err != nil {
							return out, metadata, err
						}
						if checksum != "" {
							ctx = middleware.WithStackValue(ctx, contentMD5StackKey{}, checksum)
						}

						return next.HandleSerialize(ctx, in)
					},
				),
				middleware.Before,
			); err != nil {
				return err
			}
		}

		if err := stack.Build.Add(
			middleware.BuildMiddlewareFunc(
				"LitestreamUserAgent",
				func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (
					out middleware.BuildOutput, metadata middleware.Metadata, err error,
				) {
					if req, ok := in.Request.(*smithyhttp.Request); ok {
						current := req.Header.Get("User-Agent")
						if current == "" {
							req.Header.Set("User-Agent", "litestream")
						} else if !strings.Contains(current, "litestream") {
							req.Header.Set("User-Agent", "litestream "+current)
						}
					}
					return next.HandleBuild(ctx, in)
				},
			),
			middleware.After,
		); err != nil {
			return err
		}

		if litestream.IsTigrisEndpoint(c.Endpoint) {
			if err := stack.Build.Add(
				middleware.BuildMiddlewareFunc(
					"LitestreamTigrisConsistent",
					func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (
						out middleware.BuildOutput, metadata middleware.Metadata, err error,
					) {
						if req, ok := in.Request.(*smithyhttp.Request); ok {
							req.Header.Set("X-Tigris-Consistent", "true")
						}
						return next.HandleBuild(ctx, in)
					},
				),
				middleware.After,
			); err != nil {
				return err
			}
		}

		// Many S3-compatible providers (e.g. Filebase) do not support SigV4
		// payload hashing. Switching to unsigned payload matches the behavior
		// of the AWS SDK v1 client used in Litestream v0.3.x and restores
		// compatibility.
		if !c.SignPayload {
			_ = v4.RemoveComputePayloadSHA256Middleware(stack)
			if err := v4.AddUnsignedPayloadMiddleware(stack); err != nil {
				return err
			}
			_ = v4.RemoveContentSHA256HeaderMiddleware(stack)
			if err := v4.AddContentSHA256HeaderMiddleware(stack); err != nil {
				return err
			}
		}

		// Disable AWS SDK v2's trailing checksum middleware which uses
		// aws-chunked encoding. This is required for:
		// 1. UNSIGNED-PAYLOAD requests (aws-chunked + UNSIGNED-PAYLOAD is rejected by AWS)
		// 2. S3-compatible providers (Filebase, MinIO, Backblaze B2, etc.) that don't
		//    support aws-chunked encoding at all
		// See: https://github.com/aws/aws-sdk-go-v2/discussions/2960
		// See: https://github.com/benbjohnson/litestream/issues/895
		if !c.SignPayload || c.Endpoint != "" {
			stack.Finalize.Remove("addInputChecksumTrailer")
		}

		if !c.RequireContentMD5 {
			return nil
		}

		md5Middleware := func() middleware.FinalizeMiddleware {
			return middleware.FinalizeMiddlewareFunc(
				"LitestreamDeleteContentMD5",
				func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
					out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
				) {
					if middleware.GetOperationName(ctx) != "DeleteObjects" {
						return next.HandleFinalize(ctx, in)
					}

					checksum, _ := middleware.GetStackValue(ctx, contentMD5StackKey{}).(string)
					if checksum == "" {
						return next.HandleFinalize(ctx, in)
					}

					req, ok := in.Request.(*smithyhttp.Request)
					if !ok {
						return next.HandleFinalize(ctx, in)
					}
					if req.Header.Get("Content-MD5") == "" {
						req.Header.Set("Content-MD5", checksum)
					}

					return next.HandleFinalize(ctx, in)
				},
			)
		}

		// Try to insert before AWS's checksum middleware for optimal ordering.
		// If that middleware doesn't exist (e.g., different SDK version), add at the end.
		// Our middleware checks if Content-MD5 is already set, so order is not critical.
		if err := stack.Finalize.Insert(md5Middleware(), "AWSChecksum:ComputeInputPayloadChecksum", middleware.Before); err != nil {
			if err := stack.Finalize.Add(md5Middleware(), middleware.After); err != nil {
				return err
			}
		}

		return nil
	}
}

func computeDeleteObjectsContentMD5(deleteInput *types.Delete) (string, error) {
	if deleteInput == nil {
		return "", nil
	}

	payload, err := marshalDeleteObjects(deleteInput)
	if err != nil {
		return "", err
	}
	if len(payload) == 0 {
		return "", nil
	}

	sum := md5.Sum(payload)
	return base64.StdEncoding.EncodeToString(sum[:]), nil
}

func marshalDeleteObjects(deleteInput *types.Delete) ([]byte, error) {
	if deleteInput == nil {
		return nil, nil
	}

	var buf bytes.Buffer
	encoder := smithyxml.NewEncoder(&buf)
	root := smithyxml.StartElement{
		Name: smithyxml.Name{
			Local: "Delete",
		},
		Attr: []smithyxml.Attr{
			smithyxml.NewNamespaceAttribute("", "http://s3.amazonaws.com/doc/2006-03-01/"),
		},
	}

	if err := encodeDeleteDocument(deleteInput, encoder.RootElement(root)); err != nil {
		return nil, err
	}

	return encoder.Bytes(), nil
}

func encodeDeleteDocument(v *types.Delete, value smithyxml.Value) error {
	defer value.Close()
	if v.Objects != nil {
		root := smithyxml.StartElement{
			Name: smithyxml.Name{
				Local: "Object",
			},
		}
		el := value.FlattenedElement(root)
		if err := encodeObjectIdentifierList(v.Objects, el); err != nil {
			return err
		}
	}
	if v.Quiet != nil {
		root := smithyxml.StartElement{
			Name: smithyxml.Name{
				Local: "Quiet",
			},
		}
		el := value.MemberElement(root)
		el.Boolean(*v.Quiet)
	}
	return nil
}

func encodeObjectIdentifierList(v []types.ObjectIdentifier, value smithyxml.Value) error {
	if !value.IsFlattened() {
		defer value.Close()
	}

	array := value.Array()
	for i := range v {
		member := array.Member()
		if err := encodeObjectIdentifier(&v[i], member); err != nil {
			return err
		}
	}
	return nil
}

// encodeObjectIdentifier mirrors the AWS SDK's XML serializer for DeleteObjects.
// This ensures our precomputed Content-MD5 matches the actual request body.
// Includes all ObjectIdentifier fields as of AWS SDK v2 (2024).
func encodeObjectIdentifier(v *types.ObjectIdentifier, value smithyxml.Value) error {
	defer value.Close()
	if v.ETag != nil {
		el := value.MemberElement(smithyxml.StartElement{
			Name: smithyxml.Name{
				Local: "ETag",
			},
		})
		el.String(*v.ETag)
	}
	if v.Key != nil {
		el := value.MemberElement(smithyxml.StartElement{
			Name: smithyxml.Name{
				Local: "Key",
			},
		})
		el.String(*v.Key)
	}
	if v.LastModifiedTime != nil {
		el := value.MemberElement(smithyxml.StartElement{
			Name: smithyxml.Name{
				Local: "LastModifiedTime",
			},
		})
		el.String(smithytime.FormatHTTPDate(*v.LastModifiedTime))
	}
	if v.Size != nil {
		el := value.MemberElement(smithyxml.StartElement{
			Name: smithyxml.Name{
				Local: "Size",
			},
		})
		el.Long(*v.Size)
	}
	if v.VersionId != nil {
		el := value.MemberElement(smithyxml.StartElement{
			Name: smithyxml.Name{
				Local: "VersionId",
			},
		})
		el.String(*v.VersionId)
	}
	return nil
}

// DeleteLTXFiles deletes one or more LTX files.
func (c *ReplicaClient) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	if len(a) == 0 {
		return nil
	}

	// Convert file infos to object identifiers
	objIDs := make([]types.ObjectIdentifier, 0, len(a))
	for _, info := range a {
		filename := ltx.FormatFilename(info.MinTXID, info.MaxTXID)
		key := c.Path + "/" + fmt.Sprintf("%04x/%s", info.Level, filename)
		objIDs = append(objIDs, types.ObjectIdentifier{Key: aws.String(key)})

		c.logger.Debug("deleting ltx file", "level", info.Level, "minTXID", info.MinTXID, "maxTXID", info.MaxTXID, "key", key)
	}

	// Delete in batches
	for len(objIDs) > 0 {
		n := min(len(objIDs), MaxKeys)

		out, err := c.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.Bucket),
			Delete: &types.Delete{Objects: objIDs[:n], Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("s3: delete batch of %d objects: %w", n, err)
		} else if err := deleteOutputError(out); err != nil {
			return err
		}

		objIDs = objIDs[n:]
	}

	return nil
}

// DeleteAll deletes all files.
func (c *ReplicaClient) DeleteAll(ctx context.Context) error {
	if err := c.Init(ctx); err != nil {
		return err
	}

	var objIDs []types.ObjectIdentifier

	// Create paginator for listing objects
	paginator := s3.NewListObjectsV2Paginator(c.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.Bucket),
		Prefix: aws.String(c.Path + "/"),
	})

	// Iterate through all pages
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("s3: list objects page: %w", err)
		}

		// Collect object identifiers
		for _, obj := range page.Contents {
			objIDs = append(objIDs, types.ObjectIdentifier{Key: obj.Key})
		}
	}

	// Delete all collected objects in batches
	for len(objIDs) > 0 {
		n := min(len(objIDs), MaxKeys)

		out, err := c.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.Bucket),
			Delete: &types.Delete{Objects: objIDs[:n], Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("s3: delete all batch of %d objects: %w", n, err)
		} else if err := deleteOutputError(out); err != nil {
			return err
		}

		objIDs = objIDs[n:]
	}

	return nil
}

// fileIterator represents an iterator over LTX files in S3.
type fileIterator struct {
	ctx         context.Context
	cancel      context.CancelFunc
	client      *ReplicaClient
	level       int
	seek        ltx.TXID
	useMetadata bool // When true, fetch accurate timestamps from metadata

	paginator *s3.ListObjectsV2Paginator
	page      *s3.ListObjectsV2Output
	pageIndex int

	closed bool
	err    error
	info   *ltx.FileInfo
}

func newFileIterator(ctx context.Context, client *ReplicaClient, level int, seek ltx.TXID, useMetadata bool) *fileIterator {
	ctx, cancel := context.WithCancel(ctx)

	itr := &fileIterator{
		ctx:         ctx,
		cancel:      cancel,
		client:      client,
		level:       level,
		seek:        seek,
		useMetadata: useMetadata,
	}

	// Create paginator for listing objects with level prefix
	prefix := client.Path + "/" + fmt.Sprintf("%04x/", level)
	itr.paginator = s3.NewListObjectsV2Paginator(client.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(client.Bucket),
		Prefix: aws.String(prefix),
	})

	return itr
}

// Close stops iteration.
func (itr *fileIterator) Close() (err error) {
	itr.closed = true
	itr.cancel()
	return nil
}

// Next returns the next file. Returns false when no more files are available.
func (itr *fileIterator) Next() bool {
	if itr.closed || itr.err != nil {
		return false
	}

	// Process objects until we find a valid LTX file
	for {
		// Load next page if needed
		if itr.page == nil || itr.pageIndex >= len(itr.page.Contents) {
			if !itr.paginator.HasMorePages() {
				return false
			}

			var err error
			itr.page, err = itr.paginator.NextPage(itr.ctx)
			if err != nil {
				itr.err = err
				return false
			}
			itr.pageIndex = 0
		}

		// Process current object
		if itr.pageIndex < len(itr.page.Contents) {
			obj := itr.page.Contents[itr.pageIndex]
			itr.pageIndex++

			// Extract file info from key
			key := path.Base(aws.ToString(obj.Key))
			minTXID, maxTXID, err := ltx.ParseFilename(key)
			if err != nil {
				continue // Skip non-LTX files
			}

			// Build file info
			info := &ltx.FileInfo{
				Level:   itr.level,
				MinTXID: minTXID,
				MaxTXID: maxTXID,
			}

			// Skip if below seek TXID
			if info.MinTXID < itr.seek {
				continue
			}

			// Skip if wrong level
			if info.Level != itr.level {
				continue
			}

			// Set file info
			info.Size = aws.ToInt64(obj.Size)

			// Use fast LastModified timestamp by default
			createdAt := aws.ToTime(obj.LastModified).UTC()

			// Only fetch accurate timestamp from metadata when requested (timestamp-based restore)
			if itr.useMetadata {
				head, err := itr.client.s3.HeadObject(itr.ctx, &s3.HeadObjectInput{
					Bucket: aws.String(itr.client.Bucket),
					Key:    obj.Key,
				})
				if err != nil {
					itr.err = fmt.Errorf("fetch object metadata: %w", err)
					return false
				}

				if head.Metadata != nil {
					if ts, ok := head.Metadata[MetadataKeyTimestamp]; ok {
						if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
							createdAt = parsed
						} else {
							itr.err = fmt.Errorf("parse timestamp from metadata: %w", err)
							return false
						}
					}
				}
			}

			info.CreatedAt = createdAt
			itr.info = info
			return true
		}
	}
}

// Item returns the metadata for the current file.
func (itr *fileIterator) Item() *ltx.FileInfo {
	return itr.info
}

// Err returns any error that occurred during iteration.
func (itr *fileIterator) Err() error {
	return itr.err
}

// ParseURL parses an S3 URL into its host and path parts.
// If endpoint is set, it can override the host.
func ParseURL(s, endpoint string) (bucket, region, key string, err error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", "", "", err
	}

	if u.Scheme != "s3" {
		return "", "", "", fmt.Errorf("s3: invalid url scheme")
	}

	// Special handling for filebase.com
	if u.Host == "filebase.com" {
		parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
		if len(parts) == 0 {
			return "", "", "", fmt.Errorf("s3: bucket required")
		}
		bucket = parts[0]
		if len(parts) > 1 {
			key = parts[1]
		}
		return bucket, "", key, nil
	}

	// For other hosts, check if it's a special endpoint
	bucket, region, _, _ = ParseHost(u.Host)
	if bucket == "" {
		bucket = u.Host
	}

	key = strings.TrimPrefix(u.Path, "/")
	return bucket, region, key, nil
}

// ParseHost parses the host/endpoint for an S3-like storage system.
// Endpoints: https://docs.aws.amazon.com/general/latest/gr/s3.html
func ParseHost(host string) (bucket, region, endpoint string, forcePathStyle bool) {
	// Check for MinIO-style hosts (bucket.host:port)
	if strings.Contains(host, ":") && !strings.Contains(host, ".com") {
		parts := strings.SplitN(host, ".", 2)
		if len(parts) == 2 {
			// Extract bucket from bucket.host:port format
			bucket = parts[0]
			endpoint = "http://" + parts[1]
			return bucket, DefaultRegion, endpoint, true
		}
		// No bucket in host, just host:port
		return "", "", "http://" + host, true
	}

	// Check common object storage providers
	// Check for AWS S3 URLs first
	if a := awsS3Regex.FindStringSubmatch(host); len(a) > 1 {
		bucket = a[1]
		if len(a) > 2 && a[2] != "" {
			region = a[2]
		}
		return bucket, region, "", false
	} else if a := digitaloceanRegex.FindStringSubmatch(host); len(a) > 1 {
		bucket = a[1]
		region = a[2]
		return bucket, region, fmt.Sprintf("https://%s.digitaloceanspaces.com", region), false
	} else if a := backblazeRegex.FindStringSubmatch(host); len(a) > 1 {
		region = a[2]
		bucket = a[1]
		endpoint = fmt.Sprintf("https://s3.%s.backblazeb2.com", region)
		return bucket, region, endpoint, true
	} else if a := filebaseRegex.FindStringSubmatch(host); len(a) > 1 {
		bucket = a[1]
		endpoint = "s3.filebase.com"
		return bucket, "", endpoint, false
	} else if a := scalewayRegex.FindStringSubmatch(host); len(a) > 1 {
		region = a[2]
		bucket = a[1]
		endpoint = fmt.Sprintf("s3.%s.scw.cloud", region)
		return bucket, region, endpoint, false
	}

	// For standard S3, the host is the bucket name
	return host, "", "", false
}

var (
	awsS3Regex        = regexp.MustCompile(`^(.+)\.s3(?:\.([^.]+))?\.amazonaws\.com$`)
	digitaloceanRegex = regexp.MustCompile(`^(?:(.+)\.)?([^.]+)\.digitaloceanspaces.com$`)
	backblazeRegex    = regexp.MustCompile(`^(?:(.+)\.)?s3.([^.]+)\.backblazeb2.com$`)
	filebaseRegex     = regexp.MustCompile(`^(?:(.+)\.)?s3.filebase.com$`)
	scalewayRegex     = regexp.MustCompile(`^(?:(.+)\.)?s3.([^.]+)\.scw\.cloud$`)
)

func isNotExists(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == "NoSuchKey"
	}
	return false
}

func deleteOutputError(out *s3.DeleteObjectsOutput) error {
	if len(out.Errors) == 0 {
		return nil
	}

	// Build generic error
	var b strings.Builder
	b.WriteString("failed to delete files:")
	for _, err := range out.Errors {
		fmt.Fprintf(&b, "\n%s: %s", aws.ToString(err.Key), aws.ToString(err.Message))
	}
	return errors.New(b.String())
}
