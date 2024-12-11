package s3

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// DefaultRegion is the region used if one is not specified.
const DefaultRegion = "us-east-1"

func newSession(ctx context.Context, accessKeyID, secretAccessKey, region, bucket, path, endpoint string, forcePathStyle, skipVerify bool) (sess *session.Session, err error) { // Build S3 config.
	// Build session config.
	var config aws.Config
	if accessKeyID != "" || secretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(accessKeyID, secretAccessKey, "")
	}
	if endpoint != "" {
		config.Endpoint = aws.String(endpoint)
	}
	if forcePathStyle {
		config.S3ForcePathStyle = aws.Bool(forcePathStyle)
	}
	if skipVerify {
		config.HTTPClient = &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}}
	}

	// Look up region if not specified and no endpoint is used.
	// Endpoints are typically used for non-S3 object stores and do not
	// necessarily require a region.
	if region == "" {
		if endpoint == "" {
			if region, err = findBucketRegion(ctx, config, bucket); err != nil {
				return nil, fmt.Errorf("cannot lookup bucket region: %w", err)
			}
		} else {
			region = DefaultRegion // default for non-S3 object stores
		}
	}

	// Set region if provided by user or by bucket lookup.
	if region != "" {
		config.Region = aws.String(region)
	}

	return session.NewSession(&config)
}

func findBucketRegion(ctx context.Context, config aws.Config, bucket string) (string, error) {
	// Connect to US standard region to fetch info.
	config.Region = aws.String(DefaultRegion)
	sess, err := session.NewSession(&config)
	if err != nil {
		return "", err
	}

	// Fetch bucket location, if possible. Must be bucket owner.
	// This call can return a nil location which means it's in us-east-1.
	if out, err := s3.New(sess).HeadBucketWithContext(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		return "", err
	} else if out.BucketRegion != nil {
		return *out.BucketRegion, nil
	}
	return DefaultRegion, nil
}
