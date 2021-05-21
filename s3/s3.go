package s3

import (
	"fmt"
	"net"
	"regexp"
)

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
	} else if a := gcsRegex.FindStringSubmatch(host); a != nil {
		bucket, region = a[1], "us-east-1"
		endpoint = "storage.googleapis.com"
	} else if a := digitalOceanRegex.FindStringSubmatch(host); a != nil {
		bucket, region = a[1], a[2]
		endpoint = fmt.Sprintf("%s.digitaloceanspaces.com", region)
	} else if a := linodeRegex.FindStringSubmatch(host); a != nil {
		bucket, region = a[1], a[2]
		endpoint = fmt.Sprintf("%s.linodeobjects.com", region)
	} else if a := backblazeRegex.FindStringSubmatch(host); a != nil {
		bucket, region = a[1], a[2]
		endpoint = fmt.Sprintf("s3.%s.backblazeb2.com", region)
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
	digitalOceanRegex = regexp.MustCompile(`^(?:(.+)\.)?([^.]+)\.digitaloceanspaces.com$`)
	linodeRegex       = regexp.MustCompile(`^(?:(.+)\.)?([^.]+)\.linodeobjects.com$`)
	backblazeRegex    = regexp.MustCompile(`^(?:(.+)\.)?s3.([^.]+)\.backblazeb2.com$`)
	gcsRegex          = regexp.MustCompile(`^(?:(.+)\.)?storage.googleapis.com$`)
)
