package litestream

import (
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
)

// ReplicaClientFactory is a function that creates a ReplicaClient from URL components.
// The userinfo parameter contains credentials from the URL (e.g., user:pass@host).
type ReplicaClientFactory func(scheme, host, urlPath string, query url.Values, userinfo *url.Userinfo) (ReplicaClient, error)

var (
	replicaClientFactories   = make(map[string]ReplicaClientFactory)
	replicaClientFactoriesMu sync.RWMutex
)

// RegisterReplicaClientFactory registers a factory function for creating replica clients
// for a given URL scheme. This is typically called from init() functions in backend packages.
func RegisterReplicaClientFactory(scheme string, factory ReplicaClientFactory) {
	replicaClientFactoriesMu.Lock()
	defer replicaClientFactoriesMu.Unlock()
	replicaClientFactories[scheme] = factory
}

// NewReplicaClientFromURL creates a new ReplicaClient from a URL string.
// The URL scheme determines which backend is used (s3, gs, abs, file, etc.).
func NewReplicaClientFromURL(rawURL string) (ReplicaClient, error) {
	scheme, host, urlPath, query, userinfo, err := ParseReplicaURLWithQuery(rawURL)
	if err != nil {
		return nil, err
	}

	// Normalize webdavs to webdav
	factoryScheme := scheme
	if factoryScheme == "webdavs" {
		factoryScheme = "webdav"
	}

	replicaClientFactoriesMu.RLock()
	factory, ok := replicaClientFactories[factoryScheme]
	replicaClientFactoriesMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unsupported replica URL scheme: %q", scheme)
	}

	return factory(scheme, host, urlPath, query, userinfo)
}

// ReplicaTypeFromURL returns the replica type from a URL string.
// Returns empty string if the URL is invalid or has no scheme.
func ReplicaTypeFromURL(rawURL string) string {
	scheme, _, _, _ := ParseReplicaURL(rawURL)
	if scheme == "" {
		return ""
	}
	if scheme == "webdavs" {
		return "webdav"
	}
	return scheme
}

// ParseReplicaURL parses a replica URL and returns the scheme, host, and path.
func ParseReplicaURL(s string) (scheme, host, urlPath string, err error) {
	if strings.HasPrefix(strings.ToLower(s), "s3://arn:") {
		scheme, host, urlPath, _, err = parseS3AccessPointURL(s)
		return scheme, host, urlPath, err
	}

	scheme, host, urlPath, _, _, err = ParseReplicaURLWithQuery(s)
	return scheme, host, urlPath, err
}

// ParseReplicaURLWithQuery parses a replica URL and returns query parameters and userinfo.
func ParseReplicaURLWithQuery(s string) (scheme, host, urlPath string, query url.Values, userinfo *url.Userinfo, err error) {
	// Handle S3 Access Point ARNs which can't be parsed by standard url.Parse
	if strings.HasPrefix(strings.ToLower(s), "s3://arn:") {
		scheme, host, urlPath, query, err := parseS3AccessPointURL(s)
		return scheme, host, urlPath, query, nil, err
	}

	u, err := url.Parse(s)
	if err != nil {
		return "", "", "", nil, nil, err
	}

	switch u.Scheme {
	case "file":
		scheme, u.Scheme = u.Scheme, ""
		// Remove query params from path for file URLs
		u.RawQuery = ""
		return scheme, "", path.Clean(u.String()), nil, nil, nil

	case "":
		return u.Scheme, u.Host, u.Path, nil, nil, fmt.Errorf("replica url scheme required: %s", s)

	default:
		return u.Scheme, u.Host, strings.TrimPrefix(path.Clean(u.Path), "/"), u.Query(), u.User, nil
	}
}

// parseS3AccessPointURL parses an S3 Access Point URL (s3://arn:...).
func parseS3AccessPointURL(s string) (scheme, host, urlPath string, query url.Values, err error) {
	const prefix = "s3://"
	if !strings.HasPrefix(strings.ToLower(s), prefix) {
		return "", "", "", nil, fmt.Errorf("invalid s3 access point url: %s", s)
	}

	arnWithPath := s[len(prefix):]

	// Split off query string if present
	var queryStr string
	if idx := strings.IndexByte(arnWithPath, '?'); idx != -1 {
		queryStr = arnWithPath[idx+1:]
		arnWithPath = arnWithPath[:idx]
	}

	bucket, key, err := splitS3AccessPointARN(arnWithPath)
	if err != nil {
		return "", "", "", nil, err
	}

	// Parse query string if present
	if queryStr != "" {
		query, err = url.ParseQuery(queryStr)
		if err != nil {
			return "", "", "", nil, fmt.Errorf("parse query string: %w", err)
		}
	}

	return "s3", bucket, CleanReplicaURLPath(key), query, nil
}

// splitS3AccessPointARN splits an S3 Access Point ARN into bucket and key components.
func splitS3AccessPointARN(s string) (bucket, key string, err error) {
	lower := strings.ToLower(s)
	const marker = ":accesspoint/"
	idx := strings.Index(lower, marker)
	if idx == -1 {
		return "", "", fmt.Errorf("invalid s3 access point arn: %s", s)
	}

	nameStart := idx + len(marker)
	if nameStart >= len(s) {
		return "", "", fmt.Errorf("invalid s3 access point arn: %s", s)
	}

	remainder := s[nameStart:]
	slashIdx := strings.IndexByte(remainder, '/')
	if slashIdx == -1 {
		return s, "", nil
	}

	bucketEnd := nameStart + slashIdx
	bucket = s[:bucketEnd]
	key = remainder[slashIdx+1:]
	return bucket, key, nil
}

// CleanReplicaURLPath cleans a URL path for use in replica storage.
func CleanReplicaURLPath(p string) string {
	if p == "" {
		return ""
	}
	cleaned := path.Clean("/" + p)
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "." {
		return ""
	}
	return cleaned
}

// RegionFromS3ARN extracts the region from an S3 ARN.
func RegionFromS3ARN(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) >= 4 {
		return parts[3]
	}
	return ""
}

// BoolQueryValue returns a boolean value from URL query parameters.
// It checks multiple keys in order and returns the value and whether it was set.
func BoolQueryValue(query url.Values, keys ...string) (value bool, ok bool) {
	if query == nil {
		return false, false
	}
	for _, key := range keys {
		if raw := query.Get(key); raw != "" {
			switch strings.ToLower(raw) {
			case "true", "1", "t", "yes":
				return true, true
			case "false", "0", "f", "no":
				return false, true
			default:
				return false, true
			}
		}
	}
	return false, false
}

// IsTigrisEndpoint returns true if the endpoint is the Tigris object storage service.
func IsTigrisEndpoint(endpoint string) bool {
	host := extractEndpointHost(endpoint)
	return host == "fly.storage.tigris.dev" || host == "t3.storage.dev"
}

// IsDigitalOceanEndpoint returns true if the endpoint is Digital Ocean Spaces.
func IsDigitalOceanEndpoint(endpoint string) bool {
	host := extractEndpointHost(endpoint)
	if host == "" {
		return false
	}
	return strings.HasSuffix(host, ".digitaloceanspaces.com")
}

// IsBackblazeEndpoint returns true if the endpoint is Backblaze B2.
func IsBackblazeEndpoint(endpoint string) bool {
	host := extractEndpointHost(endpoint)
	if host == "" {
		return false
	}
	return strings.HasSuffix(host, ".backblazeb2.com")
}

// IsFilebaseEndpoint returns true if the endpoint is Filebase.
func IsFilebaseEndpoint(endpoint string) bool {
	host := extractEndpointHost(endpoint)
	if host == "" {
		return false
	}
	return host == "s3.filebase.com"
}

// IsScalewayEndpoint returns true if the endpoint is Scaleway Object Storage.
func IsScalewayEndpoint(endpoint string) bool {
	host := extractEndpointHost(endpoint)
	if host == "" {
		return false
	}
	return strings.HasSuffix(host, ".scw.cloud")
}

// IsCloudflareR2Endpoint returns true if the endpoint is Cloudflare R2.
func IsCloudflareR2Endpoint(endpoint string) bool {
	host := extractEndpointHost(endpoint)
	if host == "" {
		return false
	}
	return strings.HasSuffix(host, ".r2.cloudflarestorage.com")
}

// IsMinIOEndpoint returns true if the endpoint appears to be MinIO or similar
// (a custom endpoint with a port number that is not a known cloud provider).
func IsMinIOEndpoint(endpoint string) bool {
	host := extractEndpointHost(endpoint)
	if host == "" {
		return false
	}
	// MinIO typically uses host:port format without .com domain
	// Check for port number in the host
	if !strings.Contains(host, ":") {
		return false
	}
	// Exclude known cloud providers
	if strings.Contains(host, ".amazonaws.com") ||
		strings.Contains(host, ".digitaloceanspaces.com") ||
		strings.Contains(host, ".backblazeb2.com") ||
		strings.Contains(host, ".filebase.com") ||
		strings.Contains(host, ".scw.cloud") ||
		strings.Contains(host, ".r2.cloudflarestorage.com") ||
		strings.Contains(host, "tigris.dev") ||
		strings.Contains(host, "t3.storage.dev") {
		return false
	}
	return true
}

// extractEndpointHost extracts the host from an endpoint URL or returns the
// endpoint as-is if it's not a full URL.
func extractEndpointHost(endpoint string) string {
	endpoint = strings.TrimSpace(strings.ToLower(endpoint))
	if endpoint == "" {
		return ""
	}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
			return u.Host
		}
	}
	return endpoint
}

// IsURL returns true if s appears to be a URL (has a scheme).
var isURLRegex = regexp.MustCompile(`^\w+:\/\/`)

func IsURL(s string) bool {
	return isURLRegex.MatchString(s)
}
