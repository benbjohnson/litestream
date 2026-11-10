package litestream

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path"
	"regexp"
	"runtime"
	"strconv"
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

	if scheme == "file" {
		if urlPath, err = ResolveFileReplicaURLPath(rawURL); err != nil {
			return nil, err
		}
	}

	return factory(scheme, host, urlPath, query, userinfo)
}

// ParseFileReplicaURL returns the filesystem path for a file replica URL and
// the path that versions before URL decoding used for the same URL. The two
// differ only when the URL contains characters that require escaping.
func ParseFileReplicaURL(s string) (filePath, legacyPath string, err error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", "", err
	}
	if u.Scheme != "file" {
		return "", "", fmt.Errorf("file replica url required: %s", s)
	}
	return fileReplicaURLPaths(u)
}

func fileReplicaURLPaths(u *url.URL) (filePath, legacyPath string, err error) {
	u.Scheme = ""
	u.RawQuery = ""
	// String re-escapes the filesystem path, so decode it before passing it
	// to the file replica client. Decode once to preserve literal escapes.
	escaped := u.String()
	decoded, err := url.PathUnescape(escaped)
	if err != nil {
		return "", "", err
	}
	return path.Clean(decoded), path.Clean(escaped), nil
}

// ResolveFileReplicaURLPath returns the filesystem path for a file replica URL.
// Earlier versions wrote to the escaped form of the path, so an existing
// directory at that location is still used, with a warning, until it is renamed.
func ResolveFileReplicaURLPath(s string) (string, error) {
	filePath, legacyPath, err := ParseFileReplicaURL(s)
	if err != nil {
		return "", err
	}
	return ResolveLegacyFileReplicaPath(filePath, legacyPath)
}

// ResolveLegacyFileReplicaPath chooses between the decoded file replica path
// and the escaped path used by earlier versions. The legacy path is returned,
// with a warning describing how to rename it, only when it exists and the
// decoded path does not. Both existing is an error.
func ResolveLegacyFileReplicaPath(filePath, legacyPath string) (string, error) {
	if filePath == legacyPath {
		return filePath, nil
	}

	legacyExists, err := pathExists(legacyPath)
	if err != nil {
		return "", err
	} else if !legacyExists {
		return filePath, nil
	}

	exists, err := pathExists(filePath)
	if err != nil {
		return "", err
	} else if exists {
		return "", fmt.Errorf("file replica directories %q and %q both exist: earlier versions of litestream wrote to %q; merge or remove one of them", filePath, legacyPath, legacyPath)
	}

	slog.Warn("file replica uses directory name from earlier litestream version; rename it to remove this warning",
		"path", legacyPath,
		"expected", filePath,
		"command", renameCommand(legacyPath, filePath))
	return legacyPath, nil
}

func pathExists(name string) (bool, error) {
	if _, err := os.Stat(name); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func renameCommand(from, to string) string {
	if runtime.GOOS == "windows" {
		return fmt.Sprintf(`move "%s" "%s"`, from, to)
	}
	return fmt.Sprintf("mv %s %s", shellQuote(from), shellQuote(to))
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// ReplicaTypeFromURL returns the replica type from a URL string.
// Returns empty string if the URL is invalid or has no scheme.
func ReplicaTypeFromURL(rawURL string) string {
	if !IsURL(rawURL) {
		return ""
	}
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
		filePath, _, err := fileReplicaURLPaths(u)
		if err != nil {
			return "", "", "", nil, nil, err
		}
		return "file", "", filePath, nil, nil, nil

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

// IntQueryValue returns a positive integer value from URL query parameters.
// It checks keys in order and returns the value and whether it was set.
// Returns an error if a present value is not a positive integer.
func IntQueryValue(query url.Values, keys ...string) (value int64, ok bool, err error) {
	if query == nil {
		return 0, false, nil
	}
	for _, key := range keys {
		if raw := query.Get(key); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || n <= 0 {
				return 0, false, fmt.Errorf("invalid value for query parameter %q: %q (must be a positive integer)", key, raw)
			}
			return n, true, nil
		}
	}
	return 0, false, nil
}

// IsHetznerEndpoint returns true if the endpoint is Hetzner object storage service.
func IsHetznerEndpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return strings.HasSuffix(host, ".your-objectstorage.com")
}

// IsTigrisEndpoint returns true if the endpoint is the Tigris object storage service.
func IsTigrisEndpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return host == "fly.storage.tigris.dev" || host == "t3.storage.dev"
}

// IsGoogleCloudStorageEndpoint returns true if the endpoint is Google Cloud Storage.
func IsGoogleCloudStorageEndpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	host = strings.TrimSuffix(host, ".")

	labels := strings.Split(host, ".")
	if len(labels) < 3 ||
		labels[len(labels)-2] != "googleapis" ||
		labels[len(labels)-1] != "com" {
		return false
	}
	for _, label := range labels {
		if label == "" {
			return false
		}
	}
	if len(labels) > 3 {
		for _, segment := range strings.Split(labels[1], "-") {
			if segment == "storage" {
				return false
			}
		}
	}
	for _, segment := range strings.Split(labels[0], "-") {
		if segment == "storage" {
			return true
		}
	}
	return false
}

// IsDigitalOceanEndpoint returns true if the endpoint is Digital Ocean Spaces.
func IsDigitalOceanEndpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return strings.HasSuffix(host, ".digitaloceanspaces.com")
}

// IsBackblazeEndpoint returns true if the endpoint is Backblaze B2.
func IsBackblazeEndpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return strings.HasSuffix(host, ".backblazeb2.com")
}

// IsFilebaseEndpoint returns true if the endpoint is Filebase.
func IsFilebaseEndpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return host == "s3.filebase.com"
}

// IsScalewayEndpoint returns true if the endpoint is Scaleway Object Storage.
func IsScalewayEndpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return strings.HasSuffix(host, ".scw.cloud")
}

// IsCloudflareR2Endpoint returns true if the endpoint is Cloudflare R2.
func IsCloudflareR2Endpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return strings.HasSuffix(host, ".r2.cloudflarestorage.com")
}

// IsSupabaseEndpoint returns true if the endpoint is Supabase Storage S3.
func IsSupabaseEndpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return strings.HasSuffix(host, ".supabase.co")
}

// IsNeonEndpoint returns true if the endpoint is Neon Object Storage.
// Neon storage endpoints are per-branch, e.g.
// br-example-123456.storage.c-6.us-east-2.aws.neon.tech.
func IsNeonEndpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return strings.HasSuffix(host, ".neon.tech") && strings.Contains(host, ".storage.")
}

// IsMinIOEndpoint returns true if the endpoint appears to be MinIO or similar
// (a custom endpoint with a port number that is not a known cloud provider).
func IsMinIOEndpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	if u.Port() == "" {
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
		strings.Contains(host, "t3.storage.dev") ||
		strings.Contains(host, ".supabase.co") ||
		strings.HasSuffix(host, ".neon.tech") ||
		strings.HasSuffix(host, ".your-objectstorage.com") ||
		strings.HasSuffix(host, ".googleapis.com") {
		return false
	}
	return true
}

// IsLocalEndpoint returns true if the endpoint appears to be a local development
// endpoint (localhost, 127.0.0.1, or private network addresses).
// These endpoints typically use HTTP instead of HTTPS.
func IsLocalEndpoint(endpoint string) bool {
	u, err := parseEndpoint(endpoint)
	if err != nil {
		return false
	}
	host := u.Hostname()
	// Check for common local/development hostnames
	return host == "localhost" ||
		host == "127.0.0.1" ||
		strings.HasPrefix(host, "192.168.") ||
		strings.HasPrefix(host, "10.") ||
		strings.HasPrefix(host, "172.16.") ||
		strings.HasPrefix(host, "172.17.") ||
		strings.HasPrefix(host, "172.18.") ||
		strings.HasPrefix(host, "172.19.") ||
		strings.HasPrefix(host, "172.2") || // 172.20-172.29
		strings.HasPrefix(host, "172.30.") ||
		strings.HasPrefix(host, "172.31.") ||
		strings.HasSuffix(host, ".local") ||
		strings.HasSuffix(host, ".localhost")
}

// EnsureEndpointScheme ensures an endpoint has an HTTP(S) scheme.
// For local endpoints (localhost, private IPs), it defaults to http://.
// For all other endpoints (cloud providers), it defaults to https://.
// Returns the endpoint with scheme and a boolean indicating if a scheme was added.
func EnsureEndpointScheme(endpoint string) (string, bool) {
	if endpoint == "" {
		return "", false
	}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint, false
	}
	// Default to HTTP for local development endpoints, HTTPS for everything else
	if IsLocalEndpoint(endpoint) {
		return "http://" + endpoint, true
	}
	return "https://" + endpoint, true
}

func parseEndpoint(endpoint string) (*url.URL, error) {
	endpoint = strings.TrimSpace(endpoint)
	if !strings.HasPrefix(strings.ToLower(endpoint), "http://") && !strings.HasPrefix(strings.ToLower(endpoint), "https://") {
		endpoint = "//" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.User != nil {
		return nil, fmt.Errorf("endpoint must not contain user information")
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("endpoint hostname required")
	}
	u.Host = strings.ToLower(u.Host)
	return u, nil
}

// IsURL returns true if s appears to be a URL (has a scheme).
var isURLRegex = regexp.MustCompile(`^\w+:\/\/`)

func IsURL(s string) bool {
	return isURLRegex.MatchString(s)
}
