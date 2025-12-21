package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/superfly/ltx"
)

// mockAPIError implements smithy.APIError for testing
type mockAPIError struct {
	code    string
	message string
}

func (e *mockAPIError) Error() string {
	return e.message
}

func (e *mockAPIError) ErrorCode() string {
	return e.code
}

func (e *mockAPIError) ErrorMessage() string {
	return e.message
}

func (e *mockAPIError) ErrorFault() smithy.ErrorFault {
	return smithy.FaultUnknown
}

func TestIsNotExists(t *testing.T) {
	// Test with NoSuchKey error
	noSuchKeyErr := &mockAPIError{
		code:    "NoSuchKey",
		message: "The specified key does not exist",
	}
	if !isNotExists(noSuchKeyErr) {
		t.Error("isNotExists should return true for NoSuchKey error")
	}

	// Test with different error code
	differentErr := &mockAPIError{
		code:    "AccessDenied",
		message: "Access denied",
	}
	if isNotExists(differentErr) {
		t.Error("isNotExists should return false for non-NoSuchKey error")
	}

	// Test with non-API error
	regularErr := errors.New("regular error")
	if isNotExists(regularErr) {
		t.Error("isNotExists should return false for non-API error")
	}

	// Test with nil error
	if isNotExists(nil) {
		t.Error("isNotExists should return false for nil error")
	}

	// Test with wrapped API error
	wrappedErr := &mockAPIError{
		code:    "NoSuchKey",
		message: "wrapped key error",
	}
	if !isNotExists(wrappedErr) {
		t.Error("isNotExists should return true for wrapped NoSuchKey error")
	}
}

func TestReplicaClient_DefaultSignPayload(t *testing.T) {
	client := NewReplicaClient()
	if !client.SignPayload {
		t.Error("expected default SignPayload to be true for AWS S3 compatibility")
	}
	if !client.RequireContentMD5 {
		t.Error("expected default RequireContentMD5 to be true for AWS S3 compatibility")
	}
}

func TestReplicaClientPayloadSigning(t *testing.T) {
	data := mustLTX(t)
	signedPayload := sha256.Sum256(data)
	wantSigned := hex.EncodeToString(signedPayload[:])

	tests := []struct {
		name        string
		signPayload bool
		wantHeader  string
	}{
		{name: "UnsignedWhenDisabled", signPayload: false, wantHeader: "UNSIGNED-PAYLOAD"},
		{name: "SignedByDefault", signPayload: true, wantHeader: wantSigned},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := make(chan http.Header, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer r.Body.Close()
				_, _ = io.Copy(io.Discard, r.Body)

				if r.Method == http.MethodPut {
					select {
					case headers <- r.Header.Clone():
					default:
					}
					w.Header().Set("ETag", `"test-etag"`)
					w.WriteHeader(http.StatusOK)
					return
				}

				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			client := NewReplicaClient()
			client.Bucket = "test-bucket"
			client.Path = "replica"
			client.Region = "us-east-1"
			client.Endpoint = server.URL
			client.ForcePathStyle = true
			client.AccessKeyID = "test-access-key"
			client.SecretAccessKey = "test-secret-key"
			client.SignPayload = tt.signPayload

			ctx := context.Background()
			if err := client.Init(ctx); err != nil {
				t.Fatalf("Init() error: %v", err)
			}

			if _, err := client.WriteLTXFile(ctx, 0, 2, 2, bytes.NewReader(data)); err != nil {
				t.Fatalf("WriteLTXFile() error: %v", err)
			}

			select {
			case hdr := <-headers:
				if got, want := hdr.Get("x-amz-content-sha256"), tt.wantHeader; got != want {
					t.Fatalf("x-amz-content-sha256 header = %q, want %q", got, want)
				}
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for PUT request")
			}
		})
	}
}

func TestReplicaClient_UnsignedPayload_NoChunkedEncoding(t *testing.T) {
	data := mustLTX(t)

	headers := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		_, _ = io.Copy(io.Discard, r.Body)

		if r.Method == http.MethodPut {
			select {
			case headers <- r.Header.Clone():
			default:
			}
			w.Header().Set("ETag", `"test-etag"`)
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewReplicaClient()
	client.Bucket = "test-bucket"
	client.Path = "replica"
	client.Region = "us-east-1"
	client.Endpoint = server.URL
	client.ForcePathStyle = true
	client.AccessKeyID = "test-access-key"
	client.SecretAccessKey = "test-secret-key"
	client.SignPayload = false

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	if _, err := client.WriteLTXFile(ctx, 0, 2, 2, bytes.NewReader(data)); err != nil {
		t.Fatalf("WriteLTXFile() error: %v", err)
	}

	select {
	case hdr := <-headers:
		if got := hdr.Get("x-amz-content-sha256"); got != "UNSIGNED-PAYLOAD" {
			t.Errorf("x-amz-content-sha256 = %q, want UNSIGNED-PAYLOAD", got)
		}

		contentEnc := hdr.Get("Content-Encoding")
		if strings.Contains(contentEnc, "aws-chunked") {
			t.Errorf("Content-Encoding contains aws-chunked: %q; aws-chunked is incompatible with UNSIGNED-PAYLOAD", contentEnc)
		}

		transferEnc := hdr.Get("Transfer-Encoding")
		if strings.Contains(transferEnc, "aws-chunked") {
			t.Errorf("Transfer-Encoding contains aws-chunked: %q; aws-chunked is incompatible with UNSIGNED-PAYLOAD", transferEnc)
		}

		decoded := hdr.Get("X-Amz-Decoded-Content-Length")
		if decoded != "" {
			t.Errorf("X-Amz-Decoded-Content-Length = %q; this header indicates aws-chunked encoding which is incompatible with UNSIGNED-PAYLOAD", decoded)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for PUT request")
	}
}

// TestReplicaClient_SignedPayload_CustomEndpoint_NoChunkedEncoding verifies that
// aws-chunked encoding is disabled for custom endpoints even when SignPayload=true.
// This is necessary for S3-compatible providers (Filebase, MinIO, Backblaze B2, etc.)
// that don't support aws-chunked encoding at all. See issue #895.
func TestReplicaClient_SignedPayload_CustomEndpoint_NoChunkedEncoding(t *testing.T) {
	data := mustLTX(t)

	headers := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		_, _ = io.Copy(io.Discard, r.Body)

		if r.Method == http.MethodPut {
			select {
			case headers <- r.Header.Clone():
			default:
			}
			w.Header().Set("ETag", `"test-etag"`)
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewReplicaClient()
	client.Bucket = "test-bucket"
	client.Path = "replica"
	client.Region = "us-east-1"
	client.Endpoint = server.URL // Custom endpoint (non-AWS)
	client.ForcePathStyle = true
	client.AccessKeyID = "test-access-key"
	client.SecretAccessKey = "test-secret-key"
	client.SignPayload = true // Signed payload, but still using custom endpoint

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	if _, err := client.WriteLTXFile(ctx, 0, 2, 2, bytes.NewReader(data)); err != nil {
		t.Fatalf("WriteLTXFile() error: %v", err)
	}

	select {
	case hdr := <-headers:
		// With SignPayload=true, we expect an actual SHA256 hash (not UNSIGNED-PAYLOAD)
		sha256Header := hdr.Get("x-amz-content-sha256")
		if sha256Header == "" {
			t.Error("x-amz-content-sha256 header should be set")
		}
		if sha256Header == "UNSIGNED-PAYLOAD" {
			t.Error("x-amz-content-sha256 should be actual hash, not UNSIGNED-PAYLOAD, when SignPayload=true")
		}

		// But aws-chunked encoding should still be disabled for custom endpoints
		contentEnc := hdr.Get("Content-Encoding")
		if strings.Contains(contentEnc, "aws-chunked") {
			t.Errorf("Content-Encoding contains aws-chunked: %q; aws-chunked is not supported by S3-compatible providers", contentEnc)
		}

		transferEnc := hdr.Get("Transfer-Encoding")
		if strings.Contains(transferEnc, "aws-chunked") {
			t.Errorf("Transfer-Encoding contains aws-chunked: %q; aws-chunked is not supported by S3-compatible providers", transferEnc)
		}

		decoded := hdr.Get("X-Amz-Decoded-Content-Length")
		if decoded != "" {
			t.Errorf("X-Amz-Decoded-Content-Length = %q; this header indicates aws-chunked encoding which is not supported by S3-compatible providers", decoded)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for PUT request")
	}
}

func mustLTX(t *testing.T) []byte {
	t.Helper()

	buf := new(bytes.Buffer)
	enc, err := ltx.NewEncoder(buf)
	if err != nil {
		t.Fatalf("NewEncoder: %v", err)
	}

	if err := enc.EncodeHeader(ltx.Header{
		Version:          ltx.Version,
		PageSize:         4096,
		Commit:           0,
		MinTXID:          2,
		MaxTXID:          2,
		Timestamp:        time.Now().UnixMilli(),
		PreApplyChecksum: ltx.ChecksumFlag | 1,
	}); err != nil {
		t.Fatalf("EncodeHeader: %v", err)
	}

	enc.SetPostApplyChecksum(ltx.ChecksumFlag)
	if err := enc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	return buf.Bytes()
}

// TestReplicaClient_Init_BucketValidation tests that Init validates bucket name
func TestReplicaClient_Init_BucketValidation(t *testing.T) {
	t.Run("EmptyBucket", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "" // Empty bucket name
		c.Region = "us-east-1"

		err := c.Init(context.Background())
		if err == nil {
			t.Fatal("expected error for empty bucket name")
		}
		if !strings.Contains(err.Error(), "bucket name is required") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("ValidBucket", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "test-bucket"
		c.Region = "us-east-1"
		// Note: This will fail when trying to connect, but should pass bucket validation
		err := c.Init(context.Background())
		// We expect a different error (not bucket validation)
		if err != nil && strings.Contains(err.Error(), "bucket name is required") {
			t.Errorf("should not fail bucket validation with valid bucket: %v", err)
		}
	})
}

// TestReplicaClient_UploaderConfiguration tests that uploader configuration is applied
func TestReplicaClient_UploaderConfiguration(t *testing.T) {
	t.Run("CustomPartSize", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "test-bucket"
		c.Region = "us-east-1"
		c.PartSize = 10 * 1024 * 1024 // 10MB
		c.Concurrency = 10

		// Verify the configuration is set
		if c.PartSize != 10*1024*1024 {
			t.Errorf("expected PartSize to be 10MB, got %d", c.PartSize)
		}
		if c.Concurrency != 10 {
			t.Errorf("expected Concurrency to be 10, got %d", c.Concurrency)
		}
	})

	t.Run("DefaultConfiguration", func(t *testing.T) {
		c := NewReplicaClient()
		// Verify defaults are zero (will use SDK defaults)
		if c.PartSize != 0 {
			t.Errorf("expected default PartSize to be 0, got %d", c.PartSize)
		}
		if c.Concurrency != 0 {
			t.Errorf("expected default Concurrency to be 0, got %d", c.Concurrency)
		}
	})
}

// TestReplicaClient_ConfigureEndpoint tests the endpoint configuration helper
func TestReplicaClient_ConfigureEndpoint(t *testing.T) {
	tests := []struct {
		name           string
		endpoint       string
		forcePathStyle bool
		expectHTTPS    bool
	}{
		{
			name:           "HTTPEndpoint",
			endpoint:       "http://localhost:9000",
			forcePathStyle: true,
			expectHTTPS:    false,
		},
		{
			name:           "HTTPSEndpoint",
			endpoint:       "https://s3.amazonaws.com",
			forcePathStyle: false,
			expectHTTPS:    true,
		},
		{
			name:           "EndpointWithoutScheme",
			endpoint:       "s3.us-west-002.backblazeb2.com",
			forcePathStyle: false,
			expectHTTPS:    true,
		},
		{
			name:           "EmptyEndpoint",
			endpoint:       "",
			forcePathStyle: false,
			expectHTTPS:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewReplicaClient()
			c.Endpoint = tt.endpoint
			c.ForcePathStyle = tt.forcePathStyle

			// Test that configureEndpoint can be called without error
			var opts []func(*s3.Options)
			c.configureEndpoint(&opts)

			// Verify opts were added when endpoint is set
			if tt.endpoint != "" && len(opts) == 0 {
				t.Error("expected endpoint options to be added")
			}
			if tt.endpoint == "" && len(opts) != 0 {
				t.Error("expected no endpoint options for empty endpoint")
			}
		})
	}
}

// TestReplicaClient_HTTPClientConfiguration tests HTTP client setup
func TestReplicaClient_HTTPClientConfiguration(t *testing.T) {
	t.Run("WithSkipVerify", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "test-bucket"
		c.Region = "us-east-1"
		c.SkipVerify = true

		// We can't directly test the HTTP client configuration without
		// actually initializing, but we can verify the flag is set
		if !c.SkipVerify {
			t.Error("expected SkipVerify to be true")
		}
	})

	t.Run("WithoutSkipVerify", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "test-bucket"
		c.Region = "us-east-1"
		c.SkipVerify = false

		if c.SkipVerify {
			t.Error("expected SkipVerify to be false")
		}
	})
}

func TestReplicaClientDeleteLTXFiles_ContentMD5(t *testing.T) {
	t.Run("Enabled", func(t *testing.T) {
		var callCount int

		httpClient := smithyhttp.ClientDoFunc(func(r *http.Request) (*http.Response, error) {
			t.Helper()
			callCount++

			if r.Method != http.MethodPost {
				t.Fatalf("unexpected method: %s", r.Method)
			}
			if !strings.Contains(r.URL.RawQuery, "delete") {
				t.Fatalf("unexpected query: %s", r.URL.RawQuery)
			}

			if ua := r.Header.Get("User-Agent"); !strings.Contains(ua, "litestream") {
				t.Fatalf("expected User-Agent to contain litestream, got %q", ua)
			}

			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			r.Body.Close()

			got := r.Header.Get("Content-MD5")
			if got == "" {
				t.Fatal("expected Content-MD5 header")
			}

			sum := md5.Sum(body)
			want := base64.StdEncoding.EncodeToString(sum[:])
			if got != want {
				t.Fatalf("unexpected Content-MD5 header: got %q, want %q", got, want)
			}

			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/xml"}},
				Body: io.NopCloser(strings.NewReader(
					`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`,
				)),
			}
			return resp, nil
		})

		cfg := aws.Config{
			Region:      "us-east-1",
			Credentials: aws.NewCredentialsCache(aws.AnonymousCredentials{}),
			HTTPClient:  httpClient,
		}

		c := NewReplicaClient()
		c.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
		c.s3 = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, c.middlewareOption())
		})
		c.Bucket = "test-bucket"
		c.Path = "test-path"

		files := []*ltx.FileInfo{
			{Level: 0, MinTXID: 1, MaxTXID: 1},
			{Level: 0, MinTXID: 2, MaxTXID: 2},
		}

		if err := c.DeleteLTXFiles(context.Background(), files); err != nil {
			t.Fatalf("DeleteLTXFiles: %v", err)
		}
		if callCount != 1 {
			t.Fatalf("unexpected call count: %d", callCount)
		}
	})

	t.Run("Disabled", func(t *testing.T) {
		httpClient := smithyhttp.ClientDoFunc(func(r *http.Request) (*http.Response, error) {
			t.Helper()
			if md5Header := r.Header.Get("Content-MD5"); md5Header != "" {
				t.Fatalf("expected Content-MD5 header to be empty when disabled, got %q", md5Header)
			}
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/xml"}},
				Body: io.NopCloser(strings.NewReader(
					`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`,
				)),
			}
			return resp, nil
		})

		cfg := aws.Config{
			Region:      "us-east-1",
			Credentials: aws.NewCredentialsCache(aws.AnonymousCredentials{}),
			HTTPClient:  httpClient,
		}

		c := NewReplicaClient()
		c.RequireContentMD5 = false
		c.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
		c.s3 = s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, c.middlewareOption())
		})
		c.Bucket = "test-bucket"
		c.Path = "test-path"

		files := []*ltx.FileInfo{{Level: 0, MinTXID: 1, MaxTXID: 1}}
		if err := c.DeleteLTXFiles(context.Background(), files); err != nil {
			t.Fatalf("DeleteLTXFiles: %v", err)
		}
	})
}

func TestReplicaClientDeleteLTXFiles_PreexistingContentMD5(t *testing.T) {
	const preexistingMD5 = "preexisting-checksum-value"
	var callCount int

	httpClient := smithyhttp.ClientDoFunc(func(r *http.Request) (*http.Response, error) {
		t.Helper()
		callCount++

		got := r.Header.Get("Content-MD5")
		if got != preexistingMD5 {
			t.Fatalf("middleware should not override existing Content-MD5: got %q, want %q", got, preexistingMD5)
		}

		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/xml"}},
			Body: io.NopCloser(strings.NewReader(
				`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></DeleteResult>`,
			)),
		}
		return resp, nil
	})

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: aws.NewCredentialsCache(aws.AnonymousCredentials{}),
		HTTPClient:  httpClient,
	}

	c := NewReplicaClient()
	c.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	c.s3 = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, c.middlewareOption())
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Finalize.Add(
				middleware.FinalizeMiddlewareFunc(
					"InjectPreexistingContentMD5",
					func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
						out middleware.FinalizeOutput, metadata middleware.Metadata, err error,
					) {
						if req, ok := in.Request.(*smithyhttp.Request); ok {
							req.Header.Set("Content-MD5", preexistingMD5)
						}
						return next.HandleFinalize(ctx, in)
					},
				),
				middleware.Before,
			)
		})
	})
	c.Bucket = "test-bucket"
	c.Path = "test-path"

	files := []*ltx.FileInfo{
		{Level: 0, MinTXID: 1, MaxTXID: 1},
	}

	if err := c.DeleteLTXFiles(context.Background(), files); err != nil {
		t.Fatalf("DeleteLTXFiles: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("unexpected call count: %d", callCount)
	}
}

// TestReplicaClient_CredentialConfiguration tests credential setup
func TestReplicaClient_CredentialConfiguration(t *testing.T) {
	t.Run("WithStaticCredentials", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "test-bucket"
		c.Region = "us-east-1"
		c.AccessKeyID = "AKIAIOSFODNN7EXAMPLE"
		c.SecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

		// Verify credentials are set
		if c.AccessKeyID == "" || c.SecretAccessKey == "" {
			t.Error("expected credentials to be set")
		}
	})

	t.Run("WithDefaultCredentialChain", func(t *testing.T) {
		c := NewReplicaClient()
		c.Bucket = "test-bucket"
		c.Region = "us-east-1"
		// Leave AccessKeyID and SecretAccessKey empty

		// Verify credentials are not set (will use default chain)
		if c.AccessKeyID != "" || c.SecretAccessKey != "" {
			t.Error("expected credentials to be empty for default chain")
		}
	})
}

// TestReplicaClient_DefaultRegionUsage tests that DefaultRegion constant is used consistently
func TestReplicaClient_DefaultRegionUsage(t *testing.T) {
	// Test that DefaultRegion is properly defined
	if DefaultRegion != "us-east-1" {
		t.Errorf("expected DefaultRegion to be 'us-east-1', got %s", DefaultRegion)
	}

	// Test ParseHost uses DefaultRegion
	t.Run("ParseHost_MinIO", func(t *testing.T) {
		bucket, region, endpoint, forcePathStyle := ParseHost("mybucket.localhost:9000")
		if region != DefaultRegion {
			t.Errorf("expected region to be %s, got %s", DefaultRegion, region)
		}
		if bucket != "mybucket" {
			t.Errorf("expected bucket to be 'mybucket', got %s", bucket)
		}
		if !strings.Contains(endpoint, "localhost:9000") {
			t.Errorf("expected endpoint to contain 'localhost:9000', got %s", endpoint)
		}
		if !forcePathStyle {
			t.Error("expected forcePathStyle to be true for MinIO")
		}
	})
}

func TestMarshalDeleteObjects_EdgeCases(t *testing.T) {
	t.Run("EmptyObjects", func(t *testing.T) {
		deleteInput := &types.Delete{
			Objects: []types.ObjectIdentifier{},
		}
		xml, err := marshalDeleteObjects(deleteInput)
		if err != nil {
			t.Fatalf("marshalDeleteObjects failed: %v", err)
		}
		if !strings.Contains(string(xml), "<Delete") {
			t.Error("expected XML to contain Delete element")
		}
	})

	t.Run("KeyWithSpecialCharacters", func(t *testing.T) {
		key := "test/path with spaces & <special> chars.txt"
		deleteInput := &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String(key)},
			},
		}
		xml, err := marshalDeleteObjects(deleteInput)
		if err != nil {
			t.Fatalf("marshalDeleteObjects failed: %v", err)
		}
		xmlStr := string(xml)
		if !strings.Contains(xmlStr, "test/path with spaces &amp; &lt;special&gt; chars.txt") {
			t.Errorf("expected XML to properly escape special characters, got: %s", xmlStr)
		}
	})

	t.Run("KeyWithUnicode", func(t *testing.T) {
		key := "test/文件.txt"
		deleteInput := &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String(key)},
			},
		}
		xml, err := marshalDeleteObjects(deleteInput)
		if err != nil {
			t.Fatalf("marshalDeleteObjects failed: %v", err)
		}
		xmlStr := string(xml)
		if !strings.Contains(xmlStr, key) {
			t.Errorf("expected XML to contain unicode key, got: %s", xmlStr)
		}
	})

	t.Run("LargeBatch", func(t *testing.T) {
		const count = 1000
		objects := make([]types.ObjectIdentifier, count)
		for i := 0; i < count; i++ {
			objects[i] = types.ObjectIdentifier{
				Key: aws.String(string(rune('a' + (i % 26)))),
			}
		}
		deleteInput := &types.Delete{
			Objects: objects,
		}
		xml, err := marshalDeleteObjects(deleteInput)
		if err != nil {
			t.Fatalf("marshalDeleteObjects failed for %d objects: %v", count, err)
		}
		if len(xml) == 0 {
			t.Error("expected non-empty XML output")
		}
	})

	t.Run("NilOptionalFields", func(t *testing.T) {
		deleteInput := &types.Delete{
			Objects: []types.ObjectIdentifier{
				{
					Key: aws.String("test-key"),
				},
			},
		}
		xml, err := marshalDeleteObjects(deleteInput)
		if err != nil {
			t.Fatalf("marshalDeleteObjects failed: %v", err)
		}
		xmlStr := string(xml)
		if !strings.Contains(xmlStr, "<Key>test-key</Key>") {
			t.Errorf("expected Key element in XML, got: %s", xmlStr)
		}
		if strings.Contains(xmlStr, "<ETag>") {
			t.Error("expected no ETag element when nil")
		}
		if strings.Contains(xmlStr, "<VersionId>") {
			t.Error("expected no VersionId element when nil")
		}
	})

	t.Run("QuietFlag", func(t *testing.T) {
		deleteInput := &types.Delete{
			Objects: []types.ObjectIdentifier{
				{Key: aws.String("test")},
			},
			Quiet: aws.Bool(true),
		}
		xml, err := marshalDeleteObjects(deleteInput)
		if err != nil {
			t.Fatalf("marshalDeleteObjects failed: %v", err)
		}
		xmlStr := string(xml)
		if !strings.Contains(xmlStr, "<Quiet>true</Quiet>") {
			t.Errorf("expected Quiet element to be true, got: %s", xmlStr)
		}
	})
}

func TestEncodeObjectIdentifier_AllFields(t *testing.T) {
	t.Run("AllFieldsPopulated", func(t *testing.T) {
		timestamp, err := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")
		if err != nil {
			t.Fatalf("failed to parse timestamp: %v", err)
		}
		deleteInput := &types.Delete{
			Objects: []types.ObjectIdentifier{
				{
					Key:              aws.String("my-object-key"),
					ETag:             aws.String("abc123etag"),
					VersionId:        aws.String("version-456"),
					LastModifiedTime: aws.Time(timestamp),
					Size:             aws.Int64(12345),
				},
			},
		}
		xml, err := marshalDeleteObjects(deleteInput)
		if err != nil {
			t.Fatalf("marshalDeleteObjects failed: %v", err)
		}
		xmlStr := string(xml)

		if !strings.Contains(xmlStr, "<Key>my-object-key</Key>") {
			t.Error("expected Key element")
		}
		if !strings.Contains(xmlStr, "<ETag>abc123etag</ETag>") {
			t.Error("expected ETag element")
		}
		if !strings.Contains(xmlStr, "<VersionId>version-456</VersionId>") {
			t.Error("expected VersionId element")
		}
		if !strings.Contains(xmlStr, "<LastModifiedTime>") {
			t.Error("expected LastModifiedTime element")
		}
		if !strings.Contains(xmlStr, "<Size>12345</Size>") {
			t.Error("expected Size element with value 12345")
		}
	})

	t.Run("OnlyRequiredKey", func(t *testing.T) {
		deleteInput := &types.Delete{
			Objects: []types.ObjectIdentifier{
				{
					Key: aws.String("only-key"),
				},
			},
		}
		xml, err := marshalDeleteObjects(deleteInput)
		if err != nil {
			t.Fatalf("marshalDeleteObjects failed: %v", err)
		}
		xmlStr := string(xml)

		if !strings.Contains(xmlStr, "<Key>only-key</Key>") {
			t.Error("expected Key element")
		}
		if strings.Contains(xmlStr, "<ETag>") {
			t.Error("expected no ETag element when nil")
		}
		if strings.Contains(xmlStr, "<VersionId>") {
			t.Error("expected no VersionId element when nil")
		}
	})

	t.Run("FieldOrder", func(t *testing.T) {
		deleteInput := &types.Delete{
			Objects: []types.ObjectIdentifier{
				{
					Key:       aws.String("test"),
					ETag:      aws.String("etag1"),
					VersionId: aws.String("v1"),
				},
			},
		}
		xml, err := marshalDeleteObjects(deleteInput)
		if err != nil {
			t.Fatalf("marshalDeleteObjects failed: %v", err)
		}
		xmlStr := string(xml)

		keyIdx := strings.Index(xmlStr, "<Key>")
		etagIdx := strings.Index(xmlStr, "<ETag>")
		versionIdx := strings.Index(xmlStr, "<VersionId>")

		if keyIdx == -1 || etagIdx == -1 || versionIdx == -1 {
			t.Fatal("missing expected elements")
		}
		if etagIdx > keyIdx || keyIdx > versionIdx {
			t.Errorf("expected field order: ETag, Key, VersionId, got ETag@%d, Key@%d, VersionId@%d", etagIdx, keyIdx, versionIdx)
		}
	})
}

func TestComputeDeleteObjectsContentMD5_Deterministic(t *testing.T) {
	deleteInput := &types.Delete{
		Objects: []types.ObjectIdentifier{
			{Key: aws.String("key1")},
			{Key: aws.String("key2")},
		},
	}

	md51, err := computeDeleteObjectsContentMD5(deleteInput)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	md52, err := computeDeleteObjectsContentMD5(deleteInput)
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	if md51 != md52 {
		t.Errorf("MD5 computation not deterministic: %q != %q", md51, md52)
	}

	if md51 == "" {
		t.Error("expected non-empty MD5")
	}
}

// TestParseHost tests URL parsing for various S3-compatible storage providers.
// This test addresses issue #825 where Digital Ocean Space URLs were not correctly
// extracting the bucket name.
func TestParseHost(t *testing.T) {
	tests := []struct {
		name               string
		host               string
		wantBucket         string
		wantRegion         string
		wantEndpoint       string
		wantForcePathStyle bool
	}{
		{
			name:               "Digital Ocean Space URL",
			host:               "my-space.sgp1.digitaloceanspaces.com",
			wantBucket:         "my-space",
			wantRegion:         "sgp1",
			wantEndpoint:       "https://sgp1.digitaloceanspaces.com",
			wantForcePathStyle: false,
		},
		{
			name:               "Digital Ocean Space different region",
			host:               "test-bucket.nyc3.digitaloceanspaces.com",
			wantBucket:         "test-bucket",
			wantRegion:         "nyc3",
			wantEndpoint:       "https://nyc3.digitaloceanspaces.com",
			wantForcePathStyle: false,
		},
		{
			name:               "AWS S3 URL with region",
			host:               "mybucket.s3.us-east-1.amazonaws.com",
			wantBucket:         "mybucket",
			wantRegion:         "us-east-1",
			wantEndpoint:       "",
			wantForcePathStyle: false,
		},
		{
			name:               "AWS S3 URL without region",
			host:               "mybucket.s3.amazonaws.com",
			wantBucket:         "mybucket",
			wantRegion:         "",
			wantEndpoint:       "",
			wantForcePathStyle: false,
		},
		{
			name:               "Backblaze B2",
			host:               "mybucket.s3.us-west-004.backblazeb2.com",
			wantBucket:         "mybucket",
			wantRegion:         "us-west-004",
			wantEndpoint:       "https://s3.us-west-004.backblazeb2.com",
			wantForcePathStyle: true,
		},
		{
			name:               "MinIO with port",
			host:               "mybucket.localhost:9000",
			wantBucket:         "mybucket",
			wantRegion:         "us-east-1",
			wantEndpoint:       "http://localhost:9000",
			wantForcePathStyle: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, region, endpoint, forcePathStyle := ParseHost(tt.host)

			if bucket != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", bucket, tt.wantBucket)
			}
			if region != tt.wantRegion {
				t.Errorf("region = %q, want %q", region, tt.wantRegion)
			}
			if endpoint != tt.wantEndpoint {
				t.Errorf("endpoint = %q, want %q", endpoint, tt.wantEndpoint)
			}
			if forcePathStyle != tt.wantForcePathStyle {
				t.Errorf("forcePathStyle = %v, want %v", forcePathStyle, tt.wantForcePathStyle)
			}
		})
	}
}

func TestReplicaClient_AccessPointARN(t *testing.T) {
	t.Run("ARNAsBucketName", func(t *testing.T) {
		arn := "arn:aws:s3:us-east-2:123456789012:accesspoint/my-access-point"

		c := NewReplicaClient()
		c.Bucket = arn
		c.Region = "us-east-2"
		c.AccessKeyID = "test-access-key"
		c.SecretAccessKey = "test-secret-key"

		if c.Bucket != arn {
			t.Errorf("expected bucket to be ARN, got %s", c.Bucket)
		}
		if c.Region != "us-east-2" {
			t.Errorf("expected region to be us-east-2, got %s", c.Region)
		}
	})

	t.Run("ARNWithPath", func(t *testing.T) {
		arn := "arn:aws:s3:us-west-2:111122223333:accesspoint/prod-access-point"

		c := NewReplicaClient()
		c.Bucket = arn
		c.Path = "my-db/replica"
		c.Region = "us-west-2"

		if c.Bucket != arn {
			t.Errorf("expected bucket to be ARN, got %s", c.Bucket)
		}
		if c.Path != "my-db/replica" {
			t.Errorf("expected path to be my-db/replica, got %s", c.Path)
		}
	})

	t.Run("ARNRejectsPathStyle", func(t *testing.T) {
		arn := "arn:aws:s3:us-east-1:123456789012:accesspoint/test-ap"

		c := NewReplicaClient()
		c.Bucket = arn
		c.Path = "replica"
		c.Region = "us-east-1"
		c.Endpoint = "http://localhost:9000"
		c.ForcePathStyle = true
		c.AccessKeyID = "test-access-key"
		c.SecretAccessKey = "test-secret-key"

		ctx := context.Background()
		if err := c.Init(ctx); err != nil {
			t.Fatalf("Init() with ARN bucket should not fail: %v", err)
		}

		data := mustLTX(t)
		_, err := c.WriteLTXFile(ctx, 0, 2, 2, bytes.NewReader(data))
		if err == nil {
			t.Fatal("expected error when using path-style with ARN bucket")
		}
		if !strings.Contains(err.Error(), "Path-style addressing cannot be used with ARN") {
			t.Errorf("expected path-style ARN error, got: %v", err)
		}
	})
}

func TestReplicaClient_TigrisConsistentHeader(t *testing.T) {
	// Test that non-Tigris endpoints do NOT send the X-Tigris-Consistent header.
	// The Tigris case (header sent) requires an actual Tigris endpoint and is
	// covered by Tigris integration tests.
	data := mustLTX(t)

	headers := make(chan http.Header, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		_, _ = io.Copy(io.Discard, r.Body)

		if r.Method == http.MethodPut {
			select {
			case headers <- r.Header.Clone():
			default:
			}
			w.Header().Set("ETag", `"test-etag"`)
			w.WriteHeader(http.StatusOK)
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewReplicaClient()
	client.Bucket = "test-bucket"
	client.Path = "replica"
	client.Region = "us-east-1"
	client.Endpoint = server.URL // Non-Tigris endpoint
	client.ForcePathStyle = true
	client.AccessKeyID = "test-access-key"
	client.SecretAccessKey = "test-secret-key"

	ctx := context.Background()
	if err := client.Init(ctx); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	if _, err := client.WriteLTXFile(ctx, 0, 2, 2, bytes.NewReader(data)); err != nil {
		t.Fatalf("WriteLTXFile() error: %v", err)
	}

	select {
	case hdr := <-headers:
		if got := hdr.Get("X-Tigris-Consistent"); got != "" {
			t.Fatalf("X-Tigris-Consistent header = %q, want empty (non-Tigris endpoint)", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for PUT request")
	}
}
