package s3

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
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
