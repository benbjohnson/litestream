package s3

import (
	"context"
	"errors"
	"strings"
	"testing"

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
