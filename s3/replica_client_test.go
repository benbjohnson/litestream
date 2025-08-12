package s3

import (
	"errors"
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
