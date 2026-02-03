package litestream_test

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/mock"
)

func TestRetryReplicaClient_LTXFiles_Success(t *testing.T) {
	callCount := 0
	mockClient := &mock.ReplicaClient{
		LTXFilesFunc: func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			callCount++
			return ltx.NewFileInfoSliceIterator(nil), nil
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.RetryConfig{
		InitialDelay: 10 * time.Millisecond,
		MaxRetries:   3,
	})

	_, err := client.LTXFiles(context.Background(), 0, 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d", callCount)
	}
}

func TestRetryReplicaClient_LTXFiles_RetryOnTransientError(t *testing.T) {
	callCount := 0
	mockClient := &mock.ReplicaClient{
		LTXFilesFunc: func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			callCount++
			if callCount < 3 {
				return nil, errors.New("network error")
			}
			return ltx.NewFileInfoSliceIterator(nil), nil
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.RetryConfig{
		InitialDelay: 1 * time.Millisecond,
		MaxRetries:   5,
	})

	_, err := client.LTXFiles(context.Background(), 0, 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 3 {
		t.Fatalf("expected 3 calls, got %d", callCount)
	}
}

func TestRetryReplicaClient_LTXFiles_NoRetryOnNotExist(t *testing.T) {
	callCount := 0
	mockClient := &mock.ReplicaClient{
		LTXFilesFunc: func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			callCount++
			return nil, os.ErrNotExist
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.RetryConfig{
		InitialDelay: 1 * time.Millisecond,
		MaxRetries:   5,
	})

	_, err := client.LTXFiles(context.Background(), 0, 0, false)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 call (no retry), got %d", callCount)
	}
}

func TestRetryReplicaClient_LTXFiles_MaxRetriesExceeded(t *testing.T) {
	callCount := 0
	mockClient := &mock.ReplicaClient{
		LTXFilesFunc: func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			callCount++
			return nil, errors.New("persistent error")
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.RetryConfig{
		InitialDelay: 1 * time.Millisecond,
		MaxRetries:   3,
	})

	_, err := client.LTXFiles(context.Background(), 0, 0, false)
	if err == nil {
		t.Fatal("expected error")
	}
	// 1 initial + 3 retries = 4 calls
	if callCount != 4 {
		t.Fatalf("expected 4 calls, got %d", callCount)
	}
}

func TestRetryReplicaClient_LTXFiles_ContextCancellation(t *testing.T) {
	callCount := 0
	ctx, cancel := context.WithCancel(context.Background())

	mockClient := &mock.ReplicaClient{
		LTXFilesFunc: func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			callCount++
			if callCount == 2 {
				cancel() // Cancel after second attempt
			}
			return nil, errors.New("network error")
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.RetryConfig{
		InitialDelay: 1 * time.Millisecond,
		MaxRetries:   10,
	})

	_, err := client.LTXFiles(ctx, 0, 0, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRetryReplicaClient_OpenLTXFile_Success(t *testing.T) {
	mockClient := &mock.ReplicaClient{
		OpenLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("data")), nil
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.DefaultRetryConfig())

	rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rc.Close()
}

func TestRetryReplicaClient_OpenLTXFile_RetryOnTransientError(t *testing.T) {
	callCount := 0
	mockClient := &mock.ReplicaClient{
		OpenLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			callCount++
			if callCount < 2 {
				return nil, errors.New("connection reset")
			}
			return io.NopCloser(strings.NewReader("data")), nil
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.RetryConfig{
		InitialDelay: 1 * time.Millisecond,
		MaxRetries:   5,
	})

	rc, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rc.Close()
	if callCount != 2 {
		t.Fatalf("expected 2 calls, got %d", callCount)
	}
}

func TestRetryReplicaClient_OpenLTXFile_NoRetryOnNotExist(t *testing.T) {
	callCount := 0
	mockClient := &mock.ReplicaClient{
		OpenLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
			callCount++
			return nil, os.ErrNotExist
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.RetryConfig{
		InitialDelay: 1 * time.Millisecond,
		MaxRetries:   5,
	})

	_, err := client.OpenLTXFile(context.Background(), 0, 1, 1, 0, 0)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 call (no retry), got %d", callCount)
	}
}

func TestRetryReplicaClient_WriteLTXFile_NoRetry(t *testing.T) {
	callCount := 0
	mockClient := &mock.ReplicaClient{
		WriteLTXFileFunc: func(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
			callCount++
			return nil, errors.New("write error")
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.RetryConfig{
		InitialDelay: 1 * time.Millisecond,
		MaxRetries:   5,
	})

	_, err := client.WriteLTXFile(context.Background(), 0, 1, 1, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if callCount != 1 {
		t.Fatalf("expected 1 call (no retry for writes), got %d", callCount)
	}
}

func TestRetryReplicaClient_ExponentialBackoff(t *testing.T) {
	var delays []time.Duration
	lastCall := time.Now()

	callCount := 0
	mockClient := &mock.ReplicaClient{
		LTXFilesFunc: func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			now := time.Now()
			if callCount > 0 {
				delays = append(delays, now.Sub(lastCall))
			}
			lastCall = now
			callCount++
			if callCount <= 3 {
				return nil, errors.New("error")
			}
			return ltx.NewFileInfoSliceIterator(nil), nil
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.RetryConfig{
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     100 * time.Millisecond,
		MaxRetries:   5,
	})

	_, _ = client.LTXFiles(context.Background(), 0, 0, false)

	if len(delays) < 2 {
		t.Fatalf("expected at least 2 delays, got %d", len(delays))
	}
	// First delay should be ~10ms, second ~20ms (with tolerance for timing)
	if delays[0] < 8*time.Millisecond || delays[0] > 30*time.Millisecond {
		t.Fatalf("first delay out of range: %v", delays[0])
	}
	if delays[1] < 15*time.Millisecond || delays[1] > 50*time.Millisecond {
		t.Fatalf("second delay out of range: %v", delays[1])
	}
}

func TestRetryReplicaClient_MaxDelayRespected(t *testing.T) {
	var delays []time.Duration
	lastCall := time.Now()

	callCount := 0
	mockClient := &mock.ReplicaClient{
		LTXFilesFunc: func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			now := time.Now()
			if callCount > 0 {
				delays = append(delays, now.Sub(lastCall))
			}
			lastCall = now
			callCount++
			if callCount <= 5 {
				return nil, errors.New("error")
			}
			return ltx.NewFileInfoSliceIterator(nil), nil
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.RetryConfig{
		InitialDelay: 5 * time.Millisecond,
		MaxDelay:     15 * time.Millisecond,
		MaxRetries:   10,
	})

	_, _ = client.LTXFiles(context.Background(), 0, 0, false)

	// After a few retries, delay should be capped at MaxDelay
	for i, d := range delays {
		if d > 30*time.Millisecond { // Allow some tolerance
			t.Fatalf("delay %d exceeded max: %v", i, d)
		}
	}
}

func TestRetryReplicaClient_Unwrap(t *testing.T) {
	mockClient := &mock.ReplicaClient{}
	client := litestream.NewRetryReplicaClient(mockClient, litestream.DefaultRetryConfig())

	if client.Unwrap() != mockClient {
		t.Fatal("Unwrap did not return underlying client")
	}
}

func TestRetryReplicaClient_Type(t *testing.T) {
	mockClient := &mock.ReplicaClient{}
	client := litestream.NewRetryReplicaClient(mockClient, litestream.DefaultRetryConfig())

	if client.Type() != "mock" {
		t.Fatalf("expected type 'mock', got %q", client.Type())
	}
}

func TestRetryReplicaClient_ZeroRetries(t *testing.T) {
	callCount := 0
	mockClient := &mock.ReplicaClient{
		LTXFilesFunc: func(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
			callCount++
			return nil, errors.New("error")
		},
	}

	client := litestream.NewRetryReplicaClient(mockClient, litestream.RetryConfig{
		InitialDelay: 1 * time.Millisecond,
		MaxRetries:   0,
	})

	_, err := client.LTXFiles(context.Background(), 0, 0, false)
	if err == nil {
		t.Fatal("expected error")
	}
	// With 0 retries, only 1 call (initial attempt)
	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d", callCount)
	}
}
