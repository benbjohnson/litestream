package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
)

func TestRunConfig_Validate(t *testing.T) {
	tests := []struct {
		name   string
		config runConfig
		err    error
	}{
		{
			name: "OK",
			config: runConfig{
				leaseURL:      "s3://bucket/path",
				ttl:           30 * time.Second,
				heartbeat:     15 * time.Second,
				retryInterval: time.Second,
				command:       []string{"litestream", "replicate"},
			},
		},
		{
			name: "MissingURL",
			config: runConfig{
				ttl:           30 * time.Second,
				heartbeat:     15 * time.Second,
				retryInterval: time.Second,
				command:       []string{"litestream"},
			},
			err: ErrLeaseURLRequired,
		},
		{
			name: "MissingCommand",
			config: runConfig{
				leaseURL:      "s3://bucket/path",
				ttl:           30 * time.Second,
				heartbeat:     15 * time.Second,
				retryInterval: time.Second,
			},
			err: ErrCommandRequired,
		},
		{
			name: "InvalidHeartbeat",
			config: runConfig{
				leaseURL:      "s3://bucket/path",
				ttl:           30 * time.Second,
				heartbeat:     30 * time.Second,
				retryInterval: time.Second,
				command:       []string{"litestream"},
			},
			err: ErrInvalidHeartbeat,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if !errors.Is(err, tt.err) {
				t.Fatalf("Validate() error=%v, want %v", err, tt.err)
			}
		})
	}
}

func TestMain_ParseFlags_CommandKeepsFlags(t *testing.T) {
	m := NewMain()
	m.Stderr = io.Discard

	config, err := m.parseFlags([]string{
		"-url", "s3://bucket/path",
		"-ttl", "40s",
		"litestream", "replicate", "-config", "/tmp/litestream.yml",
	})
	if err != nil {
		t.Fatalf("parseFlags() error: %v", err)
	}

	if config.leaseURL != "s3://bucket/path" {
		t.Fatalf("leaseURL=%q, want s3://bucket/path", config.leaseURL)
	}
	if config.heartbeat != 20*time.Second {
		t.Fatalf("heartbeat=%v, want 20s", config.heartbeat)
	}
	if got, want := config.command, []string{"litestream", "replicate", "-config", "/tmp/litestream.yml"}; !slices.Equal(got, want) {
		t.Fatalf("command=%v, want %v", got, want)
	}
}

func TestAcquireLease_RetriesUntilAvailable(t *testing.T) {
	var attempts int32
	leaser := &testLeaser{
		acquireFunc: func(ctx context.Context) (*litestream.Lease, error) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if leaserAcquireN := atomic.AddInt32(&attempts, 1); leaserAcquireN == 1 {
				return nil, &litestream.LeaseExistsError{ExpiresAt: time.Now().Add(time.Millisecond)}
			}
			return &litestream.Lease{ETag: "etag-1"}, nil
		},
	}

	lease, err := acquireLease(context.Background(), leaser, 0, time.Millisecond)
	if err != nil {
		t.Fatalf("acquireLease() error: %v", err)
	}
	if lease.ETag != "etag-1" {
		t.Fatalf("lease.ETag=%q, want etag-1", lease.ETag)
	}
	if got := atomic.LoadInt32(&attempts); got != 2 {
		t.Fatalf("AcquireLease() calls=%d, want 2", got)
	}
}

func TestMain_Run_ReleasesLeaseAfterCommandExit(t *testing.T) {
	t.Setenv("S3LEASE_TEST_PROCESS", "exit")
	leaser := newTestLeaser()
	m := newTestMain(leaser)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.Run(ctx, testCommandArgs()); err != nil {
		t.Fatal(err)
	}
	if got := leaser.releaseN.Load(); got != 1 {
		t.Fatalf("ReleaseLease() calls=%d, want 1", got)
	}
}

func TestMain_Run_ReleasesLeaseAfterStartError(t *testing.T) {
	leaser := newTestLeaser()
	leaser.releaseErr = errors.New("release failed")
	m := newTestMain(leaser)
	err := m.Run(context.Background(), []string{"-url", "s3://bucket/path", filepath.Join(t.TempDir(), "missing-command")})
	if !errors.Is(err, os.ErrNotExist) || !errors.Is(err, leaser.releaseErr) {
		t.Fatalf("Run() error=%v, want start and release errors", err)
	}
	if got := leaser.releaseN.Load(); got != 1 {
		t.Fatalf("ReleaseLease() calls=%d, want 1", got)
	}
}

func TestMain_Run_KillsCommandWhenRenewFails(t *testing.T) {
	t.Setenv("S3LEASE_TEST_PROCESS", "wait")
	leaser := newTestLeaser()
	leaser.renewErr = litestream.ErrLeaseNotHeld
	leaser.releaseErr = errors.New("release failed")
	m := newTestMain(leaser)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := m.Run(ctx, testCommandArgs())
	if !errors.Is(err, litestream.ErrLeaseNotHeld) || !errors.Is(err, leaser.releaseErr) {
		t.Fatalf("Run() error=%v, want renewal and release errors", err)
	}
	if ctx.Err() != nil {
		t.Fatal("command did not stop after renewal failure")
	}
	if got := leaser.releaseN.Load(); got != 1 {
		t.Fatalf("ReleaseLease() calls=%d, want 1", got)
	}
}

func TestMain_Run_ReleasesLeaseWhenSignalArrivesAfterAcquire(t *testing.T) {
	leaser := newTestLeaser()
	signals := make(chan os.Signal, 1)

	leaser.acquireFunc = func(ctx context.Context) (*litestream.Lease, error) {
		signals <- os.Interrupt
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for signal cancellation")
		}
		return &litestream.Lease{
			Generation: 1,
			ExpiresAt:  time.Now().Add(time.Minute),
			Owner:      "test",
			ETag:       "etag-1",
		}, nil
	}

	m := newTestMain(leaser)
	m.signals = signals
	var output bytes.Buffer
	m.Stdout = &output
	t.Setenv("S3LEASE_TEST_PROCESS", "exit")

	err := m.Run(context.Background(), testCommandArgs())
	if err == nil {
		t.Fatal("expected error")
	}
	if got := leaser.releaseN.Load(); got != 1 {
		t.Fatalf("ReleaseLease() calls=%d, want 1", got)
	}
	if output.Len() != 0 {
		t.Fatalf("command started after acquisition signal: %s", &output)
	}
}

func TestMain_Run_ForwardsSignal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("os.Interrupt cannot be sent to a process on Windows")
	}
	t.Setenv("S3LEASE_TEST_PROCESS", "wait")
	leaser := newTestLeaser()
	leaser.renewed = make(chan struct{}, 1)
	m := newTestMain(leaser)
	signals := make(chan os.Signal, 1)
	m.signals = signals
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	m.Stdout = writer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(reader)
		ready <- scanner.Scan() && scanner.Text() == "ready"
	}()
	errCh := make(chan error, 1)
	go func() { errCh <- m.Run(ctx, testCommandArgs()) }()
	select {
	case ok := <-ready:
		if !ok {
			cancel()
			<-errCh
			t.Fatal("child did not report readiness")
		}
	case <-ctx.Done():
		<-errCh
		t.Fatal("timed out waiting for child")
	}
	select {
	case <-leaser.renewed:
	case <-ctx.Done():
		<-errCh
		t.Fatal("timed out waiting for renewal")
	}
	signals <- os.Interrupt
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if leaser.releasedLease == nil || leaser.releasedLease.ETag != "etag-2" {
		t.Fatalf("released lease=%+v, want renewed lease", leaser.releasedLease)
	}
}

func TestProcessHarness(t *testing.T) {
	switch os.Getenv("S3LEASE_TEST_PROCESS") {
	case "exit":
		fmt.Println("exited")
		os.Exit(0)
	case "wait":
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)
		fmt.Println("ready")
		<-signals
		os.Exit(0)
	}
}

func testCommandArgs() []string {
	return []string{"-url", "s3://bucket/path", "-ttl", "20ms", "-heartbeat", "5ms", os.Args[0], "-test.run=^TestProcessHarness$"}
}

func newTestMain(leaser *testLeaser) *Main {
	return &Main{
		Stdin:     bytes.NewReader(nil),
		Stdout:    io.Discard,
		Stderr:    io.Discard,
		newLeaser: func(context.Context, string) (litestream.Leaser, error) { return leaser, nil },
	}
}

type testLeaser struct {
	acquireN atomic.Int32
	renewN   atomic.Int32
	releaseN atomic.Int32

	acquireFunc   func(context.Context) (*litestream.Lease, error)
	renewErr      error
	releaseErr    error
	releasedLease *litestream.Lease
	renewed       chan struct{}
}

func newTestLeaser() *testLeaser {
	return &testLeaser{}
}

func (l *testLeaser) Type() string {
	return "test"
}

func (l *testLeaser) AcquireLease(ctx context.Context) (*litestream.Lease, error) {
	l.acquireN.Add(1)
	if l.acquireFunc != nil {
		return l.acquireFunc(ctx)
	}
	return &litestream.Lease{
		Generation: 1,
		ExpiresAt:  time.Now().Add(time.Minute),
		Owner:      "test",
		ETag:       "etag-1",
	}, nil
}

func (l *testLeaser) RenewLease(context.Context, *litestream.Lease) (*litestream.Lease, error) {
	l.renewN.Add(1)
	select {
	case l.renewed <- struct{}{}:
	default:
	}
	if l.renewErr != nil {
		return nil, l.renewErr
	}
	return &litestream.Lease{
		Generation: 1,
		ExpiresAt:  time.Now().Add(time.Minute),
		Owner:      "test",
		ETag:       "etag-2",
	}, nil
}

func (l *testLeaser) ReleaseLease(_ context.Context, lease *litestream.Lease) error {
	l.releasedLease = lease
	l.releaseN.Add(1)
	return l.releaseErr
}
