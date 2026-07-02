package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
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
	if got, want := config.command, []string{"litestream", "replicate", "-config", "/tmp/litestream.yml"}; !equalStringSlices(got, want) {
		t.Fatalf("command=%v, want %v", got, want)
	}
}

func TestAcquireLease_RetriesUntilAvailable(t *testing.T) {
	leaser := &testLeaser{
		acquireFunc: func(ctx context.Context) (*litestream.Lease, error) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if leaserAcquireN := atomic.AddInt32(&testAcquireAttempts, 1); leaserAcquireN == 1 {
				return nil, &litestream.LeaseExistsError{ExpiresAt: time.Now().Add(time.Millisecond)}
			}
			return &litestream.Lease{ETag: "etag-1"}, nil
		},
	}
	testAcquireAttempts = 0

	lease, err := acquireLease(context.Background(), leaser, 0, time.Millisecond)
	if err != nil {
		t.Fatalf("acquireLease() error: %v", err)
	}
	if lease.ETag != "etag-1" {
		t.Fatalf("lease.ETag=%q, want etag-1", lease.ETag)
	}
	if got := atomic.LoadInt32(&testAcquireAttempts); got != 2 {
		t.Fatalf("AcquireLease() calls=%d, want 2", got)
	}
}

func TestMain_Run_ReleasesLeaseAfterCommandExit(t *testing.T) {
	leaser := newTestLeaser()
	process := newTestProcess()
	process.finishAfterStart(nil)

	m := newTestMain(leaser, process)
	err := m.Run(context.Background(), []string{
		"-url", "s3://bucket/path",
		"-ttl", "20ms",
		"-heartbeat", "5ms",
		"litestream", "replicate",
	})
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if got := leaser.releaseN.Load(); got != 1 {
		t.Fatalf("ReleaseLease() calls=%d, want 1", got)
	}
	if got := process.killN.Load(); got != 0 {
		t.Fatalf("Kill() calls=%d, want 0", got)
	}
}

func TestMain_Run_ReleasesLeaseAfterStartError(t *testing.T) {
	leaser := newTestLeaser()
	process := newTestProcess()
	process.startErr = errors.New("start failed")

	m := newTestMain(leaser, process)
	err := m.Run(context.Background(), []string{
		"-url", "s3://bucket/path",
		"-ttl", "20ms",
		"-heartbeat", "5ms",
		"litestream", "replicate",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if got := leaser.releaseN.Load(); got != 1 {
		t.Fatalf("ReleaseLease() calls=%d, want 1", got)
	}
}

func TestMain_Run_KillsCommandWhenRenewFails(t *testing.T) {
	leaser := newTestLeaser()
	leaser.renewErr = litestream.ErrLeaseNotHeld
	process := newTestProcess()

	m := newTestMain(leaser, process)
	err := m.Run(context.Background(), []string{
		"-url", "s3://bucket/path",
		"-ttl", "20ms",
		"-heartbeat", "5ms",
		"litestream", "replicate",
	})
	if !errors.Is(err, litestream.ErrLeaseNotHeld) {
		t.Fatalf("Run() error=%v, want %v", err, litestream.ErrLeaseNotHeld)
	}
	if got := process.killN.Load(); got != 1 {
		t.Fatalf("Kill() calls=%d, want 1", got)
	}
}

func TestMain_Run_ForwardsSignal(t *testing.T) {
	leaser := newTestLeaser()
	process := newTestProcess()
	signals := make(chan os.Signal, 1)

	m := newTestMain(leaser, process)
	m.signals = signals

	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Run(context.Background(), []string{
			"-url", "s3://bucket/path",
			"-ttl", "50ms",
			"-heartbeat", "10ms",
			"litestream", "replicate",
		})
	}()

	<-process.started
	signals <- os.Interrupt
	process.finish(nil)

	if err := <-errCh; err != nil {
		t.Fatalf("Run() error: %v", err)
	}
	if got := process.signalN.Load(); got != 1 {
		t.Fatalf("Signal() calls=%d, want 1", got)
	}
}

var testAcquireAttempts int32

func newTestMain(leaser *testLeaser, process *testProcess) *Main {
	return &Main{
		Stdin:  bytes.NewReader(nil),
		Stdout: io.Discard,
		Stderr: io.Discard,
		newLeaser: func(context.Context, string) (litestream.Leaser, error) {
			return leaser, nil
		},
		newProcess: func([]string) subprocess {
			return process
		},
	}
}

type testLeaser struct {
	acquireN atomic.Int32
	renewN   atomic.Int32
	releaseN atomic.Int32

	acquireFunc func(context.Context) (*litestream.Lease, error)
	renewErr    error
	releaseErr  error
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

func (l *testLeaser) ReleaseLease(context.Context, *litestream.Lease) error {
	l.releaseN.Add(1)
	return l.releaseErr
}

type testProcess struct {
	started chan struct{}
	waitCh  chan error
	once    sync.Once

	startErr error
	killN    atomic.Int32
	signalN  atomic.Int32
}

func newTestProcess() *testProcess {
	return &testProcess{
		started: make(chan struct{}),
		waitCh:  make(chan error, 1),
	}
}

func (p *testProcess) Start() error {
	if p.startErr != nil {
		return p.startErr
	}
	close(p.started)
	return nil
}

func (p *testProcess) Wait() error {
	return <-p.waitCh
}

func (p *testProcess) Signal(os.Signal) error {
	p.signalN.Add(1)
	return nil
}

func (p *testProcess) Kill() error {
	p.killN.Add(1)
	p.finish(errors.New("killed"))
	return nil
}

func (p *testProcess) finish(err error) {
	p.once.Do(func() {
		p.waitCh <- err
	})
}

func (p *testProcess) finishAfterStart(err error) {
	go func() {
		<-p.started
		p.finish(err)
	}()
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
