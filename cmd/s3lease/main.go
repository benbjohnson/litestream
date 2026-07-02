package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	_ "golang.org/x/crypto/x509roots/fallback"

	"github.com/benbjohnson/litestream"
	litestreams3 "github.com/benbjohnson/litestream/s3"
)

const (
	defaultAcquireRetryInterval = time.Second
	defaultReleaseTimeout       = 10 * time.Second
)

var (
	ErrLeaseURLRequired      = errors.New("lease url required")
	ErrCommandRequired       = errors.New("command required")
	ErrInvalidLeaseTTL       = errors.New("lease ttl must be greater than 0")
	ErrInvalidHeartbeat      = errors.New("lease heartbeat must be greater than 0 and less than ttl")
	ErrInvalidAcquireTimeout = errors.New("lease acquire timeout must be >= 0")
	ErrInvalidRetryInterval  = errors.New("lease retry interval must be greater than 0")
)

type Main struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	signals    <-chan os.Signal
	newLeaser  func(context.Context, string) (litestream.Leaser, error)
	newProcess func([]string) subprocess
}

func NewMain() *Main {
	return &Main{
		Stdin:     os.Stdin,
		Stdout:    os.Stdout,
		Stderr:    os.Stderr,
		newLeaser: newS3Leaser,
	}
}

func main() {
	m := NewMain()
	if err := m.Run(context.Background(), os.Args[1:]); errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	} else if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(exitCode(err))
	}
}

func exitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if code := exitErr.ExitCode(); code >= 0 {
			return code
		}
	}
	return 1
}

type runConfig struct {
	leaseURL       string
	ttl            time.Duration
	heartbeat      time.Duration
	acquireTimeout time.Duration
	retryInterval  time.Duration
	owner          string
	command        []string
}

func (m *Main) Run(ctx context.Context, args []string) error {
	config, err := m.parseFlags(args)
	if err != nil {
		return err
	}

	leaser, err := m.newLeaser(ctx, config.leaseURL)
	if err != nil {
		return fmt.Errorf("create s3 leaser: %w", err)
	}
	if l, ok := leaser.(*litestreams3.Leaser); ok {
		l.TTL = config.ttl
		if config.owner != "" {
			l.Owner = config.owner
		}
	}

	sigCh, stopSignals := m.signalChan()
	defer stopSignals()

	acquireCtx, stopAcquireSignalWatch := contextWithSignal(ctx, sigCh)
	lease, err := acquireLease(acquireCtx, leaser, config.acquireTimeout, config.retryInterval)
	if sig, ok := stopAcquireSignalWatch(); ok {
		return fmt.Errorf("signal received while acquiring lease: %s", sig)
	}
	if err != nil {
		return err
	}

	process := m.process(config.command)
	if err := process.Start(); err != nil {
		if releaseErr := releaseLease(leaser, lease); releaseErr != nil {
			return fmt.Errorf("start command: %w; release lease: %v", err, releaseErr)
		}
		return fmt.Errorf("start command: %w", err)
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- process.Wait()
	}()

	leaseState := &leaseState{lease: lease}
	renewCtx, cancelRenew := context.WithCancel(ctx)
	renewErrCh := make(chan error, 1)
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		if err := renewLease(renewCtx, leaser, leaseState, config.heartbeat); err != nil {
			renewErrCh <- err
		}
	}()

	err = waitForProcess(ctx, process, waitCh, renewErrCh, sigCh)
	cancelRenew()
	<-renewDone

	if releaseErr := releaseLease(leaser, leaseState.Lease()); releaseErr != nil {
		if err != nil {
			return fmt.Errorf("%w; release lease: %v", err, releaseErr)
		}
		return releaseErr
	}
	return err
}

func (m *Main) parseFlags(args []string) (runConfig, error) {
	config := runConfig{
		ttl:           litestreams3.DefaultLeaseTTL,
		retryInterval: defaultAcquireRetryInterval,
	}

	fs := flag.NewFlagSet("s3lease", flag.ContinueOnError)
	fs.SetOutput(m.Stderr)
	fs.StringVar(&config.leaseURL, "url", "", "S3 replica URL used as the lease location")
	fs.DurationVar(&config.ttl, "ttl", config.ttl, "lease time-to-live")
	fs.DurationVar(&config.heartbeat, "heartbeat", 0, "lease renewal interval; defaults to half of ttl")
	fs.DurationVar(&config.acquireTimeout, "acquire-timeout", 0, "maximum time to wait for the lease; 0 waits forever")
	fs.DurationVar(&config.retryInterval, "retry-interval", config.retryInterval, "delay between lease acquisition attempts")
	fs.StringVar(&config.owner, "owner", "", "lease owner identifier")
	fs.Usage = func() { m.Usage() }

	if err := fs.Parse(args); err != nil {
		return config, err
	}

	config.command = fs.Args()
	if config.heartbeat == 0 {
		config.heartbeat = config.ttl / 2
	}
	if err := config.Validate(); err != nil {
		return config, err
	}
	return config, nil
}

func (c runConfig) Validate() error {
	if c.leaseURL == "" {
		return ErrLeaseURLRequired
	}
	if len(c.command) == 0 {
		return ErrCommandRequired
	}
	if c.ttl <= 0 {
		return ErrInvalidLeaseTTL
	}
	if c.heartbeat <= 0 || c.heartbeat >= c.ttl {
		return ErrInvalidHeartbeat
	}
	if c.acquireTimeout < 0 {
		return ErrInvalidAcquireTimeout
	}
	if c.retryInterval <= 0 {
		return ErrInvalidRetryInterval
	}
	return nil
}

func (m *Main) Usage() {
	fmt.Fprint(m.Stderr, `
s3lease runs a command while holding a distributed S3 lease.

Usage:

	s3lease -url S3_URL [arguments] COMMAND [ARG...]

Arguments:

	-url URL
	    S3 replica URL used as the lease location. The lock is stored as
	    lock.json under the URL path.

	-ttl DURATION
	    Lease time-to-live. Defaults to 30s.

	-heartbeat DURATION
	    Lease renewal interval. Defaults to half of -ttl.

	-acquire-timeout DURATION
	    Maximum time to wait for the lease. Defaults to 0, which waits forever.

	-retry-interval DURATION
	    Delay between lease acquisition attempts. Defaults to 1s.

	-owner NAME
	    Lease owner identifier. Defaults to hostname:pid.

Example:

	s3lease -url s3://my-bucket/db litestream replicate -config /etc/litestream.yml

`[1:])
}

func (m *Main) signalChan() (<-chan os.Signal, func()) {
	if m.signals != nil {
		return m.signals, func() {}
	}
	return notifySignals()
}

func (m *Main) process(args []string) subprocess {
	if m.newProcess != nil {
		return m.newProcess(args)
	}

	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin = m.Stdin
	cmd.Stdout = m.Stdout
	cmd.Stderr = m.Stderr
	return (*execProcess)(cmd)
}

type subprocess interface {
	Start() error
	Wait() error
	Signal(os.Signal) error
	Kill() error
}

type execProcess exec.Cmd

func (p *execProcess) Start() error {
	return (*exec.Cmd)(p).Start()
}

func (p *execProcess) Wait() error {
	return (*exec.Cmd)(p).Wait()
}

func (p *execProcess) Signal(sig os.Signal) error {
	return (*exec.Cmd)(p).Process.Signal(sig)
}

func (p *execProcess) Kill() error {
	return (*exec.Cmd)(p).Process.Kill()
}

func newS3Leaser(ctx context.Context, rawURL string) (litestream.Leaser, error) {
	client, err := litestream.NewReplicaClientFromURL(rawURL)
	if err != nil {
		return nil, err
	}

	s3Client, ok := client.(*litestreams3.ReplicaClient)
	if !ok {
		return nil, fmt.Errorf("lease url must use s3 scheme")
	}
	return s3Client.NewLeaser(ctx)
}

func acquireLease(ctx context.Context, leaser litestream.Leaser, timeout, retryInterval time.Duration) (*litestream.Lease, error) {
	acquireCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		acquireCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	for {
		if err := acquireCtx.Err(); err != nil {
			return nil, fmt.Errorf("acquire lease: %w", err)
		}

		lease, err := leaser.AcquireLease(acquireCtx)
		if err == nil {
			return lease, nil
		}

		var existsErr *litestream.LeaseExistsError
		if !errors.As(err, &existsErr) {
			return nil, fmt.Errorf("acquire lease: %w", err)
		}

		wait := retryInterval
		if !existsErr.ExpiresAt.IsZero() {
			if until := time.Until(existsErr.ExpiresAt); until > 0 && until < wait {
				wait = until
			}
		}

		timer := time.NewTimer(wait)
		select {
		case <-acquireCtx.Done():
			timer.Stop()
			return nil, fmt.Errorf("acquire lease: %w", acquireCtx.Err())
		case <-timer.C:
		}
	}
}

type leaseState struct {
	lease *litestream.Lease
}

func (s *leaseState) Lease() *litestream.Lease {
	return s.lease
}

func (s *leaseState) SetLease(lease *litestream.Lease) {
	s.lease = lease
}

func renewLease(ctx context.Context, leaser litestream.Leaser, state *leaseState, heartbeat time.Duration) error {
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		lease := state.Lease()
		nextLease, err := leaser.RenewLease(ctx, lease)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("renew lease: %w", err)
		}
		state.SetLease(nextLease)
	}
}

func waitForProcess(ctx context.Context, process subprocess, waitCh <-chan error, renewCh <-chan error, sigCh <-chan os.Signal) error {
	signaled := false

	for {
		select {
		case err := <-waitCh:
			if err != nil {
				return fmt.Errorf("command exited: %w", err)
			}
			return nil

		case err := <-renewCh:
			if err != nil {
				if killErr := process.Kill(); killErr != nil {
					return fmt.Errorf("%w; kill command: %v", err, killErr)
				}
				<-waitCh
				return err
			}

		case sig := <-sigCh:
			if signaled {
				if err := process.Kill(); err != nil {
					return fmt.Errorf("kill command: %w", err)
				}
				<-waitCh
				return fmt.Errorf("signal received: %s", sig)
			}
			signaled = true
			if err := process.Signal(sig); err != nil {
				return fmt.Errorf("signal command: %w", err)
			}

		case <-ctx.Done():
			if err := process.Kill(); err != nil {
				return fmt.Errorf("%w; kill command: %v", ctx.Err(), err)
			}
			<-waitCh
			return ctx.Err()
		}
	}
}

func releaseLease(leaser litestream.Leaser, lease *litestream.Lease) error {
	if lease == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultReleaseTimeout)
	defer cancel()

	if err := leaser.ReleaseLease(ctx, lease); err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	return nil
}

func contextWithSignal(ctx context.Context, sigCh <-chan os.Signal) (context.Context, func() (os.Signal, bool)) {
	if sigCh == nil {
		return ctx, func() (os.Signal, bool) { return nil, false }
	}

	signalCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	watchDone := make(chan struct{})
	sigOut := make(chan os.Signal, 1)

	go func() {
		defer close(watchDone)
		select {
		case sig := <-sigCh:
			sigOut <- sig
			cancel()
		case <-done:
		}
	}()

	return signalCtx, func() (os.Signal, bool) {
		close(done)
		<-watchDone
		cancel()
		select {
		case sig := <-sigOut:
			return sig, true
		default:
			return nil, false
		}
	}
}
