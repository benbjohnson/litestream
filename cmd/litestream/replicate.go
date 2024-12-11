package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gcs"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
	"github.com/mattn/go-shellwords"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReplicateCommand represents a command that continuously replicates SQLite databases.
type ReplicateCommand struct {
	cmd           *exec.Cmd // subcommand
	wg            sync.WaitGroup
	execCh        chan error    // subcommand error channel
	leaseExpireCh chan struct{} // lease expiration error channel
	leaserCtx     context.Context
	leaserCancel  context.CancelCauseFunc

	// Holds the current lease, if any.
	lease atomic.Value // *litestream.Lease

	Config Config

	// Lease client for managing distributed lease.
	// May be nil if no lease config specified.
	Leaser litestream.Leaser

	// List of managed databases specified in the config.
	DBs []*litestream.DB
}

func NewReplicateCommand() *ReplicateCommand {
	c := &ReplicateCommand{
		execCh:        make(chan error),
		leaseExpireCh: make(chan struct{}),
	}
	c.leaserCtx, c.leaserCancel = context.WithCancelCause(context.Background())

	c.lease.Store((*litestream.Lease)(nil))
	return c
}

func (c *ReplicateCommand) Lease() *litestream.Lease {
	return c.lease.Load().(*litestream.Lease)
}

// ParseFlags parses the CLI flags and loads the configuration file.
func (c *ReplicateCommand) ParseFlags(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-replicate", flag.ContinueOnError)
	execFlag := fs.String("exec", "", "execute subcommand")
	configPath, noExpandEnv := registerConfigFlag(fs)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Load configuration or use CLI args to build db/replica.
	if fs.NArg() == 1 {
		return fmt.Errorf("must specify at least one replica URL for %s", fs.Arg(0))
	} else if fs.NArg() > 1 {
		if *configPath != "" {
			return fmt.Errorf("cannot specify a replica URL and the -config flag")
		}

		dbConfig := &DBConfig{Path: fs.Arg(0)}
		for _, u := range fs.Args()[1:] {
			syncInterval := litestream.DefaultSyncInterval
			dbConfig.Replicas = append(dbConfig.Replicas, &ReplicaConfig{
				URL:          u,
				SyncInterval: &syncInterval,
			})
		}
		c.Config.DBs = []*DBConfig{dbConfig}
	} else {
		if *configPath == "" {
			*configPath = DefaultConfigPath()
		}
		if c.Config, err = ReadConfigFile(*configPath, !*noExpandEnv); err != nil {
			return err
		}
	}

	// Override config exec command, if specified.
	if *execFlag != "" {
		c.Config.Exec = *execFlag
	}

	return nil
}

// Run loads all databases specified in the configuration.
func (c *ReplicateCommand) Run() (err error) {
	// Display version information.
	slog.Info("litestream", "version", Version)

	// Acquire lease if config specified.
	if c.Config.Lease != nil {
		c.Leaser, err = NewLeaserFromConfig(c.Config.Lease)
		if err != nil {
			return fmt.Errorf("initialize leaser: %w", err)
		}

		if err := c.acquireLease(context.Background()); err != nil {
			return fmt.Errorf("acquire initial lease: %w", err)
		}
	}

	// Setup databases.
	if len(c.Config.DBs) == 0 {
		slog.Error("no databases specified in configuration")
	}

	for _, dbConfig := range c.Config.DBs {
		db, err := NewDBFromConfig(dbConfig)
		if err != nil {
			return err
		}

		// Open database & attach to program.
		if err := db.Open(); err != nil {
			return err
		}
		c.DBs = append(c.DBs, db)
	}

	// Notify user that initialization is done.
	for _, db := range c.DBs {
		slog.Info("initialized db", "path", db.Path())
		for _, r := range db.Replicas {
			slog := slog.With("name", r.Name(), "type", r.Client.Type(), "sync-interval", r.SyncInterval)
			switch client := r.Client.(type) {
			case *file.ReplicaClient:
				slog.Info("replicating to", "path", client.Path())
			case *s3.ReplicaClient:
				slog.Info("replicating to", "bucket", client.Bucket, "path", client.Path, "region", client.Region, "endpoint", client.Endpoint)
			case *gcs.ReplicaClient:
				slog.Info("replicating to", "bucket", client.Bucket, "path", client.Path)
			case *abs.ReplicaClient:
				slog.Info("replicating to", "bucket", client.Bucket, "path", client.Path, "endpoint", client.Endpoint)
			case *sftp.ReplicaClient:
				slog.Info("replicating to", "host", client.Host, "user", client.User, "path", client.Path)
			default:
				slog.Info("replicating to")
			}
		}
	}

	// Serve metrics over HTTP if enabled.
	if c.Config.Addr != "" {
		hostport := c.Config.Addr
		if host, port, _ := net.SplitHostPort(c.Config.Addr); port == "" {
			return fmt.Errorf("must specify port for bind address: %q", c.Config.Addr)
		} else if host == "" {
			hostport = net.JoinHostPort("localhost", port)
		}

		slog.Info("serving metrics on", "url", fmt.Sprintf("http://%s/metrics", hostport))
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(c.Config.Addr, nil); err != nil {
				slog.Error("cannot start metrics server", "error", err)
			}
		}()
	}

	// Parse exec commands args & start subprocess.
	if c.Config.Exec != "" {
		execArgs, err := shellwords.Parse(c.Config.Exec)
		if err != nil {
			return fmt.Errorf("cannot parse exec command: %w", err)
		}

		c.cmd = exec.Command(execArgs[0], execArgs[1:]...)
		c.cmd.Env = os.Environ()
		c.cmd.Stdout = os.Stdout
		c.cmd.Stderr = os.Stderr
		if err := c.cmd.Start(); err != nil {
			return fmt.Errorf("cannot start exec command: %w", err)
		}
		go func() { c.execCh <- c.cmd.Wait() }()
	}

	return nil
}

// Close closes all open databases.
func (c *ReplicateCommand) Close() (err error) {
	for _, db := range c.DBs {
		if e := db.Close(context.Background()); e != nil {
			db.Logger.Error("error closing db", "error", e)
			if err == nil {
				err = e
			}
		}
	}

	// Stop lease monitoring.
	c.leaserCancel(errors.New("litestream shutting down"))
	c.wg.Wait()

	// Release the most recent lease.
	if lease := c.Lease(); lease != nil {
		slog.Info("releasing lease", slog.Int64("epoch", lease.Epoch))

		if e := c.Leaser.ReleaseLease(context.Background(), lease.Epoch); e != nil {
			slog.Error("failed to release lease",
				slog.Int64("epoch", lease.Epoch),
				slog.Any("error", e))
		}
	}

	return err
}

// acquireLease initializes a lease client based on the config, acquires the initial
// lease, and then continuously monitors & renews the lease in the background.
func (c *ReplicateCommand) acquireLease(ctx context.Context) (err error) {
	timer := time.NewTimer(1)
	defer timer.Stop()

	// Continually try to acquire lease if there is an existing lease.
OUTER:
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-timer.C:
			var leaseExistsError *litestream.LeaseExistsError
			lease, err := c.Leaser.AcquireLease(ctx)
			if errors.As(err, &leaseExistsError) {
				timer.Reset(litestream.LeaseRetryInterval)
				slog.Info("lease already exists, waiting to retry",
					slog.Int64("epoch", leaseExistsError.Lease.Epoch),
					slog.String("owner", leaseExistsError.Lease.Owner),
					slog.Time("expires", leaseExistsError.Lease.Deadline()))
				continue
			} else if err != nil {
				return fmt.Errorf("acquire lease: %w", err)
			}
			c.lease.Store(lease)
			break OUTER
		}
	}

	lease := c.Lease()
	slog.Info("lease acquired",
		slog.Int64("epoch", lease.Epoch),
		slog.Duration("timeout", lease.Timeout),
		slog.String("owner", lease.Owner))

	// Continuously monitor and renew lease in a separate goroutine.
	c.wg.Add(1)
	go func() { defer c.wg.Done(); c.monitorLease(c.leaserCtx) }()

	return nil
}

func (c *ReplicateCommand) monitorLease(ctx context.Context) {
	timer := time.NewTimer(c.Lease().Timeout / 2)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Error("stopping lease monitor")
			return

		case <-timer.C:
			var leaseExistsError *litestream.LeaseExistsError

			lease := c.Lease()
			slog.Debug("attempting to renew lease", slog.Int64("epoch", lease.Epoch))

			// Attempt to renew our currently held lease.
			newLease, err := c.Leaser.RenewLease(ctx, lease)
			if errors.As(err, &leaseExistsError) {
				slog.Error("cannot renew lease, another lease exists, exiting",
					slog.Int64("epoch", leaseExistsError.Lease.Epoch),
					slog.String("owner", leaseExistsError.Lease.Owner))
				c.leaseExpireCh <- struct{}{}
				return
			}

			// If our lease has expired then give up and exit.
			if lease.Expired() {
				slog.Error("lease expired, exiting")
				c.leaseExpireCh <- struct{}{}
				return
			}

			// If we hit a temporary error then aggressively retry.
			if err != nil {
				slog.Warn("temporarily unable to renew lease, retrying", slog.Any("error", err))
				timer.Reset(1 * time.Second)
				continue
			}

			// Replace lease and try to renew after halfway through the timeout.
			slog.Debug("lease renewed", slog.Int64("epoch", newLease.Epoch))
			c.lease.Store(newLease)
			timer.Reset(lease.Timeout / 2)
		}
	}
}

// Usage prints the help screen to STDOUT.
func (c *ReplicateCommand) Usage() {
	fmt.Printf(`
The replicate command starts a server to monitor & replicate databases.
You can specify your database & replicas in a configuration file or you can
replicate a single database file by specifying its path and its replicas in the
command line arguments.

Usage:

	litestream replicate [arguments]

	litestream replicate [arguments] DB_PATH REPLICA_URL [REPLICA_URL...]

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-exec CMD
	    Executes a subcommand. Litestream will exit when the child
	    process exits. Useful for simple process management.

	-no-expand-env
	    Disables environment variable expansion in configuration file.

`[1:], DefaultConfigPath())
}
