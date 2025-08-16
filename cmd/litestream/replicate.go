package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"

	"github.com/mattn/go-shellwords"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gs"
	"github.com/benbjohnson/litestream/nats"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
)

// ReplicateCommand represents a command that continuously replicates SQLite databases.
type ReplicateCommand struct {
	cmd    *exec.Cmd  // subcommand
	execCh chan error // subcommand error channel

	Config Config

	// MCP server
	MCP *MCPServer

	// Manages the set of databases & compaction levels.
	Store *litestream.Store
}

func NewReplicateCommand() *ReplicateCommand {
	return &ReplicateCommand{
		execCh: make(chan error),
	}
}

// ParseFlags parses the CLI flags and loads the configuration file.
func (c *ReplicateCommand) ParseFlags(_ context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-replicate", flag.ContinueOnError)
	execFlag := fs.String("exec", "", "execute subcommand")
	configPath, noExpandEnv := registerConfigFlag(fs)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Load configuration or use CLI args to build db/replica.
	switch fs.NArg() {
	case 0:
		// No arguments provided, use config file
		if *configPath == "" {
			*configPath = DefaultConfigPath()
		}
		if c.Config, err = ReadConfigFile(*configPath, !*noExpandEnv); err != nil {
			return err
		}

	case 1:
		// Only database path provided, missing replica URL
		return fmt.Errorf("must specify at least one replica URL for %s", fs.Arg(0))

	default:
		// Database path and replica URLs provided via CLI
		if *configPath != "" {
			return fmt.Errorf("cannot specify a replica URL and the -config flag")
		}

		// Initialize config with defaults when using command-line arguments
		c.Config = DefaultConfig()

		dbConfig := &DBConfig{Path: fs.Arg(0)}
		for _, u := range fs.Args()[1:] {
			syncInterval := litestream.DefaultSyncInterval
			dbConfig.Replicas = append(dbConfig.Replicas, &ReplicaConfig{
				URL:          u,
				SyncInterval: &syncInterval,
			})
		}
		c.Config.DBs = []*DBConfig{dbConfig}
	}

	c.Config.ConfigPath = *configPath

	// Override config exec command, if specified.
	if *execFlag != "" {
		c.Config.Exec = *execFlag
	}

	return nil
}

// Run loads all databases specified in the configuration.
func (c *ReplicateCommand) Run(ctx context.Context) (err error) {
	// Display version information.
	slog.Info("litestream", "version", Version, "level", c.Config.Logging.Level)

	// Start MCP server if enabled
	if c.Config.MCPAddr != "" {
		c.MCP, err = NewMCP(ctx, c.Config.ConfigPath)
		if err != nil {
			return err
		}
		go c.MCP.Start(c.Config.MCPAddr)
	}

	// Setup databases.
	if len(c.Config.DBs) == 0 {
		slog.Error("no databases specified in configuration")
	}

	dbs := make([]*litestream.DB, 0, len(c.Config.DBs))
	for _, dbConfig := range c.Config.DBs {
		db, err := NewDBFromConfig(dbConfig)
		if err != nil {
			return err
		}
		dbs = append(dbs, db)
	}

	levels := c.Config.CompactionLevels()
	c.Store = litestream.NewStore(dbs, levels)
	// Only override default snapshot interval if explicitly set in config
	if c.Config.Snapshot.Interval != nil {
		c.Store.SnapshotInterval = *c.Config.Snapshot.Interval
	}
	// Only override default snapshot retention if explicitly set in config
	if c.Config.Snapshot.Retention != nil {
		c.Store.SnapshotRetention = *c.Config.Snapshot.Retention
	}
	if err := c.Store.Open(ctx); err != nil {
		return fmt.Errorf("cannot open store: %w", err)
	}

	// Notify user that initialization is done.
	for _, db := range c.Store.DBs() {
		r := db.Replica
		slog.Info("initialized db", "path", db.Path())
		slogWith := slog.With("type", r.Client.Type(), "sync-interval", r.SyncInterval)
		switch client := r.Client.(type) {
		case *file.ReplicaClient:
			slogWith.Info("replicating to", "path", client.Path())
		case *s3.ReplicaClient:
			slogWith.Info("replicating to", "bucket", client.Bucket, "path", client.Path, "region", client.Region, "endpoint", client.Endpoint)
		case *gs.ReplicaClient:
			slogWith.Info("replicating to", "bucket", client.Bucket, "path", client.Path)
		case *abs.ReplicaClient:
			slogWith.Info("replicating to", "bucket", client.Bucket, "path", client.Path, "endpoint", client.Endpoint)
		case *sftp.ReplicaClient:
			slogWith.Info("replicating to", "host", client.Host, "user", client.User, "path", client.Path)
		case *nats.ReplicaClient:
			slogWith.Info("replicating to", "bucket", client.BucketName, "url", client.URL)
		default:
			slogWith.Info("replicating to")
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

		c.cmd = exec.CommandContext(ctx, execArgs[0], execArgs[1:]...)
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
func (c *ReplicateCommand) Close(ctx context.Context) error {
	if err := c.Store.Close(ctx); err != nil {
		slog.Error("failed to close database", "error", err)
	}
	if c.Config.MCPAddr != "" {
		if err := c.MCP.Close(); err != nil {
			slog.Error("error closing MCP server", "error", err)
		}
	}
	return nil
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
