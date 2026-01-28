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
	"strings"

	"github.com/mattn/go-shellwords"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gs"
	"github.com/benbjohnson/litestream/nats"
	"github.com/benbjohnson/litestream/oss"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
)

// ReplicateCommand represents a command that continuously replicates SQLite databases.
type ReplicateCommand struct {
	cmd    *exec.Cmd  // subcommand
	execCh chan error // subcommand error channel

	// One-shot replication flags
	once             bool // replicate once and exit
	forceSnapshot    bool // force snapshot to all replicas
	enforceRetention bool // enforce retention of old snapshots

	Config Config

	// MCP server
	MCP *MCPServer

	// Server for IPC control commands.
	Server *litestream.Server

	// Manages the set of databases & compaction levels.
	Store *litestream.Store

	// Directory monitors for dynamic database discovery.
	directoryMonitors []*DirectoryMonitor
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
	logLevelFlag := fs.String("log-level", "", "log level (trace, debug, info, warn, error)")
	restoreIfDBNotExists := fs.Bool("restore-if-db-not-exists", false, "restore from replica if database doesn't exist")
	onceFlag := fs.Bool("once", false, "replicate once and exit")
	forceSnapshotFlag := fs.Bool("force-snapshot", false, "force snapshot when replicating once")
	enforceRetentionFlag := fs.Bool("enforce-retention", false, "enforce retention of old snapshots when replicating once")
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
		// Override log level if CLI flag provided (takes precedence over env var)
		if *logLevelFlag != "" {
			c.Config.Logging.Level = *logLevelFlag
			// Set env var so initLog sees CLI flag as highest priority
			os.Setenv("LOG_LEVEL", *logLevelFlag)
			logOutput := os.Stdout
			if c.Config.Logging.Stderr {
				logOutput = os.Stderr
			}
			initLog(logOutput, c.Config.Logging.Level, c.Config.Logging.Type)
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
		logLevel := "INFO"
		if *logLevelFlag != "" {
			logLevel = *logLevelFlag
			// Set env var so initLog sees CLI flag as highest priority
			os.Setenv("LOG_LEVEL", *logLevelFlag)
		}
		c.Config.Logging.Level = logLevel
		initLog(os.Stdout, logLevel, "text")

		dbConfig := &DBConfig{
			Path:                 fs.Arg(0),
			RestoreIfDBNotExists: *restoreIfDBNotExists,
		}
		for _, u := range fs.Args()[1:] {
			// Check if this looks like a flag that was placed after positional arguments
			if strings.HasPrefix(u, "-") {
				return fmt.Errorf("flag %q must be positioned before DB_PATH and REPLICA_URL arguments", u)
			}
			syncInterval := litestream.DefaultSyncInterval
			dbConfig.Replicas = append(dbConfig.Replicas, &ReplicaConfig{
				URL: u,
				ReplicaSettings: ReplicaSettings{
					SyncInterval: &syncInterval,
				},
			})
		}
		c.Config.DBs = []*DBConfig{dbConfig}
	}

	c.Config.ConfigPath = *configPath

	// Override config exec command, if specified.
	if *execFlag != "" {
		c.Config.Exec = *execFlag
	}

	// Apply restore-if-db-not-exists flag to all databases if specified.
	// This allows the CLI flag to work with config files.
	if *restoreIfDBNotExists {
		for _, dbConfig := range c.Config.DBs {
			dbConfig.RestoreIfDBNotExists = true
		}
	}

	// Set one-shot replication flags and validate their usage.
	c.once = *onceFlag
	c.forceSnapshot = *forceSnapshotFlag
	c.enforceRetention = *enforceRetentionFlag

	// Validate flag combinations.
	if c.once && c.Config.Exec != "" {
		return fmt.Errorf("cannot specify -once flag with -exec")
	}
	if c.forceSnapshot && !c.once {
		return fmt.Errorf("cannot specify -force-snapshot flag without -once")
	}
	if c.enforceRetention && !c.once {
		return fmt.Errorf("cannot specify -enforce-retention flag without -once")
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

	// Attempt restore for databases that need it (before creating DB objects)
	for _, dbConfig := range c.Config.DBs {
		if dbConfig.RestoreIfDBNotExists && dbConfig.Path != "" {
			if err := c.restoreIfNeeded(ctx, dbConfig); err != nil {
				return err
			}
		}
	}

	var dbs []*litestream.DB
	var watchables []struct {
		config *DBConfig
		dbs    []*litestream.DB
	}
	for _, dbConfig := range c.Config.DBs {
		// Handle directory configuration
		if dbConfig.Dir != "" {
			dirDbs, err := NewDBsFromDirectoryConfig(dbConfig)
			if err != nil {
				return err
			}
			dbs = append(dbs, dirDbs...)
			slog.Info("found databases in directory", "dir", dbConfig.Dir, "count", len(dirDbs), "watch", dbConfig.Watch)
			if dbConfig.Watch {
				watchables = append(watchables, struct {
					config *DBConfig
					dbs    []*litestream.DB
				}{config: dbConfig, dbs: dirDbs})
			}
		} else {
			// Handle single database configuration
			db, err := NewDBFromConfig(dbConfig)
			if err != nil {
				return err
			}
			dbs = append(dbs, db)
		}
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
	if c.Config.L0Retention != nil {
		c.Store.SetL0Retention(*c.Config.L0Retention)
	}
	if c.Config.L0RetentionCheckInterval != nil {
		c.Store.L0RetentionCheckInterval = *c.Config.L0RetentionCheckInterval
	}
	if c.Config.ShutdownSyncTimeout != nil {
		c.Store.SetShutdownSyncTimeout(*c.Config.ShutdownSyncTimeout)
	}
	if c.Config.ShutdownSyncInterval != nil {
		c.Store.SetShutdownSyncInterval(*c.Config.ShutdownSyncInterval)
	}
	if c.Config.VerifyCompaction {
		c.Store.SetVerifyCompaction(true)
	}
	if c.Config.HeartbeatURL != "" {
		interval := litestream.DefaultHeartbeatInterval
		if c.Config.HeartbeatInterval != nil {
			interval = *c.Config.HeartbeatInterval
		}
		c.Store.Heartbeat = litestream.NewHeartbeatClient(c.Config.HeartbeatURL, interval)
	}

	// Disable all background monitors when running once.
	// This must be done after config settings are applied.
	if c.once {
		c.Store.CompactionMonitorEnabled = false
		c.Store.L0RetentionCheckInterval = 0
		for _, db := range dbs {
			db.MonitorInterval = 0
			if db.Replica != nil {
				db.Replica.MonitorEnabled = false
			}
		}
	}

	if err := c.Store.Open(ctx); err != nil {
		return fmt.Errorf("cannot open store: %w", err)
	}

	// Start control server if socket is enabled
	if c.Config.Socket.Enabled {
		c.Server = litestream.NewServer(c.Store)
		c.Server.SocketPath = c.Config.Socket.Path
		c.Server.SocketPerms = c.Config.Socket.Permissions
		c.Server.PathExpander = expand
		if err := c.Server.Start(); err != nil {
			slog.Warn("failed to start control server", "error", err)
		}

		// Wire up AfterSync callbacks for status monitoring
		for _, db := range c.Store.DBs() {
			if db.Replica != nil {
				db := db // capture for closure
				db.Replica.AfterSync = func(pos ltx.Pos) {
					c.Server.StatusMonitor.NotifySync(db, pos)
				}
			}
		}
	}

	for _, entry := range watchables {
		monitor, err := NewDirectoryMonitor(ctx, c.Store, entry.config, entry.dbs)
		if err != nil {
			for _, m := range c.directoryMonitors {
				m.Close()
			}
			if closeErr := c.Store.Close(ctx); closeErr != nil {
				slog.Error("failed to close store after monitor failure", "error", closeErr)
			}
			return fmt.Errorf("start directory monitor for %s: %w", entry.config.Dir, err)
		}
		c.directoryMonitors = append(c.directoryMonitors, monitor)
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
		case *oss.ReplicaClient:
			slogWith.Info("replicating to", "bucket", client.Bucket, "path", client.Path, "region", client.Region)
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
	} else if c.once {
		// Run one-shot replication in a goroutine so the caller can wait on execCh.
		go c.runOnce(ctx)
	}

	return nil
}

// runOnce performs one-shot replication for all databases.
// It syncs all databases, optionally takes snapshots, and enforces retention.
func (c *ReplicateCommand) runOnce(ctx context.Context) {
	var err error
	defer func() { c.execCh <- err }()

	for _, db := range c.Store.DBs() {
		slog.Info("syncing database", "path", db.Path())

		// Sync the database to process any pending WAL changes.
		if err = db.Sync(ctx); err != nil {
			err = fmt.Errorf("sync database %s: %w", db.Path(), err)
			return
		}

		// Sync the replica to upload any pending LTX files.
		if err = db.Replica.Sync(ctx); err != nil {
			err = fmt.Errorf("sync replica for %s: %w", db.Path(), err)
			return
		}

		// Force a snapshot if requested.
		if c.forceSnapshot {
			slog.Info("taking snapshot", "path", db.Path())
			if _, err = db.Snapshot(ctx); err != nil {
				err = fmt.Errorf("snapshot %s: %w", db.Path(), err)
				return
			}
		}

		// Enforce retention if requested.
		if c.enforceRetention {
			slog.Info("enforcing retention", "path", db.Path())
			if err = c.Store.EnforceSnapshotRetention(ctx, db); err != nil {
				err = fmt.Errorf("enforce retention for %s: %w", db.Path(), err)
				return
			}
		}
	}

	slog.Info("one-shot replication complete")
}

// Close closes all open databases.
func (c *ReplicateCommand) Close(ctx context.Context) error {
	for _, monitor := range c.directoryMonitors {
		monitor.Close()
	}
	c.directoryMonitors = nil

	if c.Server != nil {
		if err := c.Server.Close(); err != nil {
			slog.Error("error closing control server", "error", err)
		}
	}
	if c.Store != nil {
		if err := c.Store.Close(ctx); err != nil {
			slog.Error("failed to close database", "error", err)
		}
	}
	if c.Config.MCPAddr != "" && c.MCP != nil {
		if err := c.MCP.Close(); err != nil {
			slog.Error("error closing MCP server", "error", err)
		}
	}
	return nil
}

// restoreIfNeeded restores a database from its replica if the database doesn't
// exist and the RestoreIfDBNotExists option is enabled. If no backup exists
// (first start scenario), it returns nil to allow fresh replication to begin.
func (c *ReplicateCommand) restoreIfNeeded(ctx context.Context, dbConfig *DBConfig) error {
	dbPath, err := expand(dbConfig.Path)
	if err != nil {
		return err
	}

	// Skip if database already exists
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		slog.Info("database exists, skipping restore", "path", dbPath)
		return nil
	}

	// Get replica config (handles both Replica and Replicas fields)
	var rc *ReplicaConfig
	if dbConfig.Replica != nil {
		rc = dbConfig.Replica
	} else if len(dbConfig.Replicas) > 0 {
		rc = dbConfig.Replicas[0]
	} else {
		return fmt.Errorf("no replica configured for database: %s", dbPath)
	}

	// Create replica from config (nil db since we're just restoring)
	r, err := NewReplicaFromConfig(rc, nil)
	if err != nil {
		return fmt.Errorf("cannot create replica for restore: %w", err)
	}

	// Attempt restore
	opt := litestream.NewRestoreOptions()
	opt.OutputPath = dbPath

	slog.Info("attempting restore before replication", "path", dbPath)
	if err := r.Restore(ctx, opt); errors.Is(err, litestream.ErrTxNotAvailable) {
		// No backup exists yet (first start) - this is OK
		slog.Info("no backup found, starting fresh", "path", dbPath)
		return nil
	} else if err != nil {
		return fmt.Errorf("restore failed: %w", err)
	}

	slog.Info("restore completed", "path", dbPath)
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

	-once
	    Replicate once and exit. This performs a single sync of all
	    databases and their replicas, then exits. Cannot be used with -exec.

	-force-snapshot
	    Force a snapshot to be taken for all databases. Requires -once.
	    This is useful for creating a complete backup before maintenance
	    or when migrating databases between hosts.

	-enforce-retention
	    Enforce retention policies for old snapshots. Requires -once.
	    This removes snapshots that are older than the configured
	    snapshot retention period.

	-log-level LEVEL
	    Sets the log level. Overrides the config file setting.
	    Valid values: trace, debug, info, warn, error

	-no-expand-env
	    Disables environment variable expansion in configuration file.

	-restore-if-db-not-exists
	    Restores the database from the replica if it doesn't exist.
	    On first start with no backup, proceeds normally.

`[1:], DefaultConfigPath())
}
