package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/url"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/superfly/ltx"
	"gopkg.in/yaml.v2"
	_ "modernc.org/sqlite"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/gs"
	"github.com/benbjohnson/litestream/internal"
	"github.com/benbjohnson/litestream/nats"
	"github.com/benbjohnson/litestream/oss"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
	"github.com/benbjohnson/litestream/webdav"
)

// Build information.
var (
	Version = "(development build)"
)

// errStop is a terminal error for indicating program should quit.
var errStop = errors.New("stop")

// Sentinel errors for configuration validation
var (
	ErrInvalidSnapshotInterval         = errors.New("snapshot interval must be greater than 0")
	ErrInvalidSnapshotRetention        = errors.New("snapshot retention must be greater than 0")
	ErrInvalidCompactionInterval       = errors.New("compaction interval must be greater than 0")
	ErrInvalidSyncInterval             = errors.New("sync interval must be greater than 0")
	ErrInvalidL0Retention              = errors.New("l0 retention must be greater than 0")
	ErrInvalidL0RetentionCheckInterval = errors.New("l0 retention check interval must be greater than 0")
	ErrInvalidShutdownSyncTimeout      = errors.New("shutdown-sync-timeout must be >= 0")
	ErrInvalidShutdownSyncInterval     = errors.New("shutdown sync interval must be greater than 0")
	ErrConfigFileNotFound              = errors.New("config file not found")
)

// ConfigValidationError wraps a validation error with additional context
type ConfigValidationError struct {
	Err   error
	Field string
	Value interface{}
}

func (e *ConfigValidationError) Error() string {
	if e.Value != nil {
		return fmt.Sprintf("%s: %v (got %v)", e.Field, e.Err, e.Value)
	}
	return fmt.Sprintf("%s: %v", e.Field, e.Err)
}

func (e *ConfigValidationError) Unwrap() error {
	return e.Err
}

func main() {
	m := NewMain()
	if err := m.Run(context.Background(), os.Args[1:]); errors.Is(err, flag.ErrHelp) || errors.Is(err, errStop) {
		os.Exit(1)
	} else if err != nil {
		slog.Error("failed to run", "error", err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct{}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{}
}

// Run executes the program.
func (m *Main) Run(ctx context.Context, args []string) (err error) {
	// Execute replication command if running as a Windows service.
	if isService, err := isWindowsService(); err != nil {
		return err
	} else if isService {
		return runWindowsService(ctx)
	}

	// Copy "LITESTEAM" environment credentials.
	applyLitestreamEnv()

	// Extract command name.
	var cmd string
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "databases":
		return (&DatabasesCommand{}).Run(ctx, args)
	case "replicate":
		c := NewReplicateCommand()
		if err := c.ParseFlags(ctx, args); err != nil {
			return err
		}

		// Setup signal handler.
		signalCh := signalChan()

		if err := c.Run(ctx); err != nil {
			return err
		}

		// Wait for signal to stop program.
		select {
		case err = <-c.execCh:
			slog.Info("subprocess exited, litestream shutting down")
		case sig := <-signalCh:
			slog.Info("signal received, litestream shutting down")

			if c.cmd != nil {
				slog.Info("sending signal to exec process")
				if err := c.cmd.Process.Signal(sig); err != nil {
					return fmt.Errorf("cannot signal exec process: %w", err)
				}

				slog.Info("waiting for exec process to close")
				if err := <-c.execCh; err != nil && !strings.HasPrefix(err.Error(), "signal:") {
					return fmt.Errorf("cannot wait for exec process: %w", err)
				}
			}
		}

		// Gracefully close.
		if e := c.Close(ctx); e != nil && err == nil {
			err = e
		}
		slog.Info("litestream shut down")
		return err

	case "restore":
		return (&RestoreCommand{}).Run(ctx, args)
	case "version":
		return (&VersionCommand{}).Run(ctx, args)
	case "ltx":
		return (&LTXCommand{}).Run(ctx, args)
	case "wal":
		// Deprecated: Keep for backward compatibility
		fmt.Fprintln(os.Stderr, "Warning: 'wal' command is deprecated, please use 'ltx' instead")
		return (&LTXCommand{}).Run(ctx, args)
	default:
		if cmd == "" || cmd == "help" || strings.HasPrefix(cmd, "-") {
			m.Usage()
			return flag.ErrHelp
		}
		return fmt.Errorf("litestream %s: unknown command", cmd)
	}
}

// Usage prints the help screen to STDOUT.
func (m *Main) Usage() {
	fmt.Println(`
litestream is a tool for replicating SQLite databases.

Usage:

	litestream <command> [arguments]

The commands are:

	databases    list databases specified in config file
	ltx          list available LTX files for a database
	replicate    runs a server to replicate databases
	restore      recovers database backup from a replica
	version      prints the binary version
`[1:])
}

// Config represents a configuration file for the litestream daemon.
type Config struct {
	// Global replica settings that serve as defaults for all replicas
	ReplicaSettings `yaml:",inline"`

	// Bind address for serving metrics.
	Addr string `yaml:"addr"`

	// List of stages in a multi-level compaction.
	// Only includes L1 through the last non-snapshot level.
	Levels []*CompactionLevelConfig `yaml:"levels"`

	// Snapshot configuration
	Snapshot SnapshotConfig `yaml:"snapshot"`

	// L0 retention settings
	L0Retention              *time.Duration `yaml:"l0-retention"`
	L0RetentionCheckInterval *time.Duration `yaml:"l0-retention-check-interval"`

	// List of databases to manage.
	DBs []*DBConfig `yaml:"dbs"`

	// Subcommand to execute during replication.
	// Litestream will shutdown when subcommand exits.
	Exec string `yaml:"exec"`

	// Logging
	Logging LoggingConfig `yaml:"logging"`

	// MCP server options
	MCPAddr string `yaml:"mcp-addr"`

	// Shutdown sync retry settings
	ShutdownSyncTimeout  *time.Duration `yaml:"shutdown-sync-timeout"`
	ShutdownSyncInterval *time.Duration `yaml:"shutdown-sync-interval"`

	// Path to the config file
	// This is only used internally to pass the config path to the MCP tool
	ConfigPath string `yaml:"-"`
}

// SnapshotConfig configures snapshots.
type SnapshotConfig struct {
	Interval  *time.Duration `yaml:"interval"`
	Retention *time.Duration `yaml:"retention"`
}

// LoggingConfig configures logging.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Type   string `yaml:"type"`
	Stderr bool   `yaml:"stderr"`
}

// propagateGlobalSettings copies global replica settings to individual replica configs.
func (c *Config) propagateGlobalSettings() {
	for _, dbc := range c.DBs {
		// Handle both old-style 'replicas' and new-style 'replica'
		if dbc.Replica != nil {
			dbc.Replica.SetDefaults(&c.ReplicaSettings)
		}
		for _, rc := range dbc.Replicas {
			rc.SetDefaults(&c.ReplicaSettings)
		}
	}
}

// DefaultConfig returns a new instance of Config with defaults set.
func DefaultConfig() Config {
	defaultSnapshotInterval := 24 * time.Hour
	defaultSnapshotRetention := 24 * time.Hour
	defaultL0Retention := litestream.DefaultL0Retention
	defaultL0RetentionCheckInterval := litestream.DefaultL0RetentionCheckInterval
	defaultShutdownSyncTimeout := litestream.DefaultShutdownSyncTimeout
	defaultShutdownSyncInterval := litestream.DefaultShutdownSyncInterval
	return Config{
		Levels: []*CompactionLevelConfig{
			{Interval: 30 * time.Second},
			{Interval: 5 * time.Minute},
			{Interval: 1 * time.Hour},
		},
		Snapshot: SnapshotConfig{
			Interval:  &defaultSnapshotInterval,
			Retention: &defaultSnapshotRetention,
		},
		L0Retention:              &defaultL0Retention,
		L0RetentionCheckInterval: &defaultL0RetentionCheckInterval,
		ShutdownSyncTimeout:      &defaultShutdownSyncTimeout,
		ShutdownSyncInterval:     &defaultShutdownSyncInterval,
	}
}

// Validate returns an error if config contains invalid settings.
func (c *Config) Validate() error {
	// Validate snapshot intervals
	if c.Snapshot.Interval != nil && *c.Snapshot.Interval <= 0 {
		return &ConfigValidationError{
			Err:   ErrInvalidSnapshotInterval,
			Field: "snapshot.interval",
			Value: *c.Snapshot.Interval,
		}
	}
	if c.Snapshot.Retention != nil && *c.Snapshot.Retention <= 0 {
		return &ConfigValidationError{
			Err:   ErrInvalidSnapshotRetention,
			Field: "snapshot.retention",
			Value: *c.Snapshot.Retention,
		}
	}
	if c.L0Retention != nil && *c.L0Retention <= 0 {
		return &ConfigValidationError{
			Err:   ErrInvalidL0Retention,
			Field: "l0-retention",
			Value: *c.L0Retention,
		}
	}
	if c.L0RetentionCheckInterval != nil && *c.L0RetentionCheckInterval <= 0 {
		return &ConfigValidationError{
			Err:   ErrInvalidL0RetentionCheckInterval,
			Field: "l0-retention-check-interval",
			Value: *c.L0RetentionCheckInterval,
		}
	}
	if c.ShutdownSyncTimeout != nil && *c.ShutdownSyncTimeout < 0 {
		return &ConfigValidationError{
			Err:   ErrInvalidShutdownSyncTimeout,
			Field: "shutdown-sync-timeout",
			Value: *c.ShutdownSyncTimeout,
		}
	}
	if c.ShutdownSyncInterval != nil && *c.ShutdownSyncInterval <= 0 {
		return &ConfigValidationError{
			Err:   ErrInvalidShutdownSyncInterval,
			Field: "shutdown-sync-interval",
			Value: *c.ShutdownSyncInterval,
		}
	}

	// Validate compaction level intervals
	for i, level := range c.Levels {
		if level.Interval <= 0 {
			return &ConfigValidationError{
				Err:   ErrInvalidCompactionInterval,
				Field: fmt.Sprintf("levels[%d].interval", i),
				Value: level.Interval,
			}
		}
	}

	// Validate database configs
	for idx, db := range c.DBs {
		// Validate that either path or dir is specified, but not both
		if db.Path != "" && db.Dir != "" {
			return fmt.Errorf("database config #%d: cannot specify both 'path' and 'dir'", idx+1)
		}
		if db.Path == "" && db.Dir == "" {
			return fmt.Errorf("database config #%d: must specify either 'path' or 'dir'", idx+1)
		}

		// When using dir, pattern must be specified
		if db.Dir != "" && db.Pattern == "" {
			return fmt.Errorf("database config #%d: 'pattern' is required when using 'dir'", idx+1)
		}
		if db.Watch && db.Dir == "" {
			return fmt.Errorf("database config #%d: 'watch' can only be enabled with a directory", idx+1)
		}

		// Use path or dir for identifying the config in error messages
		dbIdentifier := db.Path
		if dbIdentifier == "" {
			dbIdentifier = db.Dir
		}

		// Validate sync intervals for replicas
		if db.Replica != nil && db.Replica.SyncInterval != nil && *db.Replica.SyncInterval <= 0 {
			return &ConfigValidationError{
				Err:   ErrInvalidSyncInterval,
				Field: fmt.Sprintf("dbs[%s].replica.sync-interval", dbIdentifier),
				Value: *db.Replica.SyncInterval,
			}
		}
		for i, replica := range db.Replicas {
			if replica.SyncInterval != nil && *replica.SyncInterval <= 0 {
				return &ConfigValidationError{
					Err:   ErrInvalidSyncInterval,
					Field: fmt.Sprintf("dbs[%s].replicas[%d].sync-interval", dbIdentifier, i),
					Value: *replica.SyncInterval,
				}
			}
		}
	}

	return nil
}

// CompactionLevels returns a full list of compaction levels include L0.
func (c *Config) CompactionLevels() litestream.CompactionLevels {
	levels := litestream.CompactionLevels{
		{Level: 0},
	}

	for i, lvl := range c.Levels {
		levels = append(levels, &litestream.CompactionLevel{
			Level:    i + 1,
			Interval: lvl.Interval,
		})
	}

	return levels
}

// DBConfig returns database configuration by path.
func (c *Config) DBConfig(configPath string) *DBConfig {
	for _, dbConfig := range c.DBs {
		if dbConfig.Path == configPath {
			return dbConfig
		}
	}
	return nil
}

// OpenConfigFile opens a configuration file and returns a reader.
// Expands the filename path if needed.
func OpenConfigFile(filename string) (io.ReadCloser, error) {
	// Expand filename, if necessary.
	filename, err := expand(filename)
	if err != nil {
		return nil, err
	}

	// Open configuration file.
	f, err := os.Open(filename)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("%w: %s", ErrConfigFileNotFound, filename)
	} else if err != nil {
		return nil, err
	}

	return f, nil
}

// ReadConfigFile unmarshals config from filename. Expands path if needed.
// If expandEnv is true then environment variables are expanded in the config.
func ReadConfigFile(filename string, expandEnv bool) (Config, error) {
	f, err := OpenConfigFile(filename)
	if err != nil {
		return DefaultConfig(), err
	}
	defer f.Close()

	return ParseConfig(f, expandEnv)
}

// ParseConfig unmarshals config from a reader.
// If expandEnv is true then environment variables are expanded in the config.
func ParseConfig(r io.Reader, expandEnv bool) (_ Config, err error) {
	config := DefaultConfig()

	// Read configuration.
	buf, err := io.ReadAll(r)
	if err != nil {
		return config, err
	}

	// Expand environment variables, if enabled.
	if expandEnv {
		buf = []byte(os.ExpandEnv(string(buf)))
	}

	// Save defaults before unmarshaling
	defaultSnapshotInterval := config.Snapshot.Interval
	defaultSnapshotRetention := config.Snapshot.Retention
	defaultL0Retention := config.L0Retention
	defaultL0RetentionCheckInterval := config.L0RetentionCheckInterval

	if err := yaml.Unmarshal(buf, &config); err != nil {
		return config, err
	}

	// Restore defaults if they were overwritten with nil by empty YAML sections
	if config.Snapshot.Interval == nil {
		config.Snapshot.Interval = defaultSnapshotInterval
	}
	if config.Snapshot.Retention == nil {
		config.Snapshot.Retention = defaultSnapshotRetention
	}
	if config.L0Retention == nil {
		config.L0Retention = defaultL0Retention
	}
	if config.L0RetentionCheckInterval == nil {
		config.L0RetentionCheckInterval = defaultL0RetentionCheckInterval
	}

	// Normalize paths.
	for _, dbConfig := range config.DBs {
		if dbConfig.Path == "" {
			continue
		}
		if dbConfig.Path, err = expand(dbConfig.Path); err != nil {
			return config, err
		}
	}

	// Propage settings from global config to replica configs.
	config.propagateGlobalSettings()

	// Validate configuration
	if err := config.Validate(); err != nil {
		return config, err
	}

	// Configure logging.
	logOutput := os.Stdout
	if config.Logging.Stderr {
		logOutput = os.Stderr
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		config.Logging.Level = v
	}
	initLog(logOutput, config.Logging.Level, config.Logging.Type)

	return config, nil
}

// CompactionLevelConfig the configuration for a single level of compaction.
type CompactionLevelConfig struct {
	Interval time.Duration `yaml:"interval"`
}

// DBConfig represents the configuration for a single database or directory of databases.
type DBConfig struct {
	Path               string         `yaml:"path"`
	Dir                string         `yaml:"dir"`       // Directory to scan for databases
	Pattern            string         `yaml:"pattern"`   // File pattern to match (e.g., "*.db", "*.sqlite")
	Recursive          bool           `yaml:"recursive"` // Scan subdirectories recursively
	Watch              bool           `yaml:"watch"`     // Enable directory monitoring for changes
	MetaPath           *string        `yaml:"meta-path"`
	MonitorInterval    *time.Duration `yaml:"monitor-interval"`
	CheckpointInterval *time.Duration `yaml:"checkpoint-interval"`
	BusyTimeout        *time.Duration `yaml:"busy-timeout"`
	MinCheckpointPageN *int           `yaml:"min-checkpoint-page-count"`
	TruncatePageN      *int           `yaml:"truncate-page-n"`

	Replica  *ReplicaConfig   `yaml:"replica"`
	Replicas []*ReplicaConfig `yaml:"replicas"` // Deprecated
}

// NewDBFromConfig instantiates a DB based on a configuration.
func NewDBFromConfig(dbc *DBConfig) (*litestream.DB, error) {
	configPath, err := expand(dbc.Path)
	if err != nil {
		return nil, err
	}

	// Initialize database with given path.
	db := litestream.NewDB(configPath)

	// Override default database settings if specified in configuration.
	if dbc.MetaPath != nil {
		expandedMetaPath, err := expand(*dbc.MetaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to expand meta path: %w", err)
		}
		dbc.MetaPath = &expandedMetaPath
		db.SetMetaPath(expandedMetaPath)
	}
	if dbc.MonitorInterval != nil {
		db.MonitorInterval = *dbc.MonitorInterval
	}
	if dbc.CheckpointInterval != nil {
		db.CheckpointInterval = *dbc.CheckpointInterval
	}
	if dbc.BusyTimeout != nil {
		db.BusyTimeout = *dbc.BusyTimeout
	}
	if dbc.MinCheckpointPageN != nil {
		db.MinCheckpointPageN = *dbc.MinCheckpointPageN
	}
	if dbc.TruncatePageN != nil {
		db.TruncatePageN = *dbc.TruncatePageN
	}

	// Instantiate and attach replica.
	// v0.3.x and before supported multiple replicas but that was dropped to
	// ensure there's a single remote data authority.
	switch {
	case dbc.Replica == nil && len(dbc.Replicas) == 0:
		return nil, fmt.Errorf("must specify replica for database")
	case dbc.Replica != nil && len(dbc.Replicas) > 0:
		return nil, fmt.Errorf("cannot specify 'replica' and 'replicas' on a database")
	case len(dbc.Replicas) > 1:
		return nil, fmt.Errorf("multiple replicas on a single database are no longer supported")
	}

	var rc *ReplicaConfig
	if dbc.Replica != nil {
		rc = dbc.Replica
	} else {
		rc = dbc.Replicas[0]
	}

	r, err := NewReplicaFromConfig(rc, db)
	if err != nil {
		return nil, err
	}
	db.Replica = r

	return db, nil
}

// NewDBsFromDirectoryConfig scans a directory and creates DB instances for all SQLite databases found.
func NewDBsFromDirectoryConfig(dbc *DBConfig) ([]*litestream.DB, error) {
	if dbc.Dir == "" {
		return nil, fmt.Errorf("directory path is required for directory replication")
	}

	if dbc.Pattern == "" {
		return nil, fmt.Errorf("pattern is required for directory replication")
	}

	dirPath, err := expand(dbc.Dir)
	if err != nil {
		return nil, err
	}

	// Find all SQLite databases in the directory
	dbPaths, err := FindSQLiteDatabases(dirPath, dbc.Pattern, dbc.Recursive)
	if err != nil {
		return nil, fmt.Errorf("failed to scan directory %s: %w", dirPath, err)
	}

	if len(dbPaths) == 0 && !dbc.Watch {
		return nil, fmt.Errorf("no SQLite databases found in directory %s with pattern %s", dirPath, dbc.Pattern)
	}

	// Create DB instances for each found database
	var dbs []*litestream.DB
	metaPaths := make(map[string]string)

	for _, dbPath := range dbPaths {
		db, err := newDBFromDirectoryEntry(dbc, dirPath, dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create DB for %s: %w", dbPath, err)
		}

		// Validate unique meta-path to prevent replication state corruption
		if mp := db.MetaPath(); mp != "" {
			if existingDB, exists := metaPaths[mp]; exists {
				return nil, fmt.Errorf("meta-path collision: databases %s and %s would share meta-path %s, causing replication state corruption", existingDB, dbPath, mp)
			}
			metaPaths[mp] = dbPath
		}

		dbs = append(dbs, db)
	}

	return dbs, nil
}

// newDBFromDirectoryEntry creates a DB instance for a database discovered via directory replication.
func newDBFromDirectoryEntry(dbc *DBConfig, dirPath, dbPath string) (*litestream.DB, error) {
	// Calculate relative path from directory root
	relPath, err := filepath.Rel(dirPath, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate relative path for %s: %w", dbPath, err)
	}

	// Create a copy of the config for the discovered database
	dbConfigCopy := *dbc
	dbConfigCopy.Path = dbPath
	dbConfigCopy.Dir = ""          // Clear dir field for individual DB
	dbConfigCopy.Pattern = ""      // Clear pattern field
	dbConfigCopy.Recursive = false // Clear recursive flag
	dbConfigCopy.Watch = false     // Individual DBs do not watch directories

	// Ensure every database discovered beneath a directory receives a unique
	// metadata path. Without this, all databases share the same meta-path and
	// clobber each other's replication state.
	if dbc.MetaPath != nil {
		baseMetaPath, err := expand(*dbc.MetaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to expand meta path for %s: %w", dbPath, err)
		}
		metaPathCopy := deriveMetaPathForDirectoryEntry(baseMetaPath, relPath)
		dbConfigCopy.MetaPath = &metaPathCopy
	}

	// Deep copy replica config and make path unique per database.
	// This prevents all databases from writing to the same replica path.
	if dbc.Replica != nil {
		replicaCopy, err := cloneReplicaConfigWithRelativePath(dbc.Replica, relPath)
		if err != nil {
			return nil, fmt.Errorf("failed to configure replica for %s: %w", dbPath, err)
		}
		dbConfigCopy.Replica = replicaCopy
	}

	// Also handle deprecated 'replicas' array field.
	if len(dbc.Replicas) > 0 {
		dbConfigCopy.Replicas = make([]*ReplicaConfig, len(dbc.Replicas))
		for i, replica := range dbc.Replicas {
			replicaCopy, err := cloneReplicaConfigWithRelativePath(replica, relPath)
			if err != nil {
				return nil, fmt.Errorf("failed to configure replica %d for %s: %w", i, dbPath, err)
			}
			dbConfigCopy.Replicas[i] = replicaCopy
		}
	}

	return NewDBFromConfig(&dbConfigCopy)
}

// cloneReplicaConfigWithRelativePath returns a copy of the replica configuration with the
// database-relative path appended to either the replica path or URL, depending on how the
// replica was configured.
func cloneReplicaConfigWithRelativePath(base *ReplicaConfig, relPath string) (*ReplicaConfig, error) {
	if base == nil {
		return nil, nil
	}

	replicaCopy := *base
	relPath = filepath.ToSlash(relPath)
	if relPath == "" || relPath == "." {
		return &replicaCopy, nil
	}

	if replicaCopy.URL != "" {
		u, err := url.Parse(replicaCopy.URL)
		if err != nil {
			return nil, fmt.Errorf("parse replica url: %w", err)
		}
		appendRelativePathToURL(u, relPath)
		replicaCopy.URL = u.String()
		return &replicaCopy, nil
	}

	switch base.ReplicaType() {
	case "file":
		relOSPath := filepath.FromSlash(relPath)
		if replicaCopy.Path != "" {
			replicaCopy.Path = filepath.Join(replicaCopy.Path, relOSPath)
		} else {
			replicaCopy.Path = relOSPath
		}
	default:
		// Normalize to forward slashes for cloud/object storage backends.
		basePath := filepath.ToSlash(replicaCopy.Path)
		if basePath != "" {
			replicaCopy.Path = path.Join(basePath, relPath)
		} else {
			replicaCopy.Path = relPath
		}
	}

	return &replicaCopy, nil
}

// deriveMetaPathForDirectoryEntry returns a unique metadata directory for a
// database discovered through directory replication by appending the database's
// relative path and the standard Litestream suffix to the configured base path.
func deriveMetaPathForDirectoryEntry(basePath, relPath string) string {
	relPath = filepath.Clean(relPath)
	if relPath == "." || relPath == "" {
		return basePath
	}

	relDir, relFile := filepath.Split(relPath)
	if relFile == "" || relFile == "." {
		return filepath.Join(basePath, relPath)
	}

	metaDirName := "." + relFile + litestream.MetaDirSuffix
	return filepath.Join(basePath, relDir, metaDirName)
}

// appendRelativePathToURL appends relPath to the URL's path component, ensuring
// the result remains rooted and uses forward slashes.
func appendRelativePathToURL(u *url.URL, relPath string) {
	cleanRel := strings.TrimPrefix(relPath, "/")
	if cleanRel == "" || cleanRel == "." {
		return
	}

	basePath := u.Path
	var joined string
	if basePath == "" {
		joined = cleanRel
	} else {
		joined = path.Join(basePath, cleanRel)
	}

	joined = "/" + strings.TrimPrefix(joined, "/")
	u.Path = joined
}

// FindSQLiteDatabases recursively finds all SQLite database files in a directory.
// Exported for testing.
func FindSQLiteDatabases(dir string, pattern string, recursive bool) ([]string, error) {
	var dbPaths []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories unless recursive
		if info.IsDir() {
			if !recursive && path != dir {
				return filepath.SkipDir
			}
			return nil
		}

		// Check if file matches pattern
		matched, err := filepath.Match(pattern, filepath.Base(path))
		if err != nil {
			return err
		}
		if !matched {
			return nil
		}

		// Check if it's a SQLite database
		if IsSQLiteDatabase(path) {
			dbPaths = append(dbPaths, path)
		}

		return nil
	})

	return dbPaths, err
}

// IsSQLiteDatabase checks if a file is a SQLite database by reading its header.
// Exported for testing.
func IsSQLiteDatabase(path string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()

	// SQLite files start with "SQLite format 3\x00"
	header := make([]byte, 16)
	if _, err := file.Read(header); err != nil {
		return false
	}

	return string(header) == "SQLite format 3\x00"
}

// ByteSize is a custom type for parsing byte sizes from YAML.
// It supports both SI units (KB, MB, GB using base 1000) and IEC units
// (KiB, MiB, GiB using base 1024) as well as short forms (K, M, G).
type ByteSize int64

// UnmarshalYAML implements yaml.Unmarshaler for ByteSize.
func (b *ByteSize) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	size, err := ParseByteSize(s)
	if err != nil {
		return err
	}
	*b = ByteSize(size)
	return nil
}

// ParseByteSize parses a byte size string using github.com/dustin/go-humanize.
// Supports both SI units (KB=1000, MB=1000², etc.) and IEC units (KiB=1024, MiB=1024², etc.).
// Examples: "1MB", "5MiB", "1.5GB", "100B", "1024KB"
func ParseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}

	// Use go-humanize to parse the byte size string
	bytes, err := humanize.ParseBytes(s)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %w", err)
	}

	// Check that the value fits in int64
	if bytes > math.MaxInt64 {
		return 0, fmt.Errorf("size %d exceeds maximum allowed value (%d)", bytes, int64(math.MaxInt64))
	}

	return int64(bytes), nil
}

// ReplicaSettings contains settings shared across replica configurations.
// These can be set globally in Config or per-replica in ReplicaConfig.
type ReplicaSettings struct {
	SyncInterval       *time.Duration `yaml:"sync-interval"`
	ValidationInterval *time.Duration `yaml:"validation-interval"`

	// S3 settings
	AccessKeyID       string    `yaml:"access-key-id"`
	SecretAccessKey   string    `yaml:"secret-access-key"`
	Region            string    `yaml:"region"`
	Bucket            string    `yaml:"bucket"`
	Endpoint          string    `yaml:"endpoint"`
	ForcePathStyle    *bool     `yaml:"force-path-style"`
	SignPayload       *bool     `yaml:"sign-payload"`
	RequireContentMD5 *bool     `yaml:"require-content-md5"`
	SkipVerify        bool      `yaml:"skip-verify"`
	PartSize          *ByteSize `yaml:"part-size"`
	Concurrency       *int      `yaml:"concurrency"`

	// ABS settings
	AccountName string `yaml:"account-name"`
	AccountKey  string `yaml:"account-key"`

	// SFTP settings
	Host             string `yaml:"host"`
	User             string `yaml:"user"`
	Password         string `yaml:"password"`
	KeyPath          string `yaml:"key-path"`
	ConcurrentWrites *bool  `yaml:"concurrent-writes"`
	HostKey          string `yaml:"host-key"`

	// WebDAV settings
	WebDAVURL      string `yaml:"webdav-url"`
	WebDAVUsername string `yaml:"webdav-username"`
	WebDAVPassword string `yaml:"webdav-password"`

	// NATS settings
	JWT           string         `yaml:"jwt"`
	Seed          string         `yaml:"seed"`
	Creds         string         `yaml:"creds"`
	NKey          string         `yaml:"nkey"`
	Username      string         `yaml:"username"`
	Token         string         `yaml:"token"`
	TLS           bool           `yaml:"tls"`
	RootCAs       []string       `yaml:"root-cas"`
	ClientCert    string         `yaml:"client-cert"`
	ClientKey     string         `yaml:"client-key"`
	MaxReconnects *int           `yaml:"max-reconnects"`
	ReconnectWait *time.Duration `yaml:"reconnect-wait"`
	Timeout       *time.Duration `yaml:"timeout"`

	// Encryption identities and recipients
	Age struct {
		Identities []string `yaml:"identities"`
		Recipients []string `yaml:"recipients"`
	} `yaml:"age"`
}

// SetDefaults merges default settings from src into the current ReplicaSettings.
// Individual settings override defaults when already set.
func (rs *ReplicaSettings) SetDefaults(src *ReplicaSettings) {
	if src == nil {
		return
	}

	// Timing settings
	if rs.SyncInterval == nil && src.SyncInterval != nil {
		rs.SyncInterval = src.SyncInterval
	}
	if rs.ValidationInterval == nil && src.ValidationInterval != nil {
		rs.ValidationInterval = src.ValidationInterval
	}

	// S3 settings
	if rs.AccessKeyID == "" {
		rs.AccessKeyID = src.AccessKeyID
	}
	if rs.SecretAccessKey == "" {
		rs.SecretAccessKey = src.SecretAccessKey
	}
	if rs.Region == "" {
		rs.Region = src.Region
	}
	if rs.Bucket == "" {
		rs.Bucket = src.Bucket
	}
	if rs.Endpoint == "" {
		rs.Endpoint = src.Endpoint
	}
	if rs.ForcePathStyle == nil {
		rs.ForcePathStyle = src.ForcePathStyle
	}
	if rs.SignPayload == nil {
		rs.SignPayload = src.SignPayload
	}
	if rs.RequireContentMD5 == nil {
		rs.RequireContentMD5 = src.RequireContentMD5
	}
	if src.SkipVerify {
		rs.SkipVerify = true
	}

	// ABS settings
	if rs.AccountName == "" {
		rs.AccountName = src.AccountName
	}
	if rs.AccountKey == "" {
		rs.AccountKey = src.AccountKey
	}

	// SFTP settings
	if rs.Host == "" {
		rs.Host = src.Host
	}
	if rs.User == "" {
		rs.User = src.User
	}
	if rs.Password == "" {
		rs.Password = src.Password
	}
	if rs.KeyPath == "" {
		rs.KeyPath = src.KeyPath
	}
	if rs.ConcurrentWrites == nil {
		rs.ConcurrentWrites = src.ConcurrentWrites
	}

	// NATS settings
	if rs.JWT == "" {
		rs.JWT = src.JWT
	}
	if rs.Seed == "" {
		rs.Seed = src.Seed
	}
	if rs.Creds == "" {
		rs.Creds = src.Creds
	}
	if rs.NKey == "" {
		rs.NKey = src.NKey
	}
	if rs.Username == "" {
		rs.Username = src.Username
	}
	if rs.Token == "" {
		rs.Token = src.Token
	}
	if !rs.TLS {
		rs.TLS = src.TLS
	}
	if len(rs.RootCAs) == 0 {
		rs.RootCAs = src.RootCAs
	}
	if rs.ClientCert == "" {
		rs.ClientCert = src.ClientCert
	}
	if rs.ClientKey == "" {
		rs.ClientKey = src.ClientKey
	}
	if rs.MaxReconnects == nil {
		rs.MaxReconnects = src.MaxReconnects
	}
	if rs.ReconnectWait == nil {
		rs.ReconnectWait = src.ReconnectWait
	}
	if rs.Timeout == nil {
		rs.Timeout = src.Timeout
	}

	// Age encryption settings
	if len(rs.Age.Identities) == 0 {
		rs.Age.Identities = src.Age.Identities
	}
	if len(rs.Age.Recipients) == 0 {
		rs.Age.Recipients = src.Age.Recipients
	}
}

// ReplicaConfig represents the configuration for a single replica in a database.
type ReplicaConfig struct {
	ReplicaSettings `yaml:",inline"`

	Type string `yaml:"type"` // "file", "s3"
	Name string `yaml:"name"` // Deprecated
	Path string `yaml:"path"`
	URL  string `yaml:"url"`
}

// NewReplicaFromConfig instantiates a replica for a DB based on a config.
func NewReplicaFromConfig(c *ReplicaConfig, db *litestream.DB) (_ *litestream.Replica, err error) {
	// Ensure user did not specify URL in path.
	if litestream.IsURL(c.Path) {
		return nil, fmt.Errorf("replica path cannot be a url, please use the 'url' field instead: %s", c.Path)
	}

	// Reject age encryption configuration as it's currently non-functional.
	// Age encryption support was removed during the LTX storage layer refactor
	// and has not been reimplemented. Accepting this config would silently
	// write plaintext data to remote storage instead of encrypted data.
	// See: https://github.com/benbjohnson/litestream/issues/790
	if len(c.Age.Identities) > 0 || len(c.Age.Recipients) > 0 {
		return nil, fmt.Errorf("age encryption is not currently supported, if you need encryption please revert back to Litestream v0.3.x")
	}

	// Build replica.
	r := litestream.NewReplica(db)
	if v := c.SyncInterval; v != nil {
		r.SyncInterval = *v
	}

	// Build and set client on replica.
	switch c.ReplicaType() {
	case "file":
		if r.Client, err = newFileReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "s3":
		if r.Client, err = NewS3ReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "gs":
		if r.Client, err = newGSReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "abs":
		if r.Client, err = newABSReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "sftp":
		if r.Client, err = newSFTPReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "webdav":
		if r.Client, err = newWebDAVReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "nats":
		if r.Client, err = newNATSReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	case "oss":
		if r.Client, err = newOSSReplicaClientFromConfig(c, r); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown replica type in config: %q", c.Type)
	}

	return r, nil
}

// newFileReplicaClientFromConfig returns a new instance of file.ReplicaClient built from config.
func newFileReplicaClientFromConfig(c *ReplicaConfig, r *litestream.Replica) (_ *file.ReplicaClient, err error) {
	// Ensure URL & path are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for file replica")
	}

	// Parse configPath from URL, if specified.
	configPath := c.Path
	if c.URL != "" {
		if _, _, configPath, err = litestream.ParseReplicaURL(c.URL); err != nil {
			return nil, err
		}
	}

	// Ensure path is set explicitly or derived from URL field.
	if configPath == "" {
		return nil, fmt.Errorf("file replica path required")
	}

	// Expand home prefix and return absolute path.
	if configPath, err = expand(configPath); err != nil {
		return nil, err
	}

	// Instantiate replica and apply time fields, if set.
	client := file.NewReplicaClient(configPath)
	client.Replica = r
	return client, nil
}

// NewS3ReplicaClientFromConfig returns a new instance of s3.ReplicaClient built from config.
// Exported for testing.
func NewS3ReplicaClientFromConfig(c *ReplicaConfig, _ *litestream.Replica) (_ *s3.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for s3 replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for s3 replica")
	}

	bucket, configPath := c.Bucket, c.Path
	region, endpoint, skipVerify := c.Region, c.Endpoint, c.SkipVerify
	signSetting := newBoolSetting(true)
	if v := c.SignPayload; v != nil {
		signSetting.Set(*v)
	}
	requireSetting := newBoolSetting(true)
	if v := c.RequireContentMD5; v != nil {
		requireSetting.Set(*v)
	}

	// Use path style if an endpoint is explicitly set. This works because the
	// only service to not use path style is AWS which does not use an endpoint.
	forcePathStyle := (endpoint != "")
	if v := c.ForcePathStyle; v != nil {
		forcePathStyle = *v
	}

	// Apply settings from URL, if specified.
	var (
		endpointWasSet        bool
		usignPayload          bool
		usignPayloadSet       bool
		urequireContentMD5    bool
		urequireContentMD5Set bool
	)
	if endpoint != "" {
		endpointWasSet = true
	}

	if c.URL != "" {
		_, host, upath, query, _, err := litestream.ParseReplicaURLWithQuery(c.URL)
		if err != nil {
			return nil, err
		}

		var (
			ubucket         string
			uregion         string
			uendpoint       string
			uforcePathStyle bool
		)

		if strings.HasPrefix(host, "arn:") {
			ubucket = host
			uregion = litestream.RegionFromS3ARN(host)
		} else {
			ubucket, uregion, uendpoint, uforcePathStyle = s3.ParseHost(host)
		}

		// Override with query parameters if provided
		if qEndpoint := query.Get("endpoint"); qEndpoint != "" {
			// Ensure endpoint has a scheme
			if !strings.HasPrefix(qEndpoint, "http://") && !strings.HasPrefix(qEndpoint, "https://") {
				// Default to http for non-TLS endpoints (common for local/dev)
				qEndpoint = "http://" + qEndpoint
			}
			uendpoint = qEndpoint
			// Default to path style for custom endpoints unless explicitly set to false
			if query.Get("forcePathStyle") != "false" {
				uforcePathStyle = true
			}
			endpointWasSet = true
		}
		if qRegion := query.Get("region"); qRegion != "" {
			uregion = qRegion
		}
		if qForcePathStyle := query.Get("forcePathStyle"); qForcePathStyle != "" {
			uforcePathStyle = qForcePathStyle == "true"
		}
		if qSkipVerify := query.Get("skipVerify"); qSkipVerify != "" {
			skipVerify = qSkipVerify == "true"
		}
		if v, ok := litestream.BoolQueryValue(query, "signPayload", "sign-payload"); ok {
			usignPayload = v
			usignPayloadSet = true
		}
		if v, ok := litestream.BoolQueryValue(query, "requireContentMD5", "require-content-md5"); ok {
			urequireContentMD5 = v
			urequireContentMD5Set = true
		}

		// Only apply URL parts to field that have not been overridden.
		if configPath == "" {
			configPath = upath
		}
		if bucket == "" {
			bucket = ubucket
		}
		if region == "" {
			region = uregion
		}
		if endpoint == "" {
			endpoint = uendpoint
		}
		if !forcePathStyle {
			forcePathStyle = uforcePathStyle
		}
		if !signSetting.set && usignPayloadSet {
			signSetting.Set(usignPayload)
		}
		if !requireSetting.set && urequireContentMD5Set {
			requireSetting.Set(urequireContentMD5)
		}
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for s3 replica")
	}

	// Detect S3-compatible provider endpoints for applying appropriate defaults.
	// These providers require specific settings to work correctly with AWS SDK v2.
	isTigris := litestream.IsTigrisEndpoint(endpoint)
	if !isTigris && !endpointWasSet && litestream.IsTigrisEndpoint(c.Endpoint) {
		isTigris = true
	}
	isDigitalOcean := litestream.IsDigitalOceanEndpoint(endpoint)
	isBackblaze := litestream.IsBackblazeEndpoint(endpoint)
	isFilebase := litestream.IsFilebaseEndpoint(endpoint)
	isScaleway := litestream.IsScalewayEndpoint(endpoint)
	isMinIO := litestream.IsMinIOEndpoint(endpoint)

	// Track if forcePathStyle was explicitly set by user (config or URL query param).
	forcePathStyleSet := c.ForcePathStyle != nil

	// Apply provider-specific defaults for S3-compatible providers.
	// These settings ensure compatibility with each provider's S3 implementation.
	if isTigris {
		// Tigris: requires signed payloads, no MD5
		signSetting.ApplyDefault(true)
		requireSetting.ApplyDefault(false)
	}
	if isDigitalOcean || isBackblaze || isFilebase || isScaleway || isMinIO {
		// All these providers require signed payloads (don't support UNSIGNED-PAYLOAD)
		signSetting.ApplyDefault(true)
	}
	if !forcePathStyleSet {
		// Filebase, Backblaze B2, and MinIO require path-style URLs
		if isFilebase || isBackblaze || isMinIO {
			forcePathStyle = true
		}
	}

	// Build replica.
	client := s3.NewReplicaClient()
	client.AccessKeyID = c.AccessKeyID
	client.SecretAccessKey = c.SecretAccessKey
	client.Bucket = bucket
	client.Path = configPath
	client.Region = region
	client.Endpoint = endpoint
	client.ForcePathStyle = forcePathStyle
	client.SkipVerify = skipVerify

	client.SignPayload = signSetting.value
	client.RequireContentMD5 = requireSetting.value

	// Apply upload configuration if specified.
	if c.PartSize != nil {
		client.PartSize = int64(*c.PartSize)
	}
	if c.Concurrency != nil {
		client.Concurrency = *c.Concurrency
	}

	return client, nil
}

// newGSReplicaClientFromConfig returns a new instance of gs.ReplicaClient built from config.
func newGSReplicaClientFromConfig(c *ReplicaConfig, _ *litestream.Replica) (_ *gs.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for gs replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for gs replica")
	}

	bucket, configPath := c.Bucket, c.Path

	// Apply settings from URL, if specified.
	if c.URL != "" {
		_, uhost, upath, err := litestream.ParseReplicaURL(c.URL)
		if err != nil {
			return nil, err
		}

		// Only apply URL parts to field that have not been overridden.
		if configPath == "" {
			configPath = upath
		}
		if bucket == "" {
			bucket = uhost
		}
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for gs replica")
	}

	// Build replica.
	client := gs.NewReplicaClient()
	client.Bucket = bucket
	client.Path = configPath
	return client, nil
}

// newABSReplicaClientFromConfig returns a new instance of abs.ReplicaClient built from config.
func newABSReplicaClientFromConfig(c *ReplicaConfig, _ *litestream.Replica) (_ *abs.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for abs replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for abs replica")
	}

	// Build replica.
	client := abs.NewReplicaClient()
	client.AccountName = c.AccountName
	client.AccountKey = c.AccountKey
	client.Bucket = c.Bucket
	client.Path = c.Path
	client.Endpoint = c.Endpoint

	// Apply settings from URL, if specified.
	if c.URL != "" {
		u, err := url.Parse(c.URL)
		if err != nil {
			return nil, err
		}

		if client.AccountName == "" && u.User != nil {
			client.AccountName = u.User.Username()
		}
		if client.Bucket == "" {
			client.Bucket = u.Host
		}
		if client.Path == "" {
			client.Path = strings.TrimPrefix(path.Clean(u.Path), "/")
		}
	}

	// Ensure required settings are set.
	if client.Bucket == "" {
		return nil, fmt.Errorf("bucket required for abs replica")
	}

	return client, nil
}

// newSFTPReplicaClientFromConfig returns a new instance of sftp.ReplicaClient built from config.
func newSFTPReplicaClientFromConfig(c *ReplicaConfig, _ *litestream.Replica) (_ *sftp.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for sftp replica")
	} else if c.URL != "" && c.Host != "" {
		return nil, fmt.Errorf("cannot specify url & host for sftp replica")
	}

	host, user, password, path := c.Host, c.User, c.Password, c.Path

	// Apply settings from URL, if specified.
	if c.URL != "" {
		u, err := url.Parse(c.URL)
		if err != nil {
			return nil, err
		}

		// Only apply URL parts to field that have not been overridden.
		if host == "" {
			host = u.Host
		}
		if user == "" && u.User != nil {
			user = u.User.Username()
		}
		if password == "" && u.User != nil {
			password, _ = u.User.Password()
		}
		if path == "" {
			path = u.Path
		}
	}

	// Ensure required settings are set.
	if host == "" {
		return nil, fmt.Errorf("host required for sftp replica")
	} else if user == "" {
		return nil, fmt.Errorf("user required for sftp replica")
	}

	// Build replica.
	client := sftp.NewReplicaClient()
	client.Host = host
	client.User = user
	client.Password = password
	client.Path = path
	client.KeyPath = c.KeyPath
	client.HostKey = c.HostKey

	// Set concurrent writes if specified, otherwise use default (true)
	if c.ConcurrentWrites != nil {
		client.ConcurrentWrites = *c.ConcurrentWrites
	}

	return client, nil
}

// newWebDAVReplicaClientFromConfig returns a new instance of webdav.ReplicaClient built from config.
func newWebDAVReplicaClientFromConfig(c *ReplicaConfig, _ *litestream.Replica) (_ *webdav.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for webdav replica")
	} else if c.URL != "" && c.WebDAVURL != "" {
		return nil, fmt.Errorf("cannot specify url & webdav-url for webdav replica")
	}

	webdavURL, username, password, path := c.WebDAVURL, c.WebDAVUsername, c.WebDAVPassword, c.Path

	// Apply settings from URL, if specified.
	if c.URL != "" {
		u, err := url.Parse(c.URL)
		if err != nil {
			return nil, err
		}

		// Build WebDAV URL from scheme and host
		scheme := "http"
		if u.Scheme == "webdavs" {
			scheme = "https"
		}
		if webdavURL == "" && u.Host != "" {
			webdavURL = fmt.Sprintf("%s://%s", scheme, u.Host)
		}

		// Extract credentials from URL
		if username == "" && u.User != nil {
			username = u.User.Username()
		}
		if password == "" && u.User != nil {
			password, _ = u.User.Password()
		}
		if path == "" {
			path = u.Path
		}
	}

	// Ensure required settings are set.
	if webdavURL == "" {
		return nil, fmt.Errorf("webdav-url required for webdav replica")
	}

	// Build replica.
	client := webdav.NewReplicaClient()
	client.URL = webdavURL
	client.Username = username
	client.Password = password
	client.Path = path

	return client, nil
}

// newNATSReplicaClientFromConfig returns a new instance of nats.ReplicaClient built from config.
func newNATSReplicaClientFromConfig(c *ReplicaConfig, _ *litestream.Replica) (_ *nats.ReplicaClient, err error) {
	// Parse URL if provided to extract bucket name and server URL
	var url, bucket string
	if c.URL != "" {
		scheme, host, bucketPath, err := litestream.ParseReplicaURL(c.URL)
		if err != nil {
			return nil, fmt.Errorf("invalid NATS URL: %w", err)
		}
		if scheme != "nats" {
			return nil, fmt.Errorf("invalid scheme for NATS replica: %s", scheme)
		}

		// Reconstruct URL without bucket path
		if host != "" {
			url = fmt.Sprintf("nats://%s", host)
		}

		// Extract bucket name from path
		if bucketPath != "" {
			bucket = strings.Trim(bucketPath, "/")
		}
	}

	// Use bucket from config if not extracted from URL
	if bucket == "" {
		bucket = c.Bucket
	}

	// Ensure required settings are set
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for NATS replica")
	}

	// Validate TLS configuration
	// Both client cert and key must be specified together
	if (c.ClientCert != "") != (c.ClientKey != "") {
		return nil, fmt.Errorf("client-cert and client-key must both be specified for mutual TLS authentication")
	}

	// Build replica client
	client := nats.NewReplicaClient()
	client.URL = url
	client.BucketName = bucket

	// Set authentication options
	client.JWT = c.JWT
	client.Seed = c.Seed
	client.Creds = c.Creds
	client.NKey = c.NKey
	client.Username = c.Username
	client.Password = c.Password
	client.Token = c.Token

	// Set TLS options
	client.RootCAs = c.RootCAs
	client.ClientCert = c.ClientCert
	client.ClientKey = c.ClientKey

	// Set connection options with defaults
	if c.MaxReconnects != nil {
		client.MaxReconnects = *c.MaxReconnects
	}
	if c.ReconnectWait != nil {
		client.ReconnectWait = *c.ReconnectWait
	}
	if c.Timeout != nil {
		client.Timeout = *c.Timeout
	}

	return client, nil
}

// newOSSReplicaClientFromConfig returns a new instance of oss.ReplicaClient built from config.
func newOSSReplicaClientFromConfig(c *ReplicaConfig, _ *litestream.Replica) (_ *oss.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for oss replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for oss replica")
	}

	bucket, configPath := c.Bucket, c.Path
	region, endpoint := c.Region, c.Endpoint

	// Apply settings from URL, if specified.
	if c.URL != "" {
		_, host, upath, err := litestream.ParseReplicaURL(c.URL)
		if err != nil {
			return nil, err
		}

		var (
			ubucket string
			uregion string
		)

		ubucket, uregion, _ = oss.ParseHost(host)

		// Only apply URL parts to fields that have not been overridden.
		if configPath == "" {
			configPath = upath
		}
		if bucket == "" {
			bucket = ubucket
		}
		if region == "" {
			region = uregion
		}
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for oss replica")
	}

	// Build replica client.
	client := oss.NewReplicaClient()
	client.AccessKeyID = c.AccessKeyID
	client.AccessKeySecret = c.SecretAccessKey
	client.Bucket = bucket
	client.Path = configPath
	client.Region = region
	client.Endpoint = endpoint

	// Apply upload configuration if specified.
	if c.PartSize != nil {
		client.PartSize = int64(*c.PartSize)
	}
	if c.Concurrency != nil {
		client.Concurrency = *c.Concurrency
	}

	return client, nil
}

// applyLitestreamEnv copies "LITESTREAM" prefixed environment variables to
// their AWS counterparts as the "AWS" prefix can be confusing when using a
// non-AWS S3-compatible service.
func applyLitestreamEnv() {
	if v, ok := os.LookupEnv("LITESTREAM_ACCESS_KEY_ID"); ok {
		if _, ok := os.LookupEnv("AWS_ACCESS_KEY_ID"); !ok {
			os.Setenv("AWS_ACCESS_KEY_ID", v)
		}
	}
	if v, ok := os.LookupEnv("LITESTREAM_SECRET_ACCESS_KEY"); ok {
		if _, ok := os.LookupEnv("AWS_SECRET_ACCESS_KEY"); !ok {
			os.Setenv("AWS_SECRET_ACCESS_KEY", v)
		}
	}
}

type boolSetting struct {
	value bool
	set   bool
}

func newBoolSetting(defaultValue bool) boolSetting {
	return boolSetting{value: defaultValue}
}

func (s *boolSetting) Set(value bool) {
	s.value = value
	s.set = true
}

func (s *boolSetting) ApplyDefault(value bool) {
	if !s.set {
		s.value = value
	}
}

// ReplicaType returns the type based on the type field or extracted from the URL.
func (c *ReplicaConfig) ReplicaType() string {
	if replicaType := litestream.ReplicaTypeFromURL(c.URL); replicaType != "" {
		return replicaType
	} else if c.Type != "" {
		return c.Type
	}
	return "file"
}

// DefaultConfigPath returns the default config path.
func DefaultConfigPath() string {
	if v := os.Getenv("LITESTREAM_CONFIG"); v != "" {
		return v
	}
	return defaultConfigPath
}

func registerConfigFlag(fs *flag.FlagSet) (configPath *string, noExpandEnv *bool) {
	return fs.String("config", "", "config path"),
		fs.Bool("no-expand-env", false, "do not expand env vars in config")
}

// expand returns an absolute path for s.
// It also strips SQLite connection string prefixes (sqlite://, sqlite3://).
func expand(s string) (string, error) {
	// Strip SQLite connection string prefixes if present.
	s = StripSQLitePrefix(s)

	// Just expand to absolute path if there is no home directory prefix.
	prefix := "~" + string(os.PathSeparator)
	if s != "~" && !strings.HasPrefix(s, prefix) {
		return filepath.Abs(s)
	}

	// Look up home directory.
	u, err := user.Current()
	if err != nil {
		return "", err
	} else if u.HomeDir == "" {
		return "", fmt.Errorf("cannot expand path %s, no home directory available", s)
	}

	// Return path with tilde replaced by the home directory.
	if s == "~" {
		return u.HomeDir, nil
	}
	return filepath.Join(u.HomeDir, strings.TrimPrefix(s, prefix)), nil
}

// StripSQLitePrefix removes SQLite connection string prefixes (sqlite://, sqlite3://)
// from the given path. This allows users to use standard connection string formats
// across their tooling while Litestream extracts just the file path.
func StripSQLitePrefix(s string) string {
	if len(s) < 9 || s[0] != 's' {
		return s
	}
	for _, prefix := range []string{"sqlite3://", "sqlite://"} {
		if strings.HasPrefix(s, prefix) {
			return strings.TrimPrefix(s, prefix)
		}
	}
	return s
}

// txidVar allows the flag package to parse index flags as hex-formatted TXIDs
type txidVar ltx.TXID

// Ensure type implements interface.
var _ flag.Value = (*txidVar)(nil)

// String returns an 8-character hexadecimal value.
func (v *txidVar) String() string {
	return ltx.TXID(*v).String()
}

// Set parses s into an integer from a hexadecimal value.
func (v *txidVar) Set(s string) error {
	txID, err := ltx.ParseTXID(s)
	if err != nil {
		return fmt.Errorf("invalid txid format")
	}
	*v = txidVar(txID)
	return nil
}

func initLog(w io.Writer, level, typ string) {
	logOptions := slog.HandlerOptions{
		Level:       slog.LevelInfo,
		ReplaceAttr: internal.ReplaceAttr,
	}

	// Read log level from environment, if available.
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		level = v
	}

	switch strings.ToUpper(level) {
	case "TRACE":
		logOptions.Level = internal.LevelTrace
	case "DEBUG":
		logOptions.Level = slog.LevelDebug
	case "INFO":
		logOptions.Level = slog.LevelInfo
	case "WARN", "WARNING":
		logOptions.Level = slog.LevelWarn
	case "ERROR":
		logOptions.Level = slog.LevelError
	}

	var logHandler slog.Handler
	switch typ {
	case "json":
		logHandler = slog.NewJSONHandler(w, &logOptions)
	case "text", "":
		logHandler = slog.NewTextHandler(w, &logOptions)
	}

	// Set global default logger.
	slog.SetDefault(slog.New(logHandler))
}
