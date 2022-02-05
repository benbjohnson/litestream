package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/signal"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/gcs"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/yaml.v2"
)

// Build information.
var (
	Version = "(development build)"
)

// errExit is a terminal error for indicating program should quit.
var errExit = errors.New("exit")

func main() {
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	m := NewMain(os.Stdin, os.Stdout, os.Stderr)
	if err := m.Run(context.Background(), os.Args[1:]); err == flag.ErrHelp || err == errExit {
		os.Exit(1)
	} else if err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
}

// NewMain returns a new instance of Main.
func NewMain(stdin io.Reader, stdout, stderr io.Writer) *Main {
	return &Main{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}
}

// Run executes the program.
func (m *Main) Run(ctx context.Context, args []string) (err error) {
	// Copy "LITESTEAM" environment credentials.
	applyLitestreamEnv()

	// Extract command name.
	var cmd string
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "databases":
		return NewDatabasesCommand(m.stdin, m.stdout, m.stderr).Run(ctx, args)
	case "generations":
		return NewGenerationsCommand(m.stdin, m.stdout, m.stderr).Run(ctx, args)
	case "replicate":
		c := NewReplicateCommand(m.stdin, m.stdout, m.stderr)
		if err := c.ParseFlags(ctx, args); err != nil {
			return err
		}

		// Setup signal handler.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

		if err := c.Run(ctx); err != nil {
			return err
		}

		// Wait for signal to stop program.
		select {
		case <-ctx.Done():
			fmt.Fprintln(m.stdout, "context done, litestream shutting down")
		case err = <-c.execCh:
			cancel()
			fmt.Fprintln(m.stdout, "subprocess exited, litestream shutting down")
		case sig := <-signalCh:
			cancel()
			fmt.Fprintln(m.stdout, "signal received, litestream shutting down")

			if c.cmd != nil {
				fmt.Fprintln(m.stdout, "sending signal to exec process")
				if err := c.cmd.Process.Signal(sig); err != nil {
					return fmt.Errorf("cannot signal exec process: %w", err)
				}

				fmt.Fprintln(m.stdout, "waiting for exec process to close")
				if err := <-c.execCh; err != nil && !strings.HasPrefix(err.Error(), "signal:") {
					return fmt.Errorf("cannot wait for exec process: %w", err)
				}
			}
		}

		// Gracefully close.
		if e := c.Close(); e != nil && err == nil {
			err = e
		}
		fmt.Fprintln(m.stdout, "litestream shut down")
		return err

	case "restore":
		return NewRestoreCommand(m.stdin, m.stdout, m.stderr).Run(ctx, args)
	case "snapshots":
		return NewSnapshotsCommand(m.stdin, m.stdout, m.stderr).Run(ctx, args)
	case "version":
		return NewVersionCommand(m.stdin, m.stdout, m.stderr).Run(ctx, args)
	case "wal":
		return NewWALCommand(m.stdin, m.stdout, m.stderr).Run(ctx, args)
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
	fmt.Fprintln(m.stdout, `
litestream is a tool for replicating SQLite databases.

Usage:

	litestream <command> [arguments]

The commands are:

	databases    list databases specified in config file
	generations  list available generations for a database
	replicate    runs a server to replicate databases
	restore      recovers database backup from a replica
	snapshots    list available snapshots for a database
	version      prints the binary version
	wal          list available WAL files for a database
`[1:])
}

// Config represents a configuration file for the litestream daemon.
type Config struct {
	// Bind address for serving metrics.
	Addr string `yaml:"addr"`

	// List of databases to manage.
	DBs []*DBConfig `yaml:"dbs"`

	// Subcommand to execute during replication.
	// Litestream will shutdown when subcommand exits.
	Exec string `yaml:"exec"`

	// Global S3 settings
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
}

// propagateGlobalSettings copies global S3 settings to replica configs.
func (c *Config) propagateGlobalSettings() {
	for _, dbc := range c.DBs {
		for _, rc := range dbc.Replicas {
			if rc.AccessKeyID == "" {
				rc.AccessKeyID = c.AccessKeyID
			}
			if rc.SecretAccessKey == "" {
				rc.SecretAccessKey = c.SecretAccessKey
			}
		}
	}
}

// DefaultConfig returns a new instance of Config with defaults set.
func DefaultConfig() Config {
	return Config{}
}

// DBConfig returns database configuration by path.
func (c *Config) DBConfig(path string) *DBConfig {
	for _, dbConfig := range c.DBs {
		if dbConfig.Path == path {
			return dbConfig
		}
	}
	return nil
}

// ReadConfigFile unmarshals config from filename. Expands path if needed.
// If expandEnv is true then environment variables are expanded in the config.
// If filename is blank then the default config path is used.
func ReadConfigFile(filename string, expandEnv bool) (_ Config, err error) {
	config := DefaultConfig()

	useDefaultPath := filename == ""
	if useDefaultPath {
		filename = DefaultConfigPath()
	}

	// Expand filename, if necessary.
	filename, err = expand(filename)
	if err != nil {
		return config, err
	}

	// Read configuration.
	// Do not return an error if using default path and file is missing.
	buf, err := ioutil.ReadFile(filename)
	if os.IsNotExist(err) {
		if useDefaultPath {
			return config, nil
		}
		return config, fmt.Errorf("config file not found: %s", filename)
	} else if err != nil {
		return config, err
	}

	// Expand environment variables, if enabled.
	if expandEnv {
		buf = []byte(os.ExpandEnv(string(buf)))
	}

	if err := yaml.Unmarshal(buf, &config); err != nil {
		return config, err
	}

	// Normalize paths.
	for _, dbConfig := range config.DBs {
		if dbConfig.Path, err = expand(dbConfig.Path); err != nil {
			return config, err
		}
	}

	// Propage settings from global config to replica configs.
	config.propagateGlobalSettings()

	return config, nil
}

// DBConfig represents the configuration for a single database.
type DBConfig struct {
	Path               string         `yaml:"path"`
	MonitorInterval    *time.Duration `yaml:"monitor-interval"`
	CheckpointInterval *time.Duration `yaml:"checkpoint-interval"`
	MinCheckpointPageN *int           `yaml:"min-checkpoint-page-count"`
	MaxCheckpointPageN *int           `yaml:"max-checkpoint-page-count"`

	Replicas []*ReplicaConfig `yaml:"replicas"`
}

// NewDBFromConfig instantiates a DB based on a configuration.
func NewDBFromConfig(dbc *DBConfig) (*litestream.DB, error) {
	path, err := expand(dbc.Path)
	if err != nil {
		return nil, err
	}

	// Initialize database with given path.
	db := litestream.NewDB(path)

	// Override default database settings if specified in configuration.
	if dbc.MonitorInterval != nil {
		db.MonitorInterval = *dbc.MonitorInterval
	}
	if dbc.CheckpointInterval != nil {
		db.CheckpointInterval = *dbc.CheckpointInterval
	}
	if dbc.MinCheckpointPageN != nil {
		db.MinCheckpointPageN = *dbc.MinCheckpointPageN
	}
	if dbc.MaxCheckpointPageN != nil {
		db.MaxCheckpointPageN = *dbc.MaxCheckpointPageN
	}

	// Instantiate and attach replicas.
	for _, rc := range dbc.Replicas {
		r, err := NewReplicaFromConfig(rc, db)
		if err != nil {
			return nil, err
		}
		db.Replicas = append(db.Replicas, r)
	}

	return db, nil
}

// ReplicaConfig represents the configuration for a single replica in a database.
type ReplicaConfig struct {
	Type                   string         `yaml:"type"` // "file", "s3"
	Name                   string         `yaml:"name"` // name of replica, optional.
	Path                   string         `yaml:"path"`
	URL                    string         `yaml:"url"`
	Retention              *time.Duration `yaml:"retention"`
	RetentionCheckInterval *time.Duration `yaml:"retention-check-interval"`
	SyncInterval           *time.Duration `yaml:"sync-interval"`
	SnapshotInterval       *time.Duration `yaml:"snapshot-interval"`
	ValidationInterval     *time.Duration `yaml:"validation-interval"`

	// S3 settings
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	Endpoint        string `yaml:"endpoint"`
	ForcePathStyle  *bool  `yaml:"force-path-style"`
	SkipVerify      bool   `yaml:"skip-verify"`

	// ABS settings
	AccountName string `yaml:"account-name"`
	AccountKey  string `yaml:"account-key"`

	// SFTP settings
	Host     string `yaml:"host"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	KeyPath  string `yaml:"key-path"`
}

// NewReplicaFromConfig instantiates a replica for a DB based on a config.
func NewReplicaFromConfig(c *ReplicaConfig, db *litestream.DB) (_ *litestream.Replica, err error) {
	// Ensure user did not specify URL in path.
	if isURL(c.Path) {
		return nil, fmt.Errorf("replica path cannot be a url, please use the 'url' field instead: %s", c.Path)
	}

	// Build and set client on replica.
	var client litestream.ReplicaClient
	switch typ := c.ReplicaType(); typ {
	case "file":
		if client, err = newFileReplicaClientFromConfig(c); err != nil {
			return nil, err
		}
	case "s3":
		if client, err = newS3ReplicaClientFromConfig(c); err != nil {
			return nil, err
		}
	case "gcs":
		if client, err = newGCSReplicaClientFromConfig(c); err != nil {
			return nil, err
		}
	case "abs":
		if client, err = newABSReplicaClientFromConfig(c); err != nil {
			return nil, err
		}
	case "sftp":
		if client, err = newSFTPReplicaClientFromConfig(c); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown replica type in config: %q", typ)
	}

	// Build replica.
	r := litestream.NewReplica(db, c.Name, client)
	if v := c.Retention; v != nil {
		r.Retention = *v
	}
	if v := c.RetentionCheckInterval; v != nil {
		r.RetentionCheckInterval = *v
	}
	if v := c.SyncInterval; v != nil {
		r.SyncInterval = *v
	}
	if v := c.SnapshotInterval; v != nil {
		r.SnapshotInterval = *v
	}
	if v := c.ValidationInterval; v != nil {
		r.ValidationInterval = *v
	}

	return r, nil
}

// newFileReplicaClientFromConfig returns a new instance of FileReplicaClient built from config.
func newFileReplicaClientFromConfig(c *ReplicaConfig) (_ *litestream.FileReplicaClient, err error) {
	// Ensure URL & path are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for file replica")
	}

	// Parse path from URL, if specified.
	path := c.Path
	if c.URL != "" {
		if _, _, path, err = ParseReplicaURL(c.URL); err != nil {
			return nil, err
		}
	}

	// Ensure path is set explicitly or derived from URL field.
	if path == "" {
		return nil, fmt.Errorf("file replica path required")
	}

	// Expand home prefix and return absolute path.
	if path, err = expand(path); err != nil {
		return nil, err
	}

	// Instantiate replica and apply time fields, if set.
	return litestream.NewFileReplicaClient(path), nil
}

// newS3ReplicaClientFromConfig returns a new instance of s3.ReplicaClient built from config.
func newS3ReplicaClientFromConfig(c *ReplicaConfig) (_ *s3.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for s3 replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for s3 replica")
	}

	bucket, path := c.Bucket, c.Path
	region, endpoint, skipVerify := c.Region, c.Endpoint, c.SkipVerify

	// Use path style if an endpoint is explicitly set. This works because the
	// only service to not use path style is AWS which does not use an endpoint.
	forcePathStyle := (endpoint != "")
	if v := c.ForcePathStyle; v != nil {
		forcePathStyle = *v
	}

	// Apply settings from URL, if specified.
	if c.URL != "" {
		_, host, upath, err := ParseReplicaURL(c.URL)
		if err != nil {
			return nil, err
		}
		ubucket, uregion, uendpoint, uforcePathStyle := s3.ParseHost(host)

		// Only apply URL parts to field that have not been overridden.
		if path == "" {
			path = upath
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
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for s3 replica")
	}

	// Build replica.
	client := s3.NewReplicaClient()
	client.AccessKeyID = c.AccessKeyID
	client.SecretAccessKey = c.SecretAccessKey
	client.Bucket = bucket
	client.Path = path
	client.Region = region
	client.Endpoint = endpoint
	client.ForcePathStyle = forcePathStyle
	client.SkipVerify = skipVerify
	return client, nil
}

// newGCSReplicaClientFromConfig returns a new instance of gcs.ReplicaClient built from config.
func newGCSReplicaClientFromConfig(c *ReplicaConfig) (_ *gcs.ReplicaClient, err error) {
	// Ensure URL & constituent parts are not both specified.
	if c.URL != "" && c.Path != "" {
		return nil, fmt.Errorf("cannot specify url & path for gcs replica")
	} else if c.URL != "" && c.Bucket != "" {
		return nil, fmt.Errorf("cannot specify url & bucket for gcs replica")
	}

	bucket, path := c.Bucket, c.Path

	// Apply settings from URL, if specified.
	if c.URL != "" {
		_, uhost, upath, err := ParseReplicaURL(c.URL)
		if err != nil {
			return nil, err
		}

		// Only apply URL parts to field that have not been overridden.
		if path == "" {
			path = upath
		}
		if bucket == "" {
			bucket = uhost
		}
	}

	// Ensure required settings are set.
	if bucket == "" {
		return nil, fmt.Errorf("bucket required for gcs replica")
	}

	// Build replica.
	client := gcs.NewReplicaClient()
	client.Bucket = bucket
	client.Path = path
	return client, nil
}

// newABSReplicaClientFromConfig returns a new instance of abs.ReplicaClient built from config.
func newABSReplicaClientFromConfig(c *ReplicaConfig) (_ *abs.ReplicaClient, err error) {
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
func newSFTPReplicaClientFromConfig(c *ReplicaConfig) (_ *sftp.ReplicaClient, err error) {
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

// ParseReplicaURL parses a replica URL.
func ParseReplicaURL(s string) (scheme, host, urlpath string, err error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", "", "", err
	}

	switch u.Scheme {
	case "file":
		scheme, u.Scheme = u.Scheme, ""
		return scheme, "", path.Clean(u.String()), nil

	case "":
		return u.Scheme, u.Host, u.Path, fmt.Errorf("replica url scheme required: %s", s)

	default:
		return u.Scheme, u.Host, strings.TrimPrefix(path.Clean(u.Path), "/"), nil
	}
}

// isURL returns true if s can be parsed and has a scheme.
func isURL(s string) bool {
	return regexp.MustCompile(`^\w+:\/\/`).MatchString(s)
}

// ReplicaType returns the type based on the type field or extracted from the URL.
func (c *ReplicaConfig) ReplicaType() string {
	scheme, _, _, _ := ParseReplicaURL(c.URL)
	if scheme != "" {
		return scheme
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
	return "/etc/litestream.yml"
}

func registerConfigFlag(fs *flag.FlagSet, configPath *string, noExpandEnv *bool) {
	fs.StringVar(configPath, "config", "", "config path")
	fs.BoolVar(noExpandEnv, "no-expand-env", false, "do not expand env vars in config")
}

// expand returns an absolute path for s.
func expand(s string) (string, error) {
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

// indexVar allows the flag package to parse index flags as 4-byte hexadecimal values.
type indexVar int

// Ensure type implements interface.
var _ flag.Value = (*indexVar)(nil)

// String returns an 8-character hexadecimal value.
func (v *indexVar) String() string {
	return fmt.Sprintf("%08x", int(*v))
}

// Set parses s into an integer from a hexadecimal value.
func (v *indexVar) Set(s string) error {
	i, err := strconv.ParseInt(s, 16, 32)
	if err != nil {
		return fmt.Errorf("invalid hexadecimal format")
	}
	*v = indexVar(i)
	return nil
}

// loadReplicas returns a list of replicas to use based on CLI flags. Filters
// by replicaName, if not blank. The DB is returned if pathOrURL is not a replica URL.
func loadReplicas(ctx context.Context, config Config, pathOrURL, replicaName string) ([]*litestream.Replica, *litestream.DB, error) {
	// Build a replica based on URL, if specified.
	if isURL(pathOrURL) {
		r, err := NewReplicaFromConfig(&ReplicaConfig{
			URL:             pathOrURL,
			AccessKeyID:     config.AccessKeyID,
			SecretAccessKey: config.SecretAccessKey,
		}, nil)
		if err != nil {
			return nil, nil, err
		}
		return []*litestream.Replica{r}, nil, nil
	}

	// Otherwise use replicas from the database configuration file.
	path, err := expand(pathOrURL)
	if err != nil {
		return nil, nil, err
	}
	dbc := config.DBConfig(path)
	if dbc == nil {
		return nil, nil, fmt.Errorf("database not found in config: %s", path)
	}
	db, err := NewDBFromConfig(dbc)
	if err != nil {
		return nil, nil, err
	}

	// Filter by replica, if specified.
	if replicaName != "" {
		r := db.Replica(replicaName)
		if r == nil {
			return nil, nil, fmt.Errorf("replica %q not found for database %q", replicaName, db.Path())
		}
		return []*litestream.Replica{r}, db, nil
	}

	return db.Replicas, db, nil
}
