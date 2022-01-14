package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/abs"
	"github.com/benbjohnson/litestream/gcs"
	"github.com/benbjohnson/litestream/s3"
	"github.com/benbjohnson/litestream/sftp"
	"github.com/mattn/go-shellwords"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReplicateCommand represents a command that continuously replicates SQLite databases.
type ReplicateCommand struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer

	configPath  string
	noExpandEnv bool

	cmd    *exec.Cmd  // subcommand
	execCh chan error // subcommand error channel

	Config Config

	// List of managed databases specified in the config.
	DBs []*litestream.DB
}

// NewReplicateCommand returns a new instance of ReplicateCommand.
func NewReplicateCommand(stdin io.Reader, stdout, stderr io.Writer) *ReplicateCommand {
	return &ReplicateCommand{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,

		execCh: make(chan error),
	}
}

// ParseFlags parses the CLI flags and loads the configuration file.
func (c *ReplicateCommand) ParseFlags(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-replicate", flag.ContinueOnError)
	execFlag := fs.String("exec", "", "execute subcommand")
	registerConfigFlag(fs, &c.configPath, &c.noExpandEnv)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Load configuration or use CLI args to build db/replica.
	if fs.NArg() == 1 {
		return fmt.Errorf("must specify at least one replica URL for %s", fs.Arg(0))
	} else if fs.NArg() > 1 {
		if c.configPath != "" {
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
		if c.configPath == "" {
			c.configPath = DefaultConfigPath()
		}
		if c.Config, err = ReadConfigFile(c.configPath, !c.noExpandEnv); err != nil {
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
func (c *ReplicateCommand) Run(ctx context.Context) (err error) {
	// Display version information.
	log.Printf("litestream %s", Version)

	// Setup databases.
	if len(c.Config.DBs) == 0 {
		log.Println("no databases specified in configuration")
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
		log.Printf("initialized db: %s", db.Path())
		for _, r := range db.Replicas {
			switch client := r.Client().(type) {
			case *litestream.FileReplicaClient:
				log.Printf("replicating to: name=%q type=%q path=%q", r.Name(), client.Type(), client.Path())
			case *s3.ReplicaClient:
				log.Printf("replicating to: name=%q type=%q bucket=%q path=%q region=%q endpoint=%q sync-interval=%s", r.Name(), client.Type(), client.Bucket, client.Path, client.Region, client.Endpoint, r.SyncInterval)
			case *gcs.ReplicaClient:
				log.Printf("replicating to: name=%q type=%q bucket=%q path=%q sync-interval=%s", r.Name(), client.Type(), client.Bucket, client.Path, r.SyncInterval)
			case *abs.ReplicaClient:
				log.Printf("replicating to: name=%q type=%q bucket=%q path=%q endpoint=%q sync-interval=%s", r.Name(), client.Type(), client.Bucket, client.Path, client.Endpoint, r.SyncInterval)
			case *sftp.ReplicaClient:
				log.Printf("replicating to: name=%q type=%q host=%q user=%q path=%q sync-interval=%s", r.Name(), client.Type(), client.Host, client.User, client.Path, r.SyncInterval)
			default:
				log.Printf("replicating to: name=%q type=%q", r.Name(), client.Type())
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

		log.Printf("serving metrics on http://%s/metrics", hostport)
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(c.Config.Addr, nil); err != nil {
				log.Printf("cannot start metrics server: %s", err)
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

	log.Printf("litestream initialization complete")

	return nil
}

// Close closes all open databases.
func (c *ReplicateCommand) Close() (err error) {
	for _, db := range c.DBs {
		if e := db.SoftClose(); e != nil {
			log.Printf("error closing db: path=%s err=%s", db.Path(), e)
			if err == nil {
				err = e
			}
		}
	}
	return err
}

// Usage prints the help screen to STDOUT.
func (c *ReplicateCommand) Usage() {
	fmt.Fprintf(c.stdout, `
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
