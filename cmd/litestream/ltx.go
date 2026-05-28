package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

// LTXCommand represents a command to list LTX files for a database.
type LTXCommand struct{}

// Run executes the command.
func (c *LTXCommand) Run(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-ltx", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	jsonOutput := fs.Bool("json", false, "output raw JSON")
	var level levelVar
	fs.Var(&level, "level", "compaction level (0-9 or \"all\")")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 || fs.Arg(0) == "" {
		return fmt.Errorf("database path required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	var r *litestream.Replica
	if litestream.IsURL(fs.Arg(0)) {
		if *configPath != "" {
			return fmt.Errorf("cannot specify a replica URL and the -config flag")
		}
		if r, err = NewReplicaFromConfig(&ReplicaConfig{URL: fs.Arg(0)}, nil); err != nil {
			return err
		}
		internal.InitLog(os.Stdout, "INFO", "text", false)
	} else {
		if *configPath == "" {
			*configPath = DefaultConfigPath()
		}

		// Load configuration.
		config, err := ReadConfigFile(*configPath, !*noExpandEnv)
		if err != nil {
			return err
		}

		// Lookup database from configuration file by path.
		path, err := expand(fs.Arg(0))
		if err != nil {
			return err
		}
		dbc := config.DBConfig(path)
		if dbc == nil {
			return fmt.Errorf("database not found in config: %s", path)
		}

		db, err := NewDBFromConfig(dbc)
		if err != nil {
			return err
		} else if db.Replica == nil {
			return fmt.Errorf("database has no replica")
		}
		r = db.Replica
	}

	// Determine which levels to iterate.
	var levels []int
	if int(level) == levelAll {
		for lvl := 0; lvl <= litestream.SnapshotLevel; lvl++ {
			levels = append(levels, lvl)
		}
	} else {
		levels = []int{int(level)}
	}

	var files []LTXFileInfo
	for _, lvl := range levels {
		itr, err := r.Client.LTXFiles(ctx, lvl, 0, false)
		if err != nil {
			return err
		}
		for itr.Next() {
			info := itr.Item()
			files = append(files, LTXFileInfo{
				Level:     lvl,
				MinTXID:   info.MinTXID.String(),
				MaxTXID:   info.MaxTXID.String(),
				Size:      info.Size,
				Timestamp: info.CreatedAt.Format(time.RFC3339),
			})
		}
		if err := itr.Close(); err != nil {
			return err
		}
	}

	if *jsonOutput {
		output, err := json.MarshalIndent(files, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to format response: %w", err)
		}
		fmt.Println(string(output))
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()

	fmt.Fprintln(w, "level\tmin_txid\tmax_txid\tsize\tcreated")
	for _, file := range files {
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%s\n",
			file.Level,
			file.MinTXID,
			file.MaxTXID,
			file.Size,
			file.Timestamp,
		)
	}
	return nil
}

type LTXFileInfo struct {
	Level     int    `json:"level"`
	MinTXID   string `json:"min_txid"`
	MaxTXID   string `json:"max_txid"`
	Size      int64  `json:"size"`
	Timestamp string `json:"timestamp"`
}

// Usage prints the help screen to STDOUT.
func (c *LTXCommand) Usage() {
	fmt.Printf(`
The ltx command lists all LTX files available for a database.

Usage:

	litestream ltx [arguments] DB_PATH

	litestream ltx [arguments] REPLICA_URL

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-no-expand-env
	    Disables environment variable expansion in configuration file.

	-replica NAME
	    Optional, filter by a specific replica.

	-level LEVEL
	    Compaction level to list (0-9 or "all").
	    Defaults to 0.

	-json
	    Output raw JSON instead of human-readable text.

Examples:

	# List all LTX files for a database.
	$ litestream ltx /path/to/db

	# List all LTX files on S3
	$ litestream ltx -replica s3 /path/to/db

	# List all LTX files for replica URL.
	$ litestream ltx s3://mybkt/db

	# List LTX files at snapshot level (level 9).
	$ litestream ltx -level 9 /path/to/db

	# List LTX files across all compaction levels.
	$ litestream ltx -level all /path/to/db

`[1:],
		DefaultConfigPath(),
	)
}
