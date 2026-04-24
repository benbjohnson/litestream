package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

// RestoreCommand represents a command to restore a database from a backup.
type RestoreCommand struct{}

// Run executes the command.
func (c *RestoreCommand) Run(ctx context.Context, args []string) (err error) {
	opt := litestream.NewRestoreOptions()

	fs := flag.NewFlagSet("litestream-restore", flag.ContinueOnError)
	configPath, noExpandEnv := registerConfigFlag(fs)
	fs.StringVar(&opt.OutputPath, "o", "", "output path")
	fs.Var((*txidVar)(&opt.TXID), "txid", "transaction ID")
	fs.IntVar(&opt.Parallelism, "parallelism", opt.Parallelism, "parallelism")
	ifDBNotExists := fs.Bool("if-db-not-exists", false, "")
	ifReplicaExists := fs.Bool("if-replica-exists", false, "")
	timestampStr := fs.String("timestamp", "", "timestamp")
	dryRun := fs.Bool("dry-run", false, "print restore plan without writing")
	jsonOutput := fs.Bool("json", false, "output raw JSON")
	fs.BoolVar(&opt.Follow, "f", false, "follow mode")
	fs.DurationVar(&opt.FollowInterval, "follow-interval", opt.FollowInterval, "polling interval for follow mode")
	integrityCheck := fs.String("integrity-check", "none", "post-restore integrity check: none, quick, or full")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 || fs.Arg(0) == "" {
		return fmt.Errorf("database path or replica URL required")
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	logOutput := os.Stdout
	if *jsonOutput {
		logOutput = os.Stderr
	}
	internal.InitLog(logOutput, "INFO", "text", false)

	// When follow mode is enabled, set up signal handling so Ctrl+C stops
	// the follow loop cleanly.
	if opt.Follow {
		ch := signalChan()
		cancelCtx, cancel := context.WithCancel(ctx)
		go func() {
			select {
			case <-ch:
				cancel()
			case <-cancelCtx.Done():
			}
		}()
		ctx = cancelCtx
		defer cancel()
	}

	switch *integrityCheck {
	case "none":
		opt.IntegrityCheck = litestream.IntegrityCheckNone
	case "quick":
		opt.IntegrityCheck = litestream.IntegrityCheckQuick
	case "full":
		opt.IntegrityCheck = litestream.IntegrityCheckFull
	default:
		return fmt.Errorf("invalid -integrity-check value: %s", *integrityCheck)
	}

	// Parse timestamp, if specified.
	if *timestampStr != "" {
		if opt.Timestamp, err = time.Parse(time.RFC3339, *timestampStr); err != nil {
			return errors.New("invalid -timestamp, must specify in ISO 8601 format (e.g. 2000-01-01T00:00:00Z)")
		}
	}

	// Determine replica to restore from.
	var r *litestream.Replica
	if litestream.IsURL(fs.Arg(0)) {
		if *configPath != "" {
			return fmt.Errorf("cannot specify a replica URL and the -config flag")
		}
		if r, err = c.loadFromURL(ctx, fs.Arg(0), *ifDBNotExists, &opt); errors.Is(err, errSkipDBExists) {
			slog.Info("database already exists, skipping")
			return nil
		} else if err != nil {
			return err
		}
	} else {
		if *configPath == "" {
			*configPath = DefaultConfigPath()
		}
		if r, err = c.loadFromConfig(ctx, fs.Arg(0), *configPath, !*noExpandEnv, *ifDBNotExists, &opt); errors.Is(err, errSkipDBExists) {
			slog.Info("database already exists, skipping")
			return nil
		} else if err != nil {
			return err
		}
	}

	if *dryRun {
		plan, err := c.dryRunPlan(ctx, fs.Arg(0), r, opt)
		if errors.Is(err, litestream.ErrTxNotAvailable) {
			return fmt.Errorf("no matching backup files available")
		} else if err != nil {
			return err
		}
		if *jsonOutput {
			output, err := json.MarshalIndent(plan, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to format response: %w", err)
			}
			fmt.Println(string(output))
			return nil
		}
		c.printDryRunPlan(plan)
		return nil
	}

	txid := c.restoreTXID(ctx, r, opt)
	start := time.Now()
	if err := r.Restore(ctx, opt); errors.Is(err, litestream.ErrTxNotAvailable) {
		if *ifReplicaExists {
			slog.Info("no matching backups found")
			return nil
		}
		return fmt.Errorf("no matching backup files available")
	} else if err != nil {
		return err
	}
	if *jsonOutput {
		output, err := json.MarshalIndent(RestoreResult{
			DBPath:         opt.OutputPath,
			Replica:        r.Client.Type(),
			TXID:           txid,
			DurationMS:     time.Since(start).Milliseconds(),
			IntegrityCheck: *integrityCheck,
		}, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to format response: %w", err)
		}
		fmt.Println(string(output))
	}
	return nil
}

type RestorePlan struct {
	Source     string            `json:"source"`
	TargetPath string            `json:"target_path"`
	Replica    string            `json:"replica"`
	MinTXID    string            `json:"min_txid"`
	MaxTXID    string            `json:"max_txid"`
	Files      []RestorePlanFile `json:"files"`
}

type RestorePlanFile struct {
	Level     int    `json:"level"`
	Name      string `json:"name"`
	MinTXID   string `json:"min_txid"`
	MaxTXID   string `json:"max_txid"`
	Size      int64  `json:"size"`
	Timestamp string `json:"timestamp"`
}

type RestoreResult struct {
	DBPath         string `json:"db_path"`
	Replica        string `json:"replica"`
	TXID           string `json:"txid"`
	DurationMS     int64  `json:"duration_ms"`
	IntegrityCheck string `json:"integrity_check"`
}

func (c *RestoreCommand) dryRunPlan(ctx context.Context, source string, r *litestream.Replica, opt litestream.RestoreOptions) (RestorePlan, error) {
	if opt.Follow {
		return RestorePlan{}, fmt.Errorf("cannot use -dry-run with -f")
	}

	infos, err := litestream.CalcRestorePlan(ctx, r.Client, opt.TXID, opt.Timestamp, r.Logger())
	if err != nil {
		return RestorePlan{}, err
	}
	if len(infos) == 0 {
		return RestorePlan{}, litestream.ErrTxNotAvailable
	}

	plan := RestorePlan{
		Source:     source,
		TargetPath: opt.OutputPath,
		Replica:    r.Client.Type(),
		MinTXID:    infos[0].MinTXID.String(),
		MaxTXID:    infos[len(infos)-1].MaxTXID.String(),
		Files:      make([]RestorePlanFile, 0, len(infos)),
	}
	for _, info := range infos {
		plan.Files = append(plan.Files, RestorePlanFile{
			Level:     info.Level,
			Name:      ltx.FormatFilename(info.MinTXID, info.MaxTXID),
			MinTXID:   info.MinTXID.String(),
			MaxTXID:   info.MaxTXID.String(),
			Size:      info.Size,
			Timestamp: info.CreatedAt.Format(time.RFC3339),
		})
	}
	return plan, nil
}

func (c *RestoreCommand) printDryRunPlan(plan RestorePlan) {
	fmt.Println("Restore plan:")
	fmt.Printf("  source: %s\n", plan.Source)
	fmt.Printf("  target: %s\n", plan.TargetPath)
	fmt.Printf("  replica: %s\n", plan.Replica)
	fmt.Printf("  txid range: %s - %s\n", plan.MinTXID, plan.MaxTXID)
	fmt.Println()
	fmt.Println("Files to fetch:")

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer w.Flush()
	fmt.Fprintln(w, "level\tfile\tmin_txid\tmax_txid\tsize\ttimestamp")
	for _, file := range plan.Files {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%d\t%s\n",
			file.Level,
			file.Name,
			file.MinTXID,
			file.MaxTXID,
			file.Size,
			file.Timestamp,
		)
	}
}

func (c *RestoreCommand) restoreTXID(ctx context.Context, r *litestream.Replica, opt litestream.RestoreOptions) string {
	if opt.TXID != 0 {
		return opt.TXID.String()
	}
	if opt.Follow {
		return ""
	}
	infos, err := litestream.CalcRestorePlan(ctx, r.Client, opt.TXID, opt.Timestamp, r.Logger())
	if err != nil || len(infos) == 0 {
		return ""
	}
	return infos[len(infos)-1].MaxTXID.String()
}

// loadFromURL creates a replica & updates the restore options from a replica URL.
func (c *RestoreCommand) loadFromURL(ctx context.Context, replicaURL string, ifDBNotExists bool, opt *litestream.RestoreOptions) (*litestream.Replica, error) {
	if opt.OutputPath == "" {
		return nil, fmt.Errorf("-o is required when restoring from a replica URL. Try: litestream restore -o /path/to/db %s", replicaURL)
	}

	// Exit successfully if the output file already exists.
	if _, err := os.Stat(opt.OutputPath); !os.IsNotExist(err) && ifDBNotExists {
		return nil, errSkipDBExists
	}

	syncInterval := litestream.DefaultSyncInterval
	r, err := NewReplicaFromConfig(&ReplicaConfig{
		URL: replicaURL,
		ReplicaSettings: ReplicaSettings{
			SyncInterval: &syncInterval,
		},
	}, nil)
	if err != nil {
		return nil, err
	}
	_, err = r.CalcRestoreTarget(ctx, *opt)
	return r, err
}

// loadFromConfig returns a replica & updates the restore options from a DB reference.
func (c *RestoreCommand) loadFromConfig(_ context.Context, dbPath, configPath string, expandEnv, ifDBNotExists bool, opt *litestream.RestoreOptions) (*litestream.Replica, error) {
	// Load configuration.
	config, err := ReadConfigFile(configPath, expandEnv)
	if err != nil {
		return nil, err
	}

	// Lookup database from configuration file by path.
	if dbPath, err = expand(dbPath); err != nil {
		return nil, err
	}
	dbConfig := config.DBConfig(dbPath)
	if dbConfig == nil {
		return nil, fmt.Errorf("database not found in config: %s", dbPath)
	}
	db, err := NewDBFromConfig(dbConfig)
	if err != nil {
		return nil, err
	}

	// Restore into original database path if not specified.
	if opt.OutputPath == "" {
		opt.OutputPath = dbPath
	}

	// Exit successfully if the output file already exists.
	if _, err := os.Stat(opt.OutputPath); !os.IsNotExist(err) && ifDBNotExists {
		return nil, errSkipDBExists
	}

	return db.Replica, nil
}

// Usage prints the help screen to STDOUT.
func (c *RestoreCommand) Usage() {
	fmt.Printf(`
The restore command recovers a database from a previous snapshot and WAL.

Usage:

	litestream restore [arguments] DB_PATH

	litestream restore [arguments] REPLICA_URL

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-no-expand-env
	    Disables environment variable expansion in configuration file.

	-txid TXID
	    Restore up to a specific hex-encoded transaction ID (inclusive).
	    Defaults to use the highest available transaction.

	-timestamp TIMESTAMP
	    Restore to a specific point-in-time.
	    Defaults to use the latest available backup.

	-o PATH
	    Output path of the restored database.
	    Defaults to original DB path.

	-if-db-not-exists
	    Returns exit code of 0 if the database already exists.

	-if-replica-exists
	    Returns exit code of 0 if no backups found.

	-dry-run
	    Print the restore plan and exit without writing a database.

	-json
	    Output raw JSON summary on successful restore.

	-f
	    Follow mode. After restoring, continuously poll for and apply
	    new changes. Similar to tail -f. The restored database should
	    only be opened in read-only mode by consumers.
	    Cannot be used with -txid or -timestamp.

	-follow-interval DURATION
	    Polling interval for follow mode.
	    Defaults to 1s.

	-parallelism NUM
	    Determines the number of WAL files downloaded in parallel.
	    Defaults to `+strconv.Itoa(litestream.DefaultRestoreParallelism)+`.

	-integrity-check MODE
	    Run a post-restore integrity check on the database.
	    MODE is one of: none, quick, full.
	    Defaults to none.


Examples:

	# Restore latest replica for database to original location.
	$ litestream restore /path/to/db

	# Restore replica for database to a given point in time.
	$ litestream restore -timestamp 2020-01-01T00:00:00Z /path/to/db

	# Restore latest replica for database to new /tmp directory
	$ litestream restore -o /tmp/db /path/to/db

	# Restore database from S3 replica URL.
	$ litestream restore -o /tmp/db s3://mybucket/db

	# Preview restore plan without writing a database.
	$ litestream restore -dry-run -o /tmp/db s3://mybucket/db

	# Continuously restore (follow) a database from a replica.
	$ litestream restore -f -o /tmp/read-replica.db s3://mybucket/db

`[1:],
		DefaultConfigPath(),
	)
}

var errSkipDBExists = errors.New("database already exists, skipping")
