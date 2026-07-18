package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/MadAppGang/httplog"
	"github.com/dustin/go-humanize"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
)

type MCPServer struct {
	ctx        context.Context
	mux        *http.ServeMux
	httpServer *http.Server
	configPath string
}

func NewMCP(ctx context.Context, configPath string) (*MCPServer, error) {
	s := &MCPServer{
		ctx:        ctx,
		configPath: configPath,
	}

	mcpServer := server.NewMCPServer(
		"Litestream MCP Server",
		"1.0.0",
		server.WithToolCapabilities(false),
		server.WithRecovery(),
		server.WithLogging(),
	)
	// Add the tools to the server
	mcpServer.AddTool(InfoTool(configPath))
	mcpServer.AddTool(DatabasesTool(configPath))
	mcpServer.AddTool(RestoreTool(configPath))
	mcpServer.AddTool(RestorePlanTool(configPath))
	mcpServer.AddTool(LTXTool(configPath))
	mcpServer.AddTool(VersionTool())
	mcpServer.AddTool(StatusTool(configPath))
	mcpServer.AddTool(ResetTool(configPath))

	s.mux = http.NewServeMux()
	s.mux.Handle("/", httplog.Logger(server.NewStreamableHTTPServer(mcpServer)))
	return s, nil
}

func (s *MCPServer) Start(addr string) {
	s.httpServer = &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 30 * time.Second,
	}
	go func() {
		slog.Info("Starting MCP Streamable HTTP server", "addr", addr)
		if err := s.httpServer.ListenAndServe(); err != nil {
			slog.Error("MCP server error", "error", err)
		}
	}()
}

func (s *MCPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Close attempts to gracefully shutdown the server.
func (s *MCPServer) Close() error {
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

func DatabasesTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_databases",
		mcp.WithDescription("List databases and their replicas as defined in the Litestream config file. The default path is /etc/litestream.yml but is not required."),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		resources, err := loadMCPDatabases(req.GetString("config", configPath))
		if err != nil {
			return mcpToolError(err)
		}
		output, opErr := formatMCPDatabases(resources.DBs)
		if err := closeMCPResources(opErr, resources); err != nil {
			return mcpToolError(err)
		}
		return mcp.NewToolResultText(output), nil
	}
}

func InfoTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_info",
		mcp.WithDescription("Get a comprehensive summary of Litestream's current status including databases, LTX files, and version information."),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		resources, err := loadMCPDatabases(req.GetString("config", configPath))
		if err != nil {
			return mcpToolError(err)
		}
		output, opErr := formatMCPInfo(ctx, resources.ConfigPath, resources.DBs)
		if err := closeMCPResources(opErr, resources); err != nil {
			return mcpToolError(err)
		}
		return mcp.NewToolResultText(output), nil
	}
}

func RestoreTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_restore",
		mcp.WithDescription("Restore a database from a Litestream replica."),
		mcp.WithString("path", mcp.Required(), mcp.Description("Database path or replica URL.")),
		mcp.WithString("output", mcp.Description("Output path for the restored database. Optional.")),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
		mcp.WithString("txid", mcp.Description("Restore up to a specific transaction ID. Optional.")),
		mcp.WithString("timestamp", mcp.Description("Restore to a specific point-in-time (RFC3339). Optional.")),
		mcp.WithString("parallelism", mcp.Description("Number of WAL files to download in parallel. Optional.")),
		mcp.WithString("integrity_check", mcp.Description("Post-restore integrity check mode. Optional."), mcp.Enum("none", "quick", "full"), mcp.DefaultString("none")),
		mcp.WithBoolean("if_db_not_exists", mcp.Description("Skip restore if the database already exists. Optional.")),
		mcp.WithBoolean("if_replica_exists", mcp.Description("Skip restore if no backups are found. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		path := req.GetString("path", "")
		if path == "" {
			return mcpToolError(fmt.Errorf("database path or replica URL required"))
		}

		opt := litestream.NewRestoreOptions()
		opt.OutputPath = req.GetString("output", "")
		if opt.OutputPath == "" {
			opt.OutputPath = req.GetString("o", "")
		}
		var err error
		if opt.TXID, opt.Timestamp, err = parseMCPRestoreTarget(req); err != nil {
			return mcpToolError(err)
		}
		if value := req.GetString("parallelism", ""); value != "" {
			parallelism, err := strconv.Atoi(value)
			if err != nil {
				return mcpToolError(fmt.Errorf("invalid parallelism: %w", err))
			}
			opt.Parallelism = parallelism
		}
		integrityCheck := req.GetString("integrity_check", "none")
		if opt.IntegrityCheck, err = parseMCPIntegrityCheck(integrityCheck); err != nil {
			return mcpToolError(err)
		}

		resources, err := loadMCPRestoreReplica(path, req.GetString("config", configPath), &opt)
		if err != nil {
			return mcpToolError(err)
		}

		output, opErr := restoreMCP(ctx, path, resources.Replica, opt, integrityCheck,
			req.GetBool("if_db_not_exists", false), req.GetBool("if_replica_exists", false))
		if err := closeMCPResources(opErr, resources); err != nil {
			return mcpToolError(err)
		}
		return mcp.NewToolResultText(output), nil
	}
}

func RestorePlanTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_restore_plan",
		mcp.WithDescription("Preview the ordered snapshot and LTX files for a restore without writing a database."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithString("path", mcp.Required(), mcp.Description("Database path or replica URL.")),
		mcp.WithString("output", mcp.Description("Target database path to include in the plan. Optional.")),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional, ignored for replica URLs.")),
		mcp.WithString("txid", mcp.Description("Restore up to a specific transaction ID. Optional.")),
		mcp.WithString("timestamp", mcp.Description("Restore to a specific point-in-time (RFC3339). Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		path := req.GetString("path", "")
		if path == "" {
			return mcpToolError(fmt.Errorf("database path or replica URL required"))
		}

		opt := litestream.NewRestoreOptions()
		opt.OutputPath = req.GetString("output", "")
		var err error
		if opt.TXID, opt.Timestamp, err = parseMCPRestoreTarget(req); err != nil {
			return mcpToolError(err)
		}

		resources, err := loadMCPReplica(path, req.GetString("config", configPath))
		if err != nil {
			return mcpToolError(err)
		}
		plan, opErr := (&RestoreCommand{}).dryRunPlan(ctx, path, resources.Replica, opt)
		if errors.Is(opErr, litestream.ErrTxNotAvailable) {
			opErr = fmt.Errorf("no matching backup files available")
		}
		if err := closeMCPResources(opErr, resources); err != nil {
			return mcpToolError(err)
		}

		output, err := json.MarshalIndent(plan, "", "  ")
		if err != nil {
			return mcpToolError(fmt.Errorf("format response: %w", err))
		}
		return mcp.NewToolResultText(string(output) + "\n"), nil
	}
}

func VersionTool() (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_version",
		mcp.WithDescription("Print the Litestream binary version."),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		cmd := exec.CommandContext(ctx, "litestream", "version")
		output, err := cmd.CombinedOutput()
		if err != nil {
			return mcp.NewToolResultError(strings.TrimSpace(string(output)) + ": " + err.Error()), nil
		}
		return mcp.NewToolResultText(string(output)), nil
	}
}

func LTXTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_ltx",
		mcp.WithDescription("List all LTX files for a database or replica URL."),
		mcp.WithString("path", mcp.Required(), mcp.Description("Database path or replica URL.")),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional, ignored for replica URLs.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		path := req.GetString("path", "")
		if path == "" {
			return mcpToolError(fmt.Errorf("database path or replica URL required"))
		}

		resources, err := loadMCPReplica(path, req.GetString("config", configPath))
		if err != nil {
			return mcpToolError(err)
		}
		files, opErr := listMCPLTXFiles(ctx, resources.Replica, 0)
		var output string
		if opErr == nil {
			output, opErr = formatMCPLTXFiles(files)
		}
		if err := closeMCPResources(opErr, resources); err != nil {
			return mcpToolError(err)
		}
		return mcp.NewToolResultText(output), nil
	}
}

func StatusTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_status",
		mcp.WithDescription("Display replication status including database path, status, local and remote transaction IDs, and WAL size."),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
		mcp.WithString("path", mcp.Description("Filter to a specific database path. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		resources, err := loadMCPDatabases(req.GetString("config", configPath))
		if err != nil {
			return mcpToolError(err)
		}
		statuses, opErr := loadMCPStatuses(ctx, resources.DBs, req.GetString("path", ""))
		var output string
		if opErr == nil {
			output, opErr = formatMCPStatuses(statuses)
		}
		if err := closeMCPResources(opErr, resources); err != nil {
			return mcpToolError(err)
		}
		return mcp.NewToolResultText(output), nil
	}
}

func ResetTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_reset",
		mcp.WithDescription("Clear local Litestream state for a database. Removes local LTX files, forcing fresh snapshot on next sync. Database file is not modified."),
		mcp.WithString("path", mcp.Required(), mcp.Description("Database path to reset.")),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		path := req.GetString("path", "")
		if path == "" {
			return mcpToolError(fmt.Errorf("database path required"))
		}
		db, resources, err := loadMCPResetDB(path, req.GetString("config", configPath))
		if err != nil {
			return mcpToolError(err)
		}
		opErr := db.ResetLocalState(ctx)
		if opErr != nil {
			opErr = fmt.Errorf("reset local state: %w", opErr)
		}
		if resources != nil {
			opErr = closeMCPResources(opErr, resources)
		}
		if opErr != nil {
			return mcpToolError(opErr)
		}
		return mcp.NewToolResultText("Reset complete for " + db.Path() + ".\n"), nil
	}
}

type mcpDBStatus struct {
	Database   string
	Status     string
	LocalTXID  string
	RemoteTXID string
	WALSize    string
}

type mcpDatabases struct {
	ConfigPath string
	DBs        []*litestream.DB
}

func (resources *mcpDatabases) Close() error {
	var err error
	for _, db := range resources.DBs {
		if db.Replica == nil || db.Replica.Client == nil {
			continue
		}
		if closeErr := closeMCPReplica(db.Replica); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close replica for %s: %w", db.Path(), closeErr))
		}
	}
	return err
}

type mcpReplica struct {
	Replica   *litestream.Replica
	Databases *mcpDatabases
}

type mcpCloser interface {
	Close() error
}

func closeMCPResources(opErr error, resources mcpCloser) error {
	return errors.Join(opErr, resources.Close())
}

func (resources *mcpReplica) Close() error {
	if resources.Databases != nil {
		return resources.Databases.Close()
	}
	return closeMCPReplica(resources.Replica)
}

func closeMCPReplica(r *litestream.Replica) error {
	if r == nil || r.Client == nil {
		return nil
	}
	closer, ok := r.Client.(litestream.ReplicaClientCloser)
	if !ok {
		return nil
	}
	if err := closer.Close(); err != nil {
		return fmt.Errorf("close %s replica: %w", r.Client.Type(), err)
	}
	return nil
}

func loadMCPDatabases(configPath string) (*mcpDatabases, error) {
	if configPath == "" {
		configPath = DefaultConfigPath()
	}
	config, err := readConfigFile(configPath, true)
	if err != nil {
		return nil, err
	}

	resources := &mcpDatabases{ConfigPath: configPath}
	for _, dbConfig := range config.DBs {
		if dbConfig.Dir != "" {
			dirDBs, err := NewDBsFromDirectoryConfig(dbConfig)
			if err != nil {
				return nil, errors.Join(err, resources.Close())
			}
			resources.DBs = append(resources.DBs, dirDBs...)
			continue
		}
		db, err := NewDBFromConfig(dbConfig)
		if err != nil {
			return nil, errors.Join(err, resources.Close())
		}
		resources.DBs = append(resources.DBs, db)
	}
	return resources, nil
}

func formatMCPDatabases(dbs []*litestream.DB) (string, error) {
	var output strings.Builder
	w := tabwriter.NewWriter(&output, 0, 8, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "path\treplica"); err != nil {
		return "", err
	}
	for _, db := range dbs {
		if _, err := fmt.Fprintf(w, "%s\t%s\n", db.Path(), db.Replica.Client.Type()); err != nil {
			return "", err
		}
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	return output.String(), nil
}

func formatMCPInfo(ctx context.Context, configPath string, dbs []*litestream.DB) (string, error) {
	var summary strings.Builder
	summary.WriteString("=== Litestream Status Report ===\n\n")
	summary.WriteString("Version Information:\n")
	summary.WriteString(Version)
	summary.WriteString("\n\n")
	summary.WriteString("Current Config Path:\n")
	summary.WriteString(configPath + "\n\n")

	dbOutput, err := formatMCPDatabases(dbs)
	if err != nil {
		return "", err
	}
	summary.WriteString("Databases:\n")
	summary.WriteString(dbOutput)
	summary.WriteString("\n")

	summary.WriteString("LTX Files:\n")
	for _, db := range dbs {
		files, err := listMCPLTXFiles(ctx, db.Replica, 0)
		if err != nil {
			return "", fmt.Errorf("list ltx files for %s: %w", db.Path(), err)
		}
		ltxOutput, err := formatMCPLTXFiles(files)
		if err != nil {
			return "", err
		}
		summary.WriteString("Database: " + db.Path() + "\n")
		summary.WriteString(ltxOutput)
		summary.WriteString("\n")
	}

	return summary.String(), nil
}

func loadMCPRestoreReplica(path, configPath string, opt *litestream.RestoreOptions) (*mcpReplica, error) {
	if litestream.IsURL(path) {
		if opt.OutputPath == "" {
			return nil, fmt.Errorf("output is required when restoring from a replica URL")
		}
		syncInterval := litestream.DefaultSyncInterval
		r, err := NewReplicaFromConfig(&ReplicaConfig{
			URL: path,
			ReplicaSettings: ReplicaSettings{
				SyncInterval: &syncInterval,
			},
		}, nil)
		if err != nil {
			return nil, err
		}
		return &mcpReplica{Replica: r}, nil
	}

	resources, err := loadMCPDatabases(configPath)
	if err != nil {
		return nil, err
	}
	db, err := selectMCPDatabase(resources.DBs, path)
	if err != nil {
		return nil, errors.Join(err, resources.Close())
	}
	if opt.OutputPath == "" {
		opt.OutputPath = db.Path()
	}
	return &mcpReplica{Replica: db.Replica, Databases: resources}, nil
}

func parseMCPRestoreTarget(req mcp.CallToolRequest) (ltx.TXID, time.Time, error) {
	var txID ltx.TXID
	if value := req.GetString("txid", ""); value != "" {
		var err error
		if txID, err = ltx.ParseTXID(value); err != nil {
			return 0, time.Time{}, fmt.Errorf("invalid txid: %w", err)
		}
	}

	var timestamp time.Time
	if value := req.GetString("timestamp", ""); value != "" {
		var err error
		if timestamp, err = time.Parse(time.RFC3339, value); err != nil {
			return 0, time.Time{}, fmt.Errorf("invalid timestamp: %w", err)
		}
	}
	if txID != 0 && !timestamp.IsZero() {
		return 0, time.Time{}, fmt.Errorf("cannot specify both txid and timestamp")
	}
	return txID, timestamp, nil
}

func parseMCPIntegrityCheck(value string) (litestream.IntegrityCheckMode, error) {
	switch value {
	case "none":
		return litestream.IntegrityCheckNone, nil
	case "quick":
		return litestream.IntegrityCheckQuick, nil
	case "full":
		return litestream.IntegrityCheckFull, nil
	default:
		return litestream.IntegrityCheckNone, fmt.Errorf("invalid integrity_check: %s", value)
	}
}

func loadMCPReplica(path, configPath string) (*mcpReplica, error) {
	if litestream.IsURL(path) {
		r, err := NewReplicaFromConfig(&ReplicaConfig{URL: path}, nil)
		if err != nil {
			return nil, err
		}
		return &mcpReplica{Replica: r}, nil
	}
	resources, err := loadMCPDatabases(configPath)
	if err != nil {
		return nil, err
	}
	db, err := selectMCPDatabase(resources.DBs, path)
	if err != nil {
		return nil, errors.Join(err, resources.Close())
	}
	return &mcpReplica{Replica: db.Replica, Databases: resources}, nil
}

func restoreMCP(ctx context.Context, path string, r *litestream.Replica, opt litestream.RestoreOptions, integrityCheck string, ifDBNotExists, ifReplicaExists bool) (string, error) {
	if ifDBNotExists {
		if _, err := os.Stat(opt.OutputPath); err == nil {
			return "database already exists, skipping\n", nil
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("access output path: %w", err)
		}
	}
	if litestream.IsURL(path) && !ifReplicaExists {
		if _, err := r.CalcRestoreTarget(ctx, opt); err != nil {
			return "", err
		}
	}

	cmd := &RestoreCommand{}
	if err := cmd.prepareOutputPath(opt.OutputPath, false); err != nil {
		return "", err
	}

	txID, err := cmd.restoreTXID(ctx, r, &opt)
	if errors.Is(err, litestream.ErrTxNotAvailable) && ifReplicaExists {
		return "no matching backups found, skipping\n", nil
	} else if errors.Is(err, litestream.ErrTxNotAvailable) {
		return "", fmt.Errorf("no matching backup files available")
	} else if err != nil {
		return "", err
	}

	start := time.Now()
	if err := r.Restore(ctx, opt); errors.Is(err, litestream.ErrTxNotAvailable) && ifReplicaExists {
		return "no matching backups found, skipping\n", nil
	} else if errors.Is(err, litestream.ErrTxNotAvailable) {
		return "", fmt.Errorf("no matching backup files available")
	} else if err != nil {
		return "", err
	}

	output, err := json.MarshalIndent(RestoreResult{
		DBPath:         opt.OutputPath,
		Replica:        r.Client.Type(),
		TXID:           txID,
		DurationMS:     time.Since(start).Milliseconds(),
		IntegrityCheck: integrityCheck,
	}, "", "  ")
	if err != nil {
		return "", fmt.Errorf("format response: %w", err)
	}
	return string(output) + "\n", nil
}

func selectMCPDatabase(dbs []*litestream.DB, path string) (*litestream.DB, error) {
	expandedPath, err := expand(path)
	if err != nil {
		return nil, err
	}
	for _, db := range dbs {
		if db.Path() == expandedPath {
			return db, nil
		}
	}
	return nil, fmt.Errorf("database not found in config: %s", expandedPath)
}

func listMCPLTXFiles(ctx context.Context, r *litestream.Replica, level int) ([]LTXFileInfo, error) {
	itr, err := r.Client.LTXFiles(ctx, level, 0, false)
	if err != nil {
		return nil, err
	}

	var files []LTXFileInfo
	for itr.Next() {
		info := itr.Item()
		files = append(files, LTXFileInfo{
			Level:     info.Level,
			MinTXID:   info.MinTXID.String(),
			MaxTXID:   info.MaxTXID.String(),
			Size:      info.Size,
			Timestamp: info.CreatedAt.Format(time.RFC3339),
		})
	}
	if err := errors.Join(itr.Err(), itr.Close()); err != nil {
		return nil, err
	}
	return files, nil
}

func formatMCPLTXFiles(files []LTXFileInfo) (string, error) {
	var output strings.Builder
	w := tabwriter.NewWriter(&output, 0, 8, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "level\tmin_txid\tmax_txid\tsize\tcreated"); err != nil {
		return "", err
	}
	for _, file := range files {
		if _, err := fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%s\n",
			file.Level,
			file.MinTXID,
			file.MaxTXID,
			file.Size,
			file.Timestamp,
		); err != nil {
			return "", err
		}
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	return output.String(), nil
}

func loadMCPStatuses(ctx context.Context, dbs []*litestream.DB, filterPath string) ([]mcpDBStatus, error) {
	if filterPath != "" {
		expandedPath, err := expand(filterPath)
		if err != nil {
			return nil, err
		}
		filterPath = expandedPath
	}

	statuses := make([]mcpDBStatus, 0, len(dbs))
	for _, db := range dbs {
		if filterPath != "" && db.Path() != filterPath {
			continue
		}
		status, err := loadMCPStatus(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("status for %s: %w", db.Path(), err)
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func loadMCPStatus(ctx context.Context, db *litestream.DB) (mcpDBStatus, error) {
	status := mcpDBStatus{
		Database:   db.Path(),
		Status:     "unknown",
		LocalTXID:  "-",
		RemoteTXID: "-",
		WALSize:    "0 B",
	}

	syncStatus, err := db.SyncStatus(ctx)
	if err != nil {
		return status, err
	}
	if syncStatus.LocalTXID > 0 {
		status.LocalTXID = syncStatus.LocalTXID.String()
	}
	if syncStatus.RemoteTXID > 0 {
		status.RemoteTXID = syncStatus.RemoteTXID.String()
	}

	if info, err := os.Stat(db.Path()); errors.Is(err, fs.ErrNotExist) {
		status.Status = "no database"
	} else if err != nil {
		return status, fmt.Errorf("stat database: %w", err)
	} else if info.IsDir() {
		return status, fmt.Errorf("database path is a directory")
	} else if syncStatus.InSync {
		status.Status = "ok"
	} else if syncStatus.LocalTXID == 0 {
		status.Status = "not initialized"
	} else if syncStatus.LocalTXID < syncStatus.RemoteTXID {
		status.Status = "behind"
	} else {
		status.Status = "ahead"
	}

	if info, err := os.Stat(db.WALPath()); err == nil {
		status.WALSize = humanize.Bytes(uint64(info.Size()))
	} else if !errors.Is(err, fs.ErrNotExist) {
		return status, fmt.Errorf("stat wal: %w", err)
	}
	return status, nil
}

func formatMCPStatuses(statuses []mcpDBStatus) (string, error) {
	var output strings.Builder
	w := tabwriter.NewWriter(&output, 0, 8, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "database\tstatus\tlocal txid\tremote txid\twal size"); err != nil {
		return "", err
	}
	for _, status := range statuses {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			status.Database,
			status.Status,
			status.LocalTXID,
			status.RemoteTXID,
			status.WALSize,
		); err != nil {
			return "", err
		}
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	return output.String(), nil
}

func loadMCPResetDB(path, configPath string) (*litestream.DB, *mcpDatabases, error) {
	dbPath, err := expand(path)
	if err != nil {
		return nil, nil, err
	}
	if info, err := os.Stat(dbPath); errors.Is(err, fs.ErrNotExist) {
		return nil, nil, fmt.Errorf("database does not exist: %s", dbPath)
	} else if err != nil {
		return nil, nil, fmt.Errorf("access database: %w", err)
	} else if info.IsDir() {
		return nil, nil, fmt.Errorf("database path is a directory: %s", dbPath)
	}

	if configPath != "" {
		resources, err := loadMCPDatabases(configPath)
		if err != nil {
			return nil, nil, fmt.Errorf("read config: %w", err)
		}
		for _, db := range resources.DBs {
			if db.Path() == dbPath {
				return db, resources, nil
			}
		}
		if err := resources.Close(); err != nil {
			return nil, nil, err
		}
	}
	return litestream.NewDB(dbPath), nil, nil
}

func mcpToolError(err error) (*mcp.CallToolResult, error) {
	return mcp.NewToolResultError(err.Error()), nil
}
