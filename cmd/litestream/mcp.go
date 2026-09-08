package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/MadAppGang/httplog"
	"github.com/dustin/go-humanize"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/sftp"
)

type MCPServer struct {
	ctx        context.Context
	mux        *http.ServeMux
	httpServer *http.Server
	httpCancel context.CancelFunc
	errCh      chan error
	httpDone   chan struct{}
	httpErr    error
	configPath string
}

func NewMCP(ctx context.Context, configPath string) (*MCPServer, error) {
	s := &MCPServer{
		ctx:        ctx,
		configPath: configPath,
	}

	mcpServer := server.NewMCPServer(
		"Litestream MCP Server",
		Version,
		server.WithToolCapabilities(false),
		server.WithRecovery(),
		server.WithLogging(),
	)
	// Add the tools to the server
	mcpServer.AddTool(InfoTool(configPath))
	mcpServer.AddTool(DatabasesTool(configPath))
	mcpServer.AddTool(RestoreTool(configPath))
	mcpServer.AddTool(LTXTool(configPath))
	mcpServer.AddTool(VersionTool())
	mcpServer.AddTool(StatusTool(configPath))
	mcpServer.AddTool(ResetTool(configPath))

	s.mux = http.NewServeMux()
	s.mux.Handle("/", httplog.Logger(server.NewStreamableHTTPServer(mcpServer)))
	return s, nil
}

func (s *MCPServer) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen for MCP HTTP server: %w", err)
	}
	s.httpServer = s.newHTTPServer(s.ctx, listener.Addr().String())
	s.errCh = make(chan error, 1)
	s.httpDone = make(chan struct{})
	go func() {
		s.httpErr = s.runHTTP(s.ctx, listener)
		s.errCh <- s.httpErr
		close(s.httpDone)
	}()
	return nil
}

func (s *MCPServer) runHTTP(ctx context.Context, listener net.Listener) error {
	defer s.httpCancel()
	errCh := make(chan error, 1)
	go func() {
		slog.Info("Starting MCP Streamable HTTP server", "addr", listener.Addr().String())
		errCh <- s.httpServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		if err := s.shutdownHTTP(); err != nil {
			shutdownErr := fmt.Errorf("close MCP HTTP server: %w", err)
			if err := s.httpServer.Close(); err != nil {
				shutdownErr = errors.Join(shutdownErr, fmt.Errorf("force close MCP HTTP server: %w", err))
			}
			if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
				shutdownErr = errors.Join(shutdownErr, fmt.Errorf("serve MCP HTTP server: %w", err))
			}
			return shutdownErr
		}
		if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve MCP HTTP server: %w", err)
		}
		return nil
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve MCP HTTP server: %w", err)
		}
		return nil
	}
}

func (s *MCPServer) newHTTPServer(ctx context.Context, addr string) *http.Server {
	httpCtx, cancel := context.WithCancel(ctx)
	s.httpCancel = cancel
	return &http.Server{
		Addr:    addr,
		Handler: s.mux,
		BaseContext: func(net.Listener) context.Context {
			return httpCtx
		},
		ReadHeaderTimeout: 30 * time.Second,
	}
}

func (s *MCPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Close attempts to gracefully shutdown the server.
func (s *MCPServer) Close() error {
	err := s.shutdownHTTP()
	if s.httpDone != nil {
		if err != nil {
			err = errors.Join(err, s.httpServer.Close())
		}
		<-s.httpDone
		err = errors.Join(err, s.httpErr)
	}
	return err
}

func (s *MCPServer) shutdownHTTP() error {
	if s.httpServer == nil {
		return nil
	}
	if s.httpCancel != nil {
		s.httpCancel()
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 10*time.Second)
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
		cleanup := startMCPResourceCleanup(ctx, resources)
		output, opErr := formatMCPDatabases(resources.DBs)
		if err := closeMCPResources(opErr, cleanup); err != nil {
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
		cleanup := startMCPResourceCleanup(ctx, resources)
		output, opErr := formatMCPInfo(ctx, resources.ConfigPath, resources.DBs)
		if err := closeMCPResources(opErr, cleanup); err != nil {
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
		if value := req.GetString("txid", ""); value != "" {
			txID, err := ltx.ParseTXID(value)
			if err != nil {
				return mcpToolError(fmt.Errorf("invalid txid: %w", err))
			}
			opt.TXID = txID
		}
		if value := req.GetString("timestamp", ""); value != "" {
			timestamp, err := time.Parse(time.RFC3339, value)
			if err != nil {
				return mcpToolError(fmt.Errorf("invalid timestamp: %w", err))
			}
			opt.Timestamp = timestamp
		}
		if value := req.GetString("parallelism", ""); value != "" {
			parallelism, err := strconv.Atoi(value)
			if err != nil {
				return mcpToolError(fmt.Errorf("invalid parallelism: %w", err))
			}
			opt.Parallelism = parallelism
		}

		resources, err := loadMCPRestoreReplica(path, req.GetString("config", configPath), &opt)
		if err != nil {
			return mcpToolError(err)
		}
		cleanup := startMCPResourceCleanup(ctx, resources)

		output, opErr := restoreMCP(ctx, path, resources.Replica, opt,
			req.GetBool("if_db_not_exists", false), req.GetBool("if_replica_exists", false))
		if err := closeMCPResources(opErr, cleanup); err != nil {
			return mcpToolError(err)
		}
		return mcp.NewToolResultText(output), nil
	}
}

func VersionTool() (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_version",
		mcp.WithDescription("Print the running Litestream binary version."),
	)
	return tool, func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText(Version + "\n"), nil
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
		cleanup := startMCPResourceCleanup(ctx, resources)
		files, opErr := listMCPLTXFiles(ctx, resources.Replica, 0)
		var output string
		if opErr == nil {
			output, opErr = formatMCPLTXFiles(files)
		}
		if err := closeMCPResources(opErr, cleanup); err != nil {
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
		cleanup := startMCPResourceCleanup(ctx, resources)
		statuses, opErr := loadMCPStatuses(ctx, resources.DBs, req.GetString("path", ""))
		var output string
		if opErr == nil {
			output, opErr = formatMCPStatuses(statuses)
		}
		if err := closeMCPResources(opErr, cleanup); err != nil {
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
		var cleanup mcpCloser
		if resources != nil {
			cleanup = startMCPResourceCleanup(ctx, resources)
		}
		opErr := db.ResetLocalState(ctx)
		if opErr != nil {
			opErr = fmt.Errorf("reset local state: %w", opErr)
		}
		if resources != nil {
			opErr = closeMCPResources(opErr, cleanup)
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

var errMCPReplicaClientNotClosable = errors.New("replica client does not implement cleanup contract")

type mcpCleanup func() error

func (cleanup mcpCleanup) Close() error {
	return cleanup()
}

func startMCPResourceCleanup(ctx context.Context, resources mcpCloser) mcpCloser {
	var cleanups []func() error
	addReplica := func(r *litestream.Replica, path string) {
		closeReplica := func() error {
			err := closeMCPReplica(r)
			if err != nil && path != "" {
				return fmt.Errorf("close replica for %s: %w", path, err)
			}
			return err
		}
		if r != nil {
			if _, ok := r.Client.(*sftp.ReplicaClient); ok {
				cleanups = append(cleanups, closeMCPOnCancellation(ctx, closeReplica))
				return
			}
		}
		cleanups = append(cleanups, closeReplica)
	}
	switch resources := resources.(type) {
	case *mcpDatabases:
		for _, db := range resources.DBs {
			addReplica(db.Replica, db.Path())
		}
	case *mcpReplica:
		if resources.Databases != nil {
			return startMCPResourceCleanup(ctx, resources.Databases)
		}
		addReplica(resources.Replica, "")
	default:
		cleanups = append(cleanups, resources.Close)
	}
	return mcpCleanup(func() error {
		err := context.Cause(ctx)
		for _, cleanup := range cleanups {
			err = errors.Join(err, cleanup())
		}
		return err
	})
}

func closeMCPOnCancellation(ctx context.Context, closeResource func() error) func() error {
	var once sync.Once
	var closeErr error
	closeOnce := func() { once.Do(func() { closeErr = closeResource() }) }
	stop := context.AfterFunc(ctx, closeOnce)
	return func() error {
		stop()
		closeOnce()
		return closeErr
	}
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
		return fmt.Errorf("close %s replica: %w", r.Client.Type(), errMCPReplicaClientNotClosable)
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

func restoreMCP(ctx context.Context, path string, r *litestream.Replica, opt litestream.RestoreOptions, ifDBNotExists, ifReplicaExists bool) (string, error) {
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
		IntegrityCheck: "none",
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
