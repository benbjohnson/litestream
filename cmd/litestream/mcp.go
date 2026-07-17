package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/MadAppGang/httplog"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/benbjohnson/litestream"
)

const (
	defaultDaemonSocketPath   = "/var/run/litestream.sock"
	defaultDaemonReadTimeout  = 10
	defaultDaemonWriteTimeout = 30
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
	capabilities := &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}}
	if err := json.Unmarshal([]byte(`{"logging":{}}`), capabilities); err != nil {
		return nil, fmt.Errorf("configure MCP capabilities: %w", err)
	}

	mcpServer := mcp.NewServer(
		&mcp.Implementation{Name: "Litestream MCP Server", Version: Version},
		&mcp.ServerOptions{
			Capabilities: capabilities,
		},
	)
	mcpServer.AddReceivingMiddleware(recoveryMiddleware)
	infoTool, infoHandler := InfoTool(configPath)
	mcp.AddTool(mcpServer, infoTool, infoHandler)
	databasesTool, databasesHandler := DatabasesTool(configPath)
	mcp.AddTool(mcpServer, databasesTool, databasesHandler)
	restoreTool, restoreHandler := RestoreTool(configPath)
	mcp.AddTool(mcpServer, restoreTool, restoreHandler)
	ltxTool, ltxHandler := LTXTool(configPath)
	mcp.AddTool(mcpServer, ltxTool, ltxHandler)
	versionTool, versionHandler := VersionTool()
	mcp.AddTool(mcpServer, versionTool, versionHandler)
	statusTool, statusHandler := StatusTool(configPath)
	mcp.AddTool(mcpServer, statusTool, statusHandler)
	resetTool, resetHandler := ResetTool(configPath)
	mcp.AddTool(mcpServer, resetTool, resetHandler)
	daemonListTool, daemonListHandler := DaemonListTool()
	mcp.AddTool(mcpServer, daemonListTool, daemonListHandler)
	daemonSyncTool, daemonSyncHandler := DaemonSyncTool()
	mcp.AddTool(mcpServer, daemonSyncTool, daemonSyncHandler)
	daemonInfoTool, daemonInfoHandler := DaemonInfoTool()
	mcp.AddTool(mcpServer, daemonInfoTool, daemonInfoHandler)
	daemonStartTool, daemonStartHandler := DaemonStartTool()
	mcp.AddTool(mcpServer, daemonStartTool, daemonStartHandler)
	daemonStopTool, daemonStopHandler := DaemonStopTool()
	mcp.AddTool(mcpServer, daemonStopTool, daemonStopHandler)
	daemonRegisterTool, daemonRegisterHandler := DaemonRegisterTool()
	mcp.AddTool(mcpServer, daemonRegisterTool, daemonRegisterHandler)
	daemonUnregisterTool, daemonUnregisterHandler := DaemonUnregisterTool()
	mcp.AddTool(mcpServer, daemonUnregisterTool, daemonUnregisterHandler)

	s.mux = http.NewServeMux()
	s.mux.Handle("/", httplog.Logger(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, nil)))
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

// isReplicaURL returns true if the path looks like a replica URL (s3://, gs://, etc.)
// rather than a local database path. The CLI rejects -config when using replica URLs.
func isReplicaURL(path string) bool {
	return strings.Contains(path, "://")
}

type databasesInput struct {
	Config *string `json:"config,omitempty" jsonschema:"Path to the Litestream config file. Optional."`
}

type databasesOutput struct {
	Text string `json:"text" jsonschema:"Databases and replicas from the Litestream config file."`
}

func DatabasesTool(configPath string) (*mcp.Tool, mcp.ToolHandlerFor[databasesInput, databasesOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_databases",
		Description: "List databases and their replicas as defined in the Litestream config file. The default path is /etc/litestream.yml but is not required.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
		InputSchema: nonNullableInputSchema[databasesInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input databasesInput) (*mcp.CallToolResult, databasesOutput, error) {
		args := []string{"databases"}
		config := configPath
		if input.Config != nil {
			config = *input.Config
		}
		args = append(args, "-config", config)
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return nil, databasesOutput{}, commandError(output, err)
		}
		text := string(output)
		return textResult(text), databasesOutput{Text: text}, nil
	}
}

type infoInput struct {
	Config *string `json:"config,omitempty" jsonschema:"Path to the Litestream config file. Optional."`
}

type infoOutput struct {
	Text string `json:"text" jsonschema:"Comprehensive Litestream status report."`
}

func InfoTool(configPath string) (*mcp.Tool, mcp.ToolHandlerFor[infoInput, infoOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_info",
		Description: "Get a comprehensive summary of Litestream's current status including databases, LTX files, and version information.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
		InputSchema: nonNullableInputSchema[infoInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input infoInput) (*mcp.CallToolResult, infoOutput, error) {
		var summary strings.Builder
		summary.WriteString("=== Litestream Status Report ===\n\n")

		versionCmd := exec.CommandContext(ctx, "litestream", "version")
		versionOutput, err := versionCmd.CombinedOutput()
		if err != nil {
			return nil, infoOutput{}, fmt.Errorf("get version info: %w", commandError(versionOutput, err))
		}
		summary.WriteString("Version Information:\n")
		summary.WriteString(string(versionOutput))
		summary.WriteString("\n")

		args := []string{"databases"}
		config := configPath
		if input.Config != nil {
			config = *input.Config
		}
		summary.WriteString("Current Config Path:\n")
		summary.WriteString(config + "\n\n")

		args = append(args, "-config", config)
		dbCmd := exec.CommandContext(ctx, "litestream", args...)
		dbOutput, err := dbCmd.CombinedOutput()
		if err != nil {
			return nil, infoOutput{}, fmt.Errorf("get databases info: %w", commandError(dbOutput, err))
		}

		summary.WriteString("Databases:\n")
		summary.WriteString(string(dbOutput))
		summary.WriteString("\n")

		scanner := bufio.NewScanner(strings.NewReader(string(dbOutput)))
		scanner.Scan()
		var dbPaths []string
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) > 0 {
				dbPaths = append(dbPaths, fields[0])
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, infoOutput{}, fmt.Errorf("scan databases info: %w", err)
		}

		summary.WriteString("LTX Files:\n")
		for _, dbPath := range dbPaths {
			ltxArgs := []string{"ltx"}
			if config != "" {
				ltxArgs = append(ltxArgs, "-config", config)
			}
			ltxArgs = append(ltxArgs, dbPath)
			ltxCmd := exec.CommandContext(ctx, "litestream", ltxArgs...)
			ltxOutput, err := ltxCmd.CombinedOutput()
			if err != nil {
				return nil, infoOutput{}, fmt.Errorf("get LTX files for %s: %w", dbPath, commandError(ltxOutput, err))
			}
			summary.WriteString("Database: " + dbPath + "\n")
			summary.WriteString(string(ltxOutput))
			summary.WriteString("\n")
		}

		text := summary.String()
		return textResult(text), infoOutput{Text: text}, nil
	}
}

type restoreInput struct {
	Path            string  `json:"path" jsonschema:"Database path or replica URL."`
	Output          *string `json:"o,omitempty" jsonschema:"Output path for the restored database. Optional."`
	Config          *string `json:"config,omitempty" jsonschema:"Path to the Litestream config file. Optional."`
	TXID            *string `json:"txid,omitempty" jsonschema:"Restore up to a specific transaction ID. Optional."`
	Timestamp       *string `json:"timestamp,omitempty" jsonschema:"Restore to a specific point-in-time (RFC3339). Optional."`
	Parallelism     *string `json:"parallelism,omitempty" jsonschema:"Number of WAL files to download in parallel. Optional."`
	IfDBNotExists   *bool   `json:"if_db_not_exists,omitempty" jsonschema:"Return 0 if the database already exists. Optional."`
	IfReplicaExists *bool   `json:"if_replica_exists,omitempty" jsonschema:"Return 0 if no backups are found. Optional."`
}

type restoreOutput struct {
	Text string `json:"text" jsonschema:"Restore command output."`
}

func RestoreTool(configPath string) (*mcp.Tool, mcp.ToolHandlerFor[restoreInput, restoreOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_restore",
		Description: "Restore a database from a Litestream replica.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPointer(true)},
		InputSchema: nonNullableInputSchema[restoreInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input restoreInput) (*mcp.CallToolResult, restoreOutput, error) {
		args := []string{"restore"}
		if input.Output != nil {
			args = append(args, "-o", *input.Output)
		}

		if !isReplicaURL(input.Path) {
			config := configPath
			if input.Config != nil {
				config = *input.Config
			}
			if config != "" {
				args = append(args, "-config", config)
			}
		}

		if input.TXID != nil {
			args = append(args, "-txid", *input.TXID)
		}
		if input.Timestamp != nil {
			args = append(args, "-timestamp", *input.Timestamp)
		}
		if input.Parallelism != nil {
			args = append(args, "-parallelism", *input.Parallelism)
		}
		if input.IfDBNotExists != nil {
			args = append(args, "-if-db-not-exists", strconv.FormatBool(*input.IfDBNotExists))
		}
		if input.IfReplicaExists != nil {
			args = append(args, "-if-replica-exists", strconv.FormatBool(*input.IfReplicaExists))
		}
		if input.Path != "" {
			args = append(args, input.Path)
		}
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return nil, restoreOutput{}, commandError(output, err)
		}
		text := string(output)
		return textResult(text), restoreOutput{Text: text}, nil
	}
}

type versionInput struct{}

type versionOutput struct {
	Text string `json:"text" jsonschema:"Running Litestream instance version."`
}

func VersionTool() (*mcp.Tool, mcp.ToolHandlerFor[versionInput, versionOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_version",
		Description: "Print the running Litestream instance's version.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}

	return tool, func(context.Context, *mcp.CallToolRequest, versionInput) (*mcp.CallToolResult, versionOutput, error) {
		text := Version + "\n"
		return textResult(text), versionOutput{Text: text}, nil
	}
}

type ltxInput struct {
	Path   string  `json:"path" jsonschema:"Database path or replica URL."`
	Config *string `json:"config,omitempty" jsonschema:"Path to the Litestream config file. Optional, ignored for replica URLs."`
}

type ltxOutput struct {
	Text string `json:"text" jsonschema:"LTX files for the database or replica URL."`
}

func LTXTool(configPath string) (*mcp.Tool, mcp.ToolHandlerFor[ltxInput, ltxOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_ltx",
		Description: "List all LTX files for a database or replica URL.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
		InputSchema: nonNullableInputSchema[ltxInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input ltxInput) (*mcp.CallToolResult, ltxOutput, error) {
		args := []string{"ltx"}

		if !isReplicaURL(input.Path) {
			config := configPath
			if input.Config != nil {
				config = *input.Config
			}
			if config != "" {
				args = append(args, "-config", config)
			}
		}

		if input.Path != "" {
			args = append(args, input.Path)
		}
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return nil, ltxOutput{}, commandError(output, err)
		}
		text := string(output)
		return textResult(text), ltxOutput{Text: text}, nil
	}
}

type statusInput struct {
	Config *string `json:"config,omitempty" jsonschema:"Path to the Litestream config file. Optional."`
	Path   *string `json:"path,omitempty" jsonschema:"Filter to a specific database path. Optional."`
}

type statusOutput struct {
	Text string `json:"text" jsonschema:"Litestream replication status."`
}

func StatusTool(configPath string) (*mcp.Tool, mcp.ToolHandlerFor[statusInput, statusOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_status",
		Description: "Display replication status including database path, status, local transaction ID, and WAL size.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
		InputSchema: nonNullableInputSchema[statusInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input statusInput) (*mcp.CallToolResult, statusOutput, error) {
		args := []string{"status"}
		config := configPath
		if input.Config != nil {
			config = *input.Config
		}
		args = append(args, "-config", config)
		if input.Path != nil {
			args = append(args, *input.Path)
		}
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return nil, statusOutput{}, commandError(output, err)
		}
		text := string(output)
		return textResult(text), statusOutput{Text: text}, nil
	}
}

type resetInput struct {
	Path   string  `json:"path" jsonschema:"Database path to reset."`
	Config *string `json:"config,omitempty" jsonschema:"Path to the Litestream config file. Optional."`
}

type resetOutput struct {
	Text string `json:"text" jsonschema:"Reset command output."`
}

func ResetTool(configPath string) (*mcp.Tool, mcp.ToolHandlerFor[resetInput, resetOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_reset",
		Description: "Clear local Litestream state for a database. Removes local LTX files, forcing fresh snapshot on next sync. Database file is not modified.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPointer(true)},
		InputSchema: nonNullableInputSchema[resetInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input resetInput) (*mcp.CallToolResult, resetOutput, error) {
		args := []string{"reset"}
		config := configPath
		if input.Config != nil {
			config = *input.Config
		}
		args = append(args, "-config", config)
		if input.Path != "" {
			args = append(args, input.Path)
		}
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return nil, resetOutput{}, commandError(output, err)
		}
		text := string(output)
		return textResult(text), resetOutput{Text: text}, nil
	}
}

type daemonOutput struct {
	Text string `json:"text" jsonschema:"Daemon response encoded as JSON."`
}

type daemonListInput struct {
	Socket  *string `json:"socket,omitempty" jsonschema:"Path to the Litestream daemon control socket. Optional."`
	Timeout *int    `json:"timeout,omitempty" jsonschema:"Maximum time to wait in seconds. Optional."`
}

func DaemonListTool() (*mcp.Tool, mcp.ToolHandlerFor[daemonListInput, daemonOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_list",
		Description: "List databases managed by a running Litestream daemon.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
		InputSchema: nonNullableInputSchema[daemonListInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input daemonListInput) (*mcp.CallToolResult, daemonOutput, error) {
		client, err := newDaemonClient(input.Socket, input.Timeout, defaultDaemonReadTimeout)
		if err != nil {
			return nil, daemonOutput{}, err
		}

		var response litestream.ListResponse
		if err := client.do(ctx, http.MethodGet, "/list", nil, &response); err != nil {
			return nil, daemonOutput{}, err
		}
		return daemonResult(response)
	}
}

type daemonSyncInput struct {
	Path    string  `json:"path" jsonschema:"Database path managed by the Litestream daemon."`
	Wait    *bool   `json:"wait,omitempty" jsonschema:"Wait for remote replication to complete. Optional."`
	Socket  *string `json:"socket,omitempty" jsonschema:"Path to the Litestream daemon control socket. Optional."`
	Timeout *int    `json:"timeout,omitempty" jsonschema:"Maximum time to wait in seconds. Optional."`
}

func DaemonSyncTool() (*mcp.Tool, mcp.ToolHandlerFor[daemonSyncInput, daemonOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_sync",
		Description: "Force an immediate sync for a database managed by a running Litestream daemon, optionally waiting for remote replication.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPointer(true)},
		InputSchema: nonNullableInputSchema[daemonSyncInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input daemonSyncInput) (*mcp.CallToolResult, daemonOutput, error) {
		client, err := newDaemonClient(input.Socket, input.Timeout, defaultDaemonWriteTimeout)
		if err != nil {
			return nil, daemonOutput{}, err
		}

		var response litestream.SyncResponse
		request := litestream.SyncRequest{
			Path:    input.Path,
			Wait:    input.Wait != nil && *input.Wait,
			Timeout: client.timeoutSeconds,
		}
		if err := client.do(ctx, http.MethodPost, "/sync", request, &response); err != nil {
			return nil, daemonOutput{}, err
		}
		return daemonResult(response)
	}
}

type daemonInfoInput struct {
	Socket  *string `json:"socket,omitempty" jsonschema:"Path to the Litestream daemon control socket. Optional."`
	Timeout *int    `json:"timeout,omitempty" jsonschema:"Maximum time to wait in seconds. Optional."`
}

func DaemonInfoTool() (*mcp.Tool, mcp.ToolHandlerFor[daemonInfoInput, daemonOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_daemon_info",
		Description: "Get version, process, uptime, and database count information from a running Litestream daemon.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
		InputSchema: nonNullableInputSchema[daemonInfoInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input daemonInfoInput) (*mcp.CallToolResult, daemonOutput, error) {
		client, err := newDaemonClient(input.Socket, input.Timeout, defaultDaemonReadTimeout)
		if err != nil {
			return nil, daemonOutput{}, err
		}

		var response litestream.InfoResponse
		if err := client.do(ctx, http.MethodGet, "/info", nil, &response); err != nil {
			return nil, daemonOutput{}, err
		}
		return daemonResult(response)
	}
}

type daemonStartInput struct {
	Path    string  `json:"path" jsonschema:"Database path managed by the Litestream daemon."`
	Socket  *string `json:"socket,omitempty" jsonschema:"Path to the Litestream daemon control socket. Optional."`
	Timeout *int    `json:"timeout,omitempty" jsonschema:"Maximum time to wait in seconds. Optional."`
}

func DaemonStartTool() (*mcp.Tool, mcp.ToolHandlerFor[daemonStartInput, daemonOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_start",
		Description: "Start replication for a database managed by a running Litestream daemon.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPointer(true)},
		InputSchema: nonNullableInputSchema[daemonStartInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input daemonStartInput) (*mcp.CallToolResult, daemonOutput, error) {
		client, err := newDaemonClient(input.Socket, input.Timeout, defaultDaemonWriteTimeout)
		if err != nil {
			return nil, daemonOutput{}, err
		}

		var response litestream.StartResponse
		request := litestream.StartRequest{Path: input.Path, Timeout: client.timeoutSeconds}
		if err := client.do(ctx, http.MethodPost, "/start", request, &response); err != nil {
			return nil, daemonOutput{}, err
		}
		return daemonResult(response)
	}
}

type daemonStopInput struct {
	Path    string  `json:"path" jsonschema:"Database path managed by the Litestream daemon."`
	Socket  *string `json:"socket,omitempty" jsonschema:"Path to the Litestream daemon control socket. Optional."`
	Timeout *int    `json:"timeout,omitempty" jsonschema:"Maximum time to wait in seconds. Optional."`
}

func DaemonStopTool() (*mcp.Tool, mcp.ToolHandlerFor[daemonStopInput, daemonOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_stop",
		Description: "Stop replication for a database managed by a running Litestream daemon after a final sync.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPointer(true)},
		InputSchema: nonNullableInputSchema[daemonStopInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input daemonStopInput) (*mcp.CallToolResult, daemonOutput, error) {
		client, err := newDaemonClient(input.Socket, input.Timeout, defaultDaemonWriteTimeout)
		if err != nil {
			return nil, daemonOutput{}, err
		}

		var response litestream.StopResponse
		request := litestream.StopRequest{Path: input.Path, Timeout: client.timeoutSeconds}
		if err := client.do(ctx, http.MethodPost, "/stop", request, &response); err != nil {
			return nil, daemonOutput{}, err
		}
		return daemonResult(response)
	}
}

type daemonRegisterInput struct {
	Path       string  `json:"path" jsonschema:"Database path to register with the Litestream daemon."`
	ReplicaURL string  `json:"replica_url" jsonschema:"Replica URL for the database."`
	Socket     *string `json:"socket,omitempty" jsonschema:"Path to the Litestream daemon control socket. Optional."`
	Timeout    *int    `json:"timeout,omitempty" jsonschema:"Maximum time to wait in seconds. Optional."`
}

func DaemonRegisterTool() (*mcp.Tool, mcp.ToolHandlerFor[daemonRegisterInput, daemonOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_register",
		Description: "Register a database and replica with a running Litestream daemon.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPointer(true)},
		InputSchema: nonNullableInputSchema[daemonRegisterInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input daemonRegisterInput) (*mcp.CallToolResult, daemonOutput, error) {
		client, err := newDaemonClient(input.Socket, input.Timeout, defaultDaemonWriteTimeout)
		if err != nil {
			return nil, daemonOutput{}, err
		}

		var response litestream.RegisterDatabaseResponse
		request := litestream.RegisterDatabaseRequest{Path: input.Path, ReplicaURL: input.ReplicaURL}
		if err := client.do(ctx, http.MethodPost, "/register", request, &response); err != nil {
			return nil, daemonOutput{}, err
		}
		return daemonResult(response)
	}
}

type daemonUnregisterInput struct {
	Path    string  `json:"path" jsonschema:"Database path to unregister from the Litestream daemon."`
	Socket  *string `json:"socket,omitempty" jsonschema:"Path to the Litestream daemon control socket. Optional."`
	Timeout *int    `json:"timeout,omitempty" jsonschema:"Maximum time to wait in seconds. Optional."`
}

func DaemonUnregisterTool() (*mcp.Tool, mcp.ToolHandlerFor[daemonUnregisterInput, daemonOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_unregister",
		Description: "Stop replication and unregister a database from a running Litestream daemon.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPointer(true)},
		InputSchema: nonNullableInputSchema[daemonUnregisterInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input daemonUnregisterInput) (*mcp.CallToolResult, daemonOutput, error) {
		client, err := newDaemonClient(input.Socket, input.Timeout, defaultDaemonWriteTimeout)
		if err != nil {
			return nil, daemonOutput{}, err
		}

		var response litestream.UnregisterDatabaseResponse
		request := litestream.UnregisterDatabaseRequest{Path: input.Path, Timeout: client.timeoutSeconds}
		if err := client.do(ctx, http.MethodPost, "/unregister", request, &response); err != nil {
			return nil, daemonOutput{}, err
		}
		return daemonResult(response)
	}
}

type daemonClient struct {
	socketPath     string
	timeoutSeconds int
}

func newDaemonClient(socket *string, timeout *int, defaultTimeout int) (*daemonClient, error) {
	socketPath := defaultDaemonSocketPath
	if socket != nil {
		socketPath = *socket
	}
	if socketPath == "" {
		return nil, fmt.Errorf("daemon socket path required")
	}

	timeoutSeconds := defaultTimeout
	if timeout != nil {
		timeoutSeconds = *timeout
	}
	if timeoutSeconds <= 0 {
		return nil, fmt.Errorf("timeout must be greater than 0")
	}

	return &daemonClient{socketPath: socketPath, timeoutSeconds: timeoutSeconds}, nil
}

func (c *daemonClient) do(ctx context.Context, method, endpoint string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("marshal daemon request: %w", err)
		}
		body = bytes.NewReader(data)
	}

	request, err := http.NewRequestWithContext(ctx, method, "http://localhost"+endpoint, body)
	if err != nil {
		return fmt.Errorf("create daemon request: %w", err)
	}
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	timeout := time.Duration(c.timeoutSeconds) * time.Second
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: timeout}).DialContext(ctx, "unix", c.socketPath)
		},
	}
	client := &http.Client{Transport: transport, Timeout: timeout}
	defer client.CloseIdleConnections()

	response, err := client.Do(request)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return daemonConnectionError(c.socketPath, err)
	}
	defer response.Body.Close()

	data, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("read daemon response: %w", err)
	}

	operation := strings.TrimPrefix(endpoint, "/")
	if response.StatusCode != http.StatusOK {
		var daemonError litestream.ErrorResponse
		if err := json.Unmarshal(data, &daemonError); err == nil && daemonError.Error != "" {
			return fmt.Errorf("%s failed: %s", operation, daemonError.Error)
		}
		if message := strings.TrimSpace(string(data)); message != "" {
			return fmt.Errorf("%s failed: %s", operation, message)
		}
		return fmt.Errorf("%s failed with status %s", operation, response.Status)
	}
	if err := json.Unmarshal(data, output); err != nil {
		return fmt.Errorf("parse %s response: %w", operation, err)
	}
	return nil
}

func daemonConnectionError(socketPath string, err error) error {
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("no Litestream daemon is running at socket %s: %w", socketPath, err)
	}
	return fmt.Errorf("connect to Litestream daemon at socket %s: %w", socketPath, err)
}

func daemonResult(response any) (*mcp.CallToolResult, daemonOutput, error) {
	data, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		return nil, daemonOutput{}, fmt.Errorf("format daemon response: %w", err)
	}
	text := string(data) + "\n"
	return textResult(text), daemonOutput{Text: text}, nil
}

func textResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
}

func commandError(output []byte, err error) error {
	if message := strings.TrimSpace(string(output)); message != "" {
		return fmt.Errorf("%s: %w", message, err)
	}
	return err
}

func boolPointer(value bool) *bool {
	return &value
}

func nonNullableInputSchema[Input any]() *jsonschema.Schema {
	schema, err := jsonschema.For[Input](nil)
	if err != nil {
		panic(fmt.Sprintf("infer input schema: %v", err))
	}
	for _, property := range schema.Properties {
		property.Types = slices.DeleteFunc(property.Types, func(value string) bool { return value == "null" })
		if len(property.Types) == 1 {
			property.Type = property.Types[0]
			property.Types = nil
		}
	}
	return schema
}

func recoveryMiddleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, request mcp.Request) (result mcp.Result, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				result = nil
				err = fmt.Errorf("panic recovered in %s handler: %v", method, recovered)
			}
		}()
		return next(ctx, method, request)
	}
}
