package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/MadAppGang/httplog"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/benbjohnson/litestream/internal"
)

type MCPCommand struct {
	listener net.Listener
}

func (c *MCPCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litestream-mcp", flag.ContinueOnError)
	addr := fs.String("addr", "", "HTTP bind address")
	configPath := fs.String("config", "", "config path")
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() > 0 {
		return fmt.Errorf("too many arguments")
	}
	if *configPath == "" {
		*configPath = DefaultConfigPath()
	}

	internal.InitLog(os.Stderr, "INFO", "text", false)

	server, err := NewMCP(ctx, *configPath)
	if err != nil {
		return err
	}

	runCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *addr == "" {
		err = server.RunStdio(runCtx)
	} else {
		listener := c.listener
		if listener == nil {
			listener, err = net.Listen("tcp", *addr)
			if err != nil {
				return fmt.Errorf("listen for MCP HTTP server: %w", err)
			}
		}
		err = server.RunHTTP(runCtx, listener)
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (c *MCPCommand) Usage() {
	fmt.Printf(`
The mcp command runs the Litestream MCP server without the replication daemon.

Usage:

	litestream mcp [arguments]

Arguments:

	-addr ADDR
	    Serves MCP over Streamable HTTP at ADDR instead of stdio.

	-config PATH
	    Specifies the default configuration file used by MCP tools.
	    Defaults to %s

Examples:

	$ litestream mcp
	$ litestream mcp --config /path/to/litestream.yml
	$ litestream mcp --addr 127.0.0.1:3001

`[1:], DefaultConfigPath())
}

type MCPServer struct {
	ctx        context.Context
	server     *mcp.Server
	mux        *http.ServeMux
	httpServer *http.Server
	errCh      chan error
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
	s.server = mcpServer

	s.mux = http.NewServeMux()
	s.mux.Handle("/", httplog.Logger(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, nil)))
	return s, nil
}

func (s *MCPServer) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen for MCP HTTP server: %w", err)
	}
	s.httpServer = s.newHTTPServer(listener.Addr().String())
	s.errCh = make(chan error, 1)
	go func() {
		s.errCh <- s.runHTTP(s.ctx, listener)
	}()
	return nil
}

func (s *MCPServer) RunStdio(ctx context.Context) error {
	return s.server.Run(ctx, &mcp.StdioTransport{})
}

func (s *MCPServer) RunHTTP(ctx context.Context, listener net.Listener) error {
	s.httpServer = s.newHTTPServer(listener.Addr().String())
	return s.runHTTP(ctx, listener)
}

func (s *MCPServer) runHTTP(ctx context.Context, listener net.Listener) error {
	errCh := make(chan error, 1)
	go func() {
		slog.Info("Starting MCP Streamable HTTP server", "addr", listener.Addr().String())
		errCh <- s.httpServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		if err := s.Close(); err != nil {
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

func (s *MCPServer) newHTTPServer(addr string) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 30 * time.Second,
	}
}

func (s *MCPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Close attempts to gracefully shutdown the server.
func (s *MCPServer) Close() error {
	if s.httpServer == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 10*time.Second)
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
