package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/MadAppGang/httplog"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
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
	mcpServer := mcp.NewServer(
		&mcp.Implementation{Name: "Litestream MCP Server", Version: Version},
		&mcp.ServerOptions{
			Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}},
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

	s.mux = http.NewServeMux()
	s.mux.Handle("/", httplog.Logger(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return mcpServer
	}, &mcp.StreamableHTTPOptions{Stateless: true})))
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
	Databases []DatabaseInfo `json:"databases" jsonschema:"Databases and replicas from the Litestream config file."`
}

func DatabasesTool(configPath string) (*mcp.Tool, mcp.ToolHandlerFor[databasesInput, databasesOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_databases",
		Description: "List databases and their replicas as defined in the Litestream config file. The default path is /etc/litestream.yml but is not required.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
		InputSchema: nonNullableInputSchema[databasesInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input databasesInput) (*mcp.CallToolResult, databasesOutput, error) {
		args := []string{"databases", "-json"}
		config := configPath
		if input.Config != nil {
			config = *input.Config
		}
		args = append(args, "-config", config)
		databases, err := runJSONCommand[[]DatabaseInfo](ctx, args...)
		if err != nil {
			return nil, databasesOutput{}, err
		}
		text := fmt.Sprintf("Found %s.", countNoun(len(databases), "configured database", "configured databases"))
		return textResult(text), databasesOutput{Databases: databases}, nil
	}
}

type infoInput struct {
	Config *string `json:"config,omitempty" jsonschema:"Path to the Litestream config file. Optional."`
}

type infoDatabaseOutput struct {
	DatabaseInfo
	LTXFiles []LTXFileInfo `json:"ltx_files" jsonschema:"LTX files for the database."`
}

type infoOutput struct {
	Version   string               `json:"version" jsonschema:"Running Litestream instance version."`
	Config    string               `json:"config" jsonschema:"Path to the Litestream config file."`
	Databases []infoDatabaseOutput `json:"databases" jsonschema:"Configured databases and their LTX files."`
}

func InfoTool(configPath string) (*mcp.Tool, mcp.ToolHandlerFor[infoInput, infoOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_info",
		Description: "Get a comprehensive summary of Litestream's current status including databases, LTX files, and version information.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
		InputSchema: nonNullableInputSchema[infoInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input infoInput) (*mcp.CallToolResult, infoOutput, error) {
		config := configPath
		if input.Config != nil {
			config = *input.Config
		}
		if config == "" {
			config = DefaultConfigPath()
		}

		databases, err := runJSONCommand[[]DatabaseInfo](ctx, "databases", "-json", "-config", config)
		if err != nil {
			return nil, infoOutput{}, fmt.Errorf("get databases info: %w", err)
		}

		output := infoOutput{
			Version:   Version,
			Config:    config,
			Databases: make([]infoDatabaseOutput, 0, len(databases)),
		}
		ltxFileCount := 0
		for _, database := range databases {
			ltxArgs := []string{"ltx", "-json"}
			if config != "" {
				ltxArgs = append(ltxArgs, "-config", config)
			}
			ltxArgs = append(ltxArgs, database.Path)
			files, err := runJSONCommand[[]LTXFileInfo](ctx, ltxArgs...)
			if err != nil {
				return nil, infoOutput{}, fmt.Errorf("get LTX files for %s: %w", database.Path, err)
			}
			ltxFileCount += len(files)
			output.Databases = append(output.Databases, infoDatabaseOutput{
				DatabaseInfo: database,
				LTXFiles:     files,
			})
		}

		text := fmt.Sprintf("Litestream %s has %s and %s.", Version,
			countNoun(len(databases), "configured database", "configured databases"),
			countNoun(ltxFileCount, "LTX file", "LTX files"))
		return textResult(text), output, nil
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
	Status RestoreStatus  `json:"status" jsonschema:"Restore command outcome."`
	Reason RestoreReason  `json:"reason,omitempty" jsonschema:"Stable reason when the restore was skipped."`
	Result *RestoreResult `json:"result,omitempty" jsonschema:"Restore details when a database was restored."`
}

func RestoreTool(configPath string) (*mcp.Tool, mcp.ToolHandlerFor[restoreInput, restoreOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_restore",
		Description: "Restore a database from a Litestream replica.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: boolPointer(true)},
		InputSchema: nonNullableInputSchema[restoreInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input restoreInput) (*mcp.CallToolResult, restoreOutput, error) {
		args := []string{"restore", "-json"}
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
			args = append(args, fmt.Sprintf("-if-db-not-exists=%t", *input.IfDBNotExists))
		}
		if input.IfReplicaExists != nil {
			args = append(args, fmt.Sprintf("-if-replica-exists=%t", *input.IfReplicaExists))
		}
		if input.Path != "" {
			args = append(args, input.Path)
		}
		output, err := runJSONCommand[RestoreOutput](ctx, args...)
		if err != nil {
			return nil, restoreOutput{}, err
		}
		switch output.Status {
		case RestoreStatusRestored:
			if output.RestoreResult == nil {
				return nil, restoreOutput{}, fmt.Errorf("restore JSON output missing result")
			}
			text := fmt.Sprintf("Restored %s using the %s replica", output.DBPath, output.Replica)
			if output.TXID != "" {
				text += " at TXID " + output.TXID
			}
			return textResult(text + "."), restoreOutput{Status: output.Status, Result: output.RestoreResult}, nil
		case RestoreStatusSkipped:
			switch output.Reason {
			case RestoreReasonDatabaseExists:
				path := input.Path
				if input.Output != nil && *input.Output != "" {
					path = *input.Output
				}
				return textResult(fmt.Sprintf("Restore skipped because the database already exists at %s.", path)), restoreOutput{Status: output.Status, Reason: output.Reason}, nil
			case RestoreReasonNoMatchingBackups:
				return textResult(fmt.Sprintf("Restore skipped for %s because no matching backups were found.", input.Path)), restoreOutput{Status: output.Status, Reason: output.Reason}, nil
			default:
				return nil, restoreOutput{}, fmt.Errorf("unknown restore skip reason %q", output.Reason)
			}
		default:
			return nil, restoreOutput{}, fmt.Errorf("unknown restore status %q", output.Status)
		}
	}
}

type versionInput struct{}

type versionOutput struct {
	Version string `json:"version" jsonschema:"Running Litestream instance version."`
}

func VersionTool() (*mcp.Tool, mcp.ToolHandlerFor[versionInput, versionOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_version",
		Description: "Print the running Litestream instance's version.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}

	return tool, func(context.Context, *mcp.CallToolRequest, versionInput) (*mcp.CallToolResult, versionOutput, error) {
		return textResult(fmt.Sprintf("Litestream %s.", Version)), versionOutput{Version: Version}, nil
	}
}

type ltxInput struct {
	Path   string  `json:"path" jsonschema:"Database path or replica URL."`
	Config *string `json:"config,omitempty" jsonschema:"Path to the Litestream config file. Optional, ignored for replica URLs."`
}

type ltxOutput struct {
	Files []LTXFileInfo `json:"files" jsonschema:"LTX files for the database or replica URL."`
}

func LTXTool(configPath string) (*mcp.Tool, mcp.ToolHandlerFor[ltxInput, ltxOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_ltx",
		Description: "List all LTX files for a database or replica URL.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
		InputSchema: nonNullableInputSchema[ltxInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input ltxInput) (*mcp.CallToolResult, ltxOutput, error) {
		args := []string{"ltx", "-json"}

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
		files, err := runJSONCommand[[]LTXFileInfo](ctx, args...)
		if err != nil {
			return nil, ltxOutput{}, err
		}
		text := fmt.Sprintf("Found %s for %s.", countNoun(len(files), "LTX file", "LTX files"), input.Path)
		return textResult(text), ltxOutput{Files: files}, nil
	}
}

type statusInput struct {
	Config *string `json:"config,omitempty" jsonschema:"Path to the Litestream config file. Optional."`
	Path   *string `json:"path,omitempty" jsonschema:"Filter to a specific database path. Optional."`
}

type statusOutput struct {
	Databases []DBStatus `json:"databases" jsonschema:"Replication status for configured databases."`
}

func StatusTool(configPath string) (*mcp.Tool, mcp.ToolHandlerFor[statusInput, statusOutput]) {
	tool := &mcp.Tool{
		Name:        "litestream_status",
		Description: "Display replication status including database path, status, local transaction ID, and WAL size.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
		InputSchema: nonNullableInputSchema[statusInput](),
	}

	return tool, func(ctx context.Context, _ *mcp.CallToolRequest, input statusInput) (*mcp.CallToolResult, statusOutput, error) {
		args := []string{"status", "-json"}
		config := configPath
		if input.Config != nil {
			config = *input.Config
		}
		args = append(args, "-config", config)
		if input.Path != nil {
			args = append(args, *input.Path)
		}
		statuses, err := runJSONCommand[[]DBStatus](ctx, args...)
		if err != nil {
			return nil, statusOutput{}, err
		}
		text := fmt.Sprintf("Reported replication status for %s.", countNoun(len(statuses), "database", "databases"))
		return textResult(text), statusOutput{Databases: statuses}, nil
	}
}

type resetInput struct {
	Path   string  `json:"path" jsonschema:"Database path to reset."`
	Config *string `json:"config,omitempty" jsonschema:"Path to the Litestream config file. Optional."`
}

type resetOutput struct {
	Path   string `json:"path" jsonschema:"Database path passed to the reset command."`
	Status string `json:"status" jsonschema:"Reset command outcome."`
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
		_, _, err := runCommand(ctx, args...)
		if err != nil {
			return nil, resetOutput{}, err
		}
		text := fmt.Sprintf("Reset command completed for %s.", input.Path)
		return textResult(text), resetOutput{Path: input.Path, Status: "completed"}, nil
	}
}

func textResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
}

func runJSONCommand[Output any](ctx context.Context, args ...string) (Output, error) {
	var output Output
	stdout, _, err := runCommand(ctx, args...)
	if err != nil {
		return output, err
	}
	if err := json.Unmarshal(stdout, &output); err != nil {
		return output, fmt.Errorf("decode %s JSON output: %w", args[0], err)
	}
	return output, nil
}

func runCommand(ctx context.Context, args ...string) ([]byte, []byte, error) {
	cmd := exec.CommandContext(ctx, "litestream", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			err = errors.Join(cause, err)
		}
		return nil, nil, commandError(stdout.Bytes(), stderr.Bytes(), err)
	}
	return stdout.Bytes(), stderr.Bytes(), nil
}

func commandError(stdout, stderr []byte, err error) error {
	message := strings.TrimSpace(strings.Join([]string{string(stderr), string(stdout)}, "\n"))
	if message != "" {
		return fmt.Errorf("%s: %w", message, err)
	}
	return err
}

func countNoun(count int, singular, plural string) string {
	if count == 1 {
		return fmt.Sprintf("1 %s", singular)
	}
	return fmt.Sprintf("%d %s", count, plural)
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
