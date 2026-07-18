package main

import (
	"bufio"
	"context"
	"log/slog"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/MadAppGang/httplog"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
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

// isReplicaURL returns true if the path looks like a replica URL (s3://, gs://, etc.)
// rather than a local database path. The CLI rejects -config when using replica URLs.
func isReplicaURL(path string) bool {
	return strings.Contains(path, "://")
}

func DatabasesTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_databases",
		mcp.WithDescription("List databases and their replicas as defined in the Litestream config file. The default path is /etc/litestream.yml but is not required."),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := []string{"databases"}
		config := req.GetString("config", configPath)
		if config != "" {
			args = append(args, "-config", config)
		}
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return mcp.NewToolResultError(strings.TrimSpace(string(output)) + ": " + err.Error()), nil
		}
		return mcp.NewToolResultText(string(output)), nil
	}
}

func InfoTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_info",
		mcp.WithDescription("Get a comprehensive summary of Litestream's current status including databases, LTX files, and version information."),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var summary strings.Builder
		summary.WriteString("=== Litestream Status Report ===\n\n")

		// Get version info
		versionCmd := exec.CommandContext(ctx, "litestream", "version")
		versionOutput, err := versionCmd.CombinedOutput()
		if err != nil {
			slog.Error("Failed to get version info", "error", err)
			return mcp.NewToolResultError("Failed to get version info: " + err.Error()), nil
		}
		summary.WriteString("Version Information:\n")
		summary.WriteString(string(versionOutput))
		summary.WriteString("\n")

		// Get databases info
		args := []string{"databases"}
		config := req.GetString("config", configPath)
		summary.WriteString("Current Config Path:\n")
		summary.WriteString(config + "\n\n")

		if config != "" {
			args = append(args, "-config", config)
		}
		dbCmd := exec.CommandContext(ctx, "litestream", args...)
		dbOutput, err := dbCmd.CombinedOutput()
		if err != nil {
			slog.Error("Failed to get databases info", "error", err)
			return mcp.NewToolResultError("Failed to get databases info: " + err.Error()), nil
		}

		summary.WriteString("Databases:\n")
		summary.WriteString(string(dbOutput))
		summary.WriteString("\n")

		// Parse database paths from output
		scanner := bufio.NewScanner(strings.NewReader(string(dbOutput)))
		// Skip header line
		scanner.Scan()
		var dbPaths []string
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) > 0 {
				dbPaths = append(dbPaths, fields[0])
			}
		}

		// Get LTX files info for each database
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
				summary.WriteString("Failed to get LTX files for " + dbPath + ": " + err.Error() + "\n")
				summary.WriteString(string(ltxOutput))
				continue
			}
			summary.WriteString("Database: " + dbPath + "\n")
			summary.WriteString(string(ltxOutput))
			summary.WriteString("\n")
		}

		return mcp.NewToolResultText(summary.String()), nil
	}
}

func RestoreTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_restore",
		mcp.WithDescription("Restore a database from a Litestream replica."),
		mcp.WithString("path", mcp.Required(), mcp.Description("Database path or replica URL.")),
		mcp.WithString("output", mcp.Description("Output path for the restored database. Required for replica URLs; optional for configured databases, where it defaults to the database path.")),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
		mcp.WithString("txid", mcp.Description("Restore up to a specific transaction ID. Optional.")),
		mcp.WithString("timestamp", mcp.Description("Restore to a specific point-in-time (RFC3339). Optional.")),
		mcp.WithString("parallelism", mcp.Description("Number of WAL files to download in parallel. Optional.")),
		mcp.WithBoolean("if_db_not_exists", mcp.Description("Skip restore if the database already exists. Optional.")),
		mcp.WithBoolean("if_replica_exists", mcp.Description("Skip restore if no backups are found. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		path, err := req.RequireString("path")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		args := []string{"restore"}
		outputPath := req.GetString("output", "")
		if outputPath == "" {
			outputPath = req.GetString("o", "")
		}
		if outputPath != "" {
			args = append(args, "-o", outputPath)
		}

		// Only add -config for database paths, not replica URLs
		// The CLI rejects -config when restoring from a replica URL
		if !isReplicaURL(path) {
			config := req.GetString("config", configPath)
			if config != "" {
				args = append(args, "-config", config)
			}
		}

		if txid := req.GetString("txid", ""); txid != "" {
			args = append(args, "-txid", txid)
		}
		if timestamp := req.GetString("timestamp", ""); timestamp != "" {
			args = append(args, "-timestamp", timestamp)
		}
		if parallelism := req.GetString("parallelism", ""); parallelism != "" {
			args = append(args, "-parallelism", parallelism)
		}
		if req.GetBool("if_db_not_exists", false) {
			args = append(args, "-if-db-not-exists")
		}
		if req.GetBool("if_replica_exists", false) {
			args = append(args, "-if-replica-exists")
		}
		if path != "" {
			args = append(args, path)
		}
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return mcp.NewToolResultError(strings.TrimSpace(string(output)) + ": " + err.Error()), nil
		}
		return mcp.NewToolResultText(string(output)), nil
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
		path, err := req.RequireString("path")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		args := []string{"ltx"}

		// Only add -config for database paths, not replica URLs
		// The CLI rejects -config when using a replica URL
		if !isReplicaURL(path) {
			config := req.GetString("config", configPath)
			if config != "" {
				args = append(args, "-config", config)
			}
		}

		if path != "" {
			args = append(args, path)
		}
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return mcp.NewToolResultError(strings.TrimSpace(string(output)) + ": " + err.Error()), nil
		}
		return mcp.NewToolResultText(string(output)), nil
	}
}

func StatusTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_status",
		mcp.WithDescription("Display replication status including database path, status, local transaction ID, and WAL size."),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
		mcp.WithString("path", mcp.Description("Filter to a specific database path. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := []string{"status"}
		config := req.GetString("config", configPath)
		if config != "" {
			args = append(args, "-config", config)
		}
		if path := req.GetString("path", ""); path != "" {
			args = append(args, path)
		}
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return mcp.NewToolResultError(strings.TrimSpace(string(output)) + ": " + err.Error()), nil
		}
		return mcp.NewToolResultText(string(output)), nil
	}
}

func ResetTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_reset",
		mcp.WithDescription("Clear local Litestream state for a database. Removes local LTX files, forcing fresh snapshot on next sync. Database file is not modified."),
		mcp.WithString("path", mcp.Required(), mcp.Description("Database path to reset.")),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		path, err := req.RequireString("path")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		args := []string{"reset"}
		config := req.GetString("config", configPath)
		if config != "" {
			args = append(args, "-config", config)
		}
		if path != "" {
			args = append(args, path)
		}
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return mcp.NewToolResultError(strings.TrimSpace(string(output)) + ": " + err.Error()), nil
		}
		return mcp.NewToolResultText(string(output)), nil
	}
}
