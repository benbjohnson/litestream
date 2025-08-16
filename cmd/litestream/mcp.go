package main

import (
	"bufio"
	"context"
	"log/slog"
	"net/http"
	"os/exec"
	"strconv"
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
	mcpServer.AddTool(GenerationsTool(configPath))
	mcpServer.AddTool(RestoreTool(configPath))
	mcpServer.AddTool(SnapshotsTool(configPath))
	mcpServer.AddTool(LTXTool(configPath))

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
		args := []string{"databases"}
		config := configPath
		if configVal, err := req.RequireString("config"); err == nil {
			config = configVal
		}
		args = append(args, "-config", config)
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return mcp.NewToolResultError(strings.TrimSpace(string(output)) + ": " + err.Error()), nil
		}
		return mcp.NewToolResultText(string(output)), nil
	}
}

func GenerationsTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_generations",
		mcp.WithDescription("List all generations for a database or replica."),
		mcp.WithString("path", mcp.Required(), mcp.Description("Database path or replica URL.")),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
		mcp.WithString("replica", mcp.Description("Replica name to filter by. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := []string{"generations"}
		config := configPath
		if configVal, err := req.RequireString("config"); err == nil {
			config = configVal
		}
		args = append(args, "-config", config)
		if replica, err := req.RequireString("replica"); err == nil {
			args = append(args, "-replica", replica)
		}
		if path, err := req.RequireString("path"); err == nil {
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

func InfoTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_info",
		mcp.WithDescription("Get a comprehensive summary of Litestream's current status including databases, generations, snapshots, and version information."),
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
		config := configPath
		if configVal, err := req.RequireString("config"); err == nil {
			config = configVal
		}
		summary.WriteString("Current Config Path:\n")
		summary.WriteString(config + "\n\n")

		args = append(args, "-config", config)
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

		// Get generations info for each database
		summary.WriteString("Generations:\n")
		for _, dbPath := range dbPaths {
			genArgs := []string{"generations"}
			if config != "" {
				genArgs = append(genArgs, "-config", config)
			}
			genArgs = append(genArgs, dbPath)
			genCmd := exec.CommandContext(ctx, "litestream", genArgs...)
			genOutput, err := genCmd.CombinedOutput()
			if err != nil {
				summary.WriteString("Failed to get generations for " + dbPath + ": " + err.Error() + "\n")
				summary.WriteString(string(genOutput))
				continue
			}
			summary.WriteString("Database: " + dbPath + "\n")
			summary.WriteString(string(genOutput))
			summary.WriteString("\n")
		}

		// Get snapshots info for each database
		summary.WriteString("Snapshots:\n")
		for _, dbPath := range dbPaths {
			snapArgs := []string{"snapshots"}
			if config != "" {
				snapArgs = append(snapArgs, "-config", config)
			}
			snapArgs = append(snapArgs, dbPath)
			snapCmd := exec.CommandContext(ctx, "litestream", snapArgs...)
			snapOutput, err := snapCmd.CombinedOutput()
			if err != nil {
				summary.WriteString("Failed to get snapshots for " + dbPath + ": " + err.Error() + "\n")
				summary.WriteString(string(snapOutput))
				continue
			}
			summary.WriteString("Database: " + dbPath + "\n")
			summary.WriteString(string(snapOutput))
			summary.WriteString("\n")
		}

		return mcp.NewToolResultText(summary.String()), nil
	}
}

func RestoreTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_restore",
		mcp.WithDescription("Restore a database from a Litestream replica."),
		mcp.WithString("path", mcp.Required(), mcp.Description("Database path or replica URL.")),
		mcp.WithString("o", mcp.Description("Output path for the restored database. Optional.")),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
		mcp.WithString("replica", mcp.Description("Replica name to restore from. Optional.")),
		mcp.WithString("generation", mcp.Description("Generation name to restore from. Optional.")),
		mcp.WithString("index", mcp.Description("Restore up to a specific WAL index. Optional.")),
		mcp.WithString("timestamp", mcp.Description("Restore to a specific point-in-time (RFC3339). Optional.")),
		mcp.WithString("parallelism", mcp.Description("Number of WAL files to download in parallel. Optional.")),
		mcp.WithBoolean("if_db_not_exists", mcp.Description("Return 0 if the database already exists. Optional.")),
		mcp.WithBoolean("if_replica_exists", mcp.Description("Return 0 if no backups found. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := []string{"restore"}
		if o, err := req.RequireString("o"); err == nil {
			args = append(args, "-o", o)
		}
		config := configPath
		if configVal, err := req.RequireString("config"); err == nil {
			config = configVal
		}
		args = append(args, "-config", config)
		if replica, err := req.RequireString("replica"); err == nil {
			args = append(args, "-replica", replica)
		}
		if generation, err := req.RequireString("generation"); err == nil {
			args = append(args, "-generation", generation)
		}
		if index, err := req.RequireString("index"); err == nil {
			args = append(args, "-index", index)
		}
		if timestamp, err := req.RequireString("timestamp"); err == nil {
			args = append(args, "-timestamp", timestamp)
		}
		if parallelism, err := req.RequireString("parallelism"); err == nil {
			args = append(args, "-parallelism", parallelism)
		}
		if ifDBNotExists, err := req.RequireBool("if_db_not_exists"); err == nil {
			args = append(args, "-if-db-not-exists", strconv.FormatBool(ifDBNotExists))
		}
		if ifReplicaExists, err := req.RequireBool("if_replica_exists"); err == nil {
			args = append(args, "-if-replica-exists", strconv.FormatBool(ifReplicaExists))
		}
		if path, err := req.RequireString("path"); err == nil {
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

func SnapshotsTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_snapshots",
		mcp.WithDescription("List all snapshots for a database or replica."),
		mcp.WithString("path", mcp.Required(), mcp.Description("Database path or replica URL.")),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
		mcp.WithString("replica", mcp.Description("Replica name to filter by. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := []string{"snapshots"}
		config := configPath
		if configVal, err := req.RequireString("config"); err == nil {
			config = configVal
		}
		args = append(args, "-config", config)
		if replica, err := req.RequireString("replica"); err == nil {
			args = append(args, "-replica", replica)
		}
		if path, err := req.RequireString("path"); err == nil {
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
		mcp.WithDescription("List all LTX files for a database or replica."),
		mcp.WithString("path", mcp.Required(), mcp.Description("Database path or replica URL.")),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
		mcp.WithString("replica", mcp.Description("Replica name to filter by. Optional.")),
		mcp.WithString("generation", mcp.Description("Generation name to filter by. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := []string{"ltx"}
		config := configPath
		if configVal, err := req.RequireString("config"); err == nil {
			config = configVal
		}
		args = append(args, "-config", config)
		if replica, err := req.RequireString("replica"); err == nil {
			args = append(args, "-replica", replica)
		}
		if generation, err := req.RequireString("generation"); err == nil {
			args = append(args, "-generation", generation)
		}
		if path, err := req.RequireString("path"); err == nil {
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
