package tools

import (
	"context"
	"os/exec"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func ReplicateTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_replicate",
		mcp.WithDescription("Start a server to monitor and replicate SQLite databases."),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
		mcp.WithString("exec", mcp.Description("Subcommand to execute. Optional.")),
		mcp.WithString("db_path", mcp.Description("Database path. Optional.")),
		mcp.WithString("replica_urls", mcp.Description("Comma-separated list of replica URLs. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := []string{"replicate"}
		config := configPath
		if configVal, err := req.RequireString("config"); err == nil {
			config = configVal
		}
		args = append(args, "-config", config)
		if execCmd, err := req.RequireString("exec"); err == nil {
			args = append(args, "-exec", execCmd)
		}
		if dbPath, err := req.RequireString("db_path"); err == nil {
			args = append(args, dbPath)
		}
		if replicaURLs, err := req.RequireString("replica_urls"); err == nil {
			args = append(args, strings.Split(replicaURLs, ",")...)
		}
		cmd := exec.CommandContext(ctx, "litestream", args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return mcp.NewToolResultError(strings.TrimSpace(string(output)) + ": " + err.Error()), nil
		}
		return mcp.NewToolResultText(string(output)), nil
	}
}
