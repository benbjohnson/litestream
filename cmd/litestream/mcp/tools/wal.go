package tools

import (
	"context"
	"os/exec"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func WALTool(configPath string) (mcp.Tool, server.ToolHandlerFunc) {
	tool := mcp.NewTool("litestream_wal",
		mcp.WithDescription("List all WAL files for a database or replica."),
		mcp.WithString("path", mcp.Required(), mcp.Description("Database path or replica URL.")),
		mcp.WithString("config", mcp.Description("Path to the Litestream config file. Optional.")),
		mcp.WithString("replica", mcp.Description("Replica name to filter by. Optional.")),
		mcp.WithString("generation", mcp.Description("Generation name to filter by. Optional.")),
	)

	return tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := []string{"wal"}
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
