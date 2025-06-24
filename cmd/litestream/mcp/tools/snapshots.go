package tools

import (
	"context"
	"os/exec"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

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
