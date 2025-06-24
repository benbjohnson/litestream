package tools

import (
	"context"
	"os/exec"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

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
