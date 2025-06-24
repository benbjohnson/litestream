package tools

import (
	"context"
	"os/exec"
	"strconv"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

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
