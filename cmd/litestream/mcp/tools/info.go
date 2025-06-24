package tools

import (
	"bufio"
	"context"
	"log/slog"
	"os/exec"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

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
