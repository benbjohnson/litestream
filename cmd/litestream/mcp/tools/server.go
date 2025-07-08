package tools

import "github.com/mark3labs/mcp-go/server"

func NewServer(configPath string) *server.MCPServer {
	server := server.NewMCPServer(
		"Litestream MCP Server",
		"1.0.0",
		server.WithToolCapabilities(false),
		server.WithRecovery(),
		server.WithLogging(),
	)
	// Add the tools to the server
	server.AddTool(InfoTool(configPath))
	server.AddTool(DatabasesTool(configPath))
	server.AddTool(GenerationsTool(configPath))
	server.AddTool(RestoreTool(configPath))
	server.AddTool(SnapshotsTool(configPath))
	return server
}
