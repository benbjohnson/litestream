package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/benbjohnson/litestream"
)

func TestMCPReadToolsRunWithoutLitestreamOnPATH(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	fixture := newMCPTestFixture(t)

	tests := []struct {
		name      string
		handler   server.ToolHandlerFunc
		arguments map[string]any
		contains  []string
	}{
		{
			name:    "databases",
			handler: handlerFromMCPTool(DatabasesTool(fixture.configPath)),
			contains: []string{
				"path",
				"replica",
				fixture.dbPath,
				"file",
			},
		},
		{
			name:    "info",
			handler: handlerFromMCPTool(InfoTool(fixture.configPath)),
			contains: []string{
				"Litestream Status Report",
				fixture.configPath,
				fixture.dbPath,
				"0000000000000001",
			},
		},
		{
			name:    "ltx database path",
			handler: handlerFromMCPTool(LTXTool(fixture.configPath)),
			arguments: map[string]any{
				"path": fixture.dbPath,
			},
			contains: []string{
				"min_txid",
				"max_txid",
				"0000000000000001",
			},
		},
		{
			name:    "ltx replica URL",
			handler: handlerFromMCPTool(LTXTool("")),
			arguments: map[string]any{
				"path":   "file://" + fixture.replicaPath,
				"config": filepath.Join(t.TempDir(), "missing.yml"),
			},
			contains: []string{
				"min_txid",
				"max_txid",
				"0000000000000001",
			},
		},
		{
			name:    "status",
			handler: handlerFromMCPTool(StatusTool(fixture.configPath)),
			contains: []string{
				"local txid",
				"remote txid",
				fixture.dbPath,
				"ok",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			text := callMCPToolText(t, tt.handler, tt.arguments)
			for _, value := range tt.contains {
				if !strings.Contains(text, value) {
					t.Fatalf("result does not contain %q:\n%s", value, text)
				}
			}
		})
	}
}

func TestRestoreToolRunWithoutLitestreamOnPATH(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	tests := []struct {
		name      string
		arguments func(mcpTestFixture, string) map[string]any
	}{
		{
			name: "database path",
			arguments: func(fixture mcpTestFixture, outputPath string) map[string]any {
				return map[string]any{
					"path":   fixture.dbPath,
					"output": outputPath,
					"config": fixture.configPath,
				}
			},
		},
		{
			name: "replica URL",
			arguments: func(fixture mcpTestFixture, outputPath string) map[string]any {
				return map[string]any{
					"path":   "file://" + fixture.replicaPath,
					"output": outputPath,
					"config": filepath.Join(t.TempDir(), "missing.yml"),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newMCPTestFixture(t)
			outputPath := filepath.Join(t.TempDir(), "restored.sqlite")
			text := callMCPToolText(t, handlerFromMCPTool(RestoreTool("")), tt.arguments(fixture, outputPath))

			var result RestoreResult
			if err := json.Unmarshal([]byte(text), &result); err != nil {
				t.Fatalf("decode result: %v\n%s", err, text)
			}
			if result.DBPath != outputPath {
				t.Fatalf("db path=%q, want %q", result.DBPath, outputPath)
			}
			if result.Replica != "file" {
				t.Fatalf("replica=%q, want file", result.Replica)
			}
			if result.TXID == "" {
				t.Fatal("expected restored txid")
			}
			assertRestoreCommandDB(t, outputPath)
		})
	}
}

func TestRestoreToolConditionalBehavior(t *testing.T) {
	t.Run("database exists", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "existing.sqlite")
		if err := os.WriteFile(outputPath, []byte("existing"), 0600); err != nil {
			t.Fatal(err)
		}

		text := callMCPToolText(t, handlerFromMCPTool(RestoreTool("")), map[string]any{
			"path":             "file://" + filepath.Join(t.TempDir(), "replica"),
			"output":           outputPath,
			"timestamp":        "2026-07-16T12:00:00Z",
			"if_db_not_exists": true,
		})
		if text != "database already exists, skipping\n" {
			t.Fatalf("unexpected result: %q", text)
		}
	})

	t.Run("replica does not exist", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "restored.sqlite")
		text := callMCPToolText(t, handlerFromMCPTool(RestoreTool("")), map[string]any{
			"path":              "file://" + filepath.Join(t.TempDir(), "replica"),
			"output":            outputPath,
			"if_replica_exists": true,
		})
		if text != "no matching backups found, skipping\n" {
			t.Fatalf("unexpected result: %q", text)
		}
		if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
			t.Fatalf("output exists after skipped restore: %v", err)
		}
	})
}

func TestRestoreToolRejectsInvalidOptions(t *testing.T) {
	tests := []struct {
		name      string
		arguments map[string]any
		contains  string
	}{
		{
			name: "txid",
			arguments: map[string]any{
				"path": "file:///tmp/replica",
				"txid": "bad",
			},
			contains: "invalid txid",
		},
		{
			name: "timestamp",
			arguments: map[string]any{
				"path":      "file:///tmp/replica",
				"timestamp": "bad",
			},
			contains: "invalid timestamp",
		},
		{
			name: "parallelism",
			arguments: map[string]any{
				"path":        "file:///tmp/replica",
				"parallelism": "bad",
			},
			contains: "invalid parallelism",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callMCPToolResult(t, handlerFromMCPTool(RestoreTool("")), tt.arguments)
			if !result.IsError {
				t.Fatal("expected tool error")
			}
			text := mcpToolResultText(t, result)
			if !strings.Contains(text, tt.contains) {
				t.Fatalf("error does not contain %q: %s", tt.contains, text)
			}
		})
	}
}

func TestResetToolRunWithoutLitestreamOnPATH(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	fixture := newMCPTestFixture(t)
	ltxPath := filepath.Join(filepath.Dir(fixture.dbPath), "."+filepath.Base(fixture.dbPath)+litestream.MetaDirSuffix, "ltx")
	if _, err := os.Stat(ltxPath); err != nil {
		t.Fatalf("expected local ltx state: %v", err)
	}

	text := callMCPToolText(t, handlerFromMCPTool(ResetTool(fixture.configPath)), map[string]any{
		"path": fixture.dbPath,
	})
	if !strings.Contains(text, "Reset complete") {
		t.Fatalf("unexpected result: %q", text)
	}
	if _, err := os.Stat(ltxPath); !os.IsNotExist(err) {
		t.Fatalf("local ltx state still exists: %v", err)
	}
}

func TestVersionToolRunWithoutLitestreamOnPATH(t *testing.T) {
	const version = "v1.2.3-mcp-test"
	previous := Version
	Version = version
	t.Cleanup(func() { Version = previous })
	t.Setenv("PATH", t.TempDir())

	text := callMCPToolText(t, handlerFromMCPTool(VersionTool()), nil)
	if text != version+"\n" {
		t.Fatalf("version=%q, want %q", text, version+"\n")
	}
}

func TestRestoreToolSchema(t *testing.T) {
	tool, _ := RestoreTool("")
	if _, ok := tool.InputSchema.Properties["output"]; !ok {
		t.Fatal("expected output property")
	}
	if _, ok := tool.InputSchema.Properties["o"]; ok {
		t.Fatal("did not expect o property")
	}
}

type mcpTestFixture struct {
	dbPath      string
	replicaPath string
	configPath  string
}

func newMCPTestFixture(t *testing.T) mcpTestFixture {
	t.Helper()

	replicaPath, _ := createRestoreCommandTestData(t, t.Context())
	dbPath := filepath.Join(filepath.Dir(replicaPath), "db.sqlite")
	configPath := filepath.Join(t.TempDir(), "litestream.yml")
	config := fmt.Sprintf("dbs:\n  - path: %q\n    replica:\n      url: %q\n", dbPath, "file://"+replicaPath)
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	return mcpTestFixture{
		dbPath:      dbPath,
		replicaPath: replicaPath,
		configPath:  configPath,
	}
}

func handlerFromMCPTool(_ mcp.Tool, handler server.ToolHandlerFunc) server.ToolHandlerFunc {
	return handler
}

func callMCPToolText(t *testing.T, handler server.ToolHandlerFunc, arguments map[string]any) string {
	t.Helper()

	result := callMCPToolResult(t, handler, arguments)
	if result.IsError {
		t.Fatalf("unexpected tool error: %s", mcpToolResultText(t, result))
	}
	return mcpToolResultText(t, result)
}

func callMCPToolResult(t *testing.T, handler server.ToolHandlerFunc, arguments map[string]any) *mcp.CallToolResult {
	t.Helper()

	result, err := handler(t.Context(), mcp.CallToolRequest{
		Params: mcp.CallToolParams{Arguments: arguments},
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mcpToolResultText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()

	if len(result.Content) != 1 {
		t.Fatalf("content length=%d, want 1", len(result.Content))
	}
	content, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("content type=%T, want mcp.TextContent", result.Content[0])
	}
	return content.Text
}
