package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func TestMain(m *testing.M) {
	if filepath.Base(os.Args[0]) == "litestream" && os.Getenv("LITESTREAM_MCP_TEST") == "1" {
		os.Exit(runMCPTestCommand())
	}
	os.Exit(m.Run())
}

func mcpTestExecutableName(goos string) string {
	if goos == "windows" {
		return "litestream.exe"
	}
	return "litestream"
}

func TestMCPTestExecutableName(t *testing.T) {
	tests := []struct {
		goos     string
		expected string
	}{
		{goos: "linux", expected: "litestream"},
		{goos: "darwin", expected: "litestream"},
		{goos: "windows", expected: "litestream.exe"},
	}

	for _, tt := range tests {
		t.Run(tt.goos, func(t *testing.T) {
			if got := mcpTestExecutableName(tt.goos); got != tt.expected {
				t.Fatalf("unexpected executable name: %q", got)
			}
		})
	}
}

func TestMCPToolsOmitEmptyConfig(t *testing.T) {
	tests := []struct {
		name        string
		handler     server.ToolHandlerFunc
		arguments   map[string]any
		invocations []string
	}{
		{
			name:        "databases",
			handler:     handlerFromTool(DatabasesTool("")),
			invocations: []string{"databases"},
		},
		{
			name:        "info",
			handler:     handlerFromTool(InfoTool("")),
			invocations: []string{"version", "databases"},
		},
		{
			name:        "restore",
			handler:     handlerFromTool(RestoreTool("")),
			arguments:   map[string]any{"path": "/tmp/db"},
			invocations: []string{"restore\x1f/tmp/db"},
		},
		{
			name:        "ltx",
			handler:     handlerFromTool(LTXTool("")),
			arguments:   map[string]any{"path": "/tmp/db"},
			invocations: []string{"ltx\x1f/tmp/db"},
		},
		{
			name:        "status",
			handler:     handlerFromTool(StatusTool("")),
			invocations: []string{"status"},
		},
		{
			name:        "reset",
			handler:     handlerFromTool(ResetTool("")),
			arguments:   map[string]any{"path": "/tmp/db"},
			invocations: []string{"reset\x1f/tmp/db"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			argsPath := useMCPTestCommand(t)
			callMCPTool(t, tt.handler, tt.arguments)
			assertMCPInvocations(t, argsPath, tt.invocations)
		})
	}
}

func TestRestoreToolArguments(t *testing.T) {
	tests := []struct {
		name       string
		configPath string
		arguments  map[string]any
		invocation string
	}{
		{
			name:       "all optional arguments",
			configPath: "/default.yml",
			arguments: map[string]any{
				"path":              "/tmp/db",
				"output":            "/tmp/restored.db",
				"config":            "/custom.yml",
				"txid":              "0000000000000001",
				"timestamp":         "2026-07-16T12:00:00Z",
				"parallelism":       "4",
				"if_db_not_exists":  true,
				"if_replica_exists": true,
			},
			invocation: strings.Join([]string{
				"restore",
				"-o", "/tmp/restored.db",
				"-config", "/custom.yml",
				"-txid", "0000000000000001",
				"-timestamp", "2026-07-16T12:00:00Z",
				"-parallelism", "4",
				"-if-db-not-exists",
				"-if-replica-exists",
				"/tmp/db",
			}, "\x1f"),
		},
		{
			name:       "default values",
			configPath: "/default.yml",
			arguments: map[string]any{
				"path":              "/tmp/db",
				"if_db_not_exists":  false,
				"if_replica_exists": false,
			},
			invocation: "restore\x1f-config\x1f/default.yml\x1f/tmp/db",
		},
		{
			name: "legacy output alias",
			arguments: map[string]any{
				"path": "/tmp/db",
				"o":    "/tmp/restored.db",
			},
			invocation: "restore\x1f-o\x1f/tmp/restored.db\x1f/tmp/db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			argsPath := useMCPTestCommand(t)
			_, handler := RestoreTool(tt.configPath)
			callMCPTool(t, handler, tt.arguments)
			assertMCPInvocations(t, argsPath, []string{tt.invocation})
		})
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

	descriptions := map[string]string{
		"if_db_not_exists":  "Skip restore if the database already exists. Optional.",
		"if_replica_exists": "Skip restore if no backups are found. Optional.",
	}
	for name, expected := range descriptions {
		property, ok := tool.InputSchema.Properties[name].(map[string]any)
		if !ok {
			t.Fatalf("expected %s property schema", name)
		}
		if got := property["description"]; got != expected {
			t.Fatalf("unexpected %s description: %q", name, got)
		}
	}
}

func handlerFromTool(_ mcp.Tool, handler server.ToolHandlerFunc) server.ToolHandlerFunc {
	return handler
}

func callMCPTool(t *testing.T, handler server.ToolHandlerFunc, arguments map[string]any) {
	t.Helper()

	result, err := handler(context.Background(), mcp.CallToolRequest{
		Params: mcp.CallToolParams{Arguments: arguments},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %#v", result.Content)
	}
}

func useMCPTestCommand(t *testing.T) string {
	t.Helper()

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	commandPath := filepath.Join(dir, mcpTestExecutableName(runtime.GOOS))
	var linkErr error
	if runtime.GOOS == "windows" {
		linkErr = os.Link(executable, commandPath)
	} else {
		linkErr = os.Symlink(executable, commandPath)
	}
	if linkErr != nil {
		t.Fatal(linkErr)
	}
	argsPath := filepath.Join(dir, "args")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("LITESTREAM_MCP_TEST", "1")
	t.Setenv("LITESTREAM_MCP_ARGS_PATH", argsPath)
	return argsPath
}

func runMCPTestCommand() int {
	f, err := os.OpenFile(os.Getenv("LITESTREAM_MCP_ARGS_PATH"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if _, err := fmt.Fprintln(f, strings.Join(os.Args[1:], "\x1f")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := f.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if len(os.Args) > 1 && os.Args[1] == "databases" {
		fmt.Println("path\treplicas")
	} else {
		fmt.Println("ok")
	}
	return 0
}

func assertMCPInvocations(t *testing.T, argsPath string, expected []string) {
	t.Helper()

	b, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	actual := strings.Split(strings.TrimSpace(string(b)), "\n")
	if got, want := strings.Join(actual, "\n"), strings.Join(expected, "\n"); got != want {
		t.Fatalf("unexpected invocations:\n%s\n\nexpected:\n%s", got, want)
	}
}
