package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/superfly/ltx"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/mock"
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

func TestMCPConfigDoesNotChangeDefaultLogger(t *testing.T) {
	fixture := newMCPTestFixture(t)
	data, err := os.ReadFile(fixture.configPath)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, []byte("logging:\n  level: DEBUG\n  type: json\n  stderr: true\n  source: true\n")...)
	if err := os.WriteFile(fixture.configPath, data, 0600); err != nil {
		t.Fatal(err)
	}

	previous := slog.Default()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(previous) })

	callMCPToolText(t, handlerFromMCPTool(DatabasesTool(fixture.configPath)), nil)
	if slog.Default() != logger {
		t.Fatal("MCP config load changed the default logger")
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

	for _, tt := range []struct {
		name      string
		timestamp string
	}{
		{name: "replica does not exist"},
		{name: "replica does not exist at timestamp", timestamp: "2026-07-16T12:00:00Z"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "restored.sqlite")
			text := callMCPToolText(t, handlerFromMCPTool(RestoreTool("")), map[string]any{
				"path":              "file://" + filepath.Join(t.TempDir(), "replica"),
				"output":            outputPath,
				"timestamp":         tt.timestamp,
				"if_replica_exists": true,
			})
			if text != "no matching backups found, skipping\n" {
				t.Fatalf("unexpected result: %q", text)
			}
			if _, err := os.Stat(outputPath); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("output exists after skipped restore: %v", err)
			}
		})
	}
}

func TestMCPReplicaURLSchemes(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "s3", url: "s3://bucket/path"},
		{name: "gs", url: "gs://bucket/path"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resources, err := loadMCPReplica(tt.url, filepath.Join(t.TempDir(), "missing.yml"))
			if err != nil {
				t.Fatal(err)
			}
			if got := resources.Replica.Client.Type(); got != tt.name {
				t.Fatalf("replica type=%q, want %q", got, tt.name)
			}
			if err := resources.Close(); err != nil {
				t.Fatal(err)
			}

			opt := litestream.NewRestoreOptions()
			opt.OutputPath = filepath.Join(t.TempDir(), "restored.sqlite")
			resources, err = loadMCPRestoreReplica(tt.url, filepath.Join(t.TempDir(), "missing.yml"), &opt)
			if err != nil {
				t.Fatal(err)
			}
			if got := resources.Replica.Client.Type(); got != tt.name {
				t.Fatalf("restore replica type=%q, want %q", got, tt.name)
			}
			if err := resources.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMCPRemoteReplicaLifecycle(t *testing.T) {
	operationErr := errors.New("operation failed")
	closeErr := errors.New("close failed")
	client := &mcpClosingReplicaClient{closeErr: closeErr}
	r := litestream.NewReplica(nil)
	r.Client = client

	err := closeMCPResources(operationErr, &mcpReplica{Replica: r})
	if !errors.Is(err, operationErr) {
		t.Fatalf("error does not include operation failure: %v", err)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("error does not include close failure: %v", err)
	}
	if client.closeN != 1 {
		t.Fatalf("close count=%d, want 1", client.closeN)
	}
}

func TestMCPReplicaClientCloseContract(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "abs", url: "abs://account@container/path"},
		{name: "file", url: "file:///tmp/replica"},
		{name: "gs", url: "gs://bucket/path"},
		{name: "nats", url: "nats://localhost/bucket"},
		{name: "oss", url: "oss://bucket/path"},
		{name: "s3", url: "s3://bucket/path"},
		{name: "sftp", url: "sftp://user@localhost/path"},
		{name: "webdav", url: "webdav://localhost/path"},
		{name: "webdavs", url: "webdavs://localhost/path"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := litestream.NewReplicaClientFromURL(tt.url)
			if err != nil {
				t.Fatal(err)
			}
			closer, ok := client.(litestream.ReplicaClientCloser)
			if !ok {
				t.Fatalf("%s replica client does not implement cleanup contract", client.Type())
			}
			if err := closer.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMCPReplicaClientCloseContractViolation(t *testing.T) {
	r := litestream.NewReplica(nil)
	r.Client = &mock.ReplicaClient{}

	err := closeMCPReplica(r)
	if !errors.Is(err, errMCPReplicaClientNotClosable) {
		t.Fatalf("error=%v, want %v", err, errMCPReplicaClientNotClosable)
	}
}

func TestRestoreTXID(t *testing.T) {
	t.Run("returns plan error", func(t *testing.T) {
		planErr := errors.New("list failed")
		client := &mock.ReplicaClient{
			LTXFilesFunc: func(context.Context, int, ltx.TXID, bool) (ltx.FileIterator, error) {
				return nil, planErr
			},
		}
		r := litestream.NewReplica(nil)
		r.Client = client
		opt := litestream.NewRestoreOptions()

		if _, err := (&RestoreCommand{}).restoreTXID(t.Context(), r, &opt); !errors.Is(err, planErr) {
			t.Fatalf("error=%v, want %v", err, planErr)
		}
	})

	t.Run("pins planned transaction", func(t *testing.T) {
		fixture := newMCPTestFixture(t)
		resources, err := loadMCPReplica("file://"+fixture.replicaPath, "")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := resources.Close(); err != nil {
				t.Error(err)
			}
		})
		opt := litestream.NewRestoreOptions()

		txID, err := (&RestoreCommand{}).restoreTXID(t.Context(), resources.Replica, &opt)
		if err != nil {
			t.Fatal(err)
		}
		if txID != "0000000000000001" {
			t.Fatalf("txid=%q, want 0000000000000001", txID)
		}
		if opt.TXID != 1 {
			t.Fatalf("option txid=%s, want 0000000000000001", opt.TXID)
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
	if _, err := os.Stat(ltxPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("local ltx state still exists: %v", err)
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

type mcpClosingReplicaClient struct {
	mock.ReplicaClient
	closeErr error
	closeN   int
}

func (c *mcpClosingReplicaClient) Close() error {
	c.closeN++
	return c.closeErr
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
