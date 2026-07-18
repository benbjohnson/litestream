package main

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"time"

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
			if result.IntegrityCheck != "none" {
				t.Fatalf("integrity check=%q, want none", result.IntegrityCheck)
			}
			assertRestoreCommandDB(t, outputPath)
		})
	}
}

func TestRestoreToolIntegrityCheck(t *testing.T) {
	fixture := newMCPTestFixture(t)
	corruptMCPTestFixture(t, fixture)

	t.Run("none", func(t *testing.T) {
		outputPath := filepath.Join(t.TempDir(), "restored.sqlite")
		text := callMCPToolText(t, handlerFromMCPTool(RestoreTool("")), map[string]any{
			"path":            "file://" + fixture.replicaPath,
			"output":          outputPath,
			"integrity_check": "none",
		})

		var result RestoreResult
		if err := json.Unmarshal([]byte(text), &result); err != nil {
			t.Fatalf("decode result: %v\n%s", err, text)
		}
		if result.IntegrityCheck != "none" {
			t.Fatalf("integrity check=%q, want none", result.IntegrityCheck)
		}
		if _, err := os.Stat(outputPath); err != nil {
			t.Fatalf("stat restored database: %v", err)
		}
	})

	for _, mode := range []string{"quick", "full"} {
		t.Run(mode, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "restored.sqlite")
			result := callMCPToolResult(t, handlerFromMCPTool(RestoreTool("")), map[string]any{
				"path":            "file://" + fixture.replicaPath,
				"output":          outputPath,
				"integrity_check": mode,
			})
			if !result.IsError {
				t.Fatal("expected tool error")
			}
			if text := mcpToolResultText(t, result); !strings.Contains(text, "post-restore integrity check") {
				t.Fatalf("unexpected error: %s", text)
			}
			for _, path := range []string{outputPath, outputPath + "-shm", outputPath + "-wal"} {
				if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("failed restore output exists at %q: %v", path, err)
				}
			}
		})
	}
}

func TestRestorePlanTool(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	fixture := newMCPTestFixture(t)
	baseTime := time.Now().Add(-3 * time.Hour).UTC().Truncate(time.Second)
	for txID := ltx.TXID(1); txID <= 3; txID++ {
		path := litestream.LTXFilePath(fixture.replicaPath, 0, txID, txID)
		if txID > 1 {
			if err := os.WriteFile(path, []byte("ltx"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		createdAt := baseTime.Add(time.Duration(txID-1) * time.Hour)
		if err := os.Chtimes(path, createdAt, createdAt); err != nil {
			t.Fatal(err)
		}
	}
	timestamp := baseTime.Add(90 * time.Minute).Format(time.RFC3339)

	tests := []struct {
		name          string
		arguments     map[string]any
		includeOutput bool
		fileN         int
	}{
		{
			name:          "database path with txid",
			includeOutput: true,
			fileN:         2,
			arguments: map[string]any{
				"path":   fixture.dbPath,
				"config": fixture.configPath,
				"txid":   "0000000000000002",
			},
		},
		{
			name:  "replica URL with timestamp",
			fileN: 2,
			arguments: map[string]any{
				"path":      "file://" + fixture.replicaPath,
				"config":    filepath.Join(t.TempDir(), "missing.yml"),
				"timestamp": timestamp,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "restored.sqlite")
			if tt.includeOutput {
				tt.arguments["output"] = outputPath
			}
			text := callMCPToolText(t, handlerFromMCPTool(RestorePlanTool("")), tt.arguments)
			wantSource := tt.arguments["path"].(string)

			var plan RestorePlan
			if err := json.Unmarshal([]byte(text), &plan); err != nil {
				t.Fatalf("decode result: %v\n%s", err, text)
			}
			if plan.Source != wantSource {
				t.Fatalf("source=%q, want %q", plan.Source, wantSource)
			}
			if tt.includeOutput && plan.TargetPath != outputPath {
				t.Fatalf("target path=%q, want %q", plan.TargetPath, outputPath)
			} else if !tt.includeOutput && plan.TargetPath != "" {
				t.Fatalf("target path=%q, want empty", plan.TargetPath)
			}
			if plan.Replica != "file" {
				t.Fatalf("replica=%q, want file", plan.Replica)
			}
			if plan.MinTXID != "0000000000000001" || plan.MaxTXID != "0000000000000002" {
				t.Fatalf("txid range=%s-%s, want 0000000000000001-0000000000000002", plan.MinTXID, plan.MaxTXID)
			}
			if len(plan.Files) != tt.fileN {
				t.Fatalf("files=%d, want %d", len(plan.Files), tt.fileN)
			}
			for i, file := range plan.Files {
				if file.Name == "" || file.MinTXID == "" || file.MaxTXID == "" {
					t.Fatalf("incomplete plan file at index %d: %#v", i, file)
				}
				if i > 0 && file.MaxTXID <= plan.Files[i-1].MaxTXID {
					t.Fatalf("restore plan is out of order at indexes %d and %d", i-1, i)
				}
			}
			if _, err := os.Stat(outputPath); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("restore plan created output: %v", err)
			}
		})
	}
}

func TestRestorePlanToolRejectsInvalidOptions(t *testing.T) {
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
			name: "txid and timestamp",
			arguments: map[string]any{
				"path":      "file:///tmp/replica",
				"txid":      "0000000000000001",
				"timestamp": "2026-07-17T12:00:00Z",
			},
			contains: "cannot specify both txid and timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := callMCPToolResult(t, handlerFromMCPTool(RestorePlanTool("")), tt.arguments)
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
		{
			name: "integrity check",
			arguments: map[string]any{
				"path":            "file:///tmp/replica",
				"integrity_check": "invalid",
			},
			contains: "invalid integrity_check",
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
	if _, ok := tool.InputSchema.Properties["integrity_check"]; !ok {
		t.Fatal("expected integrity_check property")
	}
}

func TestRestorePlanToolSchema(t *testing.T) {
	tool, _ := RestorePlanTool("")
	if tool.Annotations.ReadOnlyHint == nil || !*tool.Annotations.ReadOnlyHint {
		t.Fatal("expected read-only hint")
	}
	if tool.Annotations.DestructiveHint == nil || *tool.Annotations.DestructiveHint {
		t.Fatal("expected non-destructive hint")
	}
	for _, name := range []string{"path", "output", "config", "txid", "timestamp"} {
		if _, ok := tool.InputSchema.Properties[name]; !ok {
			t.Fatalf("expected %s property", name)
		}
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

func corruptMCPTestFixture(t *testing.T, fixture mcpTestFixture) {
	t.Helper()

	dbPath := filepath.Join(filepath.Dir(fixture.replicaPath), "db.sqlite")
	data, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 100 {
		t.Fatalf("database is too small: %d bytes", len(data))
	}

	pageSize := int(binary.BigEndian.Uint16(data[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	if pageSize == 0 || len(data)%pageSize != 0 || len(data) <= pageSize {
		t.Fatalf("invalid test database size=%d page_size=%d", len(data), pageSize)
	}
	for i := pageSize; i < len(data); i++ {
		data[i] = 0xde
	}

	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  uint32(pageSize),
		Commit:    uint32(len(data) / pageSize),
		MinTXID:   1,
		MaxTXID:   1,
		Timestamp: time.Now().UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(data); offset += pageSize {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: uint32(offset/pageSize + 1)}, data[offset:offset+pageSize]); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}

	path := litestream.LTXFilePath(fixture.replicaPath, 0, 1, 1)
	if err := os.WriteFile(path, buf.Bytes(), 0600); err != nil {
		t.Fatal(err)
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
