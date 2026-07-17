package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestMCPServerTools(t *testing.T) {
	const version = "v1.2.3-mcp-test"
	setVersion(t, version)

	server, err := NewMCP(t.Context(), "/etc/litestream.yml")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v1.0.0"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	})

	initializeResult := session.InitializeResult()
	if initializeResult == nil {
		t.Fatal("initialize result is nil")
	}
	if initializeResult.ServerInfo == nil {
		t.Fatal("server info is nil")
	}
	if got, want := initializeResult.ServerInfo.Name, "Litestream MCP Server"; got != want {
		t.Fatalf("server name=%q, want %q", got, want)
	}
	if got := initializeResult.ServerInfo.Version; got != version {
		t.Fatalf("server version=%q, want %q", got, version)
	}
	if initializeResult.Capabilities == nil {
		t.Fatal("server capabilities are nil")
	}
	capabilities := jsonObject(t, initializeResult.Capabilities)
	if _, ok := capabilities["logging"]; !ok {
		t.Error("logging capability is missing")
	}
	if initializeResult.Capabilities.Tools == nil {
		t.Fatal("tools capability is nil")
	}
	if initializeResult.Capabilities.Tools.ListChanged {
		t.Error("tools listChanged=true, want false")
	}

	result, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]struct {
		readOnly         bool
		destructive      bool
		properties       map[string]string
		required         []string
		outputProperties []string
		outputRequired   []string
	}{
		"litestream_databases": {
			readOnly:         true,
			properties:       map[string]string{"config": "string"},
			outputProperties: []string{"databases"},
			outputRequired:   []string{"databases"},
		},
		"litestream_info": {
			readOnly:         true,
			properties:       map[string]string{"config": "string"},
			outputProperties: []string{"config", "databases", "version"},
			outputRequired:   []string{"config", "databases", "version"},
		},
		"litestream_restore": {
			destructive: true,
			properties: map[string]string{
				"config":            "string",
				"if_db_not_exists":  "boolean",
				"if_replica_exists": "boolean",
				"o":                 "string",
				"parallelism":       "string",
				"path":              "string",
				"timestamp":         "string",
				"txid":              "string",
			},
			required:         []string{"path"},
			outputProperties: []string{"result", "status"},
			outputRequired:   []string{"status"},
		},
		"litestream_ltx": {
			readOnly:         true,
			properties:       map[string]string{"config": "string", "path": "string"},
			required:         []string{"path"},
			outputProperties: []string{"files"},
			outputRequired:   []string{"files"},
		},
		"litestream_version": {
			readOnly:         true,
			outputProperties: []string{"version"},
			outputRequired:   []string{"version"},
		},
		"litestream_status": {
			readOnly:         true,
			properties:       map[string]string{"config": "string", "path": "string"},
			outputProperties: []string{"databases"},
			outputRequired:   []string{"databases"},
		},
		"litestream_reset": {
			destructive:      true,
			properties:       map[string]string{"config": "string", "path": "string"},
			required:         []string{"path"},
			outputProperties: []string{"path", "status"},
			outputRequired:   []string{"path", "status"},
		},
	}

	if got, want := len(result.Tools), len(tests); got != want {
		t.Fatalf("tool count=%d, want %d", got, want)
	}
	for _, tool := range result.Tools {
		test, ok := tests[tool.Name]
		if !ok {
			t.Errorf("unexpected tool %q", tool.Name)
			continue
		}
		if tool.Annotations == nil {
			t.Errorf("%s annotations are nil", tool.Name)
			continue
		}
		if got := tool.Annotations.ReadOnlyHint; got != test.readOnly {
			t.Errorf("%s readOnlyHint=%t, want %t", tool.Name, got, test.readOnly)
		}
		if test.destructive {
			if tool.Annotations.DestructiveHint == nil || !*tool.Annotations.DestructiveHint {
				t.Errorf("%s destructiveHint=%v, want true", tool.Name, tool.Annotations.DestructiveHint)
			}
		} else if tool.Annotations.DestructiveHint != nil {
			t.Errorf("%s destructiveHint=%v, want nil", tool.Name, *tool.Annotations.DestructiveHint)
		}

		annotations := jsonObject(t, tool.Annotations)
		if got, ok := annotations["readOnlyHint"]; !ok || got != test.readOnly {
			t.Errorf("%s serialized readOnlyHint=%v, want %t", tool.Name, got, test.readOnly)
		}

		inputSchema := jsonObject(t, tool.InputSchema)
		properties := schemaProperties(t, inputSchema)
		if got, want := sortedMapKeys(properties), sortedMapKeys(test.properties); !reflect.DeepEqual(got, want) {
			t.Errorf("%s input properties=%v, want %v", tool.Name, got, want)
		}
		for name, property := range properties {
			propertyObject := jsonObject(t, property)
			if propertyObject["description"] == "" {
				t.Errorf("%s input property %s has no description", tool.Name, name)
			}
			if got, want := propertyObject["type"], test.properties[name]; got != want {
				t.Errorf("%s input property %s type=%v, want %s", tool.Name, name, got, want)
			}
		}
		if got := schemaRequired(inputSchema); !reflect.DeepEqual(got, test.required) {
			t.Errorf("%s required properties=%v, want %v", tool.Name, got, test.required)
		}

		outputSchema := jsonObject(t, tool.OutputSchema)
		outputProperties := schemaProperties(t, outputSchema)
		if got := sortedMapKeys(outputProperties); !reflect.DeepEqual(got, test.outputProperties) {
			t.Errorf("%s output properties=%v, want %v", tool.Name, got, test.outputProperties)
		}
		for name, property := range outputProperties {
			if property := jsonObject(t, property); property["description"] == "" {
				t.Errorf("%s output property %s has no description", tool.Name, name)
			}
		}
		if got, want := schemaRequired(outputSchema), test.outputRequired; !reflect.DeepEqual(got, want) {
			t.Errorf("%s required output properties=%v, want %v", tool.Name, got, want)
		}
	}

	callResult, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "litestream_version",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if callResult.IsError {
		t.Fatalf("version tool error: %#v", callResult.Content)
	}
	if got, want := callResult.StructuredContent, map[string]any{"version": version}; !reflect.DeepEqual(got, want) {
		t.Fatalf("version structured content=%v, want %v", got, want)
	}
	if got, want := textContent(t, callResult), "Litestream "+version+"."; got != want {
		t.Fatalf("version text content=%q, want %q", got, want)
	}
}

func TestMCPToolBehavior(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}

	const version = "v1.2.3-mcp-test"
	setVersion(t, version)
	argsPath := installFakeLitestream(t)

	server, err := NewMCP(t.Context(), "/default/litestream.yml")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v1.0.0"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	})

	tests := []struct {
		name           string
		arguments      map[string]any
		wantText       string
		wantStructured any
		wantArgs       string
	}{
		{
			name:      "litestream_databases",
			arguments: map[string]any{"config": ""},
			wantText:  "Found 1 configured database.",
			wantStructured: map[string]any{
				"databases": []any{map[string]any{"path": "/data/db", "replica": "s3"}},
			},
			wantArgs: "<databases><-json><-config><>\n",
		},
		{
			name: "litestream_restore",
			arguments: map[string]any{
				"path":              "/data/db",
				"o":                 "",
				"config":            "",
				"txid":              "",
				"timestamp":         "",
				"parallelism":       "",
				"if_db_not_exists":  false,
				"if_replica_exists": true,
			},
			wantText: "Restored /restore/db using the file replica at TXID 0000000000000004.",
			wantStructured: map[string]any{
				"status": "restored",
				"result": map[string]any{
					"db_path":         "/restore/db",
					"replica":         "file",
					"txid":            "0000000000000004",
					"duration_ms":     float64(125),
					"integrity_check": "none",
				},
			},
			wantArgs: "<restore><-json><-o><><-txid><><-timestamp><><-parallelism><><-if-db-not-exists><false><-if-replica-exists><true></data/db>\n",
		},
		{
			name:      "litestream_ltx",
			arguments: map[string]any{"path": "/data/db", "config": ""},
			wantText:  "Found 1 LTX file for /data/db.",
			wantStructured: map[string]any{
				"files": []any{map[string]any{
					"level":     float64(0),
					"min_txid":  "0000000000000001",
					"max_txid":  "0000000000000004",
					"size":      float64(8192),
					"timestamp": "2026-04-24T12:00:00Z",
				}},
			},
			wantArgs: "<ltx><-json></data/db>\n",
		},
		{
			name:      "litestream_status",
			arguments: map[string]any{"config": "", "path": ""},
			wantText:  "Reported replication status for 1 database.",
			wantStructured: map[string]any{
				"databases": []any{map[string]any{
					"database":   "/data/db",
					"status":     "ok",
					"local_txid": "0000000000000004",
					"wal_size":   "32 kB",
				}},
			},
			wantArgs: "<status><-json><-config><><>\n",
		},
		{
			name:      "litestream_reset",
			arguments: map[string]any{"path": "/data/db", "config": ""},
			wantText:  "Reset command completed for /data/db.",
			wantStructured: map[string]any{
				"path":   "/data/db",
				"status": "completed",
			},
			wantArgs: "<reset><-config><></data/db>\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := os.WriteFile(argsPath, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: test.name, Arguments: test.arguments})
			if err != nil {
				t.Fatal(err)
			}
			if result.IsError {
				t.Fatalf("tool error: %#v", result.Content)
			}
			if got := textContent(t, result); got != test.wantText {
				t.Fatalf("text content=%q, want %q", got, test.wantText)
			}
			if got := result.StructuredContent; !reflect.DeepEqual(got, test.wantStructured) {
				t.Fatalf("structured content=%v, want %v", got, test.wantStructured)
			}
			args, err := os.ReadFile(argsPath)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(args); got != test.wantArgs {
				t.Fatalf("command arguments=%q, want %q", got, test.wantArgs)
			}
		})
	}

	if err := os.WriteFile(argsPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "litestream_info",
		Arguments: map[string]any{"config": "/custom/litestream.yml"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("info tool error: %#v", result.Content)
	}
	if got, want := textContent(t, result), "Litestream "+version+" has 1 configured database and 1 LTX file."; got != want {
		t.Fatalf("info text content=%q, want %q", got, want)
	}
	wantInfo := map[string]any{
		"version": version,
		"config":  "/custom/litestream.yml",
		"databases": []any{map[string]any{
			"path":    "/data/db",
			"replica": "s3",
			"ltx_files": []any{map[string]any{
				"level":     float64(0),
				"min_txid":  "0000000000000001",
				"max_txid":  "0000000000000004",
				"size":      float64(8192),
				"timestamp": "2026-04-24T12:00:00Z",
			}},
		}},
	}
	if got := result.StructuredContent; !reflect.DeepEqual(got, wantInfo) {
		t.Fatalf("info structured content=%v, want %v", got, wantInfo)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	wantArgs := "<databases><-json><-config></custom/litestream.yml>\n<ltx><-json><-config></custom/litestream.yml></data/db>\n"
	if got := string(args); got != wantArgs {
		t.Fatalf("info command arguments=%q, want %q", got, wantArgs)
	}

	t.Setenv("LITESTREAM_TEST_FAIL", "ltx")
	result, err = session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "litestream_info",
		Arguments: map[string]any{"config": "/custom/litestream.yml"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsError {
		t.Fatal("info tool succeeded, want error")
	}
	for _, want := range []string{"get LTX files for /data/db", "failure from ltx"} {
		if got := textContent(t, result); !strings.Contains(got, want) {
			t.Errorf("info error does not contain %q: %q", want, got)
		}
	}

	t.Setenv("LITESTREAM_TEST_FAIL", "")
	t.Setenv("LITESTREAM_TEST_SKIP", "restore")
	result, err = session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "litestream_restore",
		Arguments: map[string]any{"path": "/data/db"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("skipped restore tool error: %#v", result.Content)
	}
	if got, want := textContent(t, result), "Restore skipped for /data/db."; got != want {
		t.Fatalf("skipped restore text content=%q, want %q", got, want)
	}
	if got, want := result.StructuredContent, map[string]any{"status": "skipped"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("skipped restore structured content=%v, want %v", got, want)
	}
}

func TestMCPToolContextCancellation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}

	installFakeLitestream(t)
	_, handler := LTXTool("/default/litestream.yml")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, _, err := handler(ctx, nil, ltxInput{Path: "/data/db"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want context canceled", err)
	}
	if result != nil {
		t.Fatalf("result=%v, want nil", result)
	}
}

func TestMCPRecoveryMiddleware(t *testing.T) {
	handler := recoveryMiddleware(func(context.Context, string, mcp.Request) (mcp.Result, error) {
		panic("test panic")
	})

	result, err := handler(t.Context(), "tools/call", nil)
	if result != nil {
		t.Fatalf("result=%v, want nil", result)
	}
	if err == nil || !strings.Contains(err.Error(), "panic recovered in tools/call handler: test panic") {
		t.Fatalf("error=%v, want recovered panic", err)
	}
}

func jsonObject(t *testing.T, value any) map[string]any {
	t.Helper()

	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatal(err)
	}
	return object
}

func schemaProperties(t *testing.T, schema map[string]any) map[string]any {
	t.Helper()

	value, ok := schema["properties"]
	if !ok {
		return map[string]any{}
	}
	properties, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("schema properties type=%T, want map[string]any", value)
	}
	return properties
}

func schemaRequired(schema map[string]any) []string {
	values, ok := schema["required"].([]any)
	if !ok {
		return nil
	}
	required := make([]string, 0, len(values))
	for _, value := range values {
		required = append(required, value.(string))
	}
	slices.Sort(required)
	return required
}

func sortedMapKeys[V any](values map[string]V) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func installFakeLitestream(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "litestream")
	argsPath := filepath.Join(dir, "args")
	script := `#!/bin/sh
if [ -n "$LITESTREAM_TEST_ARGS_FILE" ]; then
  for argument in "$@"; do
    printf '<%s>' "$argument" >> "$LITESTREAM_TEST_ARGS_FILE"
  done
  printf '\n' >> "$LITESTREAM_TEST_ARGS_FILE"
fi
if [ "$LITESTREAM_TEST_FAIL" = "$1" ]; then
	printf 'failure from %s\n' "$1" >&2
	exit 1
fi
if [ "$LITESTREAM_TEST_SKIP" = "$1" ]; then
	exit 0
fi
case "$1" in
  databases)
    printf '[{"path":"/data/db","replica":"s3"}]\n'
    ;;
  restore)
    printf '{"db_path":"/restore/db","replica":"file","txid":"0000000000000004","duration_ms":125,"integrity_check":"none"}\n'
    ;;
  ltx)
    printf '[{"level":0,"min_txid":"0000000000000001","max_txid":"0000000000000004","size":8192,"timestamp":"2026-04-24T12:00:00Z"}]\n'
    ;;
  status)
    printf '[{"database":"/data/db","status":"ok","local_txid":"0000000000000004","wal_size":"32 kB"}]\n'
    ;;
  reset)
    printf 'Reset complete.\n'
    ;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LITESTREAM_TEST_ARGS_FILE", argsPath)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return argsPath
}

func textContent(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()

	if len(result.Content) != 1 {
		t.Fatalf("content length=%d, want 1", len(result.Content))
	}
	content, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content type=%T, want *mcp.TextContent", result.Content[0])
	}
	return content.Text
}

func setVersion(t *testing.T, version string) {
	t.Helper()

	previous := Version
	Version = version
	t.Cleanup(func() { Version = previous })
}
