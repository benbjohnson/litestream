package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

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
	if _, ok := capabilities["logging"]; ok {
		t.Error("logging capability is present")
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
			outputProperties: []string{"reason", "result", "status"},
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
		if got, want := slices.Sorted(maps.Keys(properties)), slices.Sorted(maps.Keys(test.properties)); !reflect.DeepEqual(got, want) {
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
		if got := slices.Sorted(maps.Keys(outputProperties)); !reflect.DeepEqual(got, test.outputProperties) {
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

	rawResponse, _ := postMCPRequest(t, httpServer.URL, "tools/list", "2026-07-28", map[string]any{
		"_meta": map[string]any{
			mcp.MetaKeyProtocolVersion:    "2026-07-28",
			mcp.MetaKeyClientInfo:         map[string]any{"name": "test-client", "version": "v1.0.0"},
			mcp.MetaKeyClientCapabilities: map[string]any{},
		},
	})
	rawResult, ok := rawResponse["result"].(map[string]any)
	if !ok {
		t.Fatalf("raw tools/list result=%T, want map[string]any", rawResponse["result"])
	}
	if got := rawResult["resultType"]; got != "complete" {
		t.Fatalf("raw tools/list resultType=%v, want complete", got)
	}
	rawTools, ok := rawResult["tools"].([]any)
	if !ok {
		t.Fatalf("raw tools/list tools=%T, want []any", rawResult["tools"])
	}
	for _, value := range rawTools {
		tool, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("raw tool=%T, want map[string]any", value)
		}
		if tool["name"] != "litestream_restore" && tool["name"] != "litestream_reset" {
			continue
		}
		annotations, ok := tool["annotations"].(map[string]any)
		if !ok {
			t.Errorf("%s raw annotations=%T, want map[string]any", tool["name"], tool["annotations"])
			continue
		}
		if got, ok := annotations["readOnlyHint"]; !ok || got != false {
			t.Errorf("%s raw readOnlyHint=%v, want explicit false", tool["name"], got)
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

func TestMCPServerAbandonedSession(t *testing.T) {
	server, err := NewMCP(t.Context(), "/etc/litestream.yml")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	response, header := postMCPRequest(t, httpServer.URL, "initialize", "", map[string]any{
		"protocolVersion": "2025-11-25",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "test-client", "version": "v1.0.0"},
	})
	if _, ok := response["result"].(map[string]any); !ok {
		t.Fatalf("initialize result=%T, want map[string]any", response["result"])
	}
	if got := header.Get("Mcp-Session-Id"); got != "" {
		t.Fatalf("abandoned session ID=%q, want empty", got)
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
			wantArgs: "<restore><-json><-o><><-txid><><-timestamp><><-parallelism><><-if-db-not-exists=false><-if-replica-exists=true></data/db>\n",
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
	t.Setenv("LITESTREAM_CONFIG", "/effective/litestream.yml")
	if err := os.WriteFile(argsPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	result, err = session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      "litestream_info",
		Arguments: map[string]any{"config": ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("info tool error: %#v", result.Content)
	}
	if got, want := result.StructuredContent.(map[string]any)["config"], "/effective/litestream.yml"; got != want {
		t.Fatalf("info config=%v, want %q", got, want)
	}
	args, err = os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	wantArgs = "<databases><-json><-config></effective/litestream.yml>\n<ltx><-json><-config></effective/litestream.yml></data/db>\n"
	if got := string(args); got != wantArgs {
		t.Fatalf("info command arguments=%q, want %q", got, wantArgs)
	}
}

func TestMCPRestoreSkipReasons(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}

	installRestoreLitestream(t)
	_, handler := RestoreTool("")
	trueValue := true
	tests := []struct {
		name       string
		input      func(*testing.T) restoreInput
		wantReason RestoreReason
		wantText   func(restoreInput) string
	}{
		{
			name: "DatabaseExists",
			input: func(t *testing.T) restoreInput {
				outputPath := filepath.Join(t.TempDir(), "db")
				if err := os.WriteFile(outputPath, []byte("existing"), 0o600); err != nil {
					t.Fatal(err)
				}
				return restoreInput{
					Path:          "file://" + t.TempDir(),
					Output:        &outputPath,
					IfDBNotExists: &trueValue,
				}
			},
			wantReason: RestoreReasonDatabaseExists,
			wantText: func(input restoreInput) string {
				return fmt.Sprintf("Restore skipped because the database already exists at %s.", *input.Output)
			},
		},
		{
			name: "NoMatchingBackups",
			input: func(t *testing.T) restoreInput {
				outputPath := filepath.Join(t.TempDir(), "db")
				return restoreInput{
					Path:            "file://" + t.TempDir(),
					Output:          &outputPath,
					IfReplicaExists: &trueValue,
				}
			},
			wantReason: RestoreReasonNoMatchingBackups,
			wantText: func(input restoreInput) string {
				return fmt.Sprintf("Restore skipped for %s because no matching backups were found.", input.Path)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := tt.input(t)
			result, output, err := handler(t.Context(), nil, input)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := textContent(t, result), tt.wantText(input); got != want {
				t.Fatalf("text content=%q, want %q", got, want)
			}
			if output.Status != RestoreStatusSkipped {
				t.Fatalf("status=%q, want %q", output.Status, RestoreStatusSkipped)
			}
			if output.Reason != tt.wantReason {
				t.Fatalf("reason=%q, want %q", output.Reason, tt.wantReason)
			}
			if output.Result != nil {
				t.Fatalf("result=%v, want nil", output.Result)
			}
		})
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

func TestMCPToolContextCancellationAfterStart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}

	installFakeLitestream(t)
	readyPath := filepath.Join(t.TempDir(), "ready")
	t.Setenv("LITESTREAM_TEST_BLOCK", "ltx")
	t.Setenv("LITESTREAM_TEST_READY_FILE", readyPath)
	_, handler := LTXTool("/default/litestream.yml")
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	type response struct {
		result *mcp.CallToolResult
		err    error
	}
	responseCh := make(chan response, 1)
	go func() {
		result, _, err := handler(ctx, nil, ltxInput{Path: "/data/db"})
		responseCh <- response{result: result, err: err}
	}()

	waitForPath(t, readyPath)
	cancel()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case response := <-responseCh:
		if !errors.Is(response.err, context.Canceled) {
			t.Fatalf("error=%v, want context canceled", response.err)
		}
		var exitErr *exec.ExitError
		if !errors.As(response.err, &exitErr) {
			t.Fatalf("error=%v, want process exit error", response.err)
		}
		if response.result != nil {
			t.Fatalf("result=%v, want nil", response.result)
		}
	case <-timer.C:
		t.Fatal("timed out waiting for canceled tool call")
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
if [ "$LITESTREAM_TEST_BLOCK" = "$1" ]; then
	: > "$LITESTREAM_TEST_READY_FILE"
	exec sleep 60
fi
case "$1" in
  databases)
    printf '[{"path":"/data/db","replica":"s3"}]\n'
    ;;
  restore)
	printf '{"status":"restored","db_path":"/restore/db","replica":"file","txid":"0000000000000004","duration_ms":125,"integrity_check":"none"}\n'
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

func installRestoreLitestream(t *testing.T) {
	t.Helper()

	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "litestream")
	script := `#!/bin/sh
LITESTREAM_TEST_MCP_RESTORE=1 exec "$LITESTREAM_TEST_BINARY" -test.run=^TestMCPRestoreCommandHarness$ -- "$@"
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LITESTREAM_TEST_BINARY", testBinary)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestMCPRestoreCommandHarness(t *testing.T) {
	if os.Getenv("LITESTREAM_TEST_MCP_RESTORE") != "1" {
		t.Skip("helper process only")
	}

	separator := slices.Index(os.Args, "--")
	if separator == -1 {
		os.Exit(2)
	}
	if err := NewMain().Run(context.Background(), os.Args[separator+1:]); err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, err); writeErr != nil {
			os.Exit(2)
		}
		os.Exit(1)
	}
	os.Exit(0)
}

func waitForPath(t *testing.T, path string) {
	t.Helper()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}

		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", path)
		}
	}
}

func postMCPRequest(t *testing.T, endpoint, method, protocolVersion string, params map[string]any) (map[string]any, http.Header) {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	if protocolVersion != "" {
		request.Header.Set("Mcp-Protocol-Version", protocolVersion)
		request.Header.Set("Mcp-Method", method)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("MCP response status=%d, want %d: %s", response.StatusCode, http.StatusOK, data)
	}

	payload := bytes.TrimSpace(data)
	for line := range bytes.Lines(data) {
		if value, ok := bytes.CutPrefix(line, []byte("data: ")); ok {
			payload = bytes.TrimSpace(value)
			break
		}
	}
	var object map[string]any
	if err := json.Unmarshal(payload, &object); err != nil {
		t.Fatalf("decode MCP response %q: %v", data, err)
	}
	if rpcError := object["error"]; rpcError != nil {
		t.Fatalf("MCP response error=%v", rpcError)
	}
	return object, response.Header
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
