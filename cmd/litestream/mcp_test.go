package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/file"
	"github.com/benbjohnson/litestream/internal/testingutil"
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
		readOnly    bool
		destructive bool
		properties  map[string]string
		required    []string
	}{
		"litestream_databases": {readOnly: true, properties: map[string]string{"config": "string"}},
		"litestream_info":      {readOnly: true, properties: map[string]string{"config": "string"}},
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
			required: []string{"path"},
		},
		"litestream_ltx":     {readOnly: true, properties: map[string]string{"config": "string", "path": "string"}, required: []string{"path"}},
		"litestream_version": {readOnly: true},
		"litestream_status":  {readOnly: true, properties: map[string]string{"config": "string", "path": "string"}},
		"litestream_reset":   {destructive: true, properties: map[string]string{"config": "string", "path": "string"}, required: []string{"path"}},
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
		if got := sortedMapKeys(outputProperties); !reflect.DeepEqual(got, []string{"text"}) {
			t.Errorf("%s output properties=%v, want [text]", tool.Name, got)
		}
		if property := jsonObject(t, outputProperties["text"]); property["description"] == "" {
			t.Errorf("%s output property text has no description", tool.Name)
		} else if got := property["type"]; got != "string" {
			t.Errorf("%s output property text type=%v, want string", tool.Name, got)
		}
		if got, want := schemaRequired(outputSchema), []string{"text"}; !reflect.DeepEqual(got, want) {
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
	if got, want := callResult.StructuredContent, map[string]any{"text": version + "\n"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("version structured content=%v, want %v", got, want)
	}
	if got, want := textContent(t, callResult), version+"\n"; got != want {
		t.Fatalf("version text content=%q, want %q", got, want)
	}
}

func TestMCPToolBehavior(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}

	setVersion(t, "v1.2.3-mcp-test")
	installFakeLitestream(t)

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
		name      string
		arguments map[string]any
		want      string
	}{
		{
			name:      "litestream_databases",
			arguments: map[string]any{"config": ""},
			want:      "<databases><-config><>\n",
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
			want: "<restore><-o><><-txid><><-timestamp><><-parallelism><><-if-db-not-exists><false><-if-replica-exists><true></data/db>\n",
		},
		{
			name:      "litestream_ltx",
			arguments: map[string]any{"path": "/data/db", "config": ""},
			want:      "<ltx></data/db>\n",
		},
		{
			name:      "litestream_status",
			arguments: map[string]any{"config": "", "path": ""},
			want:      "<status><-config><><>\n",
		},
		{
			name:      "litestream_reset",
			arguments: map[string]any{"path": "/data/db", "config": ""},
			want:      "<reset><-config><></data/db>\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: test.name, Arguments: test.arguments})
			if err != nil {
				t.Fatal(err)
			}
			if result.IsError {
				t.Fatalf("tool error: %#v", result.Content)
			}
			if got := textContent(t, result); got != test.want {
				t.Fatalf("text content=%q, want %q", got, test.want)
			}
			if got, want := result.StructuredContent, map[string]any{"text": test.want}; !reflect.DeepEqual(got, want) {
				t.Fatalf("structured content=%v, want %v", got, want)
			}
		})
	}

	t.Setenv("LITESTREAM_TEST_DATABASES_TABLE", "1")
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
	for _, want := range []string{
		"Litestream test-version",
		"Current Config Path:\n/custom/litestream.yml",
		"Database: /data/db",
		"<ltx><-config></custom/litestream.yml></data/db>",
	} {
		if got := textContent(t, result); !strings.Contains(got, want) {
			t.Errorf("info text content does not contain %q: %q", want, got)
		}
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
}

func TestMCPToolIntegration(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires a POSIX shell")
	}

	const version = "v1.2.3-mcp-test"
	setVersion(t, version)
	installLitestreamTestCLI(t)
	fixture := newMCPToolFixture(t)

	server, err := NewMCP(t.Context(), fixture.configPath)
	if err != nil {
		t.Fatal(err)
	}
	session := connectMCPTestSession(t, server)

	tests := []struct {
		name             string
		successArguments map[string]any
		successContains  []string
		verifySuccess    func(*testing.T)
		errorArguments   map[string]any
		errorContains    string
	}{
		{
			name:             "litestream_databases",
			successArguments: map[string]any{},
			successContains:  []string{fixture.dbPath, "file"},
			errorArguments:   map[string]any{"config": fixture.missingConfigPath},
			errorContains:    fixture.missingConfigPath,
		},
		{
			name:             "litestream_info",
			successArguments: map[string]any{},
			successContains:  []string{"=== Litestream Status Report ===", fixture.dbPath, fixture.txid},
			errorArguments:   map[string]any{"config": fixture.missingConfigPath},
			errorContains:    fixture.missingConfigPath,
		},
		{
			name: "litestream_restore",
			successArguments: map[string]any{
				"path": fixture.replicaURL,
				"o":    fixture.restorePath,
			},
			verifySuccess: func(t *testing.T) {
				db := testingutil.MustOpenSQLDB(t, fixture.restorePath)
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				var count int
				if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM test`).Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("restored row count=%d, want 1", count)
				}
			},
			errorArguments: map[string]any{
				"path": fixture.missingReplicaURL,
				"o":    fixture.failedRestorePath,
			},
			errorContains: "no matching backup files available",
		},
		{
			name:             "litestream_ltx",
			successArguments: map[string]any{"path": fixture.replicaURL},
			successContains:  []string{fixture.txid},
			errorArguments:   map[string]any{"path": ""},
			errorContains:    "database path or replica URL required",
		},
		{
			name:             "litestream_version",
			successArguments: map[string]any{},
			successContains:  []string{version},
			errorArguments:   map[string]any{"unexpected": true},
			errorContains:    "unexpected",
		},
		{
			name:             "litestream_status",
			successArguments: map[string]any{},
			successContains:  []string{fixture.dbPath, "ok"},
			errorArguments:   map[string]any{"config": fixture.missingConfigPath},
			errorContains:    fixture.missingConfigPath,
		},
		{
			name:             "litestream_reset",
			successArguments: map[string]any{"path": fixture.dbPath},
			successContains:  []string{"Reset complete."},
			verifySuccess: func(t *testing.T) {
				if _, err := os.Stat(fixture.localLTXPath); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("local LTX file still exists after reset: %v", err)
				}
			},
			errorArguments: map[string]any{"path": fixture.missingDBPath},
			errorContains:  "database does not exist",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
				Name:      test.name,
				Arguments: test.successArguments,
			})
			if err != nil {
				t.Fatal(err)
			}
			if result.IsError {
				t.Fatalf("tool error: %s", textContent(t, result))
			}
			text := textContent(t, result)
			for _, want := range test.successContains {
				if !strings.Contains(text, want) {
					t.Errorf("success content does not contain %q: %q", want, text)
				}
			}
			if test.verifySuccess != nil {
				test.verifySuccess(t)
			}

			result, err = session.CallTool(t.Context(), &mcp.CallToolParams{
				Name:      test.name,
				Arguments: test.errorArguments,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !result.IsError {
				t.Fatalf("tool succeeded, want error: %s", textContent(t, result))
			}
			if got := textContent(t, result); !strings.Contains(got, test.errorContains) {
				t.Fatalf("error content does not contain %q: %q", test.errorContains, got)
			}
		})
	}
}

func TestMCPCLIProcess(t *testing.T) {
	if os.Getenv("LITESTREAM_MCP_TEST_CLI") != "1" {
		t.Skip("helper process only")
	}

	separator := slices.Index(os.Args, "--")
	if separator == -1 {
		fmt.Fprintln(os.Stderr, "missing command arguments")
		os.Exit(2)
	}
	if err := NewMain().Run(context.Background(), os.Args[separator+1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
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

func installFakeLitestream(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "litestream")
	script := `#!/bin/sh
if [ "$LITESTREAM_TEST_FAIL" = "$1" ]; then
	printf 'failure from %s\n' "$1" >&2
	exit 1
fi
if [ "$1" = "version" ]; then
	printf 'Litestream test-version\n'
	exit 0
fi
if [ "$1" = "databases" ] && [ "$LITESTREAM_TEST_DATABASES_TABLE" = "1" ]; then
	printf 'path replica\n'
	printf '/data/db s3://bucket/db\n'
	exit 0
fi
for argument in "$@"; do
	printf '<%s>' "$argument"
done
printf '\n'
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
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

type mcpToolFixture struct {
	configPath        string
	missingConfigPath string
	dbPath            string
	missingDBPath     string
	replicaURL        string
	missingReplicaURL string
	restorePath       string
	failedRestorePath string
	localLTXPath      string
	txid              string
}

func newMCPToolFixture(t *testing.T) mcpToolFixture {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db.sqlite")
	replicaPath := filepath.Join(dir, "replica")

	db := testingutil.NewDB(t, dbPath)
	db.MonitorInterval = 0
	db.ShutdownSyncTimeout = 0
	db.Replica = litestream.NewReplicaWithClient(db, file.NewReplicaClient(replicaPath))
	db.Replica.MonitorEnabled = false
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	dbClosed := false
	t.Cleanup(func() {
		if !dbClosed {
			if err := db.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}
	})

	sqldb := testingutil.MustOpenSQLDB(t, dbPath)
	sqldbClosed := false
	t.Cleanup(func() {
		if !sqldbClosed {
			if err := sqldb.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := sqldb.ExecContext(t.Context(), `CREATE TABLE test (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqldb.ExecContext(t.Context(), `INSERT INTO test DEFAULT VALUES`); err != nil {
		t.Fatal(err)
	}
	if err := db.SyncAndWait(t.Context()); err != nil {
		t.Fatal(err)
	}
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	localLTXPaths, err := filepath.Glob(filepath.Join(db.LTXDir(), "0", "*.ltx"))
	if err != nil {
		t.Fatal(err)
	}
	if len(localLTXPaths) == 0 {
		t.Fatal("no local LTX files created")
	}
	if err := sqldb.Close(); err != nil {
		t.Fatal(err)
	}
	sqldbClosed = true
	if err := db.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	dbClosed = true

	configPath := filepath.Join(dir, "litestream.yml")
	config := fmt.Sprintf("dbs:\n  - path: %s\n    replicas:\n      - url: file://%s\n", dbPath, replicaPath)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}

	return mcpToolFixture{
		configPath:        configPath,
		missingConfigPath: filepath.Join(dir, "missing.yml"),
		dbPath:            dbPath,
		missingDBPath:     filepath.Join(dir, "missing.db"),
		replicaURL:        "file://" + replicaPath,
		missingReplicaURL: "file://" + filepath.Join(dir, "missing-replica"),
		restorePath:       filepath.Join(dir, "restored.sqlite"),
		failedRestorePath: filepath.Join(dir, "failed.sqlite"),
		localLTXPath:      localLTXPaths[0],
		txid:              pos.TXID.String(),
	}
}

func installLitestreamTestCLI(t *testing.T) {
	t.Helper()

	testBinary, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "litestream")
	script := "#!/bin/sh\nexec \"$LITESTREAM_MCP_TEST_BINARY\" -test.run=^TestMCPCLIProcess$ -- \"$@\"\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LITESTREAM_MCP_TEST_BINARY", testBinary)
	t.Setenv("LITESTREAM_MCP_TEST_CLI", "1")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func connectMCPTestSession(t *testing.T, server *MCPServer) *mcp.ClientSession {
	t.Helper()

	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.mcpServer.Connect(t.Context(), serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := serverSession.Close(); err != nil {
			t.Error(err)
		}
	})

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v1.0.0"}, nil)
	clientSession, err := client.Connect(t.Context(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := clientSession.Close(); err != nil {
			t.Error(err)
		}
	})
	return clientSession
}
