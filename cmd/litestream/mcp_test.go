package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type daemonToolCallResult struct {
	result *mcp.CallToolResult
	err    error
}

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
		"litestream_list":    {readOnly: true, properties: map[string]string{"socket": "string", "timeout": "integer"}},
		"litestream_sync": {
			destructive: true,
			properties:  map[string]string{"path": "string", "socket": "string", "timeout": "integer", "wait": "boolean"},
			required:    []string{"path"},
		},
		"litestream_daemon_info": {readOnly: true, properties: map[string]string{"socket": "string", "timeout": "integer"}},
		"litestream_start": {
			destructive: true,
			properties:  map[string]string{"path": "string", "socket": "string", "timeout": "integer"},
			required:    []string{"path"},
		},
		"litestream_stop": {
			destructive: true,
			properties:  map[string]string{"path": "string", "socket": "string", "timeout": "integer"},
			required:    []string{"path"},
		},
		"litestream_register": {
			destructive: true,
			properties:  map[string]string{"path": "string", "replica_url": "string", "socket": "string", "timeout": "integer"},
			required:    []string{"path", "replica_url"},
		},
		"litestream_unregister": {
			destructive: true,
			properties:  map[string]string{"path": "string", "socket": "string", "timeout": "integer"},
			required:    []string{"path"},
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
		if got := slices.Sorted(maps.Keys(outputProperties)); !reflect.DeepEqual(got, []string{"text"}) {
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
	if got, want := callResult.StructuredContent, map[string]any{"text": version + "\n"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("version structured content=%v, want %v", got, want)
	}
	if got, want := textContent(t, callResult), version+"\n"; got != want {
		t.Fatalf("version text content=%q, want %q", got, want)
	}
}

func TestMCPDaemonToolBehavior(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires Unix sockets")
	}

	type capturedRequest struct {
		method string
		path   string
		body   []byte
	}

	responses := map[string]string{
		"/list":       `{"databases":[{"path":"/data/db","status":"replicating"}]}`,
		"/sync":       `{"status":"synced","path":"/data/db","txid":12,"replicated_txid":12}`,
		"/info":       `{"version":"v1.2.3","pid":123,"uptime_seconds":60,"started_at":"2026-07-17T12:00:00Z","database_count":1}`,
		"/start":      `{"status":"started","path":"/data/db","txid":12}`,
		"/stop":       `{"status":"stopped","path":"/data/db","txid":12}`,
		"/register":   `{"status":"registered","path":"/data/new.db"}`,
		"/unregister": `{"status":"unregistered","path":"/data/db","txid":12}`,
	}
	requests := make(chan capturedRequest, len(responses))
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		requests <- capturedRequest{method: r.Method, path: r.URL.Path, body: body}

		response, ok := responses[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, response); err != nil {
			t.Errorf("write response: %v", err)
		}
	})

	socketPath := startMCPDaemonTestServer(t, handler)

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

	tests := []struct {
		name      string
		method    string
		path      string
		arguments map[string]any
		body      string
	}{
		{name: "litestream_list", method: http.MethodGet, path: "/list", arguments: map[string]any{"socket": socketPath, "timeout": 5}},
		{name: "litestream_sync", method: http.MethodPost, path: "/sync", arguments: map[string]any{"path": "/data/db", "wait": true, "socket": socketPath, "timeout": 5}, body: `{"path":"/data/db","wait":true,"timeout":5}`},
		{name: "litestream_daemon_info", method: http.MethodGet, path: "/info", arguments: map[string]any{"socket": socketPath, "timeout": 5}},
		{name: "litestream_start", method: http.MethodPost, path: "/start", arguments: map[string]any{"path": "/data/db", "socket": socketPath, "timeout": 5}, body: `{"path":"/data/db","timeout":5}`},
		{name: "litestream_stop", method: http.MethodPost, path: "/stop", arguments: map[string]any{"path": "/data/db", "socket": socketPath, "timeout": 5}, body: `{"path":"/data/db","timeout":5}`},
		{name: "litestream_register", method: http.MethodPost, path: "/register", arguments: map[string]any{"path": "/data/new.db", "replica_url": "file:///backup/db", "socket": socketPath, "timeout": 5}, body: `{"path":"/data/new.db","replica_url":"file:///backup/db"}`},
		{name: "litestream_unregister", method: http.MethodPost, path: "/unregister", arguments: map[string]any{"path": "/data/db", "socket": socketPath, "timeout": 5}, body: `{"path":"/data/db","timeout":5}`},
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

			request := <-requests
			if request.method != test.method {
				t.Errorf("request method=%q, want %q", request.method, test.method)
			}
			if request.path != test.path {
				t.Errorf("request path=%q, want %q", request.path, test.path)
			}
			assertJSONEqual(t, request.body, []byte(test.body))

			text := textContent(t, result)
			assertJSONEqual(t, []byte(text), []byte(responses[test.path]))
			if got, want := result.StructuredContent, map[string]any{"text": text}; !reflect.DeepEqual(got, want) {
				t.Fatalf("structured content=%v, want %v", got, want)
			}
		})
	}
}

func TestMCPDaemonToolsNoDaemon(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires Unix sockets")
	}

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

	socketPath := mcpTestSocketPath(t)
	tests := []struct {
		name      string
		arguments map[string]any
	}{
		{name: "litestream_list", arguments: map[string]any{"socket": socketPath}},
		{name: "litestream_sync", arguments: map[string]any{"path": "/data/db", "socket": socketPath}},
		{name: "litestream_daemon_info", arguments: map[string]any{"socket": socketPath}},
		{name: "litestream_start", arguments: map[string]any{"path": "/data/db", "socket": socketPath}},
		{name: "litestream_stop", arguments: map[string]any{"path": "/data/db", "socket": socketPath}},
		{name: "litestream_register", arguments: map[string]any{"path": "/data/db", "replica_url": "file:///backup/db", "socket": socketPath}},
		{name: "litestream_unregister", arguments: map[string]any{"path": "/data/db", "socket": socketPath}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: test.name, Arguments: test.arguments})
			if err != nil {
				t.Fatal(err)
			}
			if !result.IsError {
				t.Fatal("tool succeeded, want error")
			}
			if got := textContent(t, result); !strings.Contains(got, "no Litestream daemon is running at socket "+socketPath) {
				t.Fatalf("error=%q, want no-daemon message", got)
			}
		})
	}

	t.Run("stale socket", func(t *testing.T) {
		staleSocketPath := mcpTestSocketPath(t)
		listener, err := net.Listen("unix", staleSocketPath)
		if err != nil {
			t.Fatal(err)
		}
		listener.(*net.UnixListener).SetUnlinkOnClose(false)
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}

		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
			Name:      "litestream_list",
			Arguments: map[string]any{"socket": staleSocketPath},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !result.IsError {
			t.Fatal("tool succeeded, want error")
		}
		if got := textContent(t, result); !strings.Contains(got, "no Litestream daemon is running at socket "+staleSocketPath) {
			t.Fatalf("error=%q, want no-daemon message", got)
		}
	})
}

func TestMCPDaemonToolContextCancellation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires Unix sockets")
	}

	requestStarted := make(chan struct{})
	handler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
	})
	socketPath := startMCPDaemonTestServer(t, handler)

	_, toolHandler := DaemonInfoTool()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	timeout := 10
	done := make(chan daemonToolCallResult, 1)
	go func() {
		result, _, err := toolHandler(ctx, nil, daemonInfoInput{Socket: &socketPath, Timeout: &timeout})
		done <- daemonToolCallResult{result: result, err: err}
	}()

	select {
	case <-requestStarted:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("daemon request did not start")
	}

	select {
	case call := <-done:
		if !errors.Is(call.err, context.Canceled) {
			t.Fatalf("error=%v, want context canceled", call.err)
		}
		if call.result != nil {
			t.Fatalf("result=%v, want nil", call.result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("daemon tool did not return after context cancellation")
	}
}

func TestMCPDaemonToolTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires Unix sockets")
	}

	requestStarted := make(chan struct{})
	handler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		<-r.Context().Done()
	})
	socketPath := startMCPDaemonTestServer(t, handler)

	_, toolHandler := DaemonInfoTool()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	timeout := 1
	done := make(chan daemonToolCallResult, 1)
	go func() {
		result, _, err := toolHandler(ctx, nil, daemonInfoInput{Socket: &socketPath, Timeout: &timeout})
		done <- daemonToolCallResult{result: result, err: err}
	}()

	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("daemon request did not start")
	}

	select {
	case call := <-done:
		if !errors.Is(call.err, context.DeadlineExceeded) {
			t.Fatalf("error=%v, want deadline exceeded", call.err)
		}
		if !strings.Contains(call.err.Error(), "daemon request timed out after 1s") {
			t.Fatalf("error=%v, want timeout message", call.err)
		}
		if call.result != nil {
			t.Fatalf("result=%v, want nil", call.result)
		}
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("daemon tool exceeded its timeout")
	}
}

func TestMCPDaemonToolReturnsDaemonError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires Unix sockets")
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		if _, err := io.WriteString(w, `{"error":"database is not running"}`); err != nil {
			t.Errorf("write response: %v", err)
		}
	})
	socketPath := startMCPDaemonTestServer(t, handler)

	_, toolHandler := DaemonSyncTool()
	result, _, err := toolHandler(t.Context(), nil, daemonSyncInput{Path: "/data/db", Socket: &socketPath})
	if err == nil || err.Error() != "sync failed: database is not running" {
		t.Fatalf("error=%v, want daemon error", err)
	}
	if result != nil {
		t.Fatalf("result=%v, want nil", result)
	}
}

func TestNewDaemonClientTimeoutValidation(t *testing.T) {
	type testCase struct {
		name    string
		timeout int
		want    string
	}
	tests := []testCase{
		{name: "zero", timeout: 0, want: "timeout must be greater than 0"},
		{name: "negative", timeout: -1, want: "timeout must be greater than 0"},
	}
	if strconv.IntSize == 64 {
		overflowTimeout := int64(maxDaemonTimeoutSeconds + 1)
		tests = append(tests, testCase{name: "duration overflow", timeout: int(overflowTimeout), want: "timeout must be at most"})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := newDaemonClient(nil, &test.timeout, defaultDaemonReadTimeout)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want %q", err, test.want)
			}
			if client != nil {
				t.Fatalf("client=%v, want nil", client)
			}
		})
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

func mcpTestSocketPath(t *testing.T) string {
	t.Helper()

	file, err := os.CreateTemp("/tmp", "ls-mcp-*.sock")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Error(err)
		}
	})
	return path
}

func startMCPDaemonTestServer(t *testing.T, handler http.Handler) string {
	t.Helper()

	socketPath := mcpTestSocketPath(t)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return socketPath
}

func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()

	if len(got) == 0 || len(want) == 0 {
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("JSON=%q, want %q", got, want)
		}
		return
	}

	var gotValue, wantValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("unmarshal JSON %q: %v", got, err)
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("unmarshal expected JSON %q: %v", want, err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("JSON=%v, want %v", gotValue, wantValue)
	}
}
