package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestMCPCommandStdio(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestMCPCommandStdioHelper$")
	cmd.Env = append(os.Environ(), "LITESTREAM_TEST_MCP_STDIO=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v1.0.0"}, nil)
	session, err := client.Connect(t.Context(), &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatalf("connect: %v\nstderr:\n%s", err, stderr.String())
	}

	result, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(result.Tools), 7; got != want {
		t.Fatalf("tool count=%d, want %d", got, want)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("close session: %v\nstderr:\n%s", err, stderr.String())
	}
}

func TestMCPCommandStdioHelper(t *testing.T) {
	if os.Getenv("LITESTREAM_TEST_MCP_STDIO") != "1" {
		t.Skip("helper process only")
	}
	if err := NewMain().Run(context.Background(), []string{"mcp", "--config", "/test/litestream.yml"}); err != nil {
		t.Fatal(err)
	}
}

func TestMCPCommandStdioContextCancellation(t *testing.T) {
	stdin, stdinWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stdoutReader, stdout, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	previousStdin, previousStdout := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = stdin, stdout
	t.Cleanup(func() {
		os.Stdin, os.Stdout = previousStdin, previousStdout
		for _, file := range []*os.File{stdin, stdinWriter, stdoutReader, stdout} {
			if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				t.Error(err)
			}
		}
	})

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- (&MCPCommand{}).Run(ctx, nil)
	}()
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stdio command did not stop after cancellation")
	}
}

func TestMCPCommandHTTP(t *testing.T) {
	listener := availableTCPListener(t)
	addr := listener.Addr().String()
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- (&MCPCommand{listener: listener}).Run(ctx, []string{"--addr", addr, "--config", "/test/litestream.yml"})
	}()

	session := connectMCPHTTP(t, "http://"+addr)
	result, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(result.Tools), 7; got != want {
		t.Fatalf("tool count=%d, want %d", got, want)
	}
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP command did not stop after cancellation")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMCPCommandEmbeddedHTTP(t *testing.T) {
	server, err := NewMCP(t.Context(), "/test/litestream.yml")
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}

	session := connectMCPHTTP(t, "http://"+server.httpServer.Addr)
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-server.errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("embedded HTTP server did not stop")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMCPCommandSecondSignal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires POSIX process signals")
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestMCPCommandSecondSignalHelper$")
	cmd.Env = append(os.Environ(), "LITESTREAM_TEST_MCP_SECOND_SIGNAL=1")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var waitErr error
	waitDone := make(chan struct{})
	go func() {
		waitErr = cmd.Wait()
		close(waitDone)
	}()
	t.Cleanup(func() {
		select {
		case <-waitDone:
		default:
			if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Error(err)
			}
			<-waitDone
		}
	})

	scanner := bufio.NewScanner(stderr)
	if !scanner.Scan() || scanner.Text() != "ready" {
		t.Fatalf("ready line=%q, error=%v", scanner.Text(), scanner.Err())
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if !scanner.Scan() || scanner.Text() != "canceled" {
		t.Fatalf("canceled line=%q, error=%v", scanner.Text(), scanner.Err())
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	select {
	case <-waitDone:
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) {
			t.Fatalf("wait error=%v, want exit error", waitErr)
		}
		status, ok := exitErr.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGTERM {
			t.Fatalf("wait status=%v, want SIGTERM", exitErr.Sys())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second signal did not terminate MCP command")
	}
}

func TestMCPCommandSecondSignalHelper(t *testing.T) {
	if os.Getenv("LITESTREAM_TEST_MCP_SECOND_SIGNAL") != "1" {
		t.Skip("helper process only")
	}
	err := runMCPTransport(context.Background(), func(ctx context.Context) error {
		fmt.Fprintln(os.Stderr, "ready")
		<-ctx.Done()
		fmt.Fprintln(os.Stderr, "canceled")
		select {}
	})
	t.Fatalf("run returned: %v", err)
}

func TestMCPCommandFlagsAndUsage(t *testing.T) {
	if err := (&MCPCommand{}).Run(t.Context(), []string{"unexpected"}); err == nil || err.Error() != "too many arguments" {
		t.Fatalf("error=%v, want too many arguments", err)
	}

	output := captureMCPStdout(t, func() {
		if err := (&MCPCommand{}).Run(t.Context(), []string{"-help"}); !errors.Is(err, flag.ErrHelp) {
			t.Fatalf("error=%v, want flag.ErrHelp", err)
		}
		(&MCPCommand{}).Usage()
		(&Main{}).Usage()
	})
	for _, want := range []string{"litestream mcp", "-addr ADDR", "MCP server", "mcp          runs the MCP server"} {
		if !strings.Contains(output, want) {
			t.Errorf("usage missing %q:\n%s", want, output)
		}
	}
}

func TestMCPCommandHTTPBindError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
	})

	err = (&MCPCommand{}).Run(t.Context(), []string{"--addr", listener.Addr().String()})
	if err == nil || !strings.Contains(err.Error(), "listen for MCP HTTP server") {
		t.Fatalf("error=%v, want listen error", err)
	}

	cmd := NewReplicateCommand()
	cmd.Config.MCPAddr = listener.Addr().String()
	err = cmd.Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "start MCP server: listen for MCP HTTP server") {
		t.Fatalf("embedded error=%v, want listen error", err)
	}
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

func availableTCPListener(t *testing.T) net.Listener {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func connectMCPHTTP(t *testing.T, endpoint string) *mcp.ClientSession {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v1.0.0"}, nil)
		transport := &mcp.StreamableClientTransport{
			Endpoint: endpoint,
			HTTPClient: &http.Client{
				Transport: mcpTestRoundTripper{},
			},
		}
		session, err := client.Connect(t.Context(), transport, nil)
		if err == nil {
			return session
		}
		if time.Now().After(deadline) {
			t.Fatalf("connect to %s: %v", endpoint, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type mcpTestRoundTripper struct{}

func (mcpTestRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodDelete {
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    req,
		}, nil
	}
	return http.DefaultTransport.RoundTrip(req)
}

func captureMCPStdout(t *testing.T, fn func()) string {
	t.Helper()

	stdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	t.Cleanup(func() { os.Stdout = stdout })

	fn()

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	return string(output)
}
