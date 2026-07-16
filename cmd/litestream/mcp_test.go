package main

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"slices"
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
	if got := initializeResult.ServerInfo.Version; got != version {
		t.Fatalf("server version=%q, want %q", got, version)
	}

	result, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]struct {
		readOnly    bool
		destructive bool
		properties  []string
		required    []string
	}{
		"litestream_databases": {readOnly: true, properties: []string{"config"}},
		"litestream_info":      {readOnly: true, properties: []string{"config"}},
		"litestream_restore": {
			destructive: true,
			properties:  []string{"config", "if_db_not_exists", "if_replica_exists", "o", "parallelism", "path", "timestamp", "txid"},
			required:    []string{"path"},
		},
		"litestream_ltx":     {readOnly: true, properties: []string{"config", "path"}, required: []string{"path"}},
		"litestream_version": {readOnly: true},
		"litestream_status":  {readOnly: true, properties: []string{"config", "path"}},
		"litestream_reset":   {destructive: true, properties: []string{"config", "path"}, required: []string{"path"}},
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
		if got := sortedMapKeys(properties); !reflect.DeepEqual(got, test.properties) {
			t.Errorf("%s input properties=%v, want %v", tool.Name, got, test.properties)
		}
		for name, property := range properties {
			propertyObject := jsonObject(t, property)
			if propertyObject["description"] == "" {
				t.Errorf("%s input property %s has no description", tool.Name, name)
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

func sortedMapKeys(values map[string]any) []string {
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
