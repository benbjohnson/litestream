package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

func TestMCPServerInitializeVersion(t *testing.T) {
	const version = "v1.2.3-handshake-test"
	setVersion(t, version)

	server, err := NewMCP(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	body := strings.NewReader("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\",\"clientInfo\":{\"name\":\"test-client\",\"version\":\"1.0.0\"}}}")
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, httpServer.URL, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d, want %d", resp.StatusCode, http.StatusOK)
	}

	var result struct {
		Result mcp.InitializeResult `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if got, want := result.Result.ServerInfo.Version, version; got != want {
		t.Fatalf("server version=%q, want %q", got, want)
	}
}

func TestVersionTool(t *testing.T) {
	const version = "v1.2.3-instance-test"
	setVersion(t, version)
	t.Setenv("PATH", t.TempDir())

	_, handler := VersionTool()
	result, err := handler(t.Context(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %#v", result.Content)
	}
	if len(result.Content) != 1 {
		t.Fatalf("content length=%d, want 1", len(result.Content))
	}
	content, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("content type=%T, want mcp.TextContent", result.Content[0])
	}
	if got, want := content.Text, version+"\n"; got != want {
		t.Fatalf("version=%q, want %q", got, want)
	}
}

func setVersion(t *testing.T, version string) {
	t.Helper()

	previous := Version
	Version = version
	t.Cleanup(func() { Version = previous })
}
