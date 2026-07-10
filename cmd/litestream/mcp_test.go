package main

import (
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

func TestVersionTool(t *testing.T) {
	prevVersion := Version
	t.Cleanup(func() { Version = prevVersion })
	Version = resolveVersion("v0.5.14-running", nil)
	t.Setenv("PATH", t.TempDir())

	_, handler := VersionTool()
	result, err := handler(t.Context(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("unexpected tool error: %#v", result.Content)
	}
	if got, want := len(result.Content), 1; got != want {
		t.Fatalf("content length=%d, want %d", got, want)
	}
	content, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("content type=%T, want mcp.TextContent", result.Content[0])
	}
	if got, want := content.Text, Version+"\n"; got != want {
		t.Fatalf("content=%q, want %q", got, want)
	}
}
