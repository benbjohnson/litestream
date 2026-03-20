package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestInitLog_PrettyHandler(t *testing.T) {
	var buf bytes.Buffer
	initLog(&buf, "INFO", "pretty", false)
	slog.Default().Info("test message")
}

func TestInitLog_TextHandler(t *testing.T) {
	var buf bytes.Buffer
	initLog(&buf, "INFO", "text", false)
	slog.Default().Info("test message")
}

func TestInitLog_JSONHandler(t *testing.T) {
	var buf bytes.Buffer
	initLog(&buf, "INFO", "json", false)
	slog.Default().Info("test message")
}

func TestInitLog_AddSource(t *testing.T) {
	var buf bytes.Buffer
	initLog(&buf, "INFO", "text", true)
	slog.Default().Info("test message")

	output := buf.String()
	if !strings.Contains(output, "source=") {
		t.Fatalf("expected source= in output, got: %s", output)
	}
}
