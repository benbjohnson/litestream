package main_test

import (
	"io"
	"os"
	"strings"
	"testing"
)

// captureStdout runs fn with os.Stdout redirected to a file and returns
// everything written to stdout during the call. Used by tests that assert
// command output structure (e.g. JSON shape).
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	orig := os.Stdout
	f, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = f
	defer func() {
		os.Stdout = orig
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	fn()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	return string(output)
}

func TestCaptureStdoutLargeOutput(t *testing.T) {
	want := strings.Repeat("output", 1<<18)
	got := captureStdout(t, func() {
		if _, err := io.WriteString(os.Stdout, want); err != nil {
			t.Fatal(err)
		}
	})
	if got != want {
		t.Fatalf("captured %d bytes, want %d", len(got), len(want))
	}
}
