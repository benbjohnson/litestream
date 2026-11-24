package main_test

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// captureStdout runs fn with os.Stdout redirected to a pipe and returns
// everything written to stdout during the call. Used by tests that assert
// command output structure (e.g. JSON shape).
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	t.Cleanup(func() {
		os.Stdout = orig
		_ = w.Close()
		_ = r.Close()
	})

	type readResult struct {
		output []byte
		err    error
	}
	readDone := make(chan readResult, 1)
	go func() {
		output, err := io.ReadAll(r)
		readDone <- readResult{output, err}
	}()

	fn()

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	result := <-readDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	output := result.output
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return string(output)
}

func TestCaptureStdout_OutputLargerThanPipeBuffer(t *testing.T) {
	want := strings.Repeat("x", 1<<20)

	done := make(chan string, 1)
	go func() {
		done <- captureStdout(t, func() {
			if _, err := os.Stdout.WriteString(want); err != nil {
				t.Errorf("write stdout: %v", err)
			}
		})
	}()

	select {
	case got := <-done:
		if len(got) != len(want) {
			t.Fatalf("captured %d bytes, want %d", len(got), len(want))
		}
	case <-time.After(30 * time.Second):
		t.Fatal("captureStdout deadlocked on output larger than the pipe buffer")
	}
}
