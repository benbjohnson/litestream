package testingutil

import (
	"io"
	"os"
	"testing"
)

// ReadFile reads all data from filename. Fail on error.
func ReadFile(tb testing.TB, filename string) []byte {
	tb.Helper()
	b, err := os.ReadFile(filename)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

// CopyFile copies all data from src to dst. Fail on error.
func CopyFile(tb testing.TB, src, dst string) {
	tb.Helper()
	r, err := os.Open(src)
	if err != nil {
		tb.Fatal(err)
	}
	defer r.Close()

	w, err := os.Create(dst)
	if err != nil {
		tb.Fatal(err)
	}
	defer w.Close()

	if _, err := io.Copy(w, r); err != nil {
		tb.Fatal(err)
	}
}

// Getpwd returns the working directory. Fail on error.
func Getwd(tb testing.TB) string {
	tb.Helper()

	dir, err := os.Getwd()
	if err != nil {
		tb.Fatal(err)
	}
	return dir
}

// Setenv sets the environment variable key to value. The returned function reverts it.
func Setenv(tb testing.TB, key, value string) func() {
	tb.Helper()

	prevValue := os.Getenv(key)
	if err := os.Setenv(key, value); err != nil {
		tb.Fatal(err)
	}

	return func() {
		if err := os.Setenv(key, prevValue); err != nil {
			tb.Fatal(tb)
		}
	}
}
