package testingutil

import (
	"os"
	"testing"
)

// MustReadFile reads all data from filename. Fail on error.
func MustReadFile(tb testing.TB, filename string) []byte {
	tb.Helper()
	b, err := os.ReadFile(filename)
	if err != nil {
		tb.Fatal(err)
	}
	return b
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
