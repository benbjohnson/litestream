package testingutil

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/pierrec/lz4/v4"
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

func CompressLZ4(tb testing.TB, b []byte) []byte {
	tb.Helper()

	var buf bytes.Buffer
	zw := lz4.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		tb.Fatal(err)
	} else if err := zw.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

func DecompressLZ4(tb testing.TB, b []byte) []byte {
	tb.Helper()

	buf, err := io.ReadAll(lz4.NewReader(bytes.NewReader(b)))
	if err != nil {
		tb.Fatal(err)
	}
	return buf
}
