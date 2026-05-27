package main_test

import (
	"strings"
	"testing"

	main "github.com/benbjohnson/litestream/cmd/litestream"
)

func TestDatabasesCommand_Usage(t *testing.T) {
	output := captureStdout(t, func() {
		(&main.DatabasesCommand{}).Usage()
	})

	for _, example := range []string{
		"Examples:",
		"$ litestream databases",
		"$ litestream databases -config /path/to/litestream.yml",
		"$ litestream databases -no-expand-env -config /path/to/litestream.yml",
	} {
		if !strings.Contains(output, example) {
			t.Fatalf("usage output missing %q:\n%s", example, output)
		}
	}
}
