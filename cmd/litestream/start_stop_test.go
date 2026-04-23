package main_test

import (
	"strings"
	"testing"

	main "github.com/benbjohnson/litestream/cmd/litestream"
)

func TestStartCommand_Usage(t *testing.T) {
	output := captureStdout(t, func() {
		(&main.StartCommand{}).Usage()
	})

	for _, example := range []string{
		"Examples:",
		"$ litestream start /path/to/db",
		"$ litestream start -socket /tmp/litestream.sock /path/to/db",
		"$ litestream start -timeout 10 /path/to/db",
	} {
		if !strings.Contains(output, example) {
			t.Fatalf("usage output missing %q:\n%s", example, output)
		}
	}
}

func TestStopCommand_Usage(t *testing.T) {
	output := captureStdout(t, func() {
		(&main.StopCommand{}).Usage()
	})

	for _, example := range []string{
		"Examples:",
		"$ litestream stop /path/to/db",
		"$ litestream stop -socket /tmp/litestream.sock /path/to/db",
		"$ litestream stop -timeout 10 /path/to/db",
	} {
		if !strings.Contains(output, example) {
			t.Fatalf("usage output missing %q:\n%s", example, output)
		}
	}
}
