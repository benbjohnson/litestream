package main_test

import (
	"strings"
	"testing"

	main "github.com/benbjohnson/litestream/cmd/litestream"
)

func TestVersionCommand_Default(t *testing.T) {
	out := captureStdout(t, func() {
		if err := (&main.VersionCommand{}).Run(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
	})

	if got, want := out, main.Version+"\n"; got != want {
		t.Fatalf("output=%q, want %q", got, want)
	}
}

func TestVersionCommand_Verbose(t *testing.T) {
	out := captureStdout(t, func() {
		if err := (&main.VersionCommand{}).Run(t.Context(), []string{"-verbose"}); err != nil {
			t.Fatal(err)
		}
	})

	for _, field := range []string{"Version:", "Go Version:", "OS/Arch:"} {
		if !strings.Contains(out, field) {
			t.Fatalf("verbose output missing %q:\n%s", field, out)
		}
	}
}
