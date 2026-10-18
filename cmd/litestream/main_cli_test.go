package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestMainRequiredArgumentErrorsIncludeTryHints(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		message string
		hint    string
	}{
		{
			name:    "Register",
			args:    []string{"register"},
			message: "database path required",
			hint:    "litestream register -replica s3://bucket/prefix /path/to/db",
		},
		{
			name:    "RegisterReplica",
			args:    []string{"register", "/tmp/example.db"},
			message: "-replica is required",
			hint:    "litestream register -replica s3://bucket/prefix /path/to/db",
		},
		{
			name:    "Unregister",
			args:    []string{"unregister"},
			message: "database path required",
			hint:    "litestream unregister /path/to/db",
		},
		{
			name:    "Start",
			args:    []string{"start"},
			message: "database path required",
			hint:    "litestream start /path/to/db",
		},
		{
			name:    "Stop",
			args:    []string{"stop"},
			message: "database path required",
			hint:    "litestream stop /path/to/db",
		},
		{
			name:    "Sync",
			args:    []string{"sync"},
			message: "database path required",
			hint:    "litestream sync /path/to/db",
		},
		{
			name:    "LTX",
			args:    []string{"ltx"},
			message: "database path or replica URL required",
			hint:    "litestream ltx /path/to/db",
		},
		{
			name:    "Reset",
			args:    []string{"reset"},
			message: "database path required",
			hint:    "litestream reset /path/to/db",
		},
		{
			name:    "Restore",
			args:    []string{"restore"},
			message: "database path or replica URL required",
			hint:    "litestream restore -o /path/to/db s3://bucket/prefix",
		},
		{
			name:    "RestoreOutputPath",
			args:    []string{"restore", "s3://bucket/prefix"},
			message: "-o is required when restoring from a replica URL",
			hint:    "litestream restore -o /path/to/db s3://bucket/prefix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdout, stderr, exitCode := runLitestreamMain(t, tt.args...)
			if exitCode == 0 {
				t.Fatal("expected non-zero exit code")
			}
			if stdout != "" {
				t.Fatalf("expected empty stdout, got:\n%s", stdout)
			}
			want := "Error: " + tt.message + "\nTry: " + tt.hint + "\n"
			if stderr != want {
				t.Fatalf("unexpected stderr:\n%s\nwant:\n%s", stderr, want)
			}
		})
	}
}

func TestMainFlagPlacement(t *testing.T) {
	const message = "Error: flags must come after the subcommand\n"
	const configHint = "Try: litestream <command> -config PATH\n or: LITESTREAM_CONFIG=PATH litestream <command>\n"
	const genericHint = "Try: litestream <command> [flags]\n"

	t.Run("FlagBeforeCommand", func(t *testing.T) {
		tests := []struct {
			name string
			args []string
			want string
		}{
			{
				name: "Config",
				args: []string{"-config", "/etc/litestream.yml", "databases"},
				want: message + configHint,
			},
			{
				name: "ConfigWithEquals",
				args: []string{"--config=/etc/litestream.yml", "databases"},
				want: message + configHint,
			},
			{
				// The invocation from the review: "status" may be the value
				// of -socket and "info" the intended command, so no hint can
				// name the command without guessing.
				name: "Socket",
				args: []string{"-socket", "status", "info"},
				want: message + genericHint,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				stdout, stderr, exitCode := runLitestreamMain(t, tt.args...)
				if exitCode != 1 {
					t.Fatalf("exit code=%d, want 1", exitCode)
				}
				if stdout != "" {
					t.Fatalf("expected empty stdout, got:\n%s", stdout)
				}
				if stderr != tt.want {
					t.Fatalf("unexpected stderr:\n%s\nwant:\n%s", stderr, tt.want)
				}
			})
		}
	})

	// Each help flag starts with "-", so each one has to keep reaching the
	// help branch rather than falling through to the misplaced-flag branch.
	t.Run("ExplicitHelp", func(t *testing.T) {
		for _, arg := range []string{"-h", "-help", "--help"} {
			t.Run(arg, func(t *testing.T) {
				stdout, stderr, exitCode := runLitestreamMain(t, arg)
				if exitCode != 0 {
					t.Fatalf("exit code=%d, want 0\nstderr:\n%s", exitCode, stderr)
				}
				if stderr != "" {
					t.Fatalf("expected empty stderr, got:\n%s", stderr)
				}
				if !strings.Contains(stdout, "litestream <command> [arguments]") {
					t.Fatalf("expected usage on stdout, got:\n%s", stdout)
				}
			})
		}
	})

	t.Run("NoArguments", func(t *testing.T) {
		stdout, stderr, exitCode := runLitestreamMain(t)
		if exitCode != 1 {
			t.Fatalf("exit code=%d, want 1", exitCode)
		}
		if stderr != "" {
			t.Fatalf("expected empty stderr, got:\n%s", stderr)
		}
		if !strings.Contains(stdout, "litestream <command> [arguments]") {
			t.Fatalf("expected usage on stdout, got:\n%s", stdout)
		}
	})

	t.Run("FlagAfterCommand", func(t *testing.T) {
		configPath := filepath.Join(t.TempDir(), "missing.yml")
		_, stderr, exitCode := runLitestreamMain(t, "databases", "-config", configPath)
		if exitCode != 1 {
			t.Fatalf("exit code=%d, want 1", exitCode)
		}
		if strings.Contains(stderr, "flags must come after the subcommand") {
			t.Fatalf("correctly positioned flag reported as misplaced:\n%s", stderr)
		}
	})
}

func TestMainHarness(t *testing.T) {
	if os.Getenv("LITESTREAM_TEST_MAIN") != "1" {
		t.Skip("helper process only")
	}

	var args []string
	if err := json.Unmarshal([]byte(os.Getenv("LITESTREAM_TEST_ARGS")), &args); err != nil {
		t.Fatal(err)
	}
	os.Args = append([]string{"litestream"}, args...)
	main()
}

func TestMainSubcommandExplicitHelpExitsZero(t *testing.T) {
	stdout, stderr, exitCode := runLitestreamMain(t, "register", "-help")
	if exitCode != 0 {
		t.Fatalf("exit code=%d, want 0\nstderr:\n%s", exitCode, stderr)
	}
	if stderr != "" {
		t.Fatalf("expected empty stderr, got:\n%s", stderr)
	}
	if !strings.Contains(stdout, "usage: litestream register") {
		t.Fatalf("expected register usage on stdout, got:\n%s", stdout)
	}
}

func TestReplicateCommandUsageMentionsControlSocketConfig(t *testing.T) {
	output := captureLTXCommandStdout(t, func() {
		(&ReplicateCommand{}).Usage()
	})

	for _, substr := range []string{
		"Runtime control commands require the daemon control socket.",
		"socket:",
		"enabled: true",
		"path: /tmp/litestream.sock",
	} {
		if !strings.Contains(output, substr) {
			t.Fatalf("usage missing %q:\n%s", substr, output)
		}
	}
}

func runLitestreamMain(t *testing.T, args ...string) (stdout, stderr string, exitCode int) {
	t.Helper()

	argsJSON, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=TestMainHarness")
	cmd.Env = append(os.Environ(),
		"LITESTREAM_TEST_MAIN=1",
		"LITESTREAM_TEST_ARGS="+string(argsJSON),
	)
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err = cmd.Run()
	if err == nil {
		return out.String(), errOut.String(), 0
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("unexpected command error: %v", err)
	}
	return out.String(), errOut.String(), exitErr.ExitCode()
}
