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
	os.Exit(0)
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

func TestConfig_SnapshotWarningLogging(t *testing.T) {
	t.Setenv("LOG_LEVEL", "")
	for _, tt := range []struct {
		name        string
		level       string
		wantWarning bool
	}{
		{name: "JSONStderr", level: "info", wantWarning: true},
		{name: "ErrorLevel", level: "error"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, "litestream.yml")
			config := "snapshot:\n  interval: 1h\n  retention: 30m\nlogging:\n  type: json\n  stderr: true\n  level: " + tt.level + "\n"
			if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
				t.Fatal(err)
			}
			stdout, err := os.Create(filepath.Join(dir, "stdout"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { stdout.Close() })
			stderr, err := os.Create(filepath.Join(dir, "stderr"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { stderr.Close() })

			args, err := json.Marshal([]string{"databases", "-config", configPath, "-json"})
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestMainHarness$")
			cmd.Env = append(os.Environ(), "LITESTREAM_TEST_MAIN=1", "LITESTREAM_TEST_ARGS="+string(args))
			cmd.Stdout, cmd.Stderr = stdout, stderr
			if err := cmd.Run(); err != nil {
				t.Fatal(err)
			}
			output, err := os.ReadFile(stdout.Name())
			if err != nil {
				t.Fatal(err)
			}
			var databases []DatabaseInfo
			if err := json.Unmarshal(output, &databases); err != nil {
				t.Errorf("expected valid JSON stdout, got %q: %v", output, err)
			}
			logOutput, err := os.ReadFile(stderr.Name())
			if err != nil {
				t.Fatal(err)
			}
			if !tt.wantWarning {
				if len(logOutput) != 0 {
					t.Errorf("expected no warning at error level, got %q", logOutput)
				}
				return
			}
			var warning map[string]any
			if err := json.Unmarshal(logOutput, &warning); err != nil {
				t.Fatalf("expected JSON warning on stderr, got %q: %v", logOutput, err)
			}
			for key, want := range map[string]any{
				"level":     "WARN",
				"msg":       "snapshot retention is shorter than snapshot interval",
				"field":     "snapshot.retention",
				"retention": float64(30 * 60 * 1e9),
				"interval":  float64(60 * 60 * 1e9),
			} {
				if got := warning[key]; got != want {
					t.Errorf("warning[%s] = %v, want %v", key, got, want)
				}
			}
		})
	}
}
