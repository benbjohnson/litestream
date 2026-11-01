package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal/testingutil"
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
	if filename := os.Getenv("LITESTREAM_TEST_CANCEL_FILE"); filename != "" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			for {
				if _, err := os.Stat(filename); err == nil {
					cancel()
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
		}()
		if err := NewMain().Run(ctx, args); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
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

func TestReplicateShutdown(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("requires Unix signals")
	}
	for _, tt := range []struct {
		name, timeout, want, mode string
		second, parent            bool
	}{
		{name: "SFTPTimeout", timeout: "250ms", want: "shutdown timed out", mode: "sftp"},
		{name: "SFTPSecondSignal", timeout: "1m", want: "shutdown interrupted", mode: "sftp", second: true},
		{name: "SSHStartupTimeout", timeout: "250ms", want: "shutdown timed out", mode: "sftp-ssh"},
		{name: "SSHStartupSecondSignal", timeout: "1m", want: "shutdown interrupted", mode: "sftp-ssh", second: true},
		{name: "SFTPStartupTimeout", timeout: "250ms", want: "shutdown timed out", mode: "sftp-init"},
		{name: "SFTPStartupSecondSignal", timeout: "1m", want: "shutdown interrupted", mode: "sftp-init", second: true},
		{name: "StartupParentCancellation", timeout: "250ms", want: "shutdown timed out", mode: "sftp-ssh", parent: true},
		{name: "Graceful", timeout: "5s", want: "litestream shut down", mode: "file"},
		{name: "ParentCancellation", timeout: "5s", want: "context canceled", mode: "file", parent: true},
		{name: "ExecTimeout", timeout: "250ms", want: "shutdown timed out", mode: "exec"},
		{name: "ExecSecondSignal", timeout: "1m", want: "shutdown interrupted", mode: "exec", second: true},
		{name: "ExecGraceful", timeout: "5s", want: "litestream shut down", mode: "exec-graceful"},
		{name: "ExecParentCancellation", timeout: "5s", want: "context canceled", mode: "exec-graceful", parent: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var host string
			var stalled <-chan struct{}
			if strings.HasPrefix(tt.mode, "sftp") {
				host, stalled = startStalledSFTP(t, tt.mode)
			}
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "db")
			db, err := sql.Open("sqlite", dbPath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(dir, "litestream.yml")
			config := fmt.Sprintf("shutdown-timeout: %s\ndbs:\n  - path: %q\n    replica:\n      type: sftp\n      host: %q\n      user: test\n      path: /replica\n", tt.timeout, dbPath, host)
			if !strings.HasPrefix(tt.mode, "sftp") {
				config = fmt.Sprintf("shutdown-timeout: %s\ndbs:\n  - path: %q\n    replica:\n      type: file\n      path: %q\n", tt.timeout, dbPath, filepath.Join(dir, "replica"))
			}
			if strings.HasPrefix(tt.mode, "exec") {
				config += fmt.Sprintf("exec: %q\n", fmt.Sprintf("%q -test.run=^TestShutdownExecHelper$", os.Args[0]))
			}
			if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
				t.Fatal(err)
			}
			args, err := json.Marshal([]string{"replicate", "-config", configPath})
			if err != nil {
				t.Fatal(err)
			}
			output, err := os.Create(filepath.Join(dir, "output"))
			if err != nil {
				t.Fatal(err)
			}
			defer output.Close()
			cmd := exec.Command(os.Args[0], "-test.run=^TestMainHarness$")
			cmd.Env = append(os.Environ(), "LITESTREAM_TEST_MAIN=1", "LITESTREAM_TEST_ARGS="+string(args), "LITESTREAM_TEST_SHUTDOWN_EXEC_PID="+filepath.Join(dir, "child-pid"), "LOG_LEVEL=INFO")
			if tt.parent {
				cmd.Env = append(cmd.Env, "LITESTREAM_TEST_CANCEL_FILE="+filepath.Join(dir, "cancel"))
			}
			if tt.mode == "exec-graceful" {
				cmd.Env = append(cmd.Env, "LITESTREAM_TEST_GRACEFUL_EXEC=1")
			}
			cmd.Stdout, cmd.Stderr = output, output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			go func() { exited <- cmd.Wait() }()
			defer func() { _ = cmd.Process.Kill() }()
			if strings.HasPrefix(tt.mode, "sftp") {
				select {
				case <-stalled:
				case err := <-exited:
					t.Fatalf("exited before SFTP stalled: %v", err)
				case <-time.After(10 * time.Second):
					t.Fatal("SFTP did not receive a request")
				}
			} else {
				ready := "initialized db"
				if strings.HasPrefix(tt.mode, "exec") {
					ready = "shutdown exec ready"
				}
				waitForShutdownOutput(t, output.Name(), ready)
			}
			var child *os.Process
			if strings.HasPrefix(tt.mode, "exec") {
				buf, err := os.ReadFile(filepath.Join(dir, "child-pid"))
				if err != nil {
					t.Fatal(err)
				}
				pid, err := strconv.Atoi(string(buf))
				if err != nil {
					t.Fatal(err)
				}
				child, err = os.FindProcess(pid)
				if err != nil {
					t.Fatal(err)
				}
				defer child.Release()
				defer child.Kill()
			}

			if !strings.HasPrefix(tt.mode, "sftp") {
				deadline := time.Now().Add(10 * time.Second)
				for {
					var replicated bool
					for _, level := range []int{0, litestream.SnapshotLevel} {
						files, err := filepath.Glob(filepath.Join(litestream.LTXLevelDir(filepath.Join(dir, "replica"), level), "*.ltx"))
						if err != nil {
							t.Fatal(err)
						}
						if len(files) > 0 {
							replicated = true
							break
						}
					}
					if replicated {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("initial replication did not finish")
					}
					time.Sleep(10 * time.Millisecond)
				}
				db, err := sql.Open("sqlite", dbPath)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec("INSERT INTO test (id) VALUES (1555)"); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if tt.parent {
				if err := os.WriteFile(filepath.Join(dir, "cancel"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if tt.second {
				waitForShutdownOutput(t, output.Name(), "signal received, litestream shutting down")
				if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-exited:
				if (tt.mode == "file" || tt.mode == "exec-graceful") && !tt.parent && err != nil {
					t.Errorf("graceful shutdown failed: %v", err)
				} else if (tt.parent || (tt.mode != "file" && tt.mode != "exec-graceful")) && err == nil {
					t.Error("incomplete shutdown must return an error")
				}
			case <-time.After(3 * time.Second):
				_ = cmd.Process.Kill()
				<-exited
				t.Error("replicate did not exit promptly")
			}
			if child != nil {
				deadline := time.Now().Add(time.Second)
				for child.Signal(syscall.Signal(0)) == nil {
					if time.Now().After(deadline) {
						t.Error("exec child still running after forced shutdown")
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			buf, err := os.ReadFile(output.Name())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(buf, []byte(tt.want)) {
				t.Errorf("missing %q in output:\n%s", tt.want, buf)
			}
			if tt.mode == "exec-graceful" && !bytes.Contains(buf, []byte("exec stopped gracefully")) {
				t.Errorf("exec child did not finish gracefully: %s", buf)
			}
			if tt.mode == "file" || tt.mode == "exec-graceful" {
				restored := filepath.Join(dir, "restored.db")
				if err := (&RestoreCommand{}).Run(t.Context(), []string{"-o", restored, "file://" + filepath.Join(dir, "replica")}); err != nil {
					t.Fatal(err)
				}
				db, err := sql.Open("sqlite", restored)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				var count int
				if err := db.QueryRow("SELECT count(*) FROM test WHERE id = 1555").Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != 1 {
					t.Fatalf("restored row count=%d, want 1", count)
				}
			}
		})
	}
}

func startStalledSFTP(t *testing.T, phase string) (string, <-chan struct{}) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	config := &ssh.ServerConfig{NoClientAuth: true}
	config.AddHostKey(signer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stopped, stalled, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(stopped); listener.Close(); <-finished })
	go func() {
		defer close(finished)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if phase == "sftp-ssh" {
			close(stalled)
			<-stopped
			return
		}
		go func() { <-stopped; conn.Close() }()
		server, channels, requests, err := ssh.NewServerConn(conn, config)
		if err != nil {
			return
		}
		defer server.Close()
		go ssh.DiscardRequests(requests)
		for ch := range channels {
			if ch.ChannelType() != "session" {
				_ = ch.Reject(ssh.UnknownChannelType, "session required")
				continue
			}
			channel, requests, err := ch.Accept()
			if err != nil {
				return
			}
			defer channel.Close()
			request, ok := <-requests
			if !ok || request.Type != "subsystem" {
				return
			}
			if err := request.Reply(true, nil); err != nil {
				return
			}
			go ssh.DiscardRequests(requests)
			readPacket := func() error {
				var size uint32
				if err := binary.Read(channel, binary.BigEndian, &size); err != nil {
					return err
				}
				_, err := io.CopyN(io.Discard, channel, int64(size))
				return err
			}
			if err := readPacket(); err != nil {
				return
			}
			if phase == "sftp-init" {
				close(stalled)
				<-stopped
				return
			}
			if _, err := channel.Write([]byte{0, 0, 0, 5, 2, 0, 0, 0, 3}); err != nil {
				return
			}
			if err := readPacket(); err != nil {
				return
			}
			close(stalled)
			<-stopped
			return
		}
	}()
	return listener.Addr().String(), stalled
}

func TestShutdownTimeoutConfig(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  time.Duration
	}{
		{"", 30 * time.Second}, {"null", 30 * time.Second}, {"0s", 30 * time.Second},
		{"-1s", 30 * time.Second}, {"invalid", 30 * time.Second}, {"42", 30 * time.Second},
		{"true", 30 * time.Second}, {"[1s]", 30 * time.Second}, {"2m", 2 * time.Minute},
	} {
		t.Run(tt.value, func(t *testing.T) {
			config, err := ParseConfig(strings.NewReader("shutdown-timeout: "+tt.value), false)
			if err != nil {
				t.Fatal(err)
			}
			if config.ShutdownTimeout != tt.want {
				t.Fatalf("timeout=%s, want %s", config.ShutdownTimeout, tt.want)
			}
		})
	}
}

func waitForShutdownOutput(t *testing.T, filename, message string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		buf, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(buf, []byte(message)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("missing %q: %s", message, buf)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestShutdownExecHelper(t *testing.T) {
	filename := os.Getenv("LITESTREAM_TEST_SHUTDOWN_EXEC_PID")
	if filename == "" {
		t.Skip("helper process only")
	}
	signals := make(chan os.Signal, 1)
	if os.Getenv("LITESTREAM_TEST_GRACEFUL_EXEC") == "1" {
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(signals)
	} else {
		signal.Ignore(syscall.SIGINT, syscall.SIGTERM)
	}
	if err := os.WriteFile(filename, []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	fmt.Println("shutdown exec ready")
	if os.Getenv("LITESTREAM_TEST_GRACEFUL_EXEC") == "1" {
		<-signals
		fmt.Println("exec stopped gracefully")
		return
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestConfig_SnapshotWarningLogging(t *testing.T) {
	t.Setenv("LOG_LEVEL", "")
	for _, tt := range []struct {
		name           string
		level          string
		defaultLogging bool
		wantWarning    bool
	}{
		{name: "DefaultLogging", defaultLogging: true, wantWarning: true},
		{name: "JSONStderr", level: "info", wantWarning: true},
		{name: "ErrorLevel", level: "error"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			configPath := filepath.Join(dir, "litestream.yml")
			config := "snapshot:\n  interval: 1h\n  retention: 30m\nlogging:\n  type: json\n  stderr: true\n  level: " + tt.level + "\n"
			if tt.defaultLogging {
				config = "snapshot:\n  interval: 1h\n  retention: 30m\n"
			}
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
			if tt.defaultLogging {
				for _, want := range []string{"level=WARN", "snapshot retention is shorter than snapshot interval", "field=snapshot.retention", "retention=30m0s", "interval=1h0m0s"} {
					if !strings.Contains(string(logOutput), want) {
						t.Errorf("expected stderr warning containing %q, got %q", want, logOutput)
					}
				}
				return
			}
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

func TestReplicateCommandCloseReturnsError(t *testing.T) {
	db, sqldb := testingutil.MustOpenDBs(t)
	defer testingutil.MustCloseDBs(t, db, sqldb)
	if _, err := sqldb.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	c := NewReplicateCommand()
	c.Store = litestream.NewStore([]*litestream.DB{db}, litestream.DefaultCompactionLevels)
	c.Store.CompactionMonitorEnabled = false
	c.Store.RetentionEnabled = false
	if err := c.Store.Open(t.Context()); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	c.Store.SetDone(done)
	close(done)
	if err := c.Close(context.Background()); !errors.Is(err, litestream.ErrShutdownInterrupted) {
		t.Fatalf("Close error=%v, want shutdown interruption", err)
	}
}
