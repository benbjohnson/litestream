package main_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	main "github.com/benbjohnson/litestream/cmd/litestream"
)

func TestReplicateCommand_ParseFlags_OnceFlags(t *testing.T) {
	t.Run("OnceFlag", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"-once", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("OnceWithForceSnapshot", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"-once", "-force-snapshot", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("OnceWithEnforceRetention", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"-once", "-enforce-retention", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("OnceWithAllFlags", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"-once", "-force-snapshot", "-enforce-retention", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ForceSnapshotRequiresOnce", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"-force-snapshot", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err == nil {
			t.Fatal("expected error when -force-snapshot is used without -once")
		}
		expectedError := "cannot specify -force-snapshot flag without -once"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error message to contain %q, got %q", expectedError, err.Error())
		}
	})

	t.Run("EnforceRetentionRequiresOnce", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"-enforce-retention", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err == nil {
			t.Fatal("expected error when -enforce-retention is used without -once")
		}
		expectedError := "cannot specify -enforce-retention flag without -once"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error message to contain %q, got %q", expectedError, err.Error())
		}
	})

	t.Run("OnceAndExecMutuallyExclusive", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"-once", "-exec", "echo test", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err == nil {
			t.Fatal("expected error when -once and -exec are both specified")
		}
		expectedError := "cannot specify -once flag with -exec"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error message to contain %q, got %q", expectedError, err.Error())
		}
	})
}

func TestReplicateCommand_ParseFlags_FlagPositioning(t *testing.T) {
	t.Run("ExecFlagAfterPositionalArgs", func(t *testing.T) {
		cmd := main.NewReplicateCommand()

		// Test the scenario from issue #245: -exec flag after positional arguments
		args := []string{"test.db", "s3://bucket/test.db", "-exec", "echo test"}

		err := cmd.ParseFlags(context.Background(), args)
		if err == nil {
			t.Fatal("expected error when -exec flag is positioned after positional arguments")
		}

		expectedError := `flag "-exec" must be positioned before DB_PATH and REPLICA_URL arguments`
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error message to contain %q, got %q", expectedError, err.Error())
		}
	})

	t.Run("ExecFlagBeforePositionalArgs", func(t *testing.T) {
		cmd := main.NewReplicateCommand()

		// Test the correct usage: -exec flag before positional arguments
		args := []string{"-exec", "echo test", "test.db", "s3://bucket/test.db"}

		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error when -exec flag is positioned correctly: %v", err)
		}

		// Verify the exec command was set correctly
		if cmd.Config.Exec != "echo test" {
			t.Errorf("expected exec command to be %q, got %q", "echo test", cmd.Config.Exec)
		}
	})

	t.Run("ConfigFlagAfterPositionalArgs", func(t *testing.T) {
		cmd := main.NewReplicateCommand()

		// Test other flags after positional arguments
		args := []string{"test.db", "s3://bucket/test.db", "-config", "/path/to/config"}

		err := cmd.ParseFlags(context.Background(), args)
		if err == nil {
			t.Fatal("expected error when -config flag is positioned after positional arguments")
		}

		expectedError := `flag "-config" must be positioned before DB_PATH and REPLICA_URL arguments`
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error message to contain %q, got %q", expectedError, err.Error())
		}
	})

	t.Run("MultipleFlags", func(t *testing.T) {
		cmd := main.NewReplicateCommand()

		// Test multiple flags in correct position
		args := []string{"-exec", "echo test", "-no-expand-env", "test.db", "s3://bucket/test.db"}

		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error with multiple flags positioned correctly: %v", err)
		}

		// Verify the exec command was set correctly
		if cmd.Config.Exec != "echo test" {
			t.Errorf("expected exec command to be %q, got %q", "echo test", cmd.Config.Exec)
		}
	})

	t.Run("OnlyDatabasePathProvided", func(t *testing.T) {
		cmd := main.NewReplicateCommand()

		// Test with only database path (should error but for different reason)
		args := []string{"test.db"}

		err := cmd.ParseFlags(context.Background(), args)
		if err == nil {
			t.Fatal("expected error when only database path is provided without replica URL")
		}

		// Should get the "must specify at least one replica URL" error, not the flag positioning error
		expectedError := "must specify at least one replica URL"
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error message to contain %q, got %q", expectedError, err.Error())
		}
	})
}

func TestReplicateCommand_ParseFlags_LogLevel(t *testing.T) {
	t.Run("LogLevelWithCLIArgs", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"-log-level", "debug", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cmd.Config.Logging.Level != "debug" {
			t.Errorf("expected log level to be %q, got %q", "debug", cmd.Config.Logging.Level)
		}
	})

	t.Run("LogLevelTrace", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"-log-level", "trace", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cmd.Config.Logging.Level != "trace" {
			t.Errorf("expected log level to be %q, got %q", "trace", cmd.Config.Logging.Level)
		}
	})

	t.Run("LogLevelError", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"-log-level", "error", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cmd.Config.Logging.Level != "error" {
			t.Errorf("expected log level to be %q, got %q", "error", cmd.Config.Logging.Level)
		}
	})

	t.Run("LogLevelDefaultsToInfo", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cmd.Config.Logging.Level != "INFO" {
			t.Errorf("expected log level to default to %q, got %q", "INFO", cmd.Config.Logging.Level)
		}
	})

	t.Run("LogLevelWithOtherFlags", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"-log-level", "warn", "-once", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cmd.Config.Logging.Level != "warn" {
			t.Errorf("expected log level to be %q, got %q", "warn", cmd.Config.Logging.Level)
		}
	})

	t.Run("LogLevelAfterPositionalArgs", func(t *testing.T) {
		cmd := main.NewReplicateCommand()
		args := []string{"test.db", "file:///tmp/replica", "-log-level", "debug"}
		err := cmd.ParseFlags(context.Background(), args)
		if err == nil {
			t.Fatal("expected error when -log-level flag is positioned after positional arguments")
		}
		expectedError := `flag "-log-level" must be positioned before DB_PATH and REPLICA_URL arguments`
		if !strings.Contains(err.Error(), expectedError) {
			t.Errorf("expected error message to contain %q, got %q", expectedError, err.Error())
		}
	})

	t.Run("LogLevelFlagOverridesEnvVar", func(t *testing.T) {
		// Set LOG_LEVEL env var to a different value
		oldEnv := os.Getenv("LOG_LEVEL")
		os.Setenv("LOG_LEVEL", "error")
		defer func() {
			if oldEnv == "" {
				os.Unsetenv("LOG_LEVEL")
			} else {
				os.Setenv("LOG_LEVEL", oldEnv)
			}
		}()

		cmd := main.NewReplicateCommand()
		args := []string{"-log-level", "debug", "test.db", "file:///tmp/replica"}
		err := cmd.ParseFlags(context.Background(), args)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// CLI flag should take precedence, setting LOG_LEVEL env var to "debug"
		if got := os.Getenv("LOG_LEVEL"); got != "debug" {
			t.Errorf("expected LOG_LEVEL env var to be %q (CLI flag), got %q", "debug", got)
		}
		if cmd.Config.Logging.Level != "debug" {
			t.Errorf("expected config log level to be %q, got %q", "debug", cmd.Config.Logging.Level)
		}
	})
}

func TestReplicateCommand_Run_LeaseRequiresSupportedReplica(t *testing.T) {
	cmd := main.NewReplicateCommand()
	cmd.Config = main.DefaultConfig()
	cmd.Config.DBs = []*main.DBConfig{{
		Path:  "/tmp/test.db",
		Lease: main.LeaseConfig{Required: true},
		Replica: &main.ReplicaConfig{
			Type: "file",
			Path: t.TempDir(),
		},
	}}

	err := cmd.Run(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), `replica type "file" does not support distributed leasing`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReplicateCommand_Run_ReleasesLeaseAfterStartupError(t *testing.T) {
	var deleteN atomic.Int32
	const etag = `"etag-1"`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			_, _ = io.Copy(io.Discard, r.Body)
			r.Body.Close()
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			deleteN.Add(1)
			if got := r.Header.Get("If-Match"); got != etag {
				t.Errorf("If-Match=%q, want %q", got, etag)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method: %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	cmd := main.NewReplicateCommand()
	cmd.Config = main.DefaultConfig()
	cmd.Config.Addr = "localhost"
	cmd.Config.DBs = []*main.DBConfig{{
		Path:  t.TempDir() + "/test.db",
		Lease: main.LeaseConfig{Required: true},
		Replica: &main.ReplicaConfig{
			Path: "test-path",
			Type: "s3",
			ReplicaSettings: main.ReplicaSettings{
				Bucket:          "test-bucket",
				Region:          "us-east-1",
				Endpoint:        server.URL,
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
			},
		},
	}}

	err := cmd.Run(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "must specify port for bind address") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got, want := deleteN.Load(), int32(1); got != want {
		t.Fatalf("DeleteObject calls=%d, want %d", got, want)
	}
}
