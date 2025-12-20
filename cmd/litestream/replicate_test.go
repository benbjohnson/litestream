package main_test

import (
	"context"
	"strings"
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
