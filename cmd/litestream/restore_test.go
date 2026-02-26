package main

import (
	"context"
	"flag"
	"strings"
	"testing"
	"time"

	litestream "github.com/benbjohnson/litestream"
)

func TestRestoreCommand_FollowIntervalFlag(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantVal time.Duration
		wantErr bool
	}{
		{
			name:    "Default",
			args:    []string{"/tmp/db"},
			wantVal: time.Second,
		},
		{
			name:    "CustomValue",
			args:    []string{"-follow-interval", "500ms", "/tmp/db"},
			wantVal: 500 * time.Millisecond,
		},
		{
			name:    "LongerInterval",
			args:    []string{"-follow-interval", "5s", "/tmp/db"},
			wantVal: 5 * time.Second,
		},
		{
			name:    "InvalidDuration",
			args:    []string{"-follow-interval", "notaduration", "/tmp/db"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := litestream.NewRestoreOptions()
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.DurationVar(&opt.FollowInterval, "follow-interval", opt.FollowInterval, "polling interval for follow mode")

			err := fs.Parse(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if opt.FollowInterval != tt.wantVal {
				t.Fatalf("FollowInterval=%v, want %v", opt.FollowInterval, tt.wantVal)
			}
		})
	}
}

func TestRestoreCommand_ExecImpliesFollow(t *testing.T) {
	t.Run("ExecWithTXID", func(t *testing.T) {
		cmd := &RestoreCommand{}
		err := cmd.Run(context.Background(), []string{"-exec", "echo test", "-txid", "0000000000000001", "/tmp/db"})
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "cannot use follow mode with -txid") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ExecWithTimestamp", func(t *testing.T) {
		cmd := &RestoreCommand{}
		err := cmd.Run(context.Background(), []string{"-exec", "echo test", "-timestamp", "2020-01-01T00:00:00Z", "/tmp/db"})
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "cannot use follow mode with -timestamp") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ExecEmptyCommand", func(t *testing.T) {
		cmd := &RestoreCommand{}
		err := cmd.Run(context.Background(), []string{"-exec", "   ", "/tmp/db"})
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "exec command is empty") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
