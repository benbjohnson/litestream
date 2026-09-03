//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

func TestTestDB_GenerateLoadCompletesAtDeadline(t *testing.T) {
	RequireBinaries(t)

	tests := []struct {
		name string
		run  func(context.Context, *TestDB, time.Duration) error
	}{
		{
			name: "default options",
			run: func(ctx context.Context, db *TestDB, duration time.Duration) error {
				return db.GenerateLoad(ctx, 10, duration, "constant")
			},
		},
		{
			name: "custom options",
			run: func(ctx context.Context, db *TestDB, duration time.Duration) error {
				return db.GenerateLoadWithOptions(ctx, 10, duration, "constant", 2, 128)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := SetupTestDB(t, "generate-load")
			if err := db.Create(); err != nil {
				t.Fatalf("create database: %v", err)
			}

			const duration = 100 * time.Millisecond
			ctx, cancel := context.WithTimeout(t.Context(), duration)
			defer cancel()

			if err := tt.run(ctx, db, duration); err != nil {
				t.Fatalf("generate load: %v", err)
			}
		})
	}
}

func TestTestDB_GenerateLoadStopsOnCancel(t *testing.T) {
	RequireBinaries(t)

	db := SetupTestDB(t, "cancel-load")
	if err := db.Create(); err != nil {
		t.Fatalf("create database: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	defer timer.Stop()

	if err := db.GenerateLoad(ctx, 10, 5*time.Second, "constant"); err == nil {
		t.Fatal("expected canceled load generation to fail")
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("context error=%v, want %v", ctx.Err(), context.Canceled)
	}
}

func TestTestDB_GenerateLoadStopsAtEarlierDeadline(t *testing.T) {
	RequireBinaries(t)

	db := SetupTestDB(t, "deadline-load")
	if err := db.Create(); err != nil {
		t.Fatalf("create database: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	if err := db.GenerateLoad(ctx, 10, 5*time.Second, "constant"); err == nil {
		t.Fatal("expected expired load generation to fail")
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("context error=%v, want %v", ctx.Err(), context.DeadlineExceeded)
	}
}
