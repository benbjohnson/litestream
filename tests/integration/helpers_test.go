//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

func TestTestDB_GenerateLoadCompletesAtDeadline(t *testing.T) {
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
	db := SetupTestDB(t, "cancel-load")
	if err := db.Create(); err != nil {
		t.Fatalf("create database: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	defer timer.Stop()

	start := time.Now()
	if err := db.GenerateLoad(ctx, 10, 5*time.Second, "constant"); err == nil {
		t.Fatal("expected canceled load generation to fail")
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("canceled load generation took %v", elapsed)
	}
}
