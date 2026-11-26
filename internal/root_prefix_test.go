package internal_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
)

func TestIsRootLTXKey(t *testing.T) {
	tests := []struct {
		name       string
		rootPrefix string
		key        string
		want       bool
	}{
		{name: "CleanRootL0", rootPrefix: "", key: "0000/0000000000000001-0000000000000001.ltx", want: true},
		{name: "CleanRootL9", rootPrefix: "", key: "0009/0000000000000001-0000000000000001.ltx", want: true},
		{name: "LegacyDotL0", rootPrefix: "./", key: "./0000/0000000000000001-0000000000000001.ltx", want: true},
		{name: "LegacySlashL1", rootPrefix: "/", key: "/0001/0000000000000001-0000000000000005.ltx", want: true},
		{name: "WrongPrefix", rootPrefix: "", key: "./0000/0000000000000001-0000000000000001.ltx", want: false},
		{name: "LevelOutOfRange", rootPrefix: "", key: "000a/0000000000000001-0000000000000001.ltx", want: false},
		{name: "NestedSubdir", rootPrefix: "", key: "0000/sub/0000000000000001-0000000000000001.ltx", want: false},
		{name: "NonLTXFile", rootPrefix: "", key: "0000/readme.txt", want: false},
		{name: "NoSlash", rootPrefix: "", key: "0000-readme.txt", want: false},
		{name: "UpperHexRejected", rootPrefix: "", key: "000A/0000000000000001-0000000000000001.ltx", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := internal.IsRootLTXKey(tt.rootPrefix, tt.key, litestream.SnapshotLevel); got != tt.want {
				t.Fatalf("IsRootLTXKey(%q, %q) = %v, want %v", tt.rootPrefix, tt.key, got, tt.want)
			}
		})
	}
}

func TestResolveRootPrefix(t *testing.T) {
	makeListFunc := func(keys []string, calls *[]string) internal.ListKeysFunc {
		sorted := slices.Clone(keys)
		slices.Sort(sorted)
		return func(_ context.Context, prefix string, maxKeys int32, fn func(key string) bool) error {
			if calls != nil {
				*calls = append(*calls, prefix)
			}
			var count int32
			for _, k := range sorted {
				if !strings.HasPrefix(k, prefix) {
					continue
				}
				if !fn(k) {
					return nil
				}
				count++
				if maxKeys > 0 && count >= maxKeys {
					return nil
				}
			}
			return nil
		}
	}

	t.Run("EmptyBucketUsesCleanRootWithThreeProbes", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		var calls []string

		got, err := internal.ResolveRootPrefix(context.Background(), logger, "s3", "mybucket", litestream.SnapshotLevel, makeListFunc(nil, &calls))
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("prefix = %q, want empty", got)
		}
		if buf.Len() != 0 {
			t.Fatalf("unexpected warning: %s", buf.String())
		}
		if want := []string{"000", "./", "/"}; !slices.Equal(calls, want) {
			t.Fatalf("probe prefixes = %v, want %v", calls, want)
		}
	})

	t.Run("ExistingCleanRoot", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		keys := []string{"0000/0000000000000001-0000000000000001.ltx"}

		got, err := internal.ResolveRootPrefix(context.Background(), logger, "s3", "mybucket", litestream.SnapshotLevel, makeListFunc(keys, nil))
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("prefix = %q, want empty", got)
		}
		if buf.Len() != 0 {
			t.Fatalf("unexpected warning: %s", buf.String())
		}
	})

	t.Run("LegacyDotPreservedWithWarning", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		keys := []string{"./0000/0000000000000001-0000000000000001.ltx"}

		got, err := internal.ResolveRootPrefix(context.Background(), logger, "s3", "mybucket", litestream.SnapshotLevel, makeListFunc(keys, nil))
		if err != nil {
			t.Fatal(err)
		}
		if got != "./" {
			t.Fatalf("prefix = %q, want %q", got, "./")
		}
		if out := buf.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "legacy root replica prefix detected") {
			t.Fatalf("expected warning log, got %q", out)
		}
	})

	t.Run("LegacySlashPreservedWithWarning", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		keys := []string{"/0009/0000000000000001-0000000000000001.ltx"}

		got, err := internal.ResolveRootPrefix(context.Background(), logger, "s3", "mybucket", litestream.SnapshotLevel, makeListFunc(keys, nil))
		if err != nil {
			t.Fatal(err)
		}
		if got != "/" {
			t.Fatalf("prefix = %q, want %q", got, "/")
		}
		if out := buf.String(); !strings.Contains(out, "level=WARN") {
			t.Fatalf("expected warning log, got %q", out)
		}
	})

	t.Run("UnrelatedObjectsIgnoredAndFallbackFindsLegacyLTX", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))
		keys := []string{
			"000-readme.txt",
			"./.keep",
			"./0009/0000000000000001-0000000000000001.ltx",
			"/notes.txt",
		}

		got, err := internal.ResolveRootPrefix(context.Background(), logger, "s3", "mybucket", litestream.SnapshotLevel, makeListFunc(keys, nil))
		if err != nil {
			t.Fatal(err)
		}
		if got != "./" {
			t.Fatalf("prefix = %q, want %q", got, "./")
		}
	})

	t.Run("MultipleLayoutsReturnsError", func(t *testing.T) {
		keys := []string{
			"0000/0000000000000001-0000000000000001.ltx",
			"./0000/0000000000000001-0000000000000001.ltx",
		}

		_, err := internal.ResolveRootPrefix(context.Background(), nil, "s3", "mybucket", litestream.SnapshotLevel, makeListFunc(keys, nil))
		if err == nil {
			t.Fatal("expected error for multiple root replica prefixes")
		}
		if !strings.Contains(err.Error(), "multiple root replica prefixes") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ListErrorPropagates", func(t *testing.T) {
		wantErr := errors.New("access denied")
		_, err := internal.ResolveRootPrefix(context.Background(), nil, "s3", "mybucket", litestream.SnapshotLevel, func(context.Context, string, int32, func(string) bool) error {
			return wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("err = %v, want %v", err, wantErr)
		}
	})
}
