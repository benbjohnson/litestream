package internal_test

import (
	"log/slog"
	"testing"

	"github.com/benbjohnson/litestream/internal"
)

func TestReplaceAttr_TraceLevel(t *testing.T) {
	a := slog.Attr{Key: slog.LevelKey, Value: slog.AnyValue(internal.LevelTrace)}
	got := internal.ReplaceAttr(nil, a)
	if got.Value.String() != "TRACE" {
		t.Fatalf("expected TRACE, got %s", got.Value.String())
	}
}

func TestReplaceAttr_NonTraceLevelUnchanged(t *testing.T) {
	a := slog.Attr{Key: slog.LevelKey, Value: slog.AnyValue(slog.LevelInfo)}
	got := internal.ReplaceAttr(nil, a)
	if got.Value.Any() != slog.LevelInfo {
		t.Fatalf("expected INFO level unchanged, got %v", got.Value.Any())
	}
}

func TestReplaceAttr_SourceCleaning(t *testing.T) {
	src := &slog.Source{
		Function: "github.com/benbjohnson/litestream/db.(*DB).Replicate",
		File:     "github.com/benbjohnson/litestream/db.go",
		Line:     211,
	}
	a := slog.Attr{Key: slog.SourceKey, Value: slog.AnyValue(src)}
	got := internal.ReplaceAttr(nil, a)
	gotSrc := got.Value.Any().(*slog.Source)
	if gotSrc.File != "db.go" {
		t.Fatalf("expected db.go, got %s", gotSrc.File)
	}
}

func TestCleanSourcePath(t *testing.T) {
	tests := []struct {
		name string
		file string
		want string
	}{
		{
			name: "module prefix stripped",
			file: "github.com/benbjohnson/litestream/db.go",
			want: "db.go",
		},
		{
			name: "nested path",
			file: "github.com/benbjohnson/litestream/internal/internal.go",
			want: "internal/internal.go",
		},
		{
			name: "absolute with module path",
			file: "/Users/dev/go/pkg/mod/github.com/benbjohnson/litestream@v0.5.0/db.go",
			want: "db.go",
		},
		{
			name: "absolute checkout path",
			file: "/Users/dev/projects/litestream/db.go",
			want: "db.go",
		},
		{
			name: "absolute checkout nested path",
			file: "/workspace/litestream/internal/internal.go",
			want: "internal/internal.go",
		},
		{
			name: "no module prefix",
			file: "other/package/file.go",
			want: "other/package/file.go",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &slog.Source{File: tt.file}
			internal.CleanSourcePath(src)
			if src.File != tt.want {
				t.Fatalf("got %s, want %s", src.File, tt.want)
			}
		})
	}
}
