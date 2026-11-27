package litestream_test

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/benbjohnson/litestream/internal"
	"github.com/benbjohnson/litestream/internal/testingutil"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// assertJSONLineUniqueKeys parses a single JSON object log line and fails if
// any top-level key appears more than once, returning the parsed key/value map.
func assertJSONLineUniqueKeys(t *testing.T, line string) map[string]any {
	t.Helper()

	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		t.Fatalf("decode JSON log line %q: %v", line, err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		t.Fatalf("expected JSON object in %q, got %v", line, tok)
	}

	seen := make(map[string]int)
	fields := make(map[string]any)
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			t.Fatalf("decode JSON key in %q: %v", line, err)
		}
		key, ok := keyTok.(string)
		if !ok {
			t.Fatalf("expected string key in %q, got %T", line, keyTok)
		}
		seen[key]++
		if seen[key] > 1 {
			t.Errorf("duplicate top-level JSON key %q in log line: %s", key, line)
		}

		var val any
		if err := dec.Decode(&val); err != nil {
			t.Fatalf("decode JSON value for %q in %q: %v", key, line, err)
		}
		fields[key] = val
	}

	if _, err := dec.Token(); err != nil && err != io.EOF {
		t.Fatalf("decode closing brace in %q: %v", line, err)
	}
	return fields
}

func TestStore_CompactDB_LogNoDuplicateLevelKey(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		t.Run(format, func(t *testing.T) {
			var buf syncBuffer
			opts := &slog.HandlerOptions{
				Level:       slog.LevelDebug,
				ReplaceAttr: internal.ReplaceAttr,
			}
			var handler slog.Handler
			if format == "json" {
				handler = slog.NewJSONHandler(&buf, opts)
			} else {
				handler = slog.NewTextHandler(&buf, opts)
			}
			logger := slog.New(handler)

			db, sqldb := testingutil.MustOpenDBs(t)
			defer testingutil.MustCloseDBs(t, db, sqldb)

			if _, err := sqldb.Exec(`CREATE TABLE t (x);`); err != nil {
				t.Fatal(err)
			}
			if _, err := sqldb.Exec(`INSERT INTO t VALUES (1);`); err != nil {
				t.Fatal(err)
			}
			if err := db.Sync(context.Background()); err != nil {
				t.Fatal(err)
			}

			levels := litestream.CompactionLevels{
				{Level: 0},
				{Level: 1, Interval: time.Millisecond},
			}
			store := litestream.NewStore([]*litestream.DB{db}, levels)
			store.CompactionMonitorEnabled = false
			store.Logger = logger.With(litestream.LogKeySystem, litestream.LogSystemStore)
			db.SetLogger(store.Logger.With(litestream.LogKeyDB, filepath.Base(db.Path())))
			if err := store.Open(context.Background()); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := store.Close(context.Background()); err != nil {
					t.Errorf("close store: %v", err)
				}
			}()

			if err := db.Replica.Sync(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := store.CompactDB(context.Background(), db, levels[1]); err != nil {
				t.Fatal(err)
			}

			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			if len(lines) == 0 {
				t.Fatal("expected log output, got none")
			}

			var sawUpload, sawCompaction bool
			for _, line := range lines {
				if strings.TrimSpace(line) == "" {
					continue
				}
				if format == "json" {
					fields := assertJSONLineUniqueKeys(t, line)
					sev, ok := fields[slog.LevelKey].(string)
					if !ok || (sev != "DEBUG" && sev != "INFO" && sev != "WARN" && sev != "ERROR" && sev != "TRACE") {
						t.Errorf("unexpected slog severity %v (%T) in line: %s", fields[slog.LevelKey], fields[slog.LevelKey], line)
					}
					switch fields[slog.MessageKey] {
					case "ltx file uploaded":
						sawUpload = true
						if got := fields["ltx_level"]; got != json.Number("0") {
							t.Errorf("expected ltx_level=0 on upload log, got %v (%T) in: %s", got, got, line)
						}
					case "compaction complete":
						sawCompaction = true
						if got := fields["ltx_level"]; got != json.Number("1") {
							t.Errorf("expected ltx_level=1 on compaction log, got %v (%T) in: %s", got, got, line)
						}
					}
				} else {
					if count := strings.Count(line, " level="); count != 1 {
						t.Errorf("expected exactly 1 ' level=' attribute, got %d in line: %s", count, line)
					}
					if strings.Contains(line, `msg="ltx file uploaded"`) {
						sawUpload = true
						if !strings.Contains(line, " ltx_level=0 ") {
							t.Errorf("expected ltx_level=0 in upload log: %s", line)
						}
					}
					if strings.Contains(line, `msg="compaction complete"`) {
						sawCompaction = true
						if !strings.Contains(line, " ltx_level=1 ") {
							t.Errorf("expected ltx_level=1 in compaction log: %s", line)
						}
					}
				}
			}

			if !sawUpload {
				t.Fatalf("did not observe 'ltx file uploaded' log in:\n%s", buf.String())
			}
			if !sawCompaction {
				t.Fatalf("did not observe 'compaction complete' log in:\n%s", buf.String())
			}
		})
	}
}

type reservedSlogKeyViolation struct {
	Filename string
	Line     int
	Key      string
}

// findReservedSlogKeys inspects parsed Go AST for slog calls passing built-in
// top-level keys ("time", "level", "msg", "source"). Positional key/value
// checking stops once an argument whose type cannot be determined syntactically
// (e.g. a variable that may hold either a slog.Attr or a key string) is
// encountered, while still checking explicit slog.<Attr>(key, ...) calls.
func findReservedSlogKeys(fset *token.FileSet, file *ast.File) []reservedSlogKeyViolation {
	reserved := map[string]bool{
		slog.TimeKey:    true,
		slog.LevelKey:   true,
		slog.MessageKey: true,
		slog.SourceKey:  true,
	}

	reservedKeyConsts := map[string]string{
		"TimeKey":    slog.TimeKey,
		"LevelKey":   slog.LevelKey,
		"MessageKey": slog.MessageKey,
		"SourceKey":  slog.SourceKey,
	}

	// Map slog method names to the index of the first attribute argument.
	methodAttrStart := map[string]int{
		"With":         0,
		"Debug":        1,
		"Info":         1,
		"Warn":         1,
		"Error":        1,
		"DebugContext": 2,
		"InfoContext":  2,
		"WarnContext":  2,
		"ErrorContext": 2,
		"Log":          3,
		"LogAttrs":     3,
	}

	slogAttrCtors := map[string]bool{
		"String":   true,
		"Int":      true,
		"Int64":    true,
		"Uint64":   true,
		"Float64":  true,
		"Bool":     true,
		"Time":     true,
		"Duration": true,
		"Any":      true,
		"Group":    true,
	}

	isLoggerExpr := func(expr ast.Expr) bool {
		switch e := expr.(type) {
		case *ast.Ident:
			name := strings.ToLower(e.Name)
			return name == "slog" || strings.Contains(name, "log")
		case *ast.SelectorExpr:
			return strings.Contains(strings.ToLower(e.Sel.Name), "log")
		case *ast.CallExpr:
			if sel, ok := e.Fun.(*ast.SelectorExpr); ok {
				name := strings.ToLower(sel.Sel.Name)
				return strings.Contains(name, "log") || name == "default" || name == "with"
			}
		}
		return false
	}

	isSlogAttrCtor := func(expr ast.Expr) (*ast.CallExpr, bool) {
		call, ok := expr.(*ast.CallExpr)
		if !ok {
			return nil, false
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return nil, false
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok || ident.Name != "slog" {
			return nil, false
		}
		return call, slogAttrCtors[sel.Sel.Name]
	}

	stringLiteral := func(expr ast.Expr) (string, bool) {
		lit, ok := expr.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return "", false
		}
		s, err := strconv.Unquote(lit.Value)
		if err != nil {
			return "", false
		}
		return s, true
	}

	reservedKeyExpr := func(expr ast.Expr) (string, bool) {
		if s, ok := stringLiteral(expr); ok {
			return s, reserved[s]
		}
		if sel, ok := expr.(*ast.SelectorExpr); ok {
			if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "slog" {
				if key, ok := reservedKeyConsts[sel.Sel.Name]; ok {
					return key, true
				}
			}
		}
		return "", false
	}

	isKnownKeyExpr := func(expr ast.Expr) bool {
		if _, ok := stringLiteral(expr); ok {
			return true
		}
		if ident, ok := expr.(*ast.Ident); ok && strings.HasPrefix(ident.Name, "LogKey") {
			return true
		}
		if sel, ok := expr.(*ast.SelectorExpr); ok {
			if strings.HasPrefix(sel.Sel.Name, "LogKey") {
				return true
			}
			if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == "slog" {
				_, ok := reservedKeyConsts[sel.Sel.Name]
				return ok
			}
		}
		return false
	}

	var violations []reservedSlogKeyViolation
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		attrStart, ok := methodAttrStart[sel.Sel.Name]
		if !ok || !isLoggerExpr(sel.X) {
			return true
		}

		positionalKnown := sel.Sel.Name != "LogAttrs"
		for i := attrStart; i < len(call.Args); {
			arg := call.Args[i]
			if attrCall, ok := isSlogAttrCtor(arg); ok {
				if len(attrCall.Args) > 0 {
					if key, bad := reservedKeyExpr(attrCall.Args[0]); bad {
						pos := fset.Position(attrCall.Args[0].Pos())
						violations = append(violations, reservedSlogKeyViolation{
							Filename: pos.Filename,
							Line:     pos.Line,
							Key:      key,
						})
					}
				}
				i++
				continue
			}

			if !positionalKnown {
				i++
				continue
			}

			if key, bad := reservedKeyExpr(arg); bad {
				pos := fset.Position(arg.Pos())
				violations = append(violations, reservedSlogKeyViolation{
					Filename: pos.Filename,
					Line:     pos.Line,
					Key:      key,
				})
				i += 2
				continue
			}

			if isKnownKeyExpr(arg) {
				i += 2
				continue
			}

			// Unknown argument type (could be a slog.Attr variable or key expression);
			// stop positional pairing to avoid misaligning subsequent arguments.
			positionalKnown = false
			i++
		}
		return true
	})
	return violations
}

// TestLogCalls_NoReservedSlogKeys statically verifies that no non-test Go file
// passes built-in slog attribute keys ("time", "level", "msg", "source") as
// user attributes, preventing duplicate keys in JSON and text log output.
func TestLogCalls_NoReservedSlogKeys(t *testing.T) {
	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == "vendor" || name == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}

		for _, v := range findReservedSlogKeys(fset, file) {
			t.Errorf("%s:%d: slog call uses reserved key %q", v.Filename, v.Line, v.Key)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFindReservedSlogKeys_PositiveAndNegative(t *testing.T) {
	const src = `package sample

import (
	"context"
	"log/slog"
)

func f(ctx context.Context, logger *slog.Logger, attr slog.Attr, other interface{ With(...any) }) {
	// Positive cases: must be flagged.
	slog.Info("startup", "version", "v1", "level", "info")
	logger.Debug("upload", slog.Int("level", 1))
	logger.InfoContext(ctx, "event", slog.LevelKey, 2)
	logger.Warn("grouped", slog.Group("time", "a", 1))
	logger.Info("after-attr-ctor", slog.String("db", "a.db"), "msg", "x")
	logger.Info("attr-var-plus-ctor", attr, slog.String("source", "x"))

	// Negative cases: must not be flagged.
	slog.Info("startup", "version", "v1", "log_level", "info")
	logger.Debug("upload", "ltx_level", 1)
	logger.Info("value-is-level", "state", "level")
	logger.Info("unknown-attr-var-then-value-level", attr, "state", "level")
	other.With("level", 1)
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "sample.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}

	got := findReservedSlogKeys(fset, file)
	wantKeys := []string{"level", "level", "level", "time", "msg", "source"}
	if len(got) != len(wantKeys) {
		t.Fatalf("got %d violations (%+v), want %d", len(got), got, len(wantKeys))
	}
	for i, want := range wantKeys {
		if got[i].Key != want {
			t.Errorf("violation[%d].Key = %q, want %q (full: %+v)", i, got[i].Key, want, got[i])
		}
	}
}
