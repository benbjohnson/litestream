package internal

import (
	"io"
	"log/slog"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"time"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
)

var modulePrefix string
var moduleLastSegment string

func init() {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Path != "" {
		modulePrefix = info.Main.Path + "/"
		moduleLastSegment = path.Base(info.Main.Path)
	}
}

const LevelTrace = slog.LevelDebug - 4

// LoggingConfig configures logging.
type LoggingConfig struct {
	Level  string `yaml:"level"`
	Type   string `yaml:"type"`
	Stderr bool   `yaml:"stderr"`
	Source bool   `yaml:"source"`
}

func InitLog(w io.Writer, level, typ string, addSource bool) {
	logOptions := slog.HandlerOptions{
		Level:       slog.LevelInfo,
		AddSource:   addSource,
		ReplaceAttr: ReplaceAttr,
	}

	// Read log level from environment, if available.
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		level = v
	}

	switch strings.ToUpper(level) {
	case "TRACE":
		logOptions.Level = LevelTrace
	case "DEBUG":
		logOptions.Level = slog.LevelDebug
	case "INFO":
		logOptions.Level = slog.LevelInfo
	case "WARN", "WARNING":
		logOptions.Level = slog.LevelWarn
	case "ERROR":
		logOptions.Level = slog.LevelError
	}

	var logHandler slog.Handler
	switch typ {
	case "json":
		logHandler = slog.NewJSONHandler(w, &logOptions)
	case "pretty":
		noColor := true
		if f, ok := w.(*os.File); ok {
			noColor = !isatty.IsTerminal(f.Fd()) && !isatty.IsCygwinTerminal(f.Fd())
		}
		logHandler = tint.NewHandler(w, &tint.Options{
			Level:       logOptions.Level,
			AddSource:   addSource,
			TimeFormat:  time.TimeOnly,
			NoColor:     noColor,
			ReplaceAttr: ReplaceAttr,
		})
	case "text", "":
		logHandler = slog.NewTextHandler(w, &logOptions)
	}

	// Set global default logger.
	slog.SetDefault(slog.New(logHandler))
}

func CleanSourcePath(src *slog.Source) {
	if modulePrefix == "" {
		return
	}

	if idx := strings.Index(src.File, modulePrefix); idx >= 0 {
		src.File = src.File[idx+len(modulePrefix):]
		return
	}

	moduleName := strings.TrimSuffix(modulePrefix, "/")
	if idx := strings.Index(src.File, moduleName+"@"); idx >= 0 {
		rest := src.File[idx+len(moduleName):]
		if slashIdx := strings.Index(rest, "/"); slashIdx >= 0 {
			src.File = rest[slashIdx+1:]
		}
		return
	}

	if moduleLastSegment != "" {
		sep := "/" + moduleLastSegment + "/"
		if idx := strings.LastIndex(src.File, sep); idx >= 0 {
			src.File = src.File[idx+len(sep):]
		}
	}
}

func ReplaceAttr(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.LevelKey && a.Value.Any() == LevelTrace {
		a.Value = slog.StringValue("TRACE")
	}

	if a.Key == slog.SourceKey {
		if src, ok := a.Value.Any().(*slog.Source); ok {
			CleanSourcePath(src)
		}
	}

	return a
}
